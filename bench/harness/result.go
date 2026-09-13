// Package harness runs the benchmark scenarios. The harness is the measuring
// equipment: it exists so a performance claim can be reproduced by somebody
// who does not believe it.
package harness

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"time"

	"github.com/plusiv/huxio/internal/utils"
	"github.com/rotisserie/eris"
)

// Result is one scenario run, written to bench/results as JSON so runs can be
// compared and a regression gate can fail on them.
type Result struct {
	Scenario   string    `json:"scenario"`
	StartedAt  time.Time `json:"startedAt"`
	DurationMS int64     `json:"durationMs"`

	Commit    string  `json:"commit"`
	GoVersion string  `json:"goVersion"`
	Machine   Machine `json:"machine"`

	Config ConfigSummary `json:"config"`

	Ingest   IngestStats   `json:"ingest"`
	Delivery DeliveryStats `json:"delivery"`
	Queue    QueueStats    `json:"queue"`
	Runtime  RuntimeStats  `json:"runtime"`

	// Tenants holds the per-tenant latency picture. A global p99 does not
	// surface a single tenant's degradation.
	Tenants []TenantStats `json:"tenants"`

	Pass   bool   `json:"pass"`
	Reason string `json:"reason,omitempty"`
}

// Machine records where a run happened, because a number without a machine is
// not a measurement.
type Machine struct {
	OS       string `json:"os"`
	Arch     string `json:"arch"`
	CPUs     int    `json:"cpus"`
	MaxProcs int    `json:"gomaxprocs"`
}

// ConfigSummary is the shape of the load that was applied.
type ConfigSummary struct {
	Rate               int     `json:"ratePerSecond"`
	Tenants            int     `json:"tenants"`
	EndpointsPerTenant int     `json:"endpointsPerTenant"`
	PayloadBytes       int     `json:"payloadBytes"`
	TarpitFraction     float64 `json:"tarpitFraction,omitempty"`
	FailureRate        float64 `json:"failureRate,omitempty"`
	Workers            int     `json:"workers"`
	// RetrySchedule records the delays the run used, because a compressed
	// schedule measures something different from the production one.
	RetrySchedule []string `json:"retrySchedule,omitempty"`
}

// IngestStats is the sender's view: what the API did with the requests.
type IngestStats struct {
	Sent     int64       `json:"sent"`
	Accepted int64       `json:"accepted"`
	Rejected int64       `json:"rejected"`
	RPS      float64     `json:"acceptedPerSecond"`
	Latency  Percentiles `json:"latencyMs"`
}

// DeliveryStats is the receiving end: what actually arrived.
type DeliveryStats struct {
	Delivered        int64   `json:"delivered"`
	Expected         int64   `json:"expected"`
	PerSecond        float64 `json:"deliveredPerSecond"`
	PerSecondPerCore float64 `json:"deliveredPerSecondPerCore"`
	// Latency is ingest to arrival at the endpoint: it includes the queue, so
	// it is the number that shows one tenant delaying another.
	Latency Percentiles `json:"ingestToDeliveryMs"`
	// AllocBytesPerDelivery is the allocation cost per delivery; on this
	// workload allocations are usually the CPU.
	AllocBytesPerDelivery float64 `json:"allocBytesPerDelivery"`
}

// QueueStats is what the queue looked like at the end of the run.
type QueueStats struct {
	MaxLagSeconds  float64 `json:"maxLagSeconds"`
	FinalDepth     int64   `json:"finalDepth"`
	DeadTupleRatio float64 `json:"deadTupleRatio"`
}

// RuntimeStats is the process picture, for the soak scenario.
type RuntimeStats struct {
	StartGoroutines int    `json:"startGoroutines"`
	EndGoroutines   int    `json:"endGoroutines"`
	PeakHeapBytes   uint64 `json:"peakHeapBytes"`
}

// TenantStats is one tenant's slice of the run.
type TenantStats struct {
	OrgID string `json:"orgId"`
	// Role separates the tenants under test from the ones being sabotaged.
	Role      string      `json:"role"`
	Delivered int64       `json:"delivered"`
	Expected  int64       `json:"expected"`
	Latency   Percentiles `json:"ingestToDeliveryMs"`
}

// Percentiles is the distribution summary every latency is reported as. An
// average would hide the tail, and the tail is the product.
type Percentiles struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
}

// summarize computes the percentiles of a set of millisecond samples.
func summarize(samples []float64) Percentiles {
	if len(samples) == 0 {
		return Percentiles{}
	}
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)

	return Percentiles{
		Count: len(sorted),
		P50:   round(utils.Percentile(sorted, 0.50)),
		P95:   round(utils.Percentile(sorted, 0.95)),
		P99:   round(utils.Percentile(sorted, 0.99)),
		Max:   round(sorted[len(sorted)-1]),
	}
}

func round(v float64) float64 {
	return float64(int64(v*1000+0.5)) / 1000
}

// newResult stamps a result with where and when it ran.
func newResult(scenario string, config ConfigSummary) *Result {
	result := &Result{
		Scenario:  scenario,
		StartedAt: time.Now().UTC(),
		GoVersion: runtime.Version(),
		Machine: Machine{
			OS:       runtime.GOOS,
			Arch:     runtime.GOARCH,
			CPUs:     runtime.NumCPU(),
			MaxProcs: runtime.GOMAXPROCS(0),
		},
		Config: config,
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				result.Commit = setting.Value
			}
		}
	}
	return result
}

// Write saves a result as bench/results/<scenario>-<commit>.json, which is
// what a regression gate compares against.
func (r *Result) Write(dir string) (string, error) {
	if dir == "" {
		dir = filepath.Join("bench", "results")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", eris.Wrap(err, "create results directory")
	}

	commit := r.Commit
	if commit == "" {
		commit = "dev"
	}
	if len(commit) > 12 {
		commit = commit[:12]
	}

	path := filepath.Join(dir, r.Scenario+"-"+commit+".json")
	encoded, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return "", eris.Wrap(err, "encode result")
	}
	if err := os.WriteFile(path, append(encoded, '\n'), 0o644); err != nil {
		return "", eris.Wrap(err, "write result")
	}
	return path, nil
}

// RegressionBudget is how much a headline number may move before CI fails.
// It exists to catch slow drift, which is the only kind of performance loss
// nobody notices.
const RegressionBudget = 0.15

// Load reads a previously written result, for comparing runs.
func Load(path string) (*Result, error) {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return nil, eris.Wrap(err, "read baseline result")
	}
	var result Result
	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil, eris.Wrap(err, "decode baseline result")
	}
	return &result, nil
}

// Regression is one headline number that moved the wrong way.
type Regression struct {
	Metric   string
	Baseline float64
	Current  float64
	// Change is the fractional move, negative for a drop.
	Change float64
}

// CompareTo reports the headline numbers that moved beyond the budget against
// a baseline run. Throughput dropping and latency rising are both failures.
func (r *Result) CompareTo(baseline *Result) []Regression {
	var regressions []Regression

	worse := func(metric string, base, current float64, higherIsBetter bool) {
		if base == 0 {
			return
		}
		change := (current - base) / base
		if higherIsBetter {
			if change < -RegressionBudget {
				regressions = append(regressions, Regression{metric, base, current, change})
			}
			return
		}
		if change > RegressionBudget {
			regressions = append(regressions, Regression{metric, base, current, change})
		}
	}

	worse("delivered_per_second", baseline.Delivery.PerSecond, r.Delivery.PerSecond, true)
	worse("ingest_p99_ms", baseline.Ingest.Latency.P99, r.Ingest.Latency.P99, false)
	worse("delivery_p99_ms", baseline.Delivery.Latency.P99, r.Delivery.Latency.P99, false)
	worse("alloc_bytes_per_delivery", baseline.Delivery.AllocBytesPerDelivery, r.Delivery.AllocBytesPerDelivery, false)

	return regressions
}
