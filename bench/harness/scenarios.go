package harness

import (
	"context"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/plusiv/huxio/bench/sinks"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"golang.org/x/sync/errgroup"
)

// The scenario names.
const (
	// ScenarioThroughput measures the fixed cost per delivery against a sink
	// that never misbehaves.
	ScenarioThroughput = "throughput"
	// ScenarioIsolation is the product: healthy tenants must keep their tail
	// while a share of endpoints is tarpitted.
	ScenarioIsolation = "isolation"
	// ScenarioRetryStorm checks that backoff spreads load, breakers open, and
	// recovery does not thunder.
	ScenarioRetryStorm = "retry-storm"
	// ScenarioColdStart establishes the cold-connection cost.
	ScenarioColdStart = "cold-start"
	// ScenarioSoak looks for leaks and queue growth over a long run.
	ScenarioSoak = "soak"
)

// IsolationTailBudget is the pass condition: healthy-tenant p99 must stay
// within this multiple of the same run's tarpit-free baseline.
const IsolationTailBudget = 2.0

// Defaults reports the configuration a scenario will actually run with, so a
// caller can print it before the run starts.
func Defaults(scenario string, cfg Config) Config { return applyDefaults(scenario, cfg) }

// applyDefaults fills in a scenario's shape so `huxio bench --scenario=x`
// works with no other flags.
func applyDefaults(scenario string, cfg Config) Config {
	if cfg.Duration <= 0 {
		cfg.Duration = 15 * time.Second
	}
	if cfg.Rate <= 0 {
		cfg.Rate = 200
	}
	if cfg.PayloadBytes <= 0 {
		cfg.PayloadBytes = 256
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 30 * time.Second
	}

	switch scenario {
	case ScenarioThroughput:
		if cfg.Tenants <= 0 {
			cfg.Tenants = 1
		}
		if cfg.EndpointsPerTenant <= 0 {
			cfg.EndpointsPerTenant = 10
		}
		cfg.Sink = sinks.KindNull

	case ScenarioIsolation:
		if cfg.Tenants <= 0 {
			cfg.Tenants = 10
		}
		if cfg.EndpointsPerTenant <= 0 {
			cfg.EndpointsPerTenant = 3
		}
		if cfg.TarpitFraction <= 0 {
			cfg.TarpitFraction = 0.2
		}
		cfg.Sink = sinks.KindNull

	case ScenarioRetryStorm:
		if cfg.Tenants <= 0 {
			cfg.Tenants = 4
		}
		if cfg.EndpointsPerTenant <= 0 {
			cfg.EndpointsPerTenant = 2
		}
		if cfg.FailureRate <= 0 {
			cfg.FailureRate = 1
		}
		if len(cfg.RetrySchedule) == 0 {
			// Compressed so a short run can observe a full storm and its recovery. Pass
			// --retry-schedule to use the production delays.
			cfg.RetrySchedule = []time.Duration{
				200 * time.Millisecond, 500 * time.Millisecond,
				time.Second, 2 * time.Second,
			}
		}
		cfg.Sink = sinks.KindFlaky

	case ScenarioColdStart:
		if cfg.Tenants <= 0 {
			cfg.Tenants = 4
		}
		if cfg.EndpointsPerTenant <= 0 {
			cfg.EndpointsPerTenant = 1
		}
		cfg.Sink = sinks.KindNull

	case ScenarioSoak:
		if cfg.Tenants <= 0 {
			cfg.Tenants = 4
		}
		if cfg.EndpointsPerTenant <= 0 {
			cfg.EndpointsPerTenant = 3
		}
		if cfg.Duration < time.Minute {
			cfg.Duration = 2 * time.Minute
		}
		cfg.Sink = sinks.KindNull
	}

	return cfg
}

// phase is one span of load inside a scenario.
type phase struct {
	name string
	// tenants selects which tenants send during this phase; nil means all.
	tenants []*tenant
	rate    int
	dur     time.Duration
}

// runThroughput ramps a single tenant at a fixed rate against null sinks.
func runThroughput(ctx context.Context, cfg Config) (*Result, error) {
	specs := make([]tenantSpec, 0, cfg.Tenants)
	for range cfg.Tenants {
		specs = append(specs, tenantSpec{role: "healthy", sink: sinks.KindNull, endpoints: cfg.EndpointsPerTenant})
	}

	h, err := setup(ctx, cfg, specs)
	if err != nil {
		return nil, err
	}
	defer h.close()

	result := newResult(ScenarioThroughput, summarizeConfig(cfg))
	stop := h.startWorkers(ctx)

	before := memStats()
	startGoroutines := runtime.NumGoroutine()

	stats, err := h.applyLoad(ctx, phase{name: "load", rate: cfg.Rate, dur: cfg.Duration})
	if err != nil {
		stop()
		return nil, err
	}
	h.waitForDrain(ctx, cfg.DrainTimeout)
	stop()

	h.finish(ctx, result, stats, before, startGoroutines)
	result.Pass = result.Delivery.Delivered > 0
	if !result.Pass {
		result.Reason = "nothing was delivered"
	}
	return result, nil
}

// runIsolation is the scenario the product claim rests on. It measures the
// healthy tenants twice in one run: once with every endpoint healthy, and
// again with a share of tenants pointed at a tarpit. The pass condition is a
// comparison within the same run, so nobody has to trust a number from
// another machine.
func runIsolation(ctx context.Context, cfg Config) (*Result, error) {
	tarpits := max(1, int(float64(cfg.Tenants)*cfg.TarpitFraction))
	healthy := max(1, cfg.Tenants-tarpits)

	specs := make([]tenantSpec, 0, healthy+tarpits)
	for range healthy {
		specs = append(specs, tenantSpec{role: "healthy", sink: sinks.KindNull, endpoints: cfg.EndpointsPerTenant})
	}
	for range tarpits {
		specs = append(specs, tenantSpec{
			role: "tarpit", sink: sinks.KindTarpit,
			delay: 30 * time.Second, endpoints: cfg.EndpointsPerTenant,
		})
	}

	h, err := setup(ctx, cfg, specs)
	if err != nil {
		return nil, err
	}
	defer h.close()

	result := newResult(ScenarioIsolation, summarizeConfig(cfg))
	stop := h.startWorkers(ctx)

	before := memStats()
	startGoroutines := runtime.NumGoroutine()

	healthyTenants := h.tenantsWithRole("healthy")
	tarpitTenants := h.tenantsWithRole("tarpit")

	// Phase one: healthy tenants only. This is the baseline the pass condition is
	// measured against.
	baseline, err := h.applyLoad(ctx, phase{
		name: "baseline", tenants: healthyTenants,
		rate: cfg.Rate, dur: cfg.Duration / 2,
	})
	if err != nil {
		stop()
		return nil, err
	}
	h.waitForDrain(ctx, cfg.DrainTimeout)
	baselineTail := h.tailFor(healthyTenants)
	h.resetLatencies()

	// Phase two: the same healthy load, with the tarpitted tenants sending
	// alongside it.
	stressed, err := h.applyLoad(ctx, phase{
		name: "stressed", tenants: append(append([]*tenant{}, healthyTenants...), tarpitTenants...),
		rate: cfg.Rate, dur: cfg.Duration / 2,
	})
	if err != nil {
		stop()
		return nil, err
	}
	// The tarpitted deliveries will not drain; only wait for the healthy ones.
	h.waitForHealthyDrain(ctx, healthyTenants, cfg.DrainTimeout)
	stressedTail := h.tailFor(healthyTenants)

	stop()

	combined := baseline
	combined.merge(stressed)
	h.finish(ctx, result, combined, before, startGoroutines)

	// The claim: a healthy tenant's tail stays within 2x of baseline while a
	// fifth of the fleet is tarpitted.
	result.Pass = stressedTail > 0 && baselineTail > 0 && stressedTail <= baselineTail*IsolationTailBudget
	result.Reason = formatIsolationVerdict(baselineTail, stressedTail)

	logger.FromContext(ctx).Info().
		Float64("baseline_p99_ms", baselineTail).
		Float64("stressed_p99_ms", stressedTail).
		Bool("pass", result.Pass).
		Msg("isolation verdict")

	return result, nil
}

// runRetryStorm fails everything, then recovers, and reports what the queue
// and the breakers did.
func runRetryStorm(ctx context.Context, cfg Config) (*Result, error) {
	specs := make([]tenantSpec, 0, cfg.Tenants)
	for range cfg.Tenants {
		specs = append(specs, tenantSpec{
			role: "flaky", sink: sinks.KindFlaky,
			failure: cfg.FailureRate, endpoints: cfg.EndpointsPerTenant,
		})
	}

	h, err := setup(ctx, cfg, specs)
	if err != nil {
		return nil, err
	}
	defer h.close()

	result := newResult(ScenarioRetryStorm, summarizeConfig(cfg))
	stop := h.startWorkers(ctx)

	before := memStats()
	startGoroutines := runtime.NumGoroutine()

	stats, err := h.applyLoad(ctx, phase{name: "storm", rate: cfg.Rate, dur: cfg.Duration})
	if err != nil {
		stop()
		return nil, err
	}

	// Recovery: the endpoints start answering again, and the backlog must
	// drain without thundering. Tearing the sinks down instead would measure
	// an outage, not a recovery.
	for _, t := range h.tenants {
		t.sink.SetFailureRate(0)
	}
	recovery, err := h.applyLoad(ctx, phase{name: "recovery", rate: cfg.Rate, dur: cfg.Duration / 2})
	if err != nil {
		stop()
		return nil, err
	}
	stats.merge(recovery)

	h.waitForDrain(ctx, cfg.DrainTimeout)
	stop()

	h.finish(ctx, result, stats, before, startGoroutines)

	// The storm passes when the backlog drains once the endpoints recover:
	// a queue that never empties means backoff and the breakers did not let
	// the recovery through.
	result.Pass = result.Queue.FinalDepth == 0 && result.Delivery.Delivered > 0
	switch {
	case result.Delivery.Delivered == 0:
		result.Reason = "nothing was delivered even after recovery"
	case result.Queue.FinalDepth != 0:
		result.Reason = "the backlog did not drain after the endpoints recovered"
	default:
		result.Reason = "the backlog drained after recovery"
	}
	return result, nil
}

// runColdStart forces a handshake per delivery by refusing connection reuse,
// which is the ratio the cold budget is about.
func runColdStart(ctx context.Context, cfg Config) (*Result, error) {
	cfg.TLS = true

	specs := make([]tenantSpec, 0, cfg.Tenants)
	for range cfg.Tenants {
		specs = append(specs, tenantSpec{role: "healthy", sink: sinks.KindNull, endpoints: cfg.EndpointsPerTenant})
	}

	h, err := setup(ctx, cfg, specs)
	if err != nil {
		return nil, err
	}
	defer h.close()

	result := newResult(ScenarioColdStart, summarizeConfig(cfg))
	stop := h.startWorkers(ctx)

	before := memStats()
	startGoroutines := runtime.NumGoroutine()

	stats, err := h.applyLoad(ctx, phase{name: "cold", rate: cfg.Rate, dur: cfg.Duration})
	if err != nil {
		stop()
		return nil, err
	}
	h.waitForDrain(ctx, cfg.DrainTimeout)
	stop()

	h.finish(ctx, result, stats, before, startGoroutines)
	result.Pass = result.Delivery.Delivered > 0
	result.Reason = "cold-connection cost over TLS; compare against the throughput run"
	return result, nil
}

// runSoak holds a moderate rate for a long time and watches for growth.
func runSoak(ctx context.Context, cfg Config) (*Result, error) {
	specs := make([]tenantSpec, 0, cfg.Tenants)
	for range cfg.Tenants {
		specs = append(specs, tenantSpec{role: "healthy", sink: sinks.KindNull, endpoints: cfg.EndpointsPerTenant})
	}

	h, err := setup(ctx, cfg, specs)
	if err != nil {
		return nil, err
	}
	defer h.close()

	result := newResult(ScenarioSoak, summarizeConfig(cfg))
	stop := h.startWorkers(ctx)

	before := memStats()
	startGoroutines := runtime.NumGoroutine()

	stats, err := h.applyLoad(ctx, phase{name: "soak", rate: cfg.Rate, dur: cfg.Duration})
	if err != nil {
		stop()
		return nil, err
	}
	h.waitForDrain(ctx, cfg.DrainTimeout)
	stop()

	h.finish(ctx, result, stats, before, startGoroutines)

	// A soak fails on growth, not on throughput: a goroutine count that keeps
	// climbing is the leak this scenario exists to find.
	leaked := result.Runtime.EndGoroutines > result.Runtime.StartGoroutines+50
	result.Pass = !leaked && result.Queue.FinalDepth == 0
	switch {
	case leaked:
		result.Reason = "goroutine count grew during the run"
	case result.Queue.FinalDepth != 0:
		result.Reason = "the queue did not drain"
	default:
		result.Reason = "no growth in goroutines or queue depth"
	}
	return result, nil
}

// loadStats is what one load phase produced.
type loadStats struct {
	sent      int64
	accepted  int64
	rejected  int64
	expected  int64
	elapsed   time.Duration
	latencies []float64
}

func (s *loadStats) merge(other loadStats) {
	s.sent += other.sent
	s.accepted += other.accepted
	s.rejected += other.rejected
	s.expected += other.expected
	s.elapsed += other.elapsed
	s.latencies = append(s.latencies, other.latencies...)
}

// applyLoad drives ingest at a fixed rate for the phase's duration.
func (h *harness) applyLoad(ctx context.Context, p phase) (loadStats, error) {
	tenants := p.tenants
	if len(tenants) == 0 {
		tenants = h.tenants
	}
	if len(tenants) == 0 {
		return loadStats{}, nil
	}

	// One sender per tenant, each pacing its share of the target rate.
	perTenant := max(1, p.rate/len(tenants))
	interval := time.Second / time.Duration(perTenant)

	var (
		mu        sync.Mutex
		stats     loadStats
		startedAt = time.Now()
	)

	group, groupCtx := errgroup.WithContext(ctx)
	for _, t := range tenants {
		group.Go(func() error {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			deadline := time.After(p.dur)
			for {
				select {
				case <-groupCtx.Done():
					return nil
				case <-deadline:
					return nil
				case <-ticker.C:
				}

				elapsed, err := h.send(groupCtx, t, h.cfg.PayloadBytes)

				mu.Lock()
				stats.sent++
				if err != nil {
					stats.rejected++
				} else {
					stats.accepted++
					stats.expected += int64(t.endpoints)
					stats.latencies = append(stats.latencies, float64(elapsed.Microseconds())/1000)
				}
				mu.Unlock()
			}
		})
	}
	if err := group.Wait(); err != nil {
		return stats, err
	}

	stats.elapsed = time.Since(startedAt)
	return stats, nil
}

// waitForDrain waits until the queue is empty or the timeout expires.
func (h *harness) waitForDrain(ctx context.Context, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		stats, err := h.runtime.queueRepo.Stats(ctx, []string{"default"})
		if err != nil {
			return
		}
		total := int64(0)
		for _, stat := range stats {
			total += stat.Ready + stat.Delayed + stat.Locked
		}
		if total == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitForHealthyDrain waits only for the tenants under test: a tarpit never
// drains, and waiting for it would measure the tarpit rather than the system.
func (h *harness) waitForHealthyDrain(ctx context.Context, tenants []*tenant, timeout time.Duration) {
	expected := map[string]int64{}
	for _, t := range tenants {
		expected[t.orgID] = 0
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		stalled := true
		for orgID := range expected {
			if h.delivered[orgID] != expected[orgID] {
				expected[orgID] = h.delivered[orgID]
				stalled = false
			}
		}
		h.mu.Unlock()

		if stalled {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// tailFor returns the p99 ingest-to-delivery latency across a tenant set.
func (h *harness) tailFor(tenants []*tenant) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	var samples []float64
	for _, t := range tenants {
		samples = append(samples, h.latencies[t.orgID]...)
	}
	return summarize(samples).P99
}

// resetLatencies clears the collected samples between phases.
func (h *harness) resetLatencies() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.latencies = map[string][]float64{}
	h.delivered = map[string]int64{}
	h.sentAt = map[string]sendRecord{}
	h.pending = map[string][]time.Time{}
}

func (h *harness) tenantsWithRole(role string) []*tenant {
	var matched []*tenant
	for _, t := range h.tenants {
		if t.role == role {
			matched = append(matched, t)
		}
	}
	return matched
}

// finish fills in the result from what the run observed.
func (h *harness) finish(
	ctx context.Context,
	result *Result,
	stats loadStats,
	before runtime.MemStats,
	startGoroutines int,
) {
	after := memStats()

	result.DurationMS = stats.elapsed.Milliseconds()
	seconds := max(stats.elapsed.Seconds(), 0.001)

	result.Ingest = IngestStats{
		Sent:     stats.sent,
		Accepted: stats.accepted,
		Rejected: stats.rejected,
		RPS:      round(float64(stats.accepted) / seconds),
		Latency:  summarize(stats.latencies),
	}

	h.mu.Lock()
	var (
		deliveryLatencies []float64
		delivered         int64
	)
	for _, t := range h.tenants {
		samples := h.latencies[t.orgID]
		deliveryLatencies = append(deliveryLatencies, samples...)
		delivered += h.delivered[t.orgID]

		result.Tenants = append(result.Tenants, TenantStats{
			OrgID:     t.orgID,
			Role:      t.role,
			Delivered: h.delivered[t.orgID],
			Latency:   summarize(samples),
		})
	}
	h.mu.Unlock()

	allocated := after.TotalAlloc - before.TotalAlloc
	perDelivery := 0.0
	if delivered > 0 {
		perDelivery = round(float64(allocated) / float64(delivered))
	}

	result.Delivery = DeliveryStats{
		Delivered:             delivered,
		Expected:              stats.expected,
		PerSecond:             round(float64(delivered) / seconds),
		PerSecondPerCore:      round(float64(delivered) / seconds / float64(max(1, runtime.GOMAXPROCS(0)))),
		Latency:               summarize(deliveryLatencies),
		AllocBytesPerDelivery: perDelivery,
	}

	result.Runtime = RuntimeStats{
		StartGoroutines: startGoroutines,
		EndGoroutines:   runtime.NumGoroutine(),
		PeakHeapBytes:   after.HeapAlloc,
	}

	result.Queue = h.queueSnapshot(ctx)
}

// queueSnapshot reads the queue's final state, including the bloat ratio that
// is the main failure mode of a Postgres-backed queue.
func (h *harness) queueSnapshot(ctx context.Context) QueueStats {
	snapshot := QueueStats{}

	if stats, err := h.runtime.queueRepo.Stats(ctx, []string{"default"}); err == nil {
		for _, stat := range stats {
			snapshot.FinalDepth += stat.Ready + stat.Delayed + stat.Locked
			snapshot.MaxLagSeconds = max(snapshot.MaxLagSeconds, round(stat.OldestAge.Seconds()))
		}
	}
	if ratio, err := h.runtime.maintenanceRepo.DeadTupleRatio(ctx, "delivery_task"); err == nil {
		snapshot.DeadTupleRatio = round(ratio)
	}
	return snapshot
}

func summarizeConfig(cfg Config) ConfigSummary {
	schedule := make([]string, 0, len(cfg.RetrySchedule))
	for _, delay := range cfg.RetrySchedule {
		schedule = append(schedule, delay.String())
	}

	return ConfigSummary{
		RetrySchedule:      schedule,
		Rate:               cfg.Rate,
		Tenants:            cfg.Tenants,
		EndpointsPerTenant: cfg.EndpointsPerTenant,
		PayloadBytes:       cfg.PayloadBytes,
		TarpitFraction:     cfg.TarpitFraction,
		FailureRate:        cfg.FailureRate,
		Workers:            cfg.Workers,
	}
}

func formatIsolationVerdict(baseline, stressed float64) string {
	switch {
	case baseline == 0:
		return "no baseline deliveries were measured"
	case stressed == 0:
		return "no deliveries were measured under load"
	default:
		return "healthy p99 " + formatFloat(stressed) + "ms against a baseline of " +
			formatFloat(baseline) + "ms (budget " + formatFloat(baseline*IsolationTailBudget) + "ms)"
	}
}

// formatFloat renders a millisecond figure without trailing noise.
func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}
