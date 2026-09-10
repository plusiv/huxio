package harness_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/plusiv/huxio/bench/harness"
)

func TestResultWriteAndLoadRoundTrip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	original := &harness.Result{
		Scenario:  harness.ScenarioThroughput,
		StartedAt: time.Now().UTC().Truncate(time.Second),
		Commit:    "0123456789abcdef",
		Delivery:  harness.DeliveryStats{Delivered: 100, PerSecond: 50},
		Pass:      true,
	}

	path, err := original.Write(dir)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	// The filename carries the scenario and a short commit, so runs can be
	// told apart without opening them.
	if base := filepath.Base(path); base != "throughput-0123456789ab.json" {
		t.Errorf("filename = %q", base)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stat: %v", err)
	}

	loaded, err := harness.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Scenario != original.Scenario || loaded.Delivery.Delivered != 100 || !loaded.Pass {
		t.Errorf("loaded = %+v", loaded)
	}
}

func TestLoadRejectsGarbage(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "broken.json")
	if err := os.WriteFile(path, []byte("not json"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := harness.Load(path); err == nil {
		t.Error("expected an error for an unparseable baseline")
	}
	if _, err := harness.Load(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Error("expected an error for a missing baseline")
	}
}

func TestCompareToFlagsRegressionsInBothDirections(t *testing.T) {
	t.Parallel()

	baseline := &harness.Result{
		Ingest:   harness.IngestStats{Latency: harness.Percentiles{P99: 10}},
		Delivery: harness.DeliveryStats{PerSecond: 1000, Latency: harness.Percentiles{P99: 50}, AllocBytesPerDelivery: 1000},
	}

	tests := []struct {
		name    string
		current *harness.Result
		want    []string
	}{
		{
			name: "no change",
			current: &harness.Result{
				Ingest:   harness.IngestStats{Latency: harness.Percentiles{P99: 10}},
				Delivery: harness.DeliveryStats{PerSecond: 1000, Latency: harness.Percentiles{P99: 50}, AllocBytesPerDelivery: 1000},
			},
		},
		{
			name: "inside the budget",
			current: &harness.Result{
				Ingest:   harness.IngestStats{Latency: harness.Percentiles{P99: 11}},
				Delivery: harness.DeliveryStats{PerSecond: 900, Latency: harness.Percentiles{P99: 55}, AllocBytesPerDelivery: 1100},
			},
		},
		{
			name: "throughput dropped",
			current: &harness.Result{
				Ingest:   harness.IngestStats{Latency: harness.Percentiles{P99: 10}},
				Delivery: harness.DeliveryStats{PerSecond: 700, Latency: harness.Percentiles{P99: 50}, AllocBytesPerDelivery: 1000},
			},
			want: []string{"delivered_per_second"},
		},
		{
			name: "tail grew",
			current: &harness.Result{
				Ingest:   harness.IngestStats{Latency: harness.Percentiles{P99: 20}},
				Delivery: harness.DeliveryStats{PerSecond: 1000, Latency: harness.Percentiles{P99: 80}, AllocBytesPerDelivery: 1000},
			},
			want: []string{"ingest_p99_ms", "delivery_p99_ms"},
		},
		{
			name: "allocations grew",
			current: &harness.Result{
				Ingest:   harness.IngestStats{Latency: harness.Percentiles{P99: 10}},
				Delivery: harness.DeliveryStats{PerSecond: 1000, Latency: harness.Percentiles{P99: 50}, AllocBytesPerDelivery: 2000},
			},
			want: []string{"alloc_bytes_per_delivery"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			regressions := tc.current.CompareTo(baseline)
			got := make(map[string]bool, len(regressions))
			for _, regression := range regressions {
				got[regression.Metric] = true
			}
			if len(regressions) != len(tc.want) {
				t.Fatalf("regressions = %v, want %v", keys(got), tc.want)
			}
			for _, metric := range tc.want {
				if !got[metric] {
					t.Errorf("expected %s to regress, got %v", metric, keys(got))
				}
			}
		})
	}
}

func TestCompareToIgnoresAnEmptyBaseline(t *testing.T) {
	t.Parallel()

	// A first run has nothing to compare against and must not fail CI.
	current := &harness.Result{Delivery: harness.DeliveryStats{PerSecond: 10}}
	if regressions := current.CompareTo(&harness.Result{}); len(regressions) != 0 {
		t.Errorf("regressions = %v, want none against an empty baseline", regressions)
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	return out
}
