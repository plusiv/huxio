package telemetry_test

import (
	"testing"

	"github.com/plusiv/huxio/internal/infrastructure/telemetry"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestNewRegistersCollectors(t *testing.T) {
	t.Parallel()

	m := telemetry.New()
	m.DeliveriesTotal.WithLabelValues("org_1", telemetry.OutcomeSuccess, "2xx").Inc()

	if got := testutil.ToFloat64(m.DeliveriesTotal.WithLabelValues("org_1", telemetry.OutcomeSuccess, "2xx")); got != 1 {
		t.Fatalf("deliveries_total = %v, want 1", got)
	}
	if count, err := testutil.GatherAndCount(m.Registry(), "huxio_deliveries_total"); err != nil || count != 1 {
		t.Fatalf("GatherAndCount = %d, %v", count, err)
	}
}

func TestStatusClass(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		code int
		want string
	}{
		{"no response", 0, "none"},
		{"informational", 100, "1xx"},
		{"ok", 200, "2xx"},
		{"redirect", 302, "3xx"},
		{"client error", 429, "4xx"},
		{"server error", 503, "5xx"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := telemetry.StatusClass(tc.code); got != tc.want {
				t.Errorf("StatusClass(%d) = %q, want %q", tc.code, got, tc.want)
			}
		})
	}
}
