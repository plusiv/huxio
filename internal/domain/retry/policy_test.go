package retry_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/domain/retry"
	"github.com/plusiv/huxio/internal/utils"
)

func exactPolicy() *retry.Policy {
	return retry.NewPolicy(retry.WithJitter(retry.NoJitter))
}

func TestDelayFollowsTheSchedule(t *testing.T) {
	t.Parallel()

	policy := exactPolicy()
	if got := policy.MaxAttempts(); got != len(retry.DefaultSchedule)+1 {
		t.Errorf("MaxAttempts = %d, want %d", got, len(retry.DefaultSchedule)+1)
	}

	for attempt, want := range retry.DefaultSchedule {
		got, retryable := policy.Delay(attempt, retry.Outcome{StatusCode: http.StatusInternalServerError})
		if !retryable {
			t.Fatalf("attempt %d must be retryable", attempt)
		}
		if got != want {
			t.Errorf("attempt %d delay = %s, want %s", attempt, got, want)
		}
	}

	// Eight attempts, the last one 27h35m after the first. The seventh lands
	// at 17h35m, which is the "roughly 18 hours" the design doc refers to.
	if total := utils.Sum(retry.DefaultSchedule); total != 27*time.Hour+35*time.Minute+5*time.Second {
		t.Errorf("schedule totals %s, want 27h35m5s", total)
	}
	if seventh := utils.Sum(retry.DefaultSchedule[:len(retry.DefaultSchedule)-1]); seventh != 17*time.Hour+35*time.Minute+5*time.Second {
		t.Errorf("seventh attempt lands at %s, want 17h35m5s", seventh)
	}
}

func TestDelayExhaustion(t *testing.T) {
	t.Parallel()

	policy := exactPolicy()
	last := len(retry.DefaultSchedule)

	if policy.Exhausted(last - 1) {
		t.Error("the final scheduled retry must not be exhausted")
	}
	if !policy.Exhausted(last) {
		t.Error("an attempt past the schedule must be exhausted")
	}
	if _, retryable := policy.Delay(last, retry.Outcome{}); retryable {
		t.Error("an exhausted attempt must not be retryable")
	}
	if _, retryable := policy.Delay(last+10, retry.Outcome{}); retryable {
		t.Error("an attempt far past the schedule must not be retryable")
	}
}

func TestOverloadFloor(t *testing.T) {
	t.Parallel()

	policy := exactPolicy()

	tests := []struct {
		name    string
		outcome retry.Outcome
		want    time.Duration
	}{
		{"5xx keeps the schedule", retry.Outcome{StatusCode: 500}, 5 * time.Second},
		{"timeout gets the floor", retry.Outcome{Timeout: true}, retry.OverloadFloor},
		{"429 gets the floor", retry.Outcome{StatusCode: http.StatusTooManyRequests}, retry.OverloadFloor},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, _ := policy.Delay(0, tc.outcome)
			if got != tc.want {
				t.Errorf("delay = %s, want %s", got, tc.want)
			}
		})
	}

	// Later in the schedule the floor is already exceeded, so it changes
	// nothing.
	if got, _ := policy.Delay(1, retry.Outcome{Timeout: true}); got != 5*time.Minute {
		t.Errorf("attempt 1 timeout delay = %s, want the scheduled 5m", got)
	}
}

func TestRetryAfterIsClamped(t *testing.T) {
	t.Parallel()

	policy := exactPolicy()
	scheduled := retry.DefaultSchedule[0] // 5s

	tests := []struct {
		name       string
		retryAfter time.Duration
		want       time.Duration
	}{
		{"inside the window is honoured", 8 * time.Second, 8 * time.Second},
		{"below the schedule is raised to it", time.Second, scheduled},
		{"zero is raised to the schedule", 0, scheduled},
		{"at the lower bound", scheduled, scheduled},
		{"at the upper bound", 2 * scheduled, 2 * scheduled},
		{"hostile value is capped at twice the schedule", 999999 * time.Second, 2 * scheduled},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, retryable := policy.Delay(0, retry.Outcome{StatusCode: 503, RetryAfter: utils.Ptr(tc.retryAfter)})
			if !retryable {
				t.Fatal("expected a retry")
			}
			if got != tc.want {
				t.Errorf("delay = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestRetryAfterInteractsWithTheOverloadFloor(t *testing.T) {
	t.Parallel()

	policy := exactPolicy()

	// A 429 raises the base delay to the floor first, so the clamp window
	// becomes [30s, 60s] rather than [5s, 10s].
	got, _ := policy.Delay(0, retry.Outcome{
		StatusCode: http.StatusTooManyRequests,
		RetryAfter: utils.Ptr(45 * time.Second),
	})
	if got != 45*time.Second {
		t.Errorf("delay = %s, want the honoured 45s", got)
	}

	got, _ = policy.Delay(0, retry.Outcome{
		StatusCode: http.StatusTooManyRequests,
		RetryAfter: utils.Ptr(10 * time.Second),
	})
	if got != retry.OverloadFloor {
		t.Errorf("delay = %s, want the %s floor", got, retry.OverloadFloor)
	}
}

func TestParseRetryAfter(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		header string
		want   *time.Duration
	}{
		{"empty", "", nil},
		{"delta seconds", "120", utils.Ptr(2 * time.Minute)},
		{"zero seconds", "0", utils.Ptr(time.Duration(0))},
		{"negative seconds", "-5", nil},
		{"http date in the future", now.Add(90 * time.Second).Format(http.TimeFormat), utils.Ptr(90 * time.Second)},
		{"http date in the past", now.Add(-time.Hour).Format(http.TimeFormat), nil},
		{"rfc1123z", now.Add(time.Minute).Format(time.RFC1123Z), utils.Ptr(time.Minute)},
		{"nonsense", "soon please", nil},
		{"hostile", "99999999999999999999", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := retry.ParseRetryAfter(tc.header, now)
			switch {
			case tc.want == nil && got != nil:
				t.Errorf("ParseRetryAfter(%q) = %s, want nil", tc.header, *got)
			case tc.want != nil && got == nil:
				t.Errorf("ParseRetryAfter(%q) = nil, want %s", tc.header, *tc.want)
			case tc.want != nil && *got != *tc.want:
				t.Errorf("ParseRetryAfter(%q) = %s, want %s", tc.header, *got, *tc.want)
			}
		})
	}
}

func TestJitterStaysWithinBounds(t *testing.T) {
	t.Parallel()

	policy := retry.NewPolicy()
	base := retry.DefaultSchedule[1] // 5m
	lower := time.Duration(float64(base) * (1 - retry.JitterFraction))
	upper := time.Duration(float64(base) * (1 + retry.JitterFraction))

	spread := map[time.Duration]struct{}{}
	for range 500 {
		got, retryable := policy.Delay(1, retry.Outcome{StatusCode: 500})
		if !retryable {
			t.Fatal("expected a retry")
		}
		if got < lower || got > upper {
			t.Fatalf("delay %s outside the +/-10%% band [%s, %s]", got, lower, upper)
		}
		spread[got] = struct{}{}
	}
	// Jitter exists to desynchronise retries; a constant value would defeat it.
	if len(spread) < 100 {
		t.Errorf("only %d distinct delays in 500 draws; retries would re-synchronise", len(spread))
	}
}

func TestCustomSchedule(t *testing.T) {
	t.Parallel()

	policy := retry.NewPolicy(
		retry.WithSchedule([]time.Duration{time.Second, 2 * time.Second}),
		retry.WithJitter(retry.NoJitter),
	)
	if policy.MaxAttempts() != 3 {
		t.Errorf("MaxAttempts = %d, want 3", policy.MaxAttempts())
	}
	if got, _ := policy.Delay(1, retry.Outcome{}); got != 2*time.Second {
		t.Errorf("delay = %s, want 2s", got)
	}
	if _, retryable := policy.Delay(2, retry.Outcome{}); retryable {
		t.Error("attempt 2 must be exhausted under a two-entry schedule")
	}

	// An empty schedule is ignored rather than silently disabling retries.
	if fallback := retry.NewPolicy(retry.WithSchedule(nil)); fallback.MaxAttempts() != len(retry.DefaultSchedule)+1 {
		t.Error("an empty schedule must fall back to the default")
	}
}
