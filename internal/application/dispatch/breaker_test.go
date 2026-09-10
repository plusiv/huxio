package dispatch_test

import (
	"sync"
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/application/dispatch"
)

// clock is a controllable time source.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *clock {
	return &clock{now: time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)}
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func TestBreakerOpensAfterConsecutiveFailures(t *testing.T) {
	t.Parallel()

	clk := newClock()
	breaker := dispatch.NewBreaker(dispatch.BreakerOptions{
		FailureThreshold: 3,
		Cooldown:         30 * time.Second,
		Now:              clk.Now,
	})

	if breaker.State() != dispatch.BreakerClosed || !breaker.Allow() {
		t.Fatal("a new breaker must be closed")
	}

	// A success in the middle resets the run, so an endpoint that fails
	// intermittently never trips the breaker.
	breaker.OnFailure()
	breaker.OnFailure()
	breaker.OnSuccess()
	if breaker.State() != dispatch.BreakerClosed {
		t.Error("a success must reset the failure run")
	}
	if breaker.ConsecutiveFailures() != 0 {
		t.Errorf("ConsecutiveFailures = %d, want 0", breaker.ConsecutiveFailures())
	}

	for range 3 {
		breaker.OnFailure()
	}
	if breaker.State() != dispatch.BreakerOpen {
		t.Fatalf("state = %v, want open after the threshold", breaker.State())
	}
	// Open means no dial at all: the DNS lookup, connect and handshake are
	// what the breaker exists to skip.
	if breaker.Allow() {
		t.Error("an open breaker must not admit a delivery")
	}
}

func TestBreakerHalfOpenAdmitsExactlyOneProbe(t *testing.T) {
	t.Parallel()

	clk := newClock()
	breaker := dispatch.NewBreaker(dispatch.BreakerOptions{
		FailureThreshold: 1,
		Cooldown:         30 * time.Second,
		Now:              clk.Now,
	})
	breaker.OnFailure()

	// Still inside the cooldown.
	clk.advance(29 * time.Second)
	if breaker.Allow() {
		t.Fatal("the breaker must stay open until its cooldown elapses")
	}

	clk.advance(2 * time.Second)
	if !breaker.Allow() {
		t.Fatal("the breaker must admit a probe once the cooldown elapses")
	}
	if breaker.State() != dispatch.BreakerHalfOpen {
		t.Errorf("state = %v, want half-open", breaker.State())
	}
	// Exactly one probe: a recovering endpoint must not be hit with the whole
	// backlog the instant it comes back.
	for range 5 {
		if breaker.Allow() {
			t.Fatal("half-open must admit only one probe at a time")
		}
	}

	breaker.OnSuccess()
	if breaker.State() != dispatch.BreakerClosed {
		t.Errorf("state = %v, want closed after a successful probe", breaker.State())
	}
	if !breaker.Allow() {
		t.Error("a closed breaker must admit deliveries")
	}
}

func TestBreakerCooldownDoublesUpToTheCap(t *testing.T) {
	t.Parallel()

	clk := newClock()
	breaker := dispatch.NewBreaker(dispatch.BreakerOptions{
		FailureThreshold: 1,
		Cooldown:         30 * time.Second,
		MaxCooldown:      2 * time.Minute,
		Now:              clk.Now,
	})

	breaker.OnFailure() // opens with a 30s cooldown

	// Each failed probe doubles the wait: 60s, then 120s, then capped.
	for _, want := range []time.Duration{60 * time.Second, 120 * time.Second, 120 * time.Second} {
		// Wait out the current cooldown and fail the probe.
		clk.advance(10 * time.Minute)
		if !breaker.Allow() {
			t.Fatal("expected a probe to be admitted")
		}
		breaker.OnFailure()

		// Just before the new cooldown elapses, nothing is admitted.
		clk.advance(want - time.Second)
		if breaker.Allow() {
			t.Fatalf("a probe was admitted before the %s cooldown elapsed", want)
		}
		clk.advance(2 * time.Second)
		if !breaker.Allow() {
			t.Fatalf("no probe admitted after the %s cooldown", want)
		}
		breaker.OnFailure()
	}
}

func TestBreakerOpenForTracksSustainedOutage(t *testing.T) {
	t.Parallel()

	clk := newClock()
	breaker := dispatch.NewBreaker(dispatch.BreakerOptions{
		FailureThreshold: 1,
		Cooldown:         30 * time.Second,
		Now:              clk.Now,
	})

	if breaker.OpenFor() != 0 {
		t.Error("a closed breaker has been open for no time")
	}

	breaker.OnFailure()
	clk.advance(20 * time.Minute)

	// Sustained open time is what promotes an endpoint to quarantine, so it
	// must survive the intervening failed probes rather than resetting.
	if got := breaker.OpenFor(); got < 20*time.Minute {
		t.Errorf("OpenFor = %s, want at least 20m", got)
	}
	if !breaker.Allow() {
		t.Fatal("expected a probe")
	}
	breaker.OnFailure()
	if got := breaker.OpenFor(); got < 20*time.Minute {
		t.Errorf("OpenFor = %s after a failed probe, want the original open time", got)
	}

	breaker.OnSuccess()
	if breaker.OpenFor() != 0 {
		t.Error("recovery must clear the open time")
	}
}

func TestBreakerIsSafeUnderConcurrentUse(t *testing.T) {
	t.Parallel()

	breaker := dispatch.NewBreaker(dispatch.BreakerOptions{FailureThreshold: 5})

	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for range 50 {
				breaker.Allow()
				if i%2 == 0 {
					breaker.OnSuccess()
				} else {
					breaker.OnFailure()
				}
				breaker.State()
				breaker.OpenFor()
			}
		}(i)
	}
	wg.Wait()
}
