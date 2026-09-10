package dispatch

import (
	"sync"
	"time"
)

// BreakerState is a circuit breaker's state.
type BreakerState int

// The three breaker states.
const (
	// BreakerClosed delivers normally.
	BreakerClosed BreakerState = iota
	// BreakerOpen does not dial at all. The point is skipping the DNS lookup,
	// TCP connect and TLS handshake for endpoints that are definitely down: at
	// scale, dead endpoints are a large fraction of outbound work, and this is
	// where the CPU goes if you skip it.
	BreakerOpen
	// BreakerHalfOpen allows exactly one probe in flight.
	BreakerHalfOpen
)

// String names the state for metrics and logs.
func (s BreakerState) String() string {
	switch s {
	case BreakerOpen:
		return "open"
	case BreakerHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

// BreakerOptions configures a breaker.
type BreakerOptions struct {
	// FailureThreshold is the number of consecutive failures that opens the
	// circuit.
	FailureThreshold int
	// Cooldown is the first open period. It doubles on every re-open.
	Cooldown time.Duration
	// MaxCooldown caps the doubling.
	MaxCooldown time.Duration
	// Now is injectable so the transitions can be tested without waiting.
	Now func() time.Time
}

// Breaker is a per-endpoint circuit breaker. It is owned by exactly one worker
// at a time, so its state needs no coordination beyond the mutex that guards
// concurrent deliveries to the same endpoint.
type Breaker struct {
	opts BreakerOptions

	mu               sync.Mutex
	state            BreakerState
	consecutiveFails int
	cooldown         time.Duration
	openedAt         time.Time
	retryAt          time.Time
	probeInFlight    bool
}

// NewBreaker builds a closed breaker.
func NewBreaker(opts BreakerOptions) *Breaker {
	if opts.FailureThreshold <= 0 {
		opts.FailureThreshold = 20
	}
	if opts.Cooldown <= 0 {
		opts.Cooldown = 30 * time.Second
	}
	if opts.MaxCooldown < opts.Cooldown {
		opts.MaxCooldown = max(10*time.Minute, opts.Cooldown)
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Breaker{opts: opts, state: BreakerClosed, cooldown: opts.Cooldown}
}

// Allow reports whether a delivery may be attempted, and moves an open
// breaker to half-open once its cooldown has elapsed.
//
// In the half-open state exactly one probe is admitted: a second concurrent
// delivery is refused until the probe reports back, so a recovering endpoint
// is not hit with the whole backlog at once.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case BreakerClosed:
		return true

	case BreakerOpen:
		if b.opts.Now().Before(b.retryAt) {
			return false
		}
		b.state = BreakerHalfOpen
		b.probeInFlight = true
		return true

	case BreakerHalfOpen:
		if b.probeInFlight {
			return false
		}
		b.probeInFlight = true
		return true

	default:
		return true
	}
}

// OnSuccess records a delivery the endpoint accepted.
func (b *Breaker) OnSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.consecutiveFails = 0
	b.probeInFlight = false
	b.state = BreakerClosed
	b.cooldown = b.opts.Cooldown
	b.openedAt = time.Time{}
}

// OnFailure records a delivery the endpoint did not accept.
func (b *Breaker) OnFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.opts.Now()
	b.consecutiveFails++

	if b.state == BreakerHalfOpen {
		// The probe failed: back to open, with a longer cooldown each time so
		// a long outage is not probed every thirty seconds forever.
		b.probeInFlight = false
		b.reopen(now)
		return
	}

	if b.state == BreakerClosed && b.consecutiveFails >= b.opts.FailureThreshold {
		b.openedAt = now
		b.reopen(now)
	}
}

// reopen puts the breaker into the open state and schedules the next probe.
// The caller holds the mutex.
func (b *Breaker) reopen(now time.Time) {
	if b.openedAt.IsZero() {
		b.openedAt = now
	}
	if b.state == BreakerOpen || b.state == BreakerHalfOpen {
		b.cooldown = min(b.cooldown*2, b.opts.MaxCooldown)
	}
	b.state = BreakerOpen
	b.retryAt = now.Add(b.cooldown)
}

// State reports the current state.
func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// OpenFor reports how long the breaker has been open, or zero when it is not.
// Sustained open time is what promotes an endpoint to the quarantine pool.
func (b *Breaker) OpenFor() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == BreakerClosed || b.openedAt.IsZero() {
		return 0
	}
	return b.opts.Now().Sub(b.openedAt)
}

// ConsecutiveFailures reports the current unbroken run of failures.
func (b *Breaker) ConsecutiveFailures() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.consecutiveFails
}
