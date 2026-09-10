// Package retry owns the retry schedule and the delay arithmetic. It is pure
// and clock-injectable, because these are the rules most likely to be argued
// about and they must be testable without waiting.
package retry

import (
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

// DefaultSchedule is the delay before each retry, copied from the incumbent's
// schedule so a migrating tenant's endpoints behave identically and nobody has
// to re-tune anything on the receiving side.
//
// Eight attempts in total. The eighth and last fires 27h35m after the first,
// with the seventh at 17h35m. Retention has to outlast that window or a
// message expires while a retry for it is still scheduled.
var DefaultSchedule = []time.Duration{
	5 * time.Second,
	5 * time.Minute,
	30 * time.Minute,
	2 * time.Hour,
	5 * time.Hour,
	10 * time.Hour,
	10 * time.Hour,
}

// OverloadFloor is the minimum delay after a timeout or a 429. An endpoint
// telling us it is overloaded must not be retried in five seconds.
const OverloadFloor = 30 * time.Second

// JitterFraction spreads retries so a mass failure does not re-synchronise
// into a thundering herd.
const JitterFraction = 0.10

// Outcome is what the delivery attempt produced, reduced to the facts the
// policy needs.
type Outcome struct {
	// StatusCode is the HTTP status, or zero when no response arrived.
	StatusCode int
	Timeout    bool
	// RetryAfter is the endpoint's requested delay, if it sent a usable one.
	RetryAfter *time.Duration
}

// Policy computes retry delays.
type Policy struct {
	schedule []time.Duration
	jitter   func(time.Duration, float64) time.Duration
}

// Option customises a policy.
type Option func(*Policy)

// WithSchedule replaces the delay schedule.
func WithSchedule(schedule []time.Duration) Option {
	return func(p *Policy) {
		if len(schedule) > 0 {
			p.schedule = schedule
		}
	}
}

// WithJitter replaces the jitter function, which lets tests assert exact
// delays.
func WithJitter(jitter func(d time.Duration, fraction float64) time.Duration) Option {
	return func(p *Policy) {
		if jitter != nil {
			p.jitter = jitter
		}
	}
}

// NewPolicy builds a policy with the default schedule.
func NewPolicy(opts ...Option) *Policy {
	p := &Policy{schedule: DefaultSchedule, jitter: randomJitter}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// MaxAttempts is the total number of attempts, the first one included.
func (p *Policy) MaxAttempts() int { return len(p.schedule) + 1 }

// Exhausted reports whether an attempt number has used up the schedule.
// attempt is zero-based: attempt 0 is the first delivery.
func (p *Policy) Exhausted(attempt int) bool { return attempt >= len(p.schedule) }

// Delay returns how long to wait before the retry that follows the supplied
// attempt, and whether a retry is due at all.
func (p *Policy) Delay(attempt int, outcome Outcome) (time.Duration, bool) {
	if p.Exhausted(attempt) {
		return 0, false
	}

	delay := p.schedule[attempt]

	// Overload penalty: a timeout or an explicit 429 gets a floor.
	if outcome.Timeout || outcome.StatusCode == http.StatusTooManyRequests {
		delay = max(delay, OverloadFloor)
	}

	// Honour Retry-After, but clamp it into [d, 2d] so a hostile or careless
	// value cannot pin a queue slot for a week.
	if outcome.RetryAfter != nil {
		delay = clamp(*outcome.RetryAfter, delay, 2*delay)
	}

	return p.jitter(delay, JitterFraction), true
}

// ParseRetryAfter reads a Retry-After header, which may be either
// delta-seconds or an HTTP date. It returns nil when the value is missing,
// unparseable, or already in the past.
func ParseRetryAfter(header string, now time.Time) *time.Duration {
	if header == "" {
		return nil
	}

	if seconds, err := strconv.Atoi(header); err == nil {
		if seconds < 0 {
			return nil
		}
		delay := time.Duration(seconds) * time.Second
		return &delay
	}

	for _, layout := range []string{http.TimeFormat, time.RFC1123, time.RFC1123Z, time.RFC850, time.ANSIC} {
		when, err := time.Parse(layout, header)
		if err != nil {
			continue
		}
		delay := when.Sub(now)
		if delay <= 0 {
			return nil
		}
		return &delay
	}
	return nil
}

func clamp(d, lo, hi time.Duration) time.Duration {
	if d < lo {
		return lo
	}
	if d > hi {
		return hi
	}
	return d
}

// randomJitter spreads a delay by +/- fraction.
func randomJitter(d time.Duration, fraction float64) time.Duration {
	if d <= 0 || fraction <= 0 {
		return d
	}
	spread := float64(d) * fraction
	// rand/v2's global source is safe for concurrent use.
	offset := (rand.Float64()*2 - 1) * spread
	jittered := time.Duration(float64(d) + offset)
	if jittered < 0 {
		return 0
	}
	return jittered
}

// NoJitter is a jitter function that returns the delay unchanged, for tests
// and for the benchmark harness where reproducibility matters more than
// spreading load.
func NoJitter(d time.Duration, _ float64) time.Duration { return d }
