package dispatch

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rotisserie/eris"
	"golang.org/x/time/rate"
)

// ErrLaneSaturated is returned by Acquire when the lane is full and its
// waiting room is full too. The caller should put the task back in the queue
// rather than wait.
var ErrLaneSaturated = eris.New("lane saturated: no slot and no room to wait")

// LaneOptions configures per-endpoint concurrency.
type LaneOptions struct {
	// InitialConcurrency is where a new lane starts.
	InitialConcurrency int
	// MaxConcurrency bounds the additive increase.
	MaxConcurrency int
	// SuccessesPerIncrease is how many consecutive successes earn one more
	// slot. AIMD: additive increase, multiplicative decrease.
	SuccessesPerIncrease int
	// RateLimit is an optional per-endpoint cap in requests per second.
	RateLimit int
	// MaxWaiters bounds how many deliveries may queue for a slot in this lane
	// at once. Waiting absorbs a burst without a round trip to Postgres, but an
	// endpoint that has stopped answering would otherwise accumulate waiters
	// without limit, one goroutine and one locked task each. Past the bound
	// Acquire fails immediately and the task goes back to the queue.
	MaxWaiters int
	// Breaker configures this lane's circuit breaker.
	Breaker BreakerOptions
	// Now lets a test drive the timers forward instead of sleeping.
	Now func() time.Time
}

// Lane is the per-endpoint state that makes isolation structural rather than
// best-effort: a failing endpoint consumes a bounded, dedicated slice of
// capacity and physically cannot consume more.
//
// A lane is owned by exactly one worker at a time, which is what lets this
// state live in local memory and cost nothing to read.
type Lane struct {
	endpointID string
	opts       LaneOptions

	breaker *Breaker
	limiter *rate.Limiter

	// slots is a resizable semaphore. Capacity is the AIMD concurrency; a
	// token is taken for the duration of a delivery.
	mu          sync.Mutex
	concurrency int
	inFlight    int
	waiting     int
	waiters     []chan struct{}
	successes   int

	inflightGauge atomic.Int32
	lastUsed      atomic.Int64
	// failingNotice records that the tenant has already been told this endpoint
	// is struggling, so the warning fires once per outage rather than once per
	// retry.
	failingNotice atomic.Bool

	// health is what this worker has already written about the endpoint's health.
	// It lives here rather than on the config snapshot because the snapshot is
	// shared by every concurrent delivery and must stay immutable; the lane is
	// owned by one worker.
	healthMu     sync.Mutex
	failureStart *time.Time
	disabled     bool
	quarantined  bool
}

// NewLane builds a lane for one endpoint.
func NewLane(endpointID string, opts LaneOptions) *Lane {
	if opts.MaxConcurrency <= 0 {
		opts.MaxConcurrency = 64
	}
	if opts.InitialConcurrency <= 0 || opts.InitialConcurrency > opts.MaxConcurrency {
		opts.InitialConcurrency = min(8, opts.MaxConcurrency)
	}
	if opts.SuccessesPerIncrease <= 0 {
		opts.SuccessesPerIncrease = 10
	}
	if opts.MaxWaiters <= 0 {
		// Four rounds of the lane's ceiling: deep enough that a healthy endpoint
		// drains a claim batch without touching Postgres, shallow enough that a
		// dead one pins a few hundred tasks at most.
		opts.MaxWaiters = 4 * opts.MaxConcurrency
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	lane := &Lane{
		endpointID:  endpointID,
		opts:        opts,
		breaker:     NewBreaker(opts.Breaker),
		concurrency: opts.InitialConcurrency,
	}
	if opts.RateLimit > 0 {
		lane.limiter = rate.NewLimiter(rate.Limit(opts.RateLimit), max(1, opts.RateLimit))
	}
	lane.touch()
	return lane
}

// EndpointID reports which endpoint this lane belongs to.
func (l *Lane) EndpointID() string { return l.endpointID }

// Breaker exposes the lane's circuit breaker.
func (l *Lane) Breaker() *Breaker { return l.breaker }

// Acquire takes a concurrency slot, waiting while the lane is at capacity.
// The wait is bounded two ways: by the caller's context, and by MaxWaiters,
// past which it returns ErrLaneSaturated at once. A caller that is refused
// should defer the task to the queue, where it is durable and costs nothing,
// rather than hold anything in this worker's memory.
func (l *Lane) Acquire(ctx context.Context) error {
	l.touch()

	l.mu.Lock()
	if l.waiting >= l.opts.MaxWaiters {
		l.mu.Unlock()
		return ErrLaneSaturated
	}
	l.waiting++
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		l.waiting--
		l.mu.Unlock()
	}()

	if l.limiter != nil {
		// The rate limiter is per endpoint and lives in this worker's memory,
		// so consulting it costs no network round trip.
		if err := l.limiter.Wait(ctx); err != nil {
			return err
		}
	}

	for {
		l.mu.Lock()
		if l.inFlight < l.concurrency {
			l.inFlight++
			l.inflightGauge.Store(int32(l.inFlight))
			l.mu.Unlock()
			return nil
		}
		waiter := make(chan struct{})
		l.waiters = append(l.waiters, waiter)
		l.mu.Unlock()

		select {
		case <-waiter:
			// Woken by a release; loop round and try to take the slot.
		case <-ctx.Done():
			l.cancelWaiter(waiter)
			return ctx.Err()
		}
	}
}

// TryAcquire takes a slot without waiting. It fails rather than waits on the
// rate limiter too, so a caller that gets true may deliver immediately.
func (l *Lane) TryAcquire() bool {
	l.touch()

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.inFlight >= l.concurrency {
		return false
	}
	// The slot check comes first so a refused call has not spent a rate token.
	if l.limiter != nil && !l.limiter.Allow() {
		return false
	}
	l.inFlight++
	l.inflightGauge.Store(int32(l.inFlight))
	return true
}

// Waiting reports how many deliveries are queued for a slot.
func (l *Lane) Waiting() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.waiting
}

// Release returns a slot.
func (l *Lane) Release() {
	l.mu.Lock()
	if l.inFlight > 0 {
		l.inFlight--
	}
	l.inflightGauge.Store(int32(l.inFlight))
	l.wakeWaitersLocked()
	l.mu.Unlock()
}

// cancelWaiter removes a waiter that gave up.
func (l *Lane) cancelWaiter(waiter chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, w := range l.waiters {
		if w == waiter {
			l.waiters = append(l.waiters[:i], l.waiters[i+1:]...)
			return
		}
	}
}

// wakeWaitersLocked wakes as many waiters as there are free slots.
func (l *Lane) wakeWaitersLocked() {
	free := l.concurrency - l.inFlight
	for free > 0 && len(l.waiters) > 0 {
		waiter := l.waiters[0]
		l.waiters = l.waiters[1:]
		close(waiter)
		free--
	}
}

// Allow reports whether the breaker permits a delivery.
func (l *Lane) Allow() bool { return l.breaker.Allow() }

// OnSuccess applies the additive increase: one more slot every N consecutive
// successes, up to the maximum.
func (l *Lane) OnSuccess() {
	l.breaker.OnSuccess()

	l.mu.Lock()
	defer l.mu.Unlock()

	l.successes++
	if l.successes < l.opts.SuccessesPerIncrease {
		return
	}
	l.successes = 0
	if l.concurrency < l.opts.MaxConcurrency {
		l.concurrency++
		l.wakeWaitersLocked()
	}
}

// OnFailure applies the multiplicative decrease: halve on a timeout, a 429 or
// a 5xx, and drop straight to one when the endpoint is not there at all.
// Nobody configures this, which is the point: it converges on whatever the
// endpoint can actually absorb.
func (l *Lane) OnFailure(result DeliveryResult) {
	l.breaker.OnFailure()

	l.mu.Lock()
	defer l.mu.Unlock()

	l.successes = 0
	switch {
	case result.Refused:
		// Connection refused or DNS failure: the endpoint is absent, not slow.
		l.concurrency = 1
	default:
		l.concurrency = max(1, l.concurrency/2)
	}
}

// Concurrency reports the current AIMD concurrency.
func (l *Lane) Concurrency() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.concurrency
}

// InFlight reports how many deliveries this lane is running.
func (l *Lane) InFlight() int { return int(l.inflightGauge.Load()) }

// FailureRunStart returns when this worker first saw the current unbroken run
// of failures, falling back to the value the snapshot was built with.
func (l *Lane) FailureRunStart(fromSnapshot *time.Time) *time.Time {
	l.healthMu.Lock()
	defer l.healthMu.Unlock()

	if l.failureStart != nil {
		return l.failureStart
	}
	if fromSnapshot != nil {
		// Adopt the stored value: the run started before this worker did.
		start := *fromSnapshot
		l.failureStart = &start
		return l.failureStart
	}
	return nil
}

// BeginFailureRun records the start of a failure run and reports whether this
// call started it, so the row is written once rather than once per failure.
func (l *Lane) BeginFailureRun(at time.Time) bool {
	l.healthMu.Lock()
	defer l.healthMu.Unlock()

	if l.failureStart != nil {
		return false
	}
	l.failureStart = &at
	return true
}

// EndFailureRun clears the run and reports whether one was in progress, which
// is what makes the "clear first_failure_at" write conditional.
func (l *Lane) EndFailureRun() bool {
	l.healthMu.Lock()
	defer l.healthMu.Unlock()

	had := l.failureStart != nil || l.disabled || l.quarantined
	l.failureStart = nil
	l.disabled = false
	l.quarantined = false
	return had
}

// MarkDisabled records that this worker has switched the endpoint off, and
// reports whether it was this call that did so.
func (l *Lane) MarkDisabled() bool {
	l.healthMu.Lock()
	defer l.healthMu.Unlock()

	if l.disabled {
		return false
	}
	l.disabled = true
	return true
}

// MarkQuarantined records that this worker has moved the endpoint to the
// quarantine pool, and reports whether it was this call that did so.
func (l *Lane) MarkQuarantined() bool {
	l.healthMu.Lock()
	defer l.healthMu.Unlock()

	if l.quarantined {
		return false
	}
	l.quarantined = true
	return true
}

// MarkFailingNotice records that the failing warning has been sent and
// reports whether this call was the one that sent it.
func (l *Lane) MarkFailingNotice() bool { return l.failingNotice.CompareAndSwap(false, true) }

// ClearFailingNotice clears the warning flag and reports whether one had been
// sent, which is what makes the recovery notice conditional.
func (l *Lane) ClearFailingNotice() bool { return l.failingNotice.Swap(false) }

// IdleFor reports how long since this lane was last used, for eviction.
func (l *Lane) IdleFor() time.Duration {
	last := time.Unix(0, l.lastUsed.Load())
	return l.opts.Now().Sub(last)
}

func (l *Lane) touch() { l.lastUsed.Store(l.opts.Now().UnixNano()) }
