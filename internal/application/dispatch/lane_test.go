package dispatch_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/application/config"
	"github.com/plusiv/huxio/internal/application/dispatch"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/utils"
)

func TestLaneConcurrencyNeverExceedsItsBound(t *testing.T) {
	t.Parallel()

	const (
		bound   = 4
		workers = 64
	)
	lane := dispatch.NewLane("ep_1", dispatch.LaneOptions{
		InitialConcurrency: bound,
		MaxConcurrency:     bound,
		// Every worker may queue: this test is about the concurrency bound, not
		// the waiting room.
		MaxWaiters: workers,
	})

	var (
		current atomic.Int32
		peak    atomic.Int32
	)

	ctx := context.Background()
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				if err := lane.Acquire(ctx); err != nil {
					t.Errorf("Acquire: %v", err)
					return
				}
				now := current.Add(1)
				for {
					observed := peak.Load()
					if now <= observed || peak.CompareAndSwap(observed, now) {
						break
					}
				}
				time.Sleep(time.Millisecond)
				current.Add(-1)
				lane.Release()
			}
		}()
	}
	wg.Wait()

	if got := peak.Load(); got > bound {
		t.Errorf("peak concurrency = %d, want at most %d", got, bound)
	}
	if got := peak.Load(); got < 2 {
		t.Errorf("peak concurrency = %d; the lane is not admitting concurrent deliveries", got)
	}
	if lane.InFlight() != 0 {
		t.Errorf("InFlight = %d after every release, want 0", lane.InFlight())
	}
}

// The waiting room is bounded so an endpoint that stopped answering cannot
// accumulate waiters without limit: past the bound Acquire refuses at once
// instead of parking another goroutine and another locked task.
func TestLaneAcquireRefusesWhenTheWaitingRoomIsFull(t *testing.T) {
	t.Parallel()

	lane := dispatch.NewLane("ep_1", dispatch.LaneOptions{
		InitialConcurrency: 1, MaxConcurrency: 1, MaxWaiters: 2,
	})
	if err := lane.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiting := make(chan error, 2)
	for range 2 {
		go func() { waiting <- lane.Acquire(ctx) }()
	}
	deadline := time.Now().Add(2 * time.Second)
	for lane.Waiting() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if lane.Waiting() != 2 {
		t.Fatalf("Waiting = %d, want 2 parked callers", lane.Waiting())
	}

	if err := lane.Acquire(ctx); !errors.Is(err, dispatch.ErrLaneSaturated) {
		t.Fatalf("third waiter got %v, want ErrLaneSaturated without waiting", err)
	}

	// A release admits exactly one parked caller; the room drains as slots free.
	lane.Release()
	if err := <-waiting; err != nil {
		t.Fatalf("parked caller: %v", err)
	}
	cancel()
	if err := <-waiting; err == nil {
		t.Fatal("second parked caller must fail once its context is cancelled")
	}
	if lane.Waiting() != 0 {
		t.Errorf("Waiting = %d after the room drained, want 0", lane.Waiting())
	}
}

func TestLaneAcquireRespectsContextCancellation(t *testing.T) {
	t.Parallel()

	lane := dispatch.NewLane("ep_1", dispatch.LaneOptions{InitialConcurrency: 1, MaxConcurrency: 1})
	if err := lane.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := lane.Acquire(ctx); err == nil {
		t.Fatal("Acquire must not succeed while the lane is full")
	}

	// The abandoned waiter must not hold the slot hostage.
	lane.Release()
	if !lane.TryAcquire() {
		t.Error("the slot must be available after the release")
	}
}

func TestLaneTryAcquire(t *testing.T) {
	t.Parallel()

	lane := dispatch.NewLane("ep_1", dispatch.LaneOptions{InitialConcurrency: 2, MaxConcurrency: 2})

	first := lane.TryAcquire()
	second := lane.TryAcquire()
	if !first || !second {
		t.Fatal("both slots must be available")
	}
	if lane.TryAcquire() {
		t.Error("TryAcquire must fail at capacity rather than wait")
	}
	lane.Release()
	if !lane.TryAcquire() {
		t.Error("a released slot must be reusable")
	}
}

func TestLaneAIMD(t *testing.T) {
	t.Parallel()

	lane := dispatch.NewLane("ep_1", dispatch.LaneOptions{
		InitialConcurrency:   8,
		MaxConcurrency:       16,
		SuccessesPerIncrease: 10,
	})

	if got := lane.Concurrency(); got != 8 {
		t.Fatalf("initial concurrency = %d, want 8", got)
	}

	// Additive increase: one more slot every ten consecutive successes.
	for range 9 {
		lane.OnSuccess()
	}
	if got := lane.Concurrency(); got != 8 {
		t.Errorf("concurrency = %d after nine successes, want 8", got)
	}
	lane.OnSuccess()
	if got := lane.Concurrency(); got != 9 {
		t.Errorf("concurrency = %d after ten successes, want 9", got)
	}

	// Multiplicative decrease: halve on a 5xx, a timeout or a 429.
	lane.OnFailure(dispatch.DeliveryResult{StatusCode: 500})
	if got := lane.Concurrency(); got != 4 {
		t.Errorf("concurrency = %d after a failure, want it halved to 4", got)
	}

	// A partial run of successes does not carry across a failure.
	for range 5 {
		lane.OnSuccess()
	}
	lane.OnFailure(dispatch.DeliveryResult{StatusCode: 503})
	for range 5 {
		lane.OnSuccess()
	}
	if got := lane.Concurrency(); got != 2 {
		t.Errorf("concurrency = %d, want the failure run to have reset the success count", got)
	}

	// A refused connection means the endpoint is absent, not slow: drop straight
	// to one.
	lane.OnFailure(dispatch.DeliveryResult{Refused: true})
	if got := lane.Concurrency(); got != 1 {
		t.Errorf("concurrency = %d after a refused connection, want 1", got)
	}

	// The floor is one, however many failures arrive.
	for range 10 {
		lane.OnFailure(dispatch.DeliveryResult{StatusCode: 500})
	}
	if got := lane.Concurrency(); got != 1 {
		t.Errorf("concurrency = %d, want the floor of 1", got)
	}

	// And the ceiling holds under a long success run.
	for range 500 {
		lane.OnSuccess()
	}
	if got := lane.Concurrency(); got != 16 {
		t.Errorf("concurrency = %d, want the ceiling of 16", got)
	}
}

func TestLaneAIMDConvergesUnderAFixedFailureRate(t *testing.T) {
	t.Parallel()

	// An endpoint that accepts four concurrent requests and fails anything
	// beyond that: the lane must settle around its real capacity without
	// anybody configuring it.
	const capacity = 4

	lane := dispatch.NewLane("ep_1", dispatch.LaneOptions{
		InitialConcurrency:   32,
		MaxConcurrency:       64,
		SuccessesPerIncrease: 5,
	})

	for range 300 {
		if lane.Concurrency() > capacity {
			lane.OnFailure(dispatch.DeliveryResult{StatusCode: 503})
			continue
		}
		lane.OnSuccess()
	}

	// AIMD oscillates around the limit rather than sitting exactly on it.
	if got := lane.Concurrency(); got < 2 || got > capacity+2 {
		t.Errorf("converged concurrency = %d, want roughly the endpoint's capacity of %d", got, capacity)
	}
}

func TestLaneRateLimit(t *testing.T) {
	t.Parallel()

	// Two requests per second, burst two: the third must wait.
	lane := dispatch.NewLane("ep_1", dispatch.LaneOptions{
		InitialConcurrency: 10,
		MaxConcurrency:     10,
		RateLimit:          2,
	})

	ctx := context.Background()
	started := time.Now()
	for range 3 {
		if err := lane.Acquire(ctx); err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		lane.Release()
	}
	if elapsed := time.Since(started); elapsed < 300*time.Millisecond {
		t.Errorf("three acquisitions at 2/s took %s; the rate limit is not being applied", elapsed)
	}
}

func TestLaneManagerReusesLanesPerEndpoint(t *testing.T) {
	t.Parallel()

	manager := dispatch.NewLaneManager(dispatch.LaneManagerOptions{
		Defaults: dispatch.LaneOptions{InitialConcurrency: 8, MaxConcurrency: 64},
	})

	ep := &config.Endpoint{Endpoint: &entities.Endpoint{ID: "ep_1"}}
	other := &config.Endpoint{Endpoint: &entities.Endpoint{ID: "ep_2"}}

	first := manager.For(ep)
	if manager.For(ep) != first {
		t.Error("the same endpoint must get the same lane, or its state is meaningless")
	}
	if manager.For(other) == first {
		t.Error("different endpoints must get different lanes")
	}
	if manager.Len() != 2 {
		t.Errorf("Len = %d, want 2", manager.Len())
	}
	if manager.Get("ep_1") != first {
		t.Error("Get must return the existing lane")
	}
	if manager.Get("ep_missing") != nil {
		t.Error("Get must return nil for an unknown endpoint")
	}

	manager.Drop("ep_1")
	if manager.Get("ep_1") != nil {
		t.Error("Drop must remove the lane")
	}
}

func TestLaneManagerAppliesEndpointRateLimit(t *testing.T) {
	t.Parallel()

	manager := dispatch.NewLaneManager(dispatch.LaneManagerOptions{
		Defaults: dispatch.LaneOptions{InitialConcurrency: 4, MaxConcurrency: 8},
	})

	limited := manager.For(&config.Endpoint{
		Endpoint: &entities.Endpoint{ID: "ep_limited", RateLimit: utils.Ptr(1)},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// The first acquisition consumes the burst; the second must wait, and
	// here it runs out of context instead.
	if err := limited.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	limited.Release()
	if err := limited.Acquire(ctx); err == nil {
		t.Error("the endpoint's own rate limit must apply to its lane")
	}
}

func TestLaneManagerCreatesLanesSafelyUnderConcurrency(t *testing.T) {
	t.Parallel()

	manager := dispatch.NewLaneManager(dispatch.LaneManagerOptions{
		Defaults: dispatch.LaneOptions{InitialConcurrency: 2, MaxConcurrency: 4},
	})
	ep := &config.Endpoint{Endpoint: &entities.Endpoint{ID: "ep_race"}}

	lanes := make([]*dispatch.Lane, 32)
	var wg sync.WaitGroup
	for i := range lanes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lanes[i] = manager.For(ep)
		}(i)
	}
	wg.Wait()

	// Two lanes for one endpoint would mean two independent limiters and
	// breakers, which is exactly the isolation guarantee being broken.
	for _, lane := range lanes {
		if lane != lanes[0] {
			t.Fatal("concurrent creation produced more than one lane for one endpoint")
		}
	}
	if manager.Len() != 1 {
		t.Errorf("Len = %d, want 1", manager.Len())
	}
}

func TestLaneManagerEvictsIdleLanesButKeepsTrippedOnes(t *testing.T) {
	t.Parallel()

	clk := newClock()
	manager := dispatch.NewLaneManager(dispatch.LaneManagerOptions{
		Defaults: dispatch.LaneOptions{
			InitialConcurrency: 2,
			MaxConcurrency:     4,
			Breaker:            dispatch.BreakerOptions{FailureThreshold: 1, Now: clk.Now},
			Now:                clk.Now,
		},
		IdleEviction: 10 * time.Minute,
		Now:          clk.Now,
	})

	idle := manager.For(&config.Endpoint{Endpoint: &entities.Endpoint{ID: "ep_idle"}})
	busy := manager.For(&config.Endpoint{Endpoint: &entities.Endpoint{ID: "ep_busy"}})
	tripped := manager.For(&config.Endpoint{Endpoint: &entities.Endpoint{ID: "ep_tripped"}})

	if !busy.TryAcquire() {
		t.Fatal("expected to take a slot")
	}
	tripped.OnFailure(dispatch.DeliveryResult{StatusCode: 500})
	if tripped.Breaker().State() != dispatch.BreakerOpen {
		t.Fatal("expected the breaker to be open")
	}

	clk.advance(11 * time.Minute)

	if evicted := manager.EvictIdle(); evicted != 1 {
		t.Errorf("evicted %d lanes, want only the idle one", evicted)
	}
	if manager.Get("ep_idle") != nil {
		t.Error("the idle lane must be evicted")
	}
	if manager.Get("ep_busy") == nil {
		t.Error("a lane with a delivery in flight must not be evicted")
	}
	// Discarding an open breaker would make the worker re-learn that a dead
	// endpoint is dead, which is the cost the breaker exists to avoid.
	if manager.Get("ep_tripped") == nil {
		t.Error("a lane with a tripped breaker must not be evicted")
	}

	_ = idle
	busy.Release()
}

func TestLaneManagerStatsAndCounts(t *testing.T) {
	t.Parallel()

	manager := dispatch.NewLaneManager(dispatch.LaneManagerOptions{
		Defaults: dispatch.LaneOptions{
			InitialConcurrency: 4,
			MaxConcurrency:     8,
			Breaker:            dispatch.BreakerOptions{FailureThreshold: 1},
		},
	})

	healthy := manager.For(&config.Endpoint{Endpoint: &entities.Endpoint{ID: "ep_healthy"}})
	broken := manager.For(&config.Endpoint{Endpoint: &entities.Endpoint{ID: "ep_broken"}})

	if !healthy.TryAcquire() {
		t.Fatal("expected to take a slot")
	}
	broken.OnFailure(dispatch.DeliveryResult{Refused: true})

	counts := manager.BreakerCounts()
	if counts[dispatch.BreakerClosed] != 1 || counts[dispatch.BreakerOpen] != 1 {
		t.Errorf("breaker counts = %v", counts)
	}
	if got := manager.TotalInFlight(); got != 1 {
		t.Errorf("TotalInFlight = %d, want 1", got)
	}

	stats := manager.Stats()
	if len(stats) != 2 {
		t.Fatalf("Stats returned %d lanes", len(stats))
	}
	for _, stat := range stats {
		switch stat.EndpointID {
		case "ep_healthy":
			if stat.InFlight != 1 || stat.BreakerState != dispatch.BreakerClosed {
				t.Errorf("healthy lane stats = %+v", stat)
			}
		case "ep_broken":
			if stat.Concurrency != 1 || stat.BreakerState != dispatch.BreakerOpen {
				t.Errorf("broken lane stats = %+v", stat)
			}
		default:
			t.Errorf("unexpected lane %q", stat.EndpointID)
		}
	}

	healthy.Release()
}
