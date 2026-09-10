package maintenance_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/application/maintenance"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/rotisserie/eris"
)

// fakeLeases is an in-memory lease table with a single named lease, so
// leadership can be contended in a test.
type fakeLeases struct {
	mu     sync.Mutex
	holder string
	reaped int64
	err    error
}

func (l *fakeLeases) RegisterWorker(context.Context, entities.Worker) error { return nil }
func (l *fakeLeases) DeregisterWorker(context.Context, string) error        { return nil }

func (l *fakeLeases) CountLiveWorkers(context.Context, string, time.Duration) (int, error) {
	return 1, nil
}

func (l *fakeLeases) ReapDeadWorkers(context.Context, time.Duration) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.reaped++
	return 1, nil
}

func (l *fakeLeases) ClaimPartitions(context.Context, string, string, time.Duration, int) ([]int16, error) {
	return nil, nil
}

func (l *fakeLeases) HeartbeatPartitions(context.Context, string, string, []int16) ([]int16, error) {
	return nil, nil
}

func (l *fakeLeases) ReleasePartitions(context.Context, string, string, []int16) error { return nil }

func (l *fakeLeases) ListPartitionLeases(context.Context, string) ([]entities.PartitionLease, error) {
	return nil, nil
}

func (l *fakeLeases) AcquireNamedLease(_ context.Context, _, ownerID string, _ time.Duration) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return false, l.err
	}
	if l.holder != "" && l.holder != ownerID {
		return false, nil
	}
	l.holder = ownerID
	return true, nil
}

func (l *fakeLeases) ReleaseNamedLease(_ context.Context, _, ownerID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.holder == ownerID {
		l.holder = ""
	}
	return nil
}

func (l *fakeLeases) setHolder(owner string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.holder = owner
}

func (l *fakeLeases) reapCount() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.reaped
}

// fakeQueue counts rescues and serves fixed stats.
type fakeQueue struct {
	rescues atomic.Int32
	stats   []repositories.QueueStats
	err     error
}

func (q *fakeQueue) Claim(context.Context, repositories.ClaimRequest) ([]entities.DeliveryTask, error) {
	return nil, nil
}
func (q *fakeQueue) Complete(context.Context, int64) error                     { return nil }
func (q *fakeQueue) CompleteMany(context.Context, []int64) error               { return nil }
func (q *fakeQueue) Retry(context.Context, int64, time.Duration) error         { return nil }
func (q *fakeQueue) Defer(context.Context, int64, time.Duration) error         { return nil }
func (q *fakeQueue) Release(context.Context, []int64) error                    { return nil }
func (q *fakeQueue) Enqueue(context.Context, []repositories.EnqueueTask) error { return nil }
func (q *fakeQueue) Notify(context.Context, int16) error                       { return nil }

func (q *fakeQueue) RescueStuck(context.Context) (int64, error) {
	q.rescues.Add(1)
	if q.err != nil {
		return 0, q.err
	}
	return 2, nil
}

func (q *fakeQueue) Stats(context.Context, []string) ([]repositories.QueueStats, error) {
	return q.stats, nil
}

// fakePartitions records the partition lifecycle calls.
type fakePartitions struct {
	mu      sync.Mutex
	ensured map[string]int
	dropped map[string]time.Time
	err     error
}

func newFakePartitions() *fakePartitions {
	return &fakePartitions{ensured: map[string]int{}, dropped: map[string]time.Time{}}
}

func (p *fakePartitions) EnsureDailyPartitions(_ context.Context, table string, ahead int) ([]repositories.PartitionSpec, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return nil, p.err
	}
	p.ensured[table] = ahead
	return []repositories.PartitionSpec{{Table: table}}, nil
}

func (p *fakePartitions) DropPartitionsBefore(_ context.Context, table string, cutoff time.Time) ([]repositories.PartitionSpec, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dropped[table] = cutoff
	return nil, nil
}

func (p *fakePartitions) ListPartitions(context.Context, string) ([]repositories.PartitionSpec, error) {
	return nil, nil
}
func (p *fakePartitions) DeadTupleRatio(context.Context, string) (float64, error) { return 0, nil }
func (p *fakePartitions) Healthy(context.Context) error                           { return nil }

func (p *fakePartitions) snapshot() (map[string]int, map[string]time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ensured := make(map[string]int, len(p.ensured))
	for k, v := range p.ensured {
		ensured[k] = v
	}
	dropped := make(map[string]time.Time, len(p.dropped))
	for k, v := range p.dropped {
		dropped[k] = v
	}
	return ensured, dropped
}

// fakeIdempotency counts purges.
type fakeIdempotency struct{ purges atomic.Int32 }

func (f *fakeIdempotency) Begin(context.Context, string, time.Duration) (*entities.IdempotencyRecord, bool, error) {
	return nil, true, nil
}
func (f *fakeIdempotency) Complete(context.Context, string, int16, []byte, time.Duration) error {
	return nil
}
func (f *fakeIdempotency) Abort(context.Context, string) error { return nil }

func (f *fakeIdempotency) PurgeExpired(context.Context) (int64, error) {
	f.purges.Add(1)
	return 3, nil
}

func runLoop(t *testing.T, loop *maintenance.Loop, d time.Duration) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	if err := loop.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestLoopRescuesReapsAndPurgesAsLeader(t *testing.T) {
	t.Parallel()

	leases := &fakeLeases{}
	queue := &fakeQueue{}
	partitions := newFakePartitions()
	idempotency := &fakeIdempotency{}

	loop, err := maintenance.New(maintenance.Deps{
		LeaseRepo: leases, QueueRepo: queue, PartitionRepo: partitions, IdempotencyRepo: idempotency,
	}, maintenance.Options{
		OwnerID:              "wkr_1",
		RescueInterval:       5 * time.Millisecond,
		PartitionInterval:    5 * time.Millisecond,
		StatsInterval:        time.Hour,
		PartitionsAhead:      7,
		MessageRetentionDays: 90,
		AttemptRetentionDays: 30,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	runLoop(t, loop, 120*time.Millisecond)

	if queue.rescues.Load() == 0 {
		t.Error("stuck tasks were never rescued; a worker dying mid-flight would strand them")
	}
	if leases.reapCount() == 0 {
		t.Error("dead workers were never reaped, so the fair share stays wrong")
	}
	if idempotency.purges.Load() == 0 {
		t.Error("expired idempotency keys were never purged")
	}

	ensured, dropped := partitions.snapshot()
	for _, table := range []string{repositories.TableMessage, repositories.TableDeliveryAttempt} {
		if ensured[table] != 7 {
			t.Errorf("%s: ensured %d days ahead, want 7", table, ensured[table])
		}
		if dropped[table].IsZero() {
			t.Errorf("%s: retention never dropped anything", table)
		}
	}
	// Retention is per table: attempts age out sooner here than messages.
	if !dropped[repositories.TableDeliveryAttempt].After(dropped[repositories.TableMessage]) {
		t.Errorf("attempt cutoff %s should be later than the message cutoff %s",
			dropped[repositories.TableDeliveryAttempt], dropped[repositories.TableMessage])
	}
}

func TestLoopDoesNothingPrivilegedWithoutTheLease(t *testing.T) {
	t.Parallel()

	leases := &fakeLeases{}
	leases.setHolder("another-worker")

	queue := &fakeQueue{stats: []repositories.QueueStats{{Pool: "default", Ready: 3, OldestAge: time.Second}}}
	partitions := newFakePartitions()

	var (
		mu     sync.Mutex
		lags   []float64
		depths int
	)

	loop, err := maintenance.New(maintenance.Deps{
		LeaseRepo: leases, QueueRepo: queue, PartitionRepo: partitions,
		Metrics: maintenance.Metrics{
			QueueLag: func(_ string, seconds float64) {
				mu.Lock()
				defer mu.Unlock()
				lags = append(lags, seconds)
			},
			QueueDepth: func(string, string, float64) {
				mu.Lock()
				defer mu.Unlock()
				depths++
			},
		},
	}, maintenance.Options{
		OwnerID:           "wkr_2",
		RescueInterval:    5 * time.Millisecond,
		PartitionInterval: 5 * time.Millisecond,
		StatsInterval:     5 * time.Millisecond,
		Pools:             []string{"default"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	runLoop(t, loop, 100*time.Millisecond)

	// Exactly one instance may do the privileged work, or two processes race
	// to create and drop the same partitions.
	if queue.rescues.Load() != 0 {
		t.Errorf("a follower rescued %d times, want none", queue.rescues.Load())
	}
	if ensured, _ := partitions.snapshot(); len(ensured) != 0 {
		t.Errorf("a follower touched partitions: %v", ensured)
	}
	if loop.IsLeader() {
		t.Error("IsLeader must be false while another process holds the lease")
	}

	// Queue stats are read by every worker: the numbers are the same, and a
	// missing leader must not blind the alerts.
	mu.Lock()
	defer mu.Unlock()
	if len(lags) == 0 || depths == 0 {
		t.Errorf("queue stats were not reported by a follower: %d lags, %d depths", len(lags), depths)
	}
	if lags[0] != 1 {
		t.Errorf("queue lag = %v, want 1s", lags[0])
	}
}

func TestLoopTakesOverLeadershipWhenItIsFree(t *testing.T) {
	t.Parallel()

	leases := &fakeLeases{}
	queue := &fakeQueue{}

	loop, err := maintenance.New(maintenance.Deps{
		LeaseRepo: leases, QueueRepo: queue, PartitionRepo: newFakePartitions(),
	}, maintenance.Options{
		OwnerID:           "wkr_1",
		RescueInterval:    5 * time.Millisecond,
		PartitionInterval: time.Hour,
		StatsInterval:     time.Hour,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- loop.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for queue.rescues.Load() == 0 {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("never took leadership of a free lease")
		case <-time.After(2 * time.Millisecond):
		}
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Releasing on shutdown lets another process take over at once rather
	// than waiting out the TTL.
	if leases.holder != "" {
		t.Errorf("lease still held by %q after shutdown", leases.holder)
	}
}

func TestLoopSurvivesFailures(t *testing.T) {
	t.Parallel()

	leases := &fakeLeases{}
	queue := &fakeQueue{err: eris.New("rescue failed")}
	partitions := newFakePartitions()
	partitions.err = eris.New("cannot create partition")

	loop, err := maintenance.New(maintenance.Deps{
		LeaseRepo: leases, QueueRepo: queue, PartitionRepo: partitions,
	}, maintenance.Options{
		OwnerID:           "wkr_1",
		RescueInterval:    5 * time.Millisecond,
		PartitionInterval: 5 * time.Millisecond,
		StatsInterval:     time.Hour,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// A failing maintenance pass must not take the worker down with it.
	runLoop(t, loop, 60*time.Millisecond)
	if queue.rescues.Load() == 0 {
		t.Error("the loop stopped retrying after a failure")
	}
}

func TestLoopRunsProcessLocalHooksEvenAsFollower(t *testing.T) {
	t.Parallel()

	leases := &fakeLeases{}
	leases.setHolder("another-worker")

	var ticks atomic.Int32
	loop, err := maintenance.New(maintenance.Deps{
		LeaseRepo: leases, QueueRepo: &fakeQueue{}, PartitionRepo: newFakePartitions(),
		Hooks: maintenance.Hooks{OnTick: func(context.Context) { ticks.Add(1) }},
	}, maintenance.Options{
		OwnerID:           "wkr_2",
		RescueInterval:    5 * time.Millisecond,
		PartitionInterval: time.Hour,
		StatsInterval:     time.Hour,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	runLoop(t, loop, 60*time.Millisecond)

	// Lane eviction is per process, so it must not depend on leadership.
	if ticks.Load() == 0 {
		t.Error("process-local hooks must run on every worker")
	}
}

func TestNewValidatesDependencies(t *testing.T) {
	t.Parallel()

	if _, err := maintenance.New(maintenance.Deps{}, maintenance.Options{OwnerID: "wkr_1"}); err == nil {
		t.Error("a loop without dependencies must not be constructible")
	}
	if _, err := maintenance.New(maintenance.Deps{
		LeaseRepo: &fakeLeases{}, QueueRepo: &fakeQueue{}, PartitionRepo: newFakePartitions(),
	}, maintenance.Options{}); err == nil {
		t.Error("a loop without an owner id cannot hold the leader lease")
	}
}
