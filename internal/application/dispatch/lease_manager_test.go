package dispatch_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/application/dispatch"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/utils"
	"github.com/rotisserie/eris"
)

// fakeLeaseRepo is an in-memory lease table shared by several managers, so a
// rebalance can be observed without a database.
type fakeLeaseRepo struct {
	mu         sync.Mutex
	partitions int
	owners     map[int16]string
	workers    map[string][]string
	named      map[string]string

	countErr     error
	claimErr     error
	heartbeatErr error
	releases     int
}

func newFakeLeaseRepo(partitions int) *fakeLeaseRepo {
	return &fakeLeaseRepo{
		partitions: partitions,
		owners:     map[int16]string{},
		workers:    map[string][]string{},
		named:      map[string]string{},
	}
}

func (r *fakeLeaseRepo) RegisterWorker(_ context.Context, worker entities.Worker) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workers[worker.ID] = worker.Pools
	return nil
}

func (r *fakeLeaseRepo) DeregisterWorker(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.workers, id)
	return nil
}

func (r *fakeLeaseRepo) CountLiveWorkers(_ context.Context, pool string, _ time.Duration) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.countErr != nil {
		return 0, r.countErr
	}
	count := 0
	for _, pools := range r.workers {
		if utils.Contains(pools, pool) {
			count++
		}
	}
	return count, nil
}

func (r *fakeLeaseRepo) ReapDeadWorkers(context.Context, time.Duration) (int64, error) {
	return 0, nil
}

func (r *fakeLeaseRepo) ClaimPartitions(_ context.Context, _, ownerID string, _ time.Duration, limit int) ([]int16, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.claimErr != nil {
		return nil, r.claimErr
	}

	owned := make([]int16, 0, limit)
	for partition := int16(0); partition < int16(r.partitions); partition++ {
		if r.owners[partition] == ownerID {
			owned = append(owned, partition)
		}
	}
	for partition := int16(0); partition < int16(r.partitions) && len(owned) < limit; partition++ {
		if owner, taken := r.owners[partition]; taken && owner != "" {
			continue
		}
		r.owners[partition] = ownerID
		owned = append(owned, partition)
	}
	if len(owned) > limit {
		owned = owned[:limit]
	}
	return owned, nil
}

func (r *fakeLeaseRepo) HeartbeatPartitions(_ context.Context, _, ownerID string, partitions []int16) ([]int16, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.heartbeatErr != nil {
		return nil, r.heartbeatErr
	}
	kept := make([]int16, 0, len(partitions))
	for _, partition := range partitions {
		if r.owners[partition] == ownerID {
			kept = append(kept, partition)
		}
	}
	return kept, nil
}

func (r *fakeLeaseRepo) ReleasePartitions(_ context.Context, _, ownerID string, partitions []int16) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.releases++

	if partitions == nil {
		for partition, owner := range r.owners {
			if owner == ownerID {
				delete(r.owners, partition)
			}
		}
		return nil
	}
	for _, partition := range partitions {
		if r.owners[partition] == ownerID {
			delete(r.owners, partition)
		}
	}
	return nil
}

func (r *fakeLeaseRepo) ListPartitionLeases(context.Context, string) ([]entities.PartitionLease, error) {
	return nil, nil
}

func (r *fakeLeaseRepo) AcquireNamedLease(_ context.Context, name, ownerID string, _ time.Duration) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if owner, held := r.named[name]; held && owner != ownerID {
		return false, nil
	}
	r.named[name] = ownerID
	return true, nil
}

func (r *fakeLeaseRepo) ReleaseNamedLease(_ context.Context, name, ownerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.named[name] == ownerID {
		delete(r.named, name)
	}
	return nil
}

func (r *fakeLeaseRepo) ownedBy(ownerID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, owner := range r.owners {
		if owner == ownerID {
			count++
		}
	}
	return count
}

// steal reassigns a partition to another worker, standing in for a rebalance
// that happened elsewhere.
func (r *fakeLeaseRepo) steal(partition int16, ownerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.owners[partition] = ownerID
}

// noopGauge satisfies the metric port.
type noopGauge struct{}

func (noopGauge) Set(float64) {}

func newLeaseManager(t *testing.T, repo *fakeLeaseRepo, workerID string, partitions int) *dispatch.LeaseManager {
	t.Helper()
	manager, err := dispatch.NewLeaseManager(repo, noopGauge{}, dispatch.LeaseManagerOptions{
		WorkerID:   workerID,
		Pool:       "default",
		Partitions: partitions,
		TTL:        15 * time.Second,
		Heartbeat:  time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager: %v", err)
	}
	return manager
}

// runUntil drives a lease manager until cond holds or the deadline passes.
func runUntil(t *testing.T, manager *dispatch.LeaseManager, cond func() bool, timeout time.Duration) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()

	deadline := time.After(timeout)
	for !cond() {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("condition never held")
		case <-time.After(2 * time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("lease manager Run: %v", err)
	}
}

func TestLeaseManagerClaimsEverythingWhenAlone(t *testing.T) {
	t.Parallel()

	const partitions = 16
	repo := newFakeLeaseRepo(partitions)
	if err := repo.RegisterWorker(context.Background(), entities.Worker{ID: "wkr_1", Pools: []string{"default"}}); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}

	manager := newLeaseManager(t, repo, "wkr_1", partitions)
	runUntil(t, manager, func() bool { return len(manager.Owned()) == partitions }, 2*time.Second)

	// Releasing on shutdown is what lets a replacement worker take over
	// immediately instead of waiting out the TTL.
	if repo.ownedBy("wkr_1") != 0 {
		t.Errorf("worker still owns %d partitions after shutdown", repo.ownedBy("wkr_1"))
	}
	if len(manager.Owned()) != 0 {
		t.Errorf("Owned = %v after shutdown, want empty", manager.Owned())
	}
}

func TestLeaseManagerSharesFairlyWithPeers(t *testing.T) {
	t.Parallel()

	const (
		partitions = 16
		workers    = 4
	)
	repo := newFakeLeaseRepo(partitions)
	ctx := context.Background()
	for _, id := range []string{"wkr_1", "wkr_2", "wkr_3", "wkr_4"} {
		if err := repo.RegisterWorker(ctx, entities.Worker{ID: id, Pools: []string{"default"}}); err != nil {
			t.Fatalf("RegisterWorker: %v", err)
		}
	}

	// ceil(16/4) = 4 each: no worker may take more, or the others starve.
	share := partitions / workers
	managers := make([]*dispatch.LeaseManager, 0, workers)
	cancels := make([]context.CancelFunc, 0, workers)
	dones := make([]chan error, 0, workers)

	for i := range workers {
		manager := newLeaseManager(t, repo, []string{"wkr_1", "wkr_2", "wkr_3", "wkr_4"}[i], partitions)
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- manager.Run(runCtx) }()

		managers = append(managers, manager)
		cancels = append(cancels, cancel)
		dones = append(dones, done)
	}

	deadline := time.After(3 * time.Second)
	for {
		total := 0
		balanced := true
		for _, manager := range managers {
			owned := len(manager.Owned())
			total += owned
			if owned > share {
				balanced = false
			}
		}
		if balanced && total == partitions {
			break
		}
		select {
		case <-deadline:
			owned := make([]int, 0, workers)
			for _, manager := range managers {
				owned = append(owned, len(manager.Owned()))
			}
			for _, cancel := range cancels {
				cancel()
			}
			t.Fatalf("partitions never balanced: %v", owned)
		case <-time.After(5 * time.Millisecond):
		}
	}

	// Nobody may hold a partition another worker also thinks it holds.
	seen := map[int16]string{}
	for i, manager := range managers {
		for _, partition := range manager.Owned() {
			if previous, dupe := seen[partition]; dupe {
				t.Fatalf("partition %d claimed by both %s and manager %d", partition, previous, i)
			}
			seen[partition] = strconv.Itoa(i)
		}
	}

	for _, cancel := range cancels {
		cancel()
	}
	for _, done := range dones {
		if err := <-done; err != nil {
			t.Errorf("lease manager Run: %v", err)
		}
	}
}

func TestLeaseManagerShedsExcessWhenAPeerJoins(t *testing.T) {
	t.Parallel()

	const partitions = 16
	repo := newFakeLeaseRepo(partitions)
	ctx := context.Background()
	if err := repo.RegisterWorker(ctx, entities.Worker{ID: "wkr_1", Pools: []string{"default"}}); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}

	manager := newLeaseManager(t, repo, "wkr_1", partitions)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- manager.Run(runCtx) }()

	waitFor(t, func() bool { return len(manager.Owned()) == partitions }, 2*time.Second, "sole worker never claimed everything")

	// A second worker registers: the incumbent must give half back rather
	// than starve it.
	if err := repo.RegisterWorker(ctx, entities.Worker{ID: "wkr_2", Pools: []string{"default"}}); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	waitFor(t, func() bool { return len(manager.Owned()) == partitions/2 }, 2*time.Second, "incumbent never shed its excess")

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("lease manager Run: %v", err)
	}
}

func TestLeaseManagerStopsWorkingPartitionsItLost(t *testing.T) {
	t.Parallel()

	const partitions = 8
	repo := newFakeLeaseRepo(partitions)
	ctx := context.Background()
	if err := repo.RegisterWorker(ctx, entities.Worker{ID: "wkr_1", Pools: []string{"default"}}); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}

	manager := newLeaseManager(t, repo, "wkr_1", partitions)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- manager.Run(runCtx) }()

	waitFor(t, func() bool { return len(manager.Owned()) == partitions }, 2*time.Second, "never claimed everything")

	// Another worker takes two partitions, and a third worker registers so
	// the fair share drops and they are not simply re-claimed.
	if err := repo.RegisterWorker(ctx, entities.Worker{ID: "wkr_2", Pools: []string{"default"}}); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	repo.steal(0, "wkr_2")
	repo.steal(1, "wkr_2")

	waitFor(t, func() bool {
		return !utils.Contains(manager.Owned(), 0) && !utils.Contains(manager.Owned(), 1)
	}, 2*time.Second, "kept working partitions it had lost")

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("lease manager Run: %v", err)
	}
}

func TestLeaseManagerSurvivesRepositoryErrors(t *testing.T) {
	t.Parallel()

	repo := newFakeLeaseRepo(8)
	repo.countErr = eris.New("database down")

	manager := newLeaseManager(t, repo, "wkr_1", 8)
	runCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// A failing rebalance must not take the worker down: it keeps whatever it
	// holds and tries again on the next tick.
	if err := manager.Run(runCtx); err != nil {
		t.Errorf("Run = %v, want the loop to tolerate repository errors", err)
	}
}

func TestNewLeaseManagerValidatesInput(t *testing.T) {
	t.Parallel()

	if _, err := dispatch.NewLeaseManager(nil, noopGauge{}, dispatch.LeaseManagerOptions{WorkerID: "wkr_1"}); err == nil {
		t.Error("a lease manager without a repository must not be constructible")
	}
	if _, err := dispatch.NewLeaseManager(newFakeLeaseRepo(8), noopGauge{}, dispatch.LeaseManagerOptions{}); err == nil {
		t.Error("a lease manager without a worker id must not be constructible")
	}
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, cond func() bool, timeout time.Duration, message string) {
	t.Helper()

	deadline := time.After(timeout)
	for !cond() {
		select {
		case <-deadline:
			t.Fatal(message)
		case <-time.After(2 * time.Millisecond):
		}
	}
}
