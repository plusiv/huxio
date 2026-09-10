package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/application/dispatch"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/utils"
)

// noopGauge satisfies the lease manager's metric port.
type noopGauge struct{}

func (noopGauge) Set(float64) {}

func newRealLeaseManager(t *testing.T, env *testEnv, workerID string) *dispatch.LeaseManager {
	t.Helper()

	manager, err := dispatch.NewLeaseManager(env.Leases, noopGauge{}, dispatch.LeaseManagerOptions{
		WorkerID:   workerID,
		Pool:       "default",
		Partitions: entities.QueuePartitions,
		TTL:        2 * time.Second,
		Heartbeat:  20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager: %v", err)
	}
	return manager
}

// startLeaseManager runs a manager and returns a stop function.
func startLeaseManager(t *testing.T, manager *dispatch.LeaseManager) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()

	stopped := false
	return func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("lease manager Run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("lease manager did not stop")
		}
	}
}

func TestLeaseManagersShareThePartitionSpaceOnRealPostgres(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	ctx := context.Background()

	// Both workers are registered, so each computes a fair share of two.
	for _, id := range []string{"wkr_a", "wkr_b"} {
		if err := env.Leases.RegisterWorker(ctx, entities.Worker{
			ID: id, Pools: []string{"default"}, Version: "test",
		}); err != nil {
			t.Fatalf("RegisterWorker: %v", err)
		}
	}

	first := newRealLeaseManager(t, env, "wkr_a")
	second := newRealLeaseManager(t, env, "wkr_b")

	stopFirst := startLeaseManager(t, first)
	defer stopFirst()
	stopSecond := startLeaseManager(t, second)
	defer stopSecond()

	share := entities.QueuePartitions / 2
	waitFor(t, func() bool {
		return len(first.Owned()) == share && len(second.Owned()) == share
	}, 10*time.Second, "the two workers never settled on an even split")

	// Ownership must be exclusive: two workers holding one partition would
	// mean two independent breakers and limiters for the same endpoints.
	owned := map[int16]string{}
	for _, partition := range first.Owned() {
		owned[partition] = "wkr_a"
	}
	for _, partition := range second.Owned() {
		if previous, dupe := owned[partition]; dupe {
			t.Fatalf("partition %d owned by both %s and wkr_b", partition, previous)
		}
		owned[partition] = "wkr_b"
	}
	if len(owned) != entities.QueuePartitions {
		t.Errorf("%d partitions owned in total, want all %d", len(owned), entities.QueuePartitions)
	}

	// One worker leaves: the survivor must take the whole space over rather
	// than leaving half the queue unworked.
	stopSecond()
	waitFor(t, func() bool { return len(first.Owned()) == entities.QueuePartitions }, 10*time.Second,
		"the survivor never took over the departed worker's partitions")

	// The lease table is meant to be readable, which is much of why fixed
	// partitions beat a hash ring.
	leases, err := env.Leases.ListPartitionLeases(ctx, "default")
	if err != nil {
		t.Fatalf("ListPartitionLeases: %v", err)
	}
	if len(leases) != entities.QueuePartitions {
		t.Errorf("lease table holds %d rows, want %d", len(leases), entities.QueuePartitions)
	}
	for _, lease := range leases {
		if utils.Deref(lease.OwnerID, "") != "wkr_a" {
			t.Fatalf("partition %d owned by %v, want wkr_a", lease.PartitionKey, lease.OwnerID)
		}
	}
}

func TestLeaseManagerReleasesEverythingOnShutdown(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	ctx := context.Background()

	if err := env.Leases.RegisterWorker(ctx, entities.Worker{
		ID: "wkr_solo", Pools: []string{"default"}, Version: "test",
	}); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}

	manager := newRealLeaseManager(t, env, "wkr_solo")
	stop := startLeaseManager(t, manager)

	waitFor(t, func() bool { return len(manager.Owned()) == entities.QueuePartitions }, 10*time.Second,
		"the sole worker never claimed the partition space")

	stop()

	// Releasing on SIGTERM is what lets a replacement start working immediately
	// instead of waiting out the lease TTL.
	leases, err := env.Leases.ListPartitionLeases(ctx, "default")
	if err != nil {
		t.Fatalf("ListPartitionLeases: %v", err)
	}
	for _, lease := range leases {
		if utils.Deref(lease.OwnerID, "") != "" {
			t.Fatalf("partition %d still owned by %v after shutdown", lease.PartitionKey, lease.OwnerID)
		}
	}
}

func TestWorkerOnlyClaimsTasksInItsOwnPartitions(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	receiver := newSink(t, 200)

	appID := api.createApp(t, "Partitioned", nil)
	endpointID, _ := api.createEndpoint(t, appID, map[string]any{"url": receiver.server.URL})

	// This worker owns every partition except the endpoint's, so its deliveries
	// are invisible to it.
	endpointPartition := entities.PartitionKeyFor(endpointID)
	owned := make([]int16, 0, entities.QueuePartitions-1)
	for partition := int16(0); partition < entities.QueuePartitions; partition++ {
		if partition != endpointPartition {
			owned = append(owned, partition)
		}
	}

	worker := newWorkerEnv(t, api, workerOptions{})
	worker.engine = rebuildEngine(t, api, worker, engineOverrides{partitions: owned})

	stop := worker.start(t)
	defer stop()

	worker.ingest(t, appID, "invoice.paid", map[string]any{"a": 1})

	// The fan-out runs (its partition is owned), but the deliver task sits in
	// the one partition this worker does not hold.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var pending int
		if err := api.Pool.QueryRow(context.Background(),
			`SELECT count(*) FROM delivery_task WHERE kind = $1`, int16(entities.TaskDeliver),
		).Scan(&pending); err != nil {
			t.Fatalf("count deliver tasks: %v", err)
		}
		if pending == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected one unclaimed deliver task, found %d", pending)
		}
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(300 * time.Millisecond)
	if got := len(receiver.requests()); got != 0 {
		t.Errorf("the worker delivered %d requests from a partition it does not own", got)
	}
}
