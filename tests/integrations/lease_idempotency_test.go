package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/plusiv/huxio/configs"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/utils"
	"github.com/rotisserie/eris"
)

func TestLeaseFairShareBetweenWorkers(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	const ttl = 15 * time.Second

	for _, id := range []string{"wkr_1", "wkr_2"} {
		if err := env.Leases.RegisterWorker(ctx, entities.Worker{
			ID: id, Pools: []string{configs.DefaultPool}, Version: "test",
		}); err != nil {
			t.Fatalf("RegisterWorker(%s): %v", id, err)
		}
	}

	live, err := env.Leases.CountLiveWorkers(ctx, configs.DefaultPool, ttl)
	if err != nil {
		t.Fatalf("CountLiveWorkers: %v", err)
	}
	if live != 2 {
		t.Fatalf("CountLiveWorkers = %d, want 2", live)
	}

	// ceil(256/2) each: the first worker takes its half, the second takes the
	// rest and neither can take a partition the other holds.
	share := (entities.QueuePartitions + live - 1) / live

	first, err := env.Leases.ClaimPartitions(ctx, configs.DefaultPool, "wkr_1", ttl, share)
	if err != nil {
		t.Fatalf("ClaimPartitions(wkr_1): %v", err)
	}
	if len(first) != share {
		t.Fatalf("wkr_1 claimed %d partitions, want %d", len(first), share)
	}

	second, err := env.Leases.ClaimPartitions(ctx, configs.DefaultPool, "wkr_2", ttl, share)
	if err != nil {
		t.Fatalf("ClaimPartitions(wkr_2): %v", err)
	}
	if len(second) != entities.QueuePartitions-share {
		t.Fatalf("wkr_2 claimed %d partitions, want %d", len(second), entities.QueuePartitions-share)
	}

	owned := make(map[int16]string, entities.QueuePartitions)
	for _, p := range first {
		owned[p] = "wkr_1"
	}
	for _, p := range second {
		if prev, dupe := owned[p]; dupe {
			t.Fatalf("partition %d owned by both %s and wkr_2", p, prev)
		}
		owned[p] = "wkr_2"
	}
	if len(owned) != entities.QueuePartitions {
		t.Errorf("%d partitions leased in total, want %d", len(owned), entities.QueuePartitions)
	}

	// Heartbeats keep ownership and report what is still owned.
	kept, err := env.Leases.HeartbeatPartitions(ctx, configs.DefaultPool, "wkr_1", first)
	if err != nil {
		t.Fatalf("HeartbeatPartitions: %v", err)
	}
	if len(kept) != len(first) {
		t.Errorf("heartbeat kept %d of %d partitions", len(kept), len(first))
	}
	// Heartbeating a partition owned by someone else reports it as lost.
	stolen, err := env.Leases.HeartbeatPartitions(ctx, configs.DefaultPool, "wkr_1", second[:1])
	if err != nil {
		t.Fatalf("HeartbeatPartitions(other owner): %v", err)
	}
	if len(stolen) != 0 {
		t.Error("a worker must not renew a lease it does not hold")
	}

	// Releasing a subset is how a worker sheds its excess after a rebalance.
	if err := env.Leases.ReleasePartitions(ctx, configs.DefaultPool, "wkr_1", first[:10]); err != nil {
		t.Fatalf("ReleasePartitions(subset): %v", err)
	}
	regained, err := env.Leases.ClaimPartitions(ctx, configs.DefaultPool, "wkr_2", ttl, 10)
	if err != nil {
		t.Fatalf("ClaimPartitions after release: %v", err)
	}
	if len(regained) == 0 {
		t.Error("released partitions must become claimable")
	}

	// SIGTERM: release everything so the rebalance starts before drain ends.
	if err := env.Leases.ReleasePartitions(ctx, configs.DefaultPool, "wkr_1", nil); err != nil {
		t.Fatalf("ReleasePartitions(all): %v", err)
	}
	leases, err := env.Leases.ListPartitionLeases(ctx, configs.DefaultPool)
	if err != nil {
		t.Fatalf("ListPartitionLeases: %v", err)
	}
	for _, lease := range leases {
		if utils.Deref(lease.OwnerID, "") == "wkr_1" {
			t.Fatalf("partition %d still owned after a full release", lease.PartitionKey)
		}
	}
}

func TestLeaseExpiryLetsAnotherWorkerTakeOver(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	const ttl = 15 * time.Second

	if _, err := env.Leases.ClaimPartitions(ctx, configs.DefaultPool, "wkr_dead", ttl, 5); err != nil {
		t.Fatalf("ClaimPartitions: %v", err)
	}

	// A fresh lease is not claimable by anyone else.
	if taken, err := env.Leases.ClaimPartitions(ctx, configs.DefaultPool, "wkr_new", ttl, 5); err != nil {
		t.Fatalf("ClaimPartitions(fresh): %v", err)
	} else {
		for _, p := range taken {
			if p < 5 {
				t.Fatalf("partition %d stolen from a live owner", p)
			}
		}
	}

	// The owner dies: its heartbeat goes stale and the lease frees up.
	if _, err := env.Pool.Exec(ctx,
		`UPDATE partition_lease SET heartbeat_at = now() - interval '1 hour' WHERE owner_id = 'wkr_dead'`,
	); err != nil {
		t.Fatalf("expire heartbeat: %v", err)
	}
	recovered, err := env.Leases.ClaimPartitions(ctx, configs.DefaultPool, "wkr_new", ttl, 5)
	if err != nil {
		t.Fatalf("ClaimPartitions(expired): %v", err)
	}
	if len(recovered) != 5 {
		t.Errorf("recovered %d expired partitions, want 5", len(recovered))
	}

	// Dead workers are reaped from the registry.
	if err := env.Leases.RegisterWorker(ctx, entities.Worker{
		ID: "wkr_dead", Pools: []string{configs.DefaultPool}, Version: "test",
	}); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	if _, err := env.Pool.Exec(ctx,
		`UPDATE worker_registry SET heartbeat_at = now() - interval '1 hour' WHERE id = 'wkr_dead'`,
	); err != nil {
		t.Fatalf("expire worker heartbeat: %v", err)
	}
	reaped, err := env.Leases.ReapDeadWorkers(ctx, ttl)
	if err != nil {
		t.Fatalf("ReapDeadWorkers: %v", err)
	}
	if reaped != 1 {
		t.Errorf("reaped %d dead workers, want 1", reaped)
	}
}

func TestNamedLeaseIsASingleton(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	const (
		name = "maintenance"
		ttl  = 30 * time.Second
	)

	got, err := env.Leases.AcquireNamedLease(ctx, name, "wkr_1", ttl)
	if err != nil || !got {
		t.Fatalf("first acquire = %v, %v; want true", got, err)
	}
	// The holder renews freely.
	if got, err := env.Leases.AcquireNamedLease(ctx, name, "wkr_1", ttl); err != nil || !got {
		t.Fatalf("renew = %v, %v; want true", got, err)
	}
	// Nobody else gets it while it is live.
	if got, err := env.Leases.AcquireNamedLease(ctx, name, "wkr_2", ttl); err != nil || got {
		t.Fatalf("contended acquire = %v, %v; want false", got, err)
	}

	// After expiry it is up for grabs.
	if _, err := env.Pool.Exec(ctx,
		`UPDATE named_lease SET heartbeat_at = now() - interval '1 hour' WHERE name = $1`, name,
	); err != nil {
		t.Fatalf("expire named lease: %v", err)
	}
	if got, err := env.Leases.AcquireNamedLease(ctx, name, "wkr_2", ttl); err != nil || !got {
		t.Fatalf("acquire after expiry = %v, %v; want true", got, err)
	}

	if err := env.Leases.ReleaseNamedLease(ctx, name, "wkr_2"); err != nil {
		t.Fatalf("ReleaseNamedLease: %v", err)
	}
	if got, err := env.Leases.AcquireNamedLease(ctx, name, "wkr_1", ttl); err != nil || !got {
		t.Fatalf("acquire after release = %v, %v; want true", got, err)
	}
}

func TestIdempotencyLockAndReplay(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	const key = "hash_of_org_method_path_and_client_key"

	// First caller wins the race and handles the request.
	existing, won, err := env.Idempotency.Begin(ctx, key, 30*time.Second)
	if err != nil || !won || existing != nil {
		t.Fatalf("first Begin = (%v, %v, %v); want (nil, true, nil)", existing, won, err)
	}

	// A concurrent duplicate sees the in-progress lock, which becomes a 409.
	inFlight, won, err := env.Idempotency.Begin(ctx, key, 30*time.Second)
	if err != nil {
		t.Fatalf("concurrent Begin: %v", err)
	}
	if won || inFlight == nil || inFlight.Status != entities.IdempotencyInProgress {
		t.Fatalf("concurrent Begin = (%+v, %v)", inFlight, won)
	}

	if err := env.Idempotency.Complete(ctx, key, 202, []byte(`{"id":"msg_1"}`), 12*time.Hour); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// A later duplicate replays the stored response verbatim.
	replayed, won, err := env.Idempotency.Begin(ctx, key, 30*time.Second)
	if err != nil || won {
		t.Fatalf("Begin after Complete = (%v, %v)", won, err)
	}
	if replayed.Status != entities.IdempotencyComplete {
		t.Fatalf("Status = %v, want complete", replayed.Status)
	}
	if string(replayed.Response) != `{"id":"msg_1"}` || utils.Deref(replayed.StatusCode, 0) != 202 {
		t.Errorf("replayed response = %q / %v", replayed.Response, replayed.StatusCode)
	}

	// An expired lock is retakeable, so a crashed request does not wedge the key.
	if _, err := env.Pool.Exec(ctx, `UPDATE idempotency_key SET expires_at = now() - interval '1 second'`); err != nil {
		t.Fatalf("expire idempotency key: %v", err)
	}
	if _, won, err := env.Idempotency.Begin(ctx, key, 30*time.Second); err != nil || !won {
		t.Fatalf("Begin after expiry = (%v, %v); want won", won, err)
	}

	// Abort releases the lock immediately for a failed request.
	if err := env.Idempotency.Abort(ctx, key); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if _, won, err := env.Idempotency.Begin(ctx, key, 30*time.Second); err != nil || !won {
		t.Fatalf("Begin after Abort = (%v, %v); want won", won, err)
	}

	if err := env.Idempotency.Complete(ctx, "unknown-key", 200, nil, time.Hour); !eris.Is(err, repositories.ErrNotFound) {
		t.Errorf("completing an unknown key = %v, want ErrNotFound", err)
	}

	if _, err := env.Idempotency.PurgeExpired(ctx); err != nil {
		t.Fatalf("PurgeExpired: %v", err)
	}
}
