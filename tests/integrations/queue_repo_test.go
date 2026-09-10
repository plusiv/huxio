package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/plusiv/huxio/configs"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/utils"
)

// allPartitions is the full partition set a single-worker test owns.
func allPartitions() []int16 {
	out := make([]int16, entities.QueuePartitions)
	for i := range out {
		out[i] = int16(i)
	}
	return out
}

func (e *testEnv) enqueueDeliver(t *testing.T, msg *entities.Message, ep *entities.Endpoint, visibleAt time.Time) {
	t.Helper()
	err := e.Queue.Enqueue(context.Background(), []repositories.EnqueueTask{{
		PartitionKey: entities.PartitionKeyFor(ep.ID),
		Pool:         configs.DefaultPool,
		Kind:         entities.TaskDeliver,
		OrgID:        msg.OrgID,
		AppID:        msg.AppID,
		MsgID:        msg.ID,
		MsgCreatedAt: msg.CreatedAt,
		EndpointID:   utils.Ptr(ep.ID),
		VisibleAt:    visibleAt,
	}})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
}

func TestQueueClaimCompleteAndRetry(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	org := env.seedOrg(t, "Queue")
	app := env.seedApp(t, org.ID, "customer", nil)
	ep := env.seedEndpoint(t, app, "https://example.test/hook")
	msg := env.seedMessage(t, app, "invoice.paid")

	env.enqueueDeliver(t, msg, ep, time.Now().UTC())

	req := repositories.ClaimRequest{
		Pool:       configs.DefaultPool,
		Partitions: allPartitions(),
		OwnerID:    "wkr_1",
		LockTTL:    90 * time.Second,
		Limit:      100,
	}

	claimed, err := env.Queue.Claim(ctx, req)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("Claim returned %d tasks, want 1", len(claimed))
	}
	task := claimed[0]
	if task.Kind != entities.TaskDeliver || task.EndpointID == nil || *task.EndpointID != ep.ID {
		t.Fatalf("claimed task = %+v", task)
	}
	if task.LockedBy == nil || *task.LockedBy != "wkr_1" || task.LockedUntil == nil {
		t.Error("a claimed task must carry its lock")
	}
	if task.MsgCreatedAt.UTC() != msg.CreatedAt.UTC() {
		t.Errorf("msg_created_at = %s, want %s", task.MsgCreatedAt, msg.CreatedAt)
	}

	// A locked task is invisible to any other claim.
	if again, err := env.Queue.Claim(ctx, req); err != nil || len(again) != 0 {
		t.Fatalf("second Claim = %d tasks, %v; want none while locked", len(again), err)
	}

	// Retry is a re-enqueue with a future visibility time, not a sleep.
	if err := env.Queue.Retry(ctx, task.ID, 5*time.Minute); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if delayed, err := env.Queue.Claim(ctx, req); err != nil || len(delayed) != 0 {
		t.Fatalf("a delayed task must not be claimable: %d tasks, %v", len(delayed), err)
	}

	// Make it visible again and confirm the attempt counter advanced.
	if _, err := env.Pool.Exec(ctx, `UPDATE delivery_task SET visible_at = now() WHERE id = $1`, task.ID); err != nil {
		t.Fatalf("advance visible_at: %v", err)
	}
	retried, err := env.Queue.Claim(ctx, req)
	if err != nil || len(retried) != 1 {
		t.Fatalf("Claim after retry = %d tasks, %v", len(retried), err)
	}
	if retried[0].Attempt != 1 {
		t.Errorf("Attempt = %d, want 1 after one retry", retried[0].Attempt)
	}

	if err := env.Queue.Complete(ctx, task.ID); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	var remaining int
	if err := env.Pool.QueryRow(ctx, `SELECT count(*) FROM delivery_task`).Scan(&remaining); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	if remaining != 0 {
		t.Errorf("completed tasks must be deleted, %d rows remain", remaining)
	}
}

func TestQueueClaimIgnoresUnownedPartitions(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	org := env.seedOrg(t, "Partitions")
	app := env.seedApp(t, org.ID, "customer", nil)
	ep := env.seedEndpoint(t, app, "https://example.test/hook")
	msg := env.seedMessage(t, app, "invoice.paid")
	env.enqueueDeliver(t, msg, ep, time.Now().UTC())

	owned := entities.PartitionKeyFor(ep.ID)
	notOwned := (owned + 1) % entities.QueuePartitions

	if tasks, err := env.Queue.Claim(ctx, repositories.ClaimRequest{
		Pool: configs.DefaultPool, Partitions: []int16{notOwned}, OwnerID: "wkr_other", LockTTL: time.Minute, Limit: 10,
	}); err != nil || len(tasks) != 0 {
		t.Fatalf("claiming a partition we do not own returned %d tasks, %v", len(tasks), err)
	}

	tasks, err := env.Queue.Claim(ctx, repositories.ClaimRequest{
		Pool: configs.DefaultPool, Partitions: []int16{owned}, OwnerID: "wkr_owner", LockTTL: time.Minute, Limit: 10,
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claiming the owning partition returned %d tasks, %v", len(tasks), err)
	}

	// A claim in another pool sees nothing, which is what makes quarantine work.
	if quarantine, err := env.Queue.Claim(ctx, repositories.ClaimRequest{
		Pool: configs.QuarantinePool, Partitions: allPartitions(), OwnerID: "wkr_q", LockTTL: time.Minute, Limit: 10,
	}); err != nil || len(quarantine) != 0 {
		t.Fatalf("quarantine pool claim returned %d tasks, %v", len(quarantine), err)
	}
}

func TestQueueRescueStuckAndRelease(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	org := env.seedOrg(t, "Rescue")
	app := env.seedApp(t, org.ID, "customer", nil)
	ep := env.seedEndpoint(t, app, "https://example.test/hook")
	msg := env.seedMessage(t, app, "invoice.paid")
	env.enqueueDeliver(t, msg, ep, time.Now().UTC())

	req := repositories.ClaimRequest{
		Pool: configs.DefaultPool, Partitions: allPartitions(), OwnerID: "wkr_dead", LockTTL: time.Minute, Limit: 10,
	}
	claimed, err := env.Queue.Claim(ctx, req)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim = %d tasks, %v", len(claimed), err)
	}

	// Nothing to rescue while the lock is fresh.
	if rescued, err := env.Queue.RescueStuck(ctx); err != nil || rescued != 0 {
		t.Fatalf("RescueStuck rescued %d fresh locks, %v", rescued, err)
	}

	// Simulate the holder dying: the lock expires.
	if _, err := env.Pool.Exec(ctx,
		`UPDATE delivery_task SET locked_until = now() - interval '1 second' WHERE id = $1`, claimed[0].ID,
	); err != nil {
		t.Fatalf("expire lock: %v", err)
	}
	if rescued, err := env.Queue.RescueStuck(ctx); err != nil || rescued != 1 {
		t.Fatalf("RescueStuck rescued %d tasks, %v; want 1", rescued, err)
	}

	again, err := env.Queue.Claim(ctx, req)
	if err != nil || len(again) != 1 {
		t.Fatalf("a rescued task must be claimable again: %d tasks, %v", len(again), err)
	}
	if again[0].Attempt != 0 {
		t.Errorf("Attempt = %d; a rescue must not consume an attempt", again[0].Attempt)
	}

	// Releasing on shutdown also leaves the attempt counter alone.
	if err := env.Queue.Release(ctx, []int64{again[0].ID}); err != nil {
		t.Fatalf("Release: %v", err)
	}
	released, err := env.Queue.Claim(ctx, req)
	if err != nil || len(released) != 1 || released[0].Attempt != 0 {
		t.Fatalf("after Release: %d tasks, attempt %d, %v", len(released), released[0].Attempt, err)
	}
}

func TestQueueStatsReportsLag(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	org := env.seedOrg(t, "Stats")
	app := env.seedApp(t, org.ID, "customer", nil)
	ep := env.seedEndpoint(t, app, "https://example.test/hook")
	msg := env.seedMessage(t, app, "invoice.paid")

	env.enqueueDeliver(t, msg, ep, time.Now().UTC().Add(-30*time.Second))
	env.enqueueDeliver(t, msg, ep, time.Now().UTC().Add(time.Hour))

	stats, err := env.Queue.Stats(ctx, []string{configs.DefaultPool})
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if len(stats) != 1 {
		t.Fatalf("Stats returned %d rows", len(stats))
	}
	got := stats[0]
	if got.Ready != 1 || got.Delayed != 1 || got.Locked != 0 {
		t.Errorf("stats = %+v; want 1 ready, 1 delayed, 0 locked", got)
	}
	if got.OldestAge < 25*time.Second {
		t.Errorf("OldestAge = %s, want roughly 30s", got.OldestAge)
	}

	if _, err := env.Queue.Claim(ctx, repositories.ClaimRequest{
		Pool: configs.DefaultPool, Partitions: allPartitions(), OwnerID: "wkr_1", LockTTL: time.Minute, Limit: 10,
	}); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	locked, err := env.Queue.Stats(ctx, nil)
	if err != nil {
		t.Fatalf("Stats(all pools): %v", err)
	}
	if len(locked) != 1 || locked[0].Locked != 1 || locked[0].Ready != 0 {
		t.Errorf("stats after claim = %+v", locked)
	}
}

func TestIngestTransactionIsAtomic(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	org := env.seedOrg(t, "Atomic")
	app := env.seedApp(t, org.ID, "customer", nil)

	// The message insert and the queue insert must commit together. If they could
	// not, a crash between them would leave a message nobody ever delivers, or a
	// queue row pointing at a message that does not exist.
	failing := env.Store.WithinTransaction(ctx, func(txCtx context.Context) error {
		msg := &entities.Message{
			ID:          "msg_atomic_rollback",
			CreatedAt:   time.Now().UTC(),
			OrgID:       org.ID,
			AppID:       app.ID,
			EventType:   "invoice.paid",
			Payload:     []byte(`{}`),
			PayloadSize: 2,
			ExpiresAt:   time.Now().UTC().AddDate(0, 0, 90),
		}
		if err := env.Messages.CreateMessage(txCtx, msg); err != nil {
			return err
		}
		if err := env.Queue.Enqueue(txCtx, []repositories.EnqueueTask{{
			PartitionKey: entities.PartitionKeyFor(app.ID),
			Pool:         configs.DefaultPool,
			Kind:         entities.TaskFanout,
			OrgID:        org.ID,
			AppID:        app.ID,
			MsgID:        msg.ID,
			MsgCreatedAt: msg.CreatedAt,
		}}); err != nil {
			return err
		}
		return context.Canceled // force a rollback after both writes
	})
	if failing == nil {
		t.Fatal("expected the unit of work to fail")
	}

	var messages, tasks int
	if err := env.Pool.QueryRow(ctx, `SELECT count(*) FROM message`).Scan(&messages); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if err := env.Pool.QueryRow(ctx, `SELECT count(*) FROM delivery_task`).Scan(&tasks); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	if messages != 0 || tasks != 0 {
		t.Fatalf("rollback left %d messages and %d tasks behind", messages, tasks)
	}

	// The committing path leaves exactly one of each.
	committed := env.Store.WithinTransaction(ctx, func(txCtx context.Context) error {
		msg := env.seedMessageTx(t, txCtx, app)
		return env.Queue.Enqueue(txCtx, []repositories.EnqueueTask{{
			PartitionKey: entities.PartitionKeyFor(app.ID),
			Pool:         configs.DefaultPool,
			Kind:         entities.TaskFanout,
			OrgID:        org.ID,
			AppID:        app.ID,
			MsgID:        msg.ID,
			MsgCreatedAt: msg.CreatedAt,
		}})
	})
	if committed != nil {
		t.Fatalf("unit of work failed: %v", committed)
	}
	if err := env.Pool.QueryRow(ctx, `SELECT count(*) FROM message`).Scan(&messages); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if err := env.Pool.QueryRow(ctx, `SELECT count(*) FROM delivery_task`).Scan(&tasks); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	if messages != 1 || tasks != 1 {
		t.Errorf("commit produced %d messages and %d tasks, want one of each", messages, tasks)
	}
}
