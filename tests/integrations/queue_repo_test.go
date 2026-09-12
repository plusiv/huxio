package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/plusiv/huxio/configs"
	"github.com/plusiv/huxio/internal/adapters/outbound/persistence/postgres"
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

// TestQueueClaimCostDoesNotTrackBacklogDepth pins the shape of the claim plan.
//
// The claim orders by (visible_at, id) across every partition a worker owns.
// If the index it reads leads with partition_key, that order is not available
// and Postgres reads every ready row and sorts it to find the batch, so a
// worker that has fallen behind pays for the whole backlog on the one query
// that has to run before the backlog can shrink. The assertion is on rows
// read, not on wall time, because timing is not stable under -race or on a
// loaded CI box.
func TestQueueClaimCostDoesNotTrackBacklogDepth(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	org := env.seedOrg(t, "Backlog")
	app := env.seedApp(t, org.ID, "customer", nil)

	const (
		backlog = 5000
		limit   = 100
	)

	// One ready task per row, spread over the partition space the way crc32 of
	// an endpoint id spreads real work.
	visible := time.Now().UTC().Add(-time.Second)
	tasks := make([]repositories.EnqueueTask, backlog)
	for i := range tasks {
		endpointID := fmt.Sprintf("ep_backlog_%d", i)
		tasks[i] = repositories.EnqueueTask{
			PartitionKey: entities.PartitionKeyFor(endpointID),
			Pool:         configs.DefaultPool,
			Kind:         entities.TaskDeliver,
			OrgID:        org.ID,
			AppID:        app.ID,
			MsgID:        fmt.Sprintf("msg_backlog_%d", i),
			MsgCreatedAt: visible,
			EndpointID:   utils.Ptr(endpointID),
			VisibleAt:    visible,
		}
	}
	if err := env.Queue.Enqueue(ctx, tasks); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := env.Pool.Exec(ctx, `ANALYZE delivery_task`); err != nil {
		t.Fatalf("ANALYZE: %v", err)
	}

	// EXPLAIN the claim exactly as QueueRepo.Claim issues it. The statement is
	// rolled back so the test leaves no locked rows behind.
	tx, err := env.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var plan []byte
	err = tx.QueryRow(ctx, `EXPLAIN (ANALYZE, FORMAT JSON) `+postgres.ClaimQuery,
		"wkr_1", float64(90), configs.DefaultPool, allPartitions(), limit,
	).Scan(&plan)
	if err != nil {
		t.Fatalf("EXPLAIN the claim: %v", err)
	}

	read := claimScanRows(t, plan)
	// The batch is 100 rows. A plan that stops when the batch is full reads a
	// few hundred; a plan that sorts the backlog reads all 5000 to find them.
	if read > 10*limit {
		t.Errorf("claiming %d tasks from a %d-row backlog read %.0f rows; the claim is scanning the backlog instead of stopping at the batch\nplan: %s",
			limit, backlog, read, plan)
	}
}

// claimScanRows totals the rows the scans feeding the LIMIT produced, which is
// the part of the plan the claim index governs. It deliberately ignores the
// outer UPDATE's own join to delivery_task: the planner picks a hash join or a
// primary-key nested loop there depending on table size, and neither choice
// says anything about whether the claim stops at the batch.
func claimScanRows(t *testing.T, plan []byte) float64 {
	t.Helper()
	var parsed []struct {
		Plan map[string]any `json:"Plan"`
	}
	if err := json.Unmarshal(plan, &parsed); err != nil {
		t.Fatalf("parse the plan: %v", err)
	}
	if len(parsed) == 0 {
		t.Fatal("EXPLAIN returned no plan")
	}

	children := func(node map[string]any) []map[string]any {
		raw, _ := node["Plans"].([]any)
		out := make([]map[string]any, 0, len(raw))
		for _, child := range raw {
			if m, ok := child.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out
	}

	var limitNode map[string]any
	var findLimit func(node map[string]any)
	findLimit = func(node map[string]any) {
		if limitNode != nil {
			return
		}
		if nodeType, _ := node["Node Type"].(string); nodeType == "Limit" {
			limitNode = node
			return
		}
		for _, child := range children(node) {
			findLimit(child)
		}
	}
	findLimit(parsed[0].Plan)
	if limitNode == nil {
		t.Fatalf("no LIMIT node in the claim plan: %s", plan)
	}

	var total float64
	var walk func(node map[string]any)
	walk = func(node map[string]any) {
		if nodeType, _ := node["Node Type"].(string); strings.Contains(nodeType, "Scan") {
			rows, _ := node["Actual Rows"].(float64)
			loops, _ := node["Actual Loops"].(float64)
			total += rows * max(loops, 1)
		}
		for _, child := range children(node) {
			walk(child)
		}
	}
	walk(limitNode)
	return total
}

// TestNotifyWakesWorkersOncePerBatch pins the shape of the wakeup: one
// notification per Notify call, carrying every partition the batch landed in.
// It used to be a pg_notify per partition, so a fan-out to ten endpoints cost
// ten round trips, and each one woke the engine into a claim that found
// nothing, the first having taken the whole batch.
//
// The notify is a statement of its own after the enqueue's commit, not part of
// the transaction. That is not an oversight: Postgres serializes the commit of
// every transaction that has executed NOTIFY, and folding the wakeup into the
// ingest and fan-out transactions measured as ~250 ms COMMITs under load. See
// QueueRepo.Notify.
func TestNotifyWakesWorkersOncePerBatch(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	listener, err := env.Pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire listener connection: %v", err)
	}
	defer listener.Release()
	if _, err := listener.Exec(ctx, `LISTEN `+postgres.TaskNotifyChannel); err != nil {
		t.Fatalf("LISTEN: %v", err)
	}
	// next returns the next notification payload, or "" when none arrives in time.
	next := func(timeout time.Duration) string {
		waitCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		notification, err := listener.Conn().WaitForNotification(waitCtx)
		if err != nil {
			return ""
		}
		return notification.Payload
	}

	// Three tasks in two partitions wake each partition once, in one message.
	tasks := []repositories.EnqueueTask{{PartitionKey: 7}, {PartitionKey: 7}, {PartitionKey: 42}}
	if err := env.Queue.Notify(ctx, repositories.DistinctPartitions(tasks)); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	partitions := strings.Split(next(2*time.Second), ",")
	sort.Strings(partitions)
	if got := strings.Join(partitions, ","); got != "42,7" {
		t.Errorf("notification payload = %q, want the two touched partitions", got)
	}
	if extra := next(300 * time.Millisecond); extra != "" {
		t.Errorf("a second notification %q arrived; one batch must wake exactly once", extra)
	}

	// Nothing to wake is not a round trip.
	if err := env.Queue.Notify(ctx, nil); err != nil {
		t.Fatalf("Notify(nil): %v", err)
	}
	if extra := next(300 * time.Millisecond); extra != "" {
		t.Errorf("an empty batch sent %q", extra)
	}
}

// TestQueueClaimStaysBoundedWhenStatisticsGoStale pins the claim to a plan
// that does not depend on the planner's opinion of the queue table.
//
// A healthy queue is nearly empty most of the time, so autovacuum's statistics
// usually say "one row, many pages" (the pages are the bloat the churn leaves
// behind). The first backlog after that is planned against those numbers. The
// previous claim shape, UPDATE ... FROM (subquery), left the planner a join to
// orient, and under those statistics it scanned every live row and re-ran the
// locking subquery for each one: one claim took 2.8 s and locked 2,800 rows
// for a LIMIT of 100, because every re-run locked a different hundred. The
// capacity ladder showed it as Postgres CPU tripling from one step to the
// next in one iteration out of three.
//
// The test builds exactly that state, then EXPLAINs the claim as a prepared
// statement with the generic plan forced, which is what pgx settles into after
// a handful of executions, and asserts that the lock runs once, the batch is
// the batch, and nothing scans the heap.
func TestQueueClaimStaysBoundedWhenStatisticsGoStale(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	org := env.seedOrg(t, "Stale")
	app := env.seedApp(t, org.ID, "customer", nil)

	const (
		fill    = 3000
		backlog = 3000
		limit   = 100
	)
	visible := time.Now().UTC().Add(-time.Second)
	batch := func(prefix string, n int) []repositories.EnqueueTask {
		tasks := make([]repositories.EnqueueTask, n)
		for i := range tasks {
			endpointID := fmt.Sprintf("ep_%s_%d", prefix, i)
			tasks[i] = repositories.EnqueueTask{
				PartitionKey: entities.PartitionKeyFor(endpointID),
				Pool:         configs.DefaultPool,
				Kind:         entities.TaskDeliver,
				OrgID:        org.ID,
				AppID:        app.ID,
				MsgID:        fmt.Sprintf("msg_%s_%d", prefix, i),
				MsgCreatedAt: visible,
				EndpointID:   utils.Ptr(endpointID),
				VisibleAt:    visible,
			}
		}
		return tasks
	}

	// Churn: fill the heap, then complete everything but the newest row, so the
	// heap keeps its pages while holding one live tuple, and let VACUUM ANALYZE
	// record exactly that.
	if err := env.Queue.Enqueue(ctx, batch("fill", fill)); err != nil {
		t.Fatalf("Enqueue fill: %v", err)
	}
	if _, err := env.Pool.Exec(ctx, `DELETE FROM delivery_task WHERE id < (SELECT max(id) FROM delivery_task)`); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if _, err := env.Pool.Exec(ctx, `VACUUM ANALYZE delivery_task`); err != nil {
		t.Fatalf("VACUUM ANALYZE: %v", err)
	}
	var reltuples float64
	var relpages int
	if err := env.Pool.QueryRow(ctx, `SELECT reltuples, relpages FROM pg_class WHERE relname = 'delivery_task'`).Scan(&reltuples, &relpages); err != nil {
		t.Fatalf("read statistics: %v", err)
	}
	if reltuples > 5 || relpages < 10 {
		t.Fatalf("did not reproduce the stale state: reltuples=%.0f relpages=%d, want ~1 row across many pages", reltuples, relpages)
	}

	// The backlog the statistics know nothing about.
	if err := env.Queue.Enqueue(ctx, batch("backlog", backlog)); err != nil {
		t.Fatalf("Enqueue backlog: %v", err)
	}

	// The real claim, through the repository, must return the batch and no more.
	tasks, err := env.Queue.Claim(ctx, repositories.ClaimRequest{
		Pool: configs.DefaultPool, Partitions: allPartitions(), OwnerID: "wkr_stale", LockTTL: 90 * time.Second, Limit: limit,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(tasks) != limit {
		t.Fatalf("claimed %d tasks for a limit of %d", len(tasks), limit)
	}

	// Now the plan itself, generic, as the statement cache will run it.
	tx, err := env.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL plan_cache_mode = force_generic_plan`); err != nil {
		t.Fatalf("force generic plan: %v", err)
	}
	if _, err := tx.Exec(ctx, `PREPARE claim_probe(text, float8, text, smallint[], int) AS `+postgres.ClaimQuery); err != nil {
		t.Fatalf("PREPARE the claim: %v", err)
	}
	partitions := make([]string, entities.QueuePartitions)
	for i := range partitions {
		partitions[i] = fmt.Sprint(i)
	}
	var plan []byte
	err = tx.QueryRow(ctx, fmt.Sprintf(
		`EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE claim_probe('wkr_probe', 90, '%s', '{%s}', %d)`,
		configs.DefaultPool, strings.Join(partitions, ","), limit,
	)).Scan(&plan)
	if err != nil {
		t.Fatalf("EXPLAIN the claim: %v", err)
	}

	nodes := planNodes(t, plan)
	for _, node := range nodes {
		nodeType, _ := node["Node Type"].(string)
		loops, _ := node["Actual Loops"].(float64)
		if nodeType == "LockRows" && loops != 1 {
			t.Errorf("the locking select ran %.0f times in one claim; it must run once\nplan: %s", loops, plan)
		}
		if nodeType == "Seq Scan" {
			t.Errorf("the claim scanned the heap; it must stay on the indexes whatever the statistics say\nplan: %s", plan)
		}
	}
	if updated, _ := nodes[0]["Actual Rows"].(float64); updated != limit {
		t.Errorf("the claim updated %.0f rows for a limit of %d\nplan: %s", updated, limit, plan)
	}
}

// planNodes flattens an EXPLAIN (FORMAT JSON) plan into its nodes, root first.
func planNodes(t *testing.T, plan []byte) []map[string]any {
	t.Helper()
	var parsed []struct {
		Plan map[string]any `json:"Plan"`
	}
	if err := json.Unmarshal(plan, &parsed); err != nil {
		t.Fatalf("parse the plan: %v", err)
	}
	if len(parsed) == 0 {
		t.Fatal("EXPLAIN returned no plan")
	}
	var nodes []map[string]any
	var walk func(node map[string]any)
	walk = func(node map[string]any) {
		nodes = append(nodes, node)
		raw, _ := node["Plans"].([]any)
		for _, child := range raw {
			if m, ok := child.(map[string]any); ok {
				walk(m)
			}
		}
	}
	walk(parsed[0].Plan)
	return nodes
}
