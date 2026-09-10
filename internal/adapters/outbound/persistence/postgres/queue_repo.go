package postgres

import (
	"context"
	"strconv"
	"time"

	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/rotisserie/eris"
)

// TaskNotifyChannel is the LISTEN/NOTIFY channel workers wake on. The payload
// is the partition key, so a worker only wakes for partitions it owns.
const TaskNotifyChannel = "huxio_task"

// QueueRepo is the Postgres implementation of repositories.QueueRepository.
// Keeping the queue in the same database as the message is what removes the
// dual-write failure class entirely.
type QueueRepo struct {
	store *Store
}

// NewQueueRepo builds the repository.
func NewQueueRepo(store *Store) *QueueRepo { return &QueueRepo{store: store} }

// claimQuery locks a batch of ready tasks for the owned partitions and hands
// them back in the same round trip. The shape is worth reading closely:
//
//   - the inner SELECT ... FOR UPDATE SKIP LOCKED is what makes this a queue.
//     Without SKIP LOCKED, two workers claiming at once would block on each
//     other's rows instead of taking different ones.
//   - the UPDATE ... FROM (subquery) marks the rows locked and RETURNINGs
//     them together, so claiming costs one statement, not a select then an
//     update with a race in between.
//   - partition_key = ANY(...) restricts each worker to the partitions it
//     leases. Two workers therefore never look at the same rows, and
//     SKIP LOCKED almost never has to skip anything.
//   - locked_until is a timestamp, not a boolean: a worker that dies holds
//     nothing forever, and the maintenance loop returns the row once the
//     lease has passed.
const claimQuery = `
	UPDATE delivery_task t
	SET    locked_by = $1,
	       locked_until = now() + ($2 * interval '1 second')
	FROM (
	    SELECT id
	    FROM   delivery_task
	    WHERE  pool = $3
	      AND  partition_key = ANY($4::smallint[])
	      AND  visible_at <= now()
	      AND  locked_until IS NULL
	      AND  deleted_at IS NULL
	    ORDER  BY visible_at, id
	    LIMIT  $5
	    FOR UPDATE SKIP LOCKED
	) s
	WHERE t.id = s.id
	RETURNING t.id, t.partition_key, t.pool, t.kind, t.org_id, t.app_id, t.msg_id,
	          t.msg_created_at, t.endpoint_id, t.attempt, t.trigger_type, t.visible_at,
	          t.locked_by, t.locked_until, t.created_at`

// Claim locks and returns up to req.Limit ready tasks.
func (r *QueueRepo) Claim(ctx context.Context, req repositories.ClaimRequest) ([]entities.DeliveryTask, error) {
	if len(req.Partitions) == 0 || req.Limit <= 0 {
		return nil, nil
	}
	rows, err := r.store.Querier(ctx).Query(ctx, claimQuery,
		req.OwnerID, req.LockTTL.Seconds(), req.Pool, req.Partitions, req.Limit,
	)
	if err != nil {
		return nil, eris.Wrap(err, "claim tasks")
	}
	defer rows.Close()

	tasks := make([]entities.DeliveryTask, 0, req.Limit)
	for rows.Next() {
		var (
			task entities.DeliveryTask
			kind int16
			trig int16
		)
		if err := rows.Scan(
			&task.ID, &task.PartitionKey, &task.Pool, &kind, &task.OrgID, &task.AppID,
			&task.MsgID, &task.MsgCreatedAt, &task.EndpointID, &task.Attempt, &trig,
			&task.VisibleAt, &task.LockedBy, &task.LockedUntil, &task.CreatedAt,
		); err != nil {
			return nil, eris.Wrap(err, "scan claimed task")
		}
		task.Kind = entities.TaskKind(kind)
		task.TriggerType = entities.TriggerType(trig)
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "iterate claimed tasks")
	}
	return tasks, nil
}

// Complete removes a finished task. The queue is the one table whose rows are
// physically deleted: it must drain rather than accumulate.
func (r *QueueRepo) Complete(ctx context.Context, id int64) error {
	if _, err := r.store.Querier(ctx).Exec(ctx, `DELETE FROM delivery_task WHERE id = $1`, id); err != nil {
		return eris.Wrap(err, "complete task")
	}
	return nil
}

// CompleteMany removes a batch of finished tasks in one round trip.
func (r *QueueRepo) CompleteMany(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	if _, err := r.store.Querier(ctx).Exec(ctx, `DELETE FROM delivery_task WHERE id = ANY($1::bigint[])`, ids); err != nil {
		return eris.Wrap(err, "complete tasks")
	}
	return nil
}

// Retry re-enqueues a task with a future visibility time, as a single
// statement rather than delete-then-insert. No delivery path ever sleeps.
func (r *QueueRepo) Retry(ctx context.Context, id int64, delay time.Duration) error {
	const query = `
		UPDATE delivery_task
		SET    attempt      = attempt + 1,
		       visible_at   = now() + ($2 * interval '1 millisecond'),
		       locked_by    = NULL,
		       locked_until = NULL
		WHERE  id = $1`
	if _, err := r.store.Querier(ctx).Exec(ctx, query, id, delay.Milliseconds()); err != nil {
		return eris.Wrap(err, "retry task")
	}
	return nil
}

// Defer pushes a task into the future without counting an attempt. It is used
// when a worker cannot decide the task yet, such as a fan-out whose config
// snapshot predates the message it would expand.
func (r *QueueRepo) Defer(ctx context.Context, id int64, delay time.Duration) error {
	const query = `
		UPDATE delivery_task
		SET    visible_at   = now() + ($2 * interval '1 millisecond'),
		       locked_by    = NULL,
		       locked_until = NULL
		WHERE  id = $1`
	if _, err := r.store.Querier(ctx).Exec(ctx, query, id, delay.Milliseconds()); err != nil {
		return eris.Wrap(err, "defer task")
	}
	return nil
}

// Release drops locks without consuming an attempt, used when a worker loses a
// partition lease or shuts down with tasks still claimed.
func (r *QueueRepo) Release(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	const query = `
		UPDATE delivery_task
		SET    locked_by = NULL, locked_until = NULL
		WHERE  id = ANY($1::bigint[])`
	if _, err := r.store.Querier(ctx).Exec(ctx, query, ids); err != nil {
		return eris.Wrap(err, "release tasks")
	}
	return nil
}

// Enqueue inserts tasks in a single round trip. When called with a context
// carrying a transaction it joins it, which is how the message insert and the
// queue insert commit together.
func (r *QueueRepo) Enqueue(ctx context.Context, tasks []repositories.EnqueueTask) error {
	if len(tasks) == 0 {
		return nil
	}

	partitionKeys := make([]int16, len(tasks))
	pools := make([]string, len(tasks))
	kinds := make([]int16, len(tasks))
	orgIDs := make([]string, len(tasks))
	appIDs := make([]string, len(tasks))
	msgIDs := make([]string, len(tasks))
	msgCreatedAt := make([]time.Time, len(tasks))
	endpointIDs := make([]*string, len(tasks))
	attempts := make([]int16, len(tasks))
	triggers := make([]int16, len(tasks))
	visibleAt := make([]time.Time, len(tasks))

	for i, t := range tasks {
		partitionKeys[i] = t.PartitionKey
		pools[i] = t.Pool
		kinds[i] = int16(t.Kind)
		orgIDs[i] = t.OrgID
		appIDs[i] = t.AppID
		msgIDs[i] = t.MsgID
		msgCreatedAt[i] = t.MsgCreatedAt
		endpointIDs[i] = t.EndpointID
		attempts[i] = t.Attempt
		triggers[i] = int16(t.TriggerType)
		if t.VisibleAt.IsZero() {
			visibleAt[i] = time.Now().UTC()
		} else {
			visibleAt[i] = t.VisibleAt
		}
	}

	const query = `
		INSERT INTO delivery_task (
			partition_key, pool, kind, org_id, app_id, msg_id, msg_created_at,
			endpoint_id, attempt, trigger_type, visible_at
		)
		SELECT * FROM unnest(
			$1::smallint[], $2::text[], $3::smallint[], $4::text[], $5::text[], $6::text[],
			$7::timestamptz[], $8::text[], $9::smallint[], $10::smallint[], $11::timestamptz[]
		)`
	_, err := r.store.Querier(ctx).Exec(ctx, query,
		partitionKeys, pools, kinds, orgIDs, appIDs, msgIDs, msgCreatedAt,
		endpointIDs, attempts, triggers, visibleAt,
	)
	if err != nil {
		return eris.Wrap(err, "enqueue tasks")
	}
	return nil
}

// Notify wakes workers listening for a partition. Workers keep a fallback poll
// as well, because notifications are lost on connection loss.
func (r *QueueRepo) Notify(ctx context.Context, partitionKey int16) error {
	const query = `SELECT pg_notify($1, $2)`
	if _, err := r.store.Querier(ctx).Exec(ctx, query, TaskNotifyChannel, strconv.Itoa(int(partitionKey))); err != nil {
		return eris.Wrap(err, "notify task")
	}
	return nil
}

// RescueStuck returns tasks whose holder died back to the queue. This is what
// makes delivery survive a worker dying mid-flight.
func (r *QueueRepo) RescueStuck(ctx context.Context) (int64, error) {
	const query = `
		UPDATE delivery_task
		SET    locked_by = NULL, locked_until = NULL
		WHERE  locked_until < now()`
	tag, err := r.store.Querier(ctx).Exec(ctx, query)
	if err != nil {
		return 0, eris.Wrap(err, "rescue stuck tasks")
	}
	return tag.RowsAffected(), nil
}

// Stats reports depth and lag per pool.
func (r *QueueRepo) Stats(ctx context.Context, pools []string) ([]repositories.QueueStats, error) {
	const query = `
		SELECT pool,
		       count(*) FILTER (WHERE locked_until IS NULL AND visible_at <= now()) AS ready,
		       count(*) FILTER (WHERE locked_until IS NULL AND visible_at >  now()) AS delayed,
		       count(*) FILTER (WHERE locked_until IS NOT NULL)                     AS locked,
		       COALESCE(
		           EXTRACT(EPOCH FROM (now() - min(visible_at) FILTER (
		               WHERE locked_until IS NULL AND visible_at <= now()
		           ))), 0
		       ) AS oldest_seconds
		FROM   delivery_task
		WHERE  ($1::text[] IS NULL OR pool = ANY($1::text[]))
		  AND  deleted_at IS NULL
		GROUP  BY pool`

	rows, err := r.store.Querier(ctx).Query(ctx, query, nilIfEmpty(pools))
	if err != nil {
		return nil, eris.Wrap(err, "queue stats")
	}
	defer rows.Close()

	stats := make([]repositories.QueueStats, 0, len(pools))
	for rows.Next() {
		var (
			s             repositories.QueueStats
			oldestSeconds float64
		)
		if err := rows.Scan(&s.Pool, &s.Ready, &s.Delayed, &s.Locked, &oldestSeconds); err != nil {
			return nil, eris.Wrap(err, "scan queue stats")
		}
		s.OldestAge = time.Duration(oldestSeconds * float64(time.Second))
		stats = append(stats, s)
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "iterate queue stats")
	}
	return stats, nil
}
