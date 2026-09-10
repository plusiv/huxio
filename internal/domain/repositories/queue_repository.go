package repositories

import (
	"context"
	"time"

	"github.com/plusiv/huxio/internal/domain/entities"
)

// EnqueueTask is one row to insert into the queue.
type EnqueueTask struct {
	PartitionKey int16
	Pool         string
	Kind         entities.TaskKind
	OrgID        string
	AppID        string
	MsgID        string
	MsgCreatedAt time.Time
	EndpointID   *string
	Attempt      int16
	TriggerType  entities.TriggerType
	VisibleAt    time.Time
}

// ClaimRequest describes one batch claim against the queue.
type ClaimRequest struct {
	Pool string
	// Partitions the worker currently owns. Claiming is filtered to these, so
	// two workers never contend for the same row and no worker has to skip
	// rows another one locked.
	Partitions []int16
	OwnerID    string
	// LockTTL must exceed the worker request timeout plus a buffer, or a slow
	// delivery is re-claimed while still in flight.
	LockTTL time.Duration
	// Limit caps the batch. 50-200 keeps it one round trip per batch.
	Limit int
}

// QueueStats is the per-pool queue snapshot used for lag metrics and alerts.
type QueueStats struct {
	Pool      string
	Ready     int64
	Delayed   int64
	Locked    int64
	OldestAge time.Duration
}

// QueueRepository is the port for the work queue. The Postgres implementation
// is the only one in v1; the interface exists so a Redis or NATS backend is a
// config flag rather than a rewrite.
type QueueRepository interface {
	// Claim locks and returns a batch of ready tasks for the owned partitions.
	Claim(ctx context.Context, req ClaimRequest) ([]entities.DeliveryTask, error)
	// Complete removes a finished task.
	Complete(ctx context.Context, id int64) error
	// CompleteMany removes a batch of finished tasks in one round trip.
	CompleteMany(ctx context.Context, ids []int64) error
	// Retry re-enqueues a task with a future visibility time and counts the
	// attempt. Retries are re-enqueues, never in-process sleeps.
	Retry(ctx context.Context, id int64, delay time.Duration) error
	// Defer pushes a task into the future without counting an attempt, for
	// when the worker cannot yet act on it: a config snapshot that has not
	// caught up is not a delivery failure.
	Defer(ctx context.Context, id int64, delay time.Duration) error
	// Release drops a task's lock without consuming an attempt, used when a
	// worker loses its partition lease or is shutting down.
	Release(ctx context.Context, ids []int64) error
	// Enqueue inserts tasks outside any caller-managed transaction.
	Enqueue(ctx context.Context, tasks []EnqueueTask) error
	// Notify wakes workers listening for a partition.
	Notify(ctx context.Context, partitionKey int16) error
	// RescueStuck returns tasks whose holder died back to the queue.
	RescueStuck(ctx context.Context) (int64, error)
	// Stats reports depth and lag per pool for metrics and alerting.
	Stats(ctx context.Context, pools []string) ([]QueueStats, error)
}
