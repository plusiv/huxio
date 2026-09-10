// Package dispatch is the delivery engine: the worker loop, fan-out, lanes,
// breakers and the code that turns a queued task into an HTTP request. It
// takes interfaces for the queue, the store and the HTTP client, so the whole
// engine is testable with no Postgres and no network. It must stay free of
// database types.
package dispatch

import (
	"context"
	"time"

	"github.com/plusiv/huxio/internal/application/config"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
)

// DeliveryRequest is one outbound webhook.
type DeliveryRequest struct {
	URL     string
	Body    []byte
	Headers map[string]string
	// Timeout bounds this request only. It is applied with a per-request
	// context rather than on the shared client.
	Timeout time.Duration
}

// DeliveryResult is what came back, reduced to what the engine records and
// decides on.
type DeliveryResult struct {
	// StatusCode is the HTTP status, or zero when no response arrived: DNS
	// failure, refused connection, timeout.
	StatusCode int
	// Body is the truncated response body, for the portal's delivery log.
	Body string
	// Duration is the wall-clock time of the request.
	Duration time.Duration
	// Err is the transport error, if any. A non-2xx status is not an error.
	Err     error
	Timeout bool
	// Refused reports a refused connection, a DNS failure or a blocked
	// destination: signals that an endpoint is not merely slow but absent, and
	// that the lane should drop straight to one in flight.
	Refused bool
	// ConnectionReused reports whether a warm connection was used, which is
	// the ratio to watch for handshake cost.
	ConnectionReused bool
	// RetryAfter is the endpoint's requested delay, when it sent a usable one.
	RetryAfter *time.Duration
}

// Succeeded reports whether the endpoint accepted the delivery. Only 2xx
// counts: a 3xx is recorded as-is because redirects are deliberately not
// followed.
func (r DeliveryResult) Succeeded() bool {
	return r.Err == nil && r.StatusCode >= 200 && r.StatusCode < 300
}

// DeliveryClient performs one outbound webhook request.
type DeliveryClient interface {
	Deliver(ctx context.Context, req DeliveryRequest) DeliveryResult
}

// MessageStore loads messages for delivery. Both key columns are always
// passed so Postgres prunes to the single time partition holding the row.
type MessageStore interface {
	LoadMessage(ctx context.Context, id string, createdAt time.Time) (*entities.Message, error)
	LoadPayload(ctx context.Context, id string, createdAt time.Time) ([]byte, error)
}

// PartitionSource reports which queue partitions this worker currently owns.
// In v1 a worker owns every partition; the lease manager replaces this with a
// fair share once partition ownership lands.
type PartitionSource interface {
	Partitions() []int16
}

// PayloadDecoder expands a stored payload into the bytes written to the socket.
type PayloadDecoder interface {
	Decode(stored []byte) ([]byte, error)
}

// TaskAction is what happens to the queue row once the attempt that produced
// it has been durably written.
type TaskAction int

const (
	// TaskComplete removes the queue row: delivered, or out of attempts.
	TaskComplete TaskAction = iota
	// TaskRetry re-enqueues the row with a future visibility time.
	TaskRetry
)

// TaskOutcome pairs a queue row with what should happen to it.
type TaskOutcome struct {
	TaskID int64
	Action TaskAction
	Delay  time.Duration
}

// AttemptRecord is one attempt to record, together with the fate of the queue
// row that produced it. The two travel together because the row may only be
// removed after the flush that contains its attempt: otherwise a crash inside
// the flush window loses both the delivery and its record.
type AttemptRecord struct {
	Attempt entities.DeliveryAttempt
	Task    *TaskOutcome
}

// AttemptSink accepts attempt records. It buffers and flushes in batches; the
// engine never writes one row per delivery.
type AttemptSink interface {
	// Enqueue blocks when the buffer is full, which is the correct backpressure:
	// a full buffer means Postgres is the bottleneck.
	Enqueue(ctx context.Context, record AttemptRecord) error
}

// EndpointStateWriter records endpoint health transitions that outlive a
// single worker: the failure-run start, auto-disable, and pool moves.
type EndpointStateWriter interface {
	SetFirstFailure(ctx context.Context, id string, at *time.Time) error
	SetDisabled(ctx context.Context, id string, at *time.Time) error
	SetPool(ctx context.Context, id, pool string) error
}

// SnapshotProvider hands out the current configuration snapshot.
type SnapshotProvider interface {
	Current() *config.Snapshot
}

// SnapshotInvalidator asks the local snapshot to reload now rather than at the
// next scheduled refresh.
type SnapshotInvalidator interface {
	Invalidate()
}

// The operational event types.
const (
	EventAttemptFailing   = "message.attempt.failing"
	EventAttemptRecovered = "message.attempt.recovered"
	EventAttemptExhausted = "message.attempt.exhausted"
	EventEndpointDisabled = "endpoint.disabled"
)

// OperationalEvent is a notification about the delivery system itself.
type OperationalEvent struct {
	OrgID      string
	Type       string
	AppID      string
	EndpointID string
	MsgID      string
	Detail     map[string]any
}

// OperationalEmitter publishes operational events. They go through the normal
// ingest path, so they retry, sign and appear in the delivery log like any
// other message; there is deliberately no second delivery mechanism.
type OperationalEmitter interface {
	Emit(ctx context.Context, event OperationalEvent) error
}

// Queue is the subset of the queue port the engine uses.
type Queue interface {
	Claim(ctx context.Context, req repositories.ClaimRequest) ([]entities.DeliveryTask, error)
	Complete(ctx context.Context, id int64) error
	Retry(ctx context.Context, id int64, delay time.Duration) error
	Defer(ctx context.Context, id int64, delay time.Duration) error
	Release(ctx context.Context, ids []int64) error
	Enqueue(ctx context.Context, tasks []repositories.EnqueueTask) error
	Notify(ctx context.Context, partitionKey int16) error
}

// TxManager runs a unit of work inside one transaction, so a fan-out cannot
// half-expand.
type TxManager interface {
	WithinTransaction(ctx context.Context, fn func(ctx context.Context) error) error
}
