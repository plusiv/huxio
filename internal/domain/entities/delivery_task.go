package entities

import (
	"hash/crc32"
	"time"
)

// TaskKind distinguishes the two units of queued work. The numeric values are
// persisted, so they may never be renumbered.
type TaskKind int16

// Queue task kinds.
const (
	// TaskFanout expands one message into one deliver task per matching endpoint.
	// Fan-out costs exactly one enqueue at ingest.
	TaskFanout TaskKind = 0
	// TaskDeliver sends one message to one endpoint.
	TaskDeliver TaskKind = 1
)

// QueuePartitions is the fixed partition count the queue is split into. It is
// deliberately far larger than any realistic worker count so
// ceil(partitions/workers) is close to even for every N.
const QueuePartitions = 256

// DeliveryTask is one row of the work queue. Unlike every other entity, these
// rows are physically deleted as work completes, because the queue must drain
// rather than accumulate.
type DeliveryTask struct {
	ID           int64
	PartitionKey int16
	Pool         string
	Kind         TaskKind
	OrgID        string
	AppID        string
	MsgID        string
	MsgCreatedAt time.Time
	EndpointID   *string
	Attempt      int16
	TriggerType  TriggerType
	VisibleAt    time.Time
	LockedBy     *string
	LockedUntil  *time.Time
	CreatedAt    time.Time
}

// PartitionKeyFor routes an id to its queue partition. Delivery tasks route on
// endpoint id and fan-out tasks on application id, so every task for one
// endpoint lands on one worker and its rate limiter, breaker and in-flight
// counters can live in that worker's memory.
func PartitionKeyFor(id string) int16 {
	return int16(crc32.ChecksumIEEE([]byte(id)) % QueuePartitions)
}
