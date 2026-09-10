package repositories

import (
	"context"
	"time"
)

// The partitioned tables, named here so callers of the port do not have to
// import an adapter to say which table they mean.
const (
	TableMessage         = "message"
	TableDeliveryAttempt = "delivery_attempt"
)

// PartitionSpec identifies one daily time partition of a partitioned table.
type PartitionSpec struct {
	Table string
	Name  string
	Day   time.Time
}

// MaintenanceRepository is the port for the maintenance loop: partition
// lifecycle and queue hygiene. Retention detaches and drops whole partitions
// rather than deleting rows, which is the entire reason for partitioning.
type MaintenanceRepository interface {
	// EnsureDailyPartitions creates the partitions for [today, today+ahead].
	EnsureDailyPartitions(ctx context.Context, table string, ahead int) ([]PartitionSpec, error)
	// DropPartitionsBefore drops whole partitions older than the cutoff.
	DropPartitionsBefore(ctx context.Context, table string, cutoff time.Time) ([]PartitionSpec, error)
	// ListPartitions reports the existing partitions of a table.
	ListPartitions(ctx context.Context, table string) ([]PartitionSpec, error)
	// DeadTupleRatio reports the bloat ratio of a table, the primary failure
	// mode of a Postgres-backed queue.
	DeadTupleRatio(ctx context.Context, table string) (float64, error)
	// Healthy reports whether the connection pool can serve queries.
	Healthy(ctx context.Context) error
}
