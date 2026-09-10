package repositories

import (
	"context"
	"time"

	"github.com/plusiv/huxio/internal/domain/entities"
)

// LeaseRepository is the port for partition ownership, singleton locks and the
// worker registry: the coordination that lets per-endpoint state live in one
// worker's memory.
type LeaseRepository interface {
	// RegisterWorker upserts this process's registry row and heartbeat.
	RegisterWorker(ctx context.Context, worker entities.Worker) error
	// DeregisterWorker removes this process's registry row on shutdown.
	DeregisterWorker(ctx context.Context, id string) error
	// CountLiveWorkers counts workers serving a pool with a fresh heartbeat,
	// which is the denominator of the fair share calculation.
	CountLiveWorkers(ctx context.Context, pool string, ttl time.Duration) (int, error)
	// ReapDeadWorkers deletes registry rows whose heartbeat has expired.
	ReapDeadWorkers(ctx context.Context, ttl time.Duration) (int64, error)

	// ClaimPartitions takes ownership of up to limit free or expired leases in
	// a pool and returns the partitions now owned by ownerID.
	ClaimPartitions(ctx context.Context, pool, ownerID string, ttl time.Duration, limit int) ([]int16, error)
	// HeartbeatPartitions refreshes ownership of the supplied partitions and
	// returns the subset still owned, so a worker that lost a lease mid-flight
	// learns about it.
	HeartbeatPartitions(ctx context.Context, pool, ownerID string, partitions []int16) ([]int16, error)
	// ReleasePartitions gives up ownership immediately, so a rebalance starts
	// before drain finishes on SIGTERM. A nil partition list releases every
	// lease this worker holds in the pool; a non-nil one releases just those,
	// which is how a worker sheds its excess after new workers join.
	ReleasePartitions(ctx context.Context, pool, ownerID string, partitions []int16) error
	// ListPartitionLeases returns the lease table for debugging and the admin
	// endpoint.
	ListPartitionLeases(ctx context.Context, pool string) ([]entities.PartitionLease, error)

	// AcquireNamedLease takes or renews a singleton lock. It reports false
	// when another live owner holds it.
	AcquireNamedLease(ctx context.Context, name, ownerID string, ttl time.Duration) (bool, error)
	// ReleaseNamedLease gives up a singleton lock.
	ReleaseNamedLease(ctx context.Context, name, ownerID string) error
}
