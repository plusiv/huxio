package entities

import "time"

// PartitionLease records which worker currently owns which queue partition,
// per pool. Per-endpoint state lives in the owning worker's memory.
type PartitionLease struct {
	PartitionKey int16
	Pool         string
	OwnerID      *string
	HeartbeatAt  *time.Time
	CreatedAt    time.Time
}

// Expired reports whether the lease is free for anyone to claim.
func (l PartitionLease) Expired(now time.Time, ttl time.Duration) bool {
	if l.OwnerID == nil || *l.OwnerID == "" || l.HeartbeatAt == nil {
		return true
	}
	return now.Sub(*l.HeartbeatAt) > ttl
}

// NamedLease is a singleton lock for jobs that must run on exactly one process
// across the whole cluster, such as creating and dropping time partitions.
type NamedLease struct {
	Name        string
	OwnerID     string
	HeartbeatAt time.Time
	CreatedAt   time.Time
}

// Worker is a live process and the pools it serves. Workers use the registry
// to compute their fair share of partitions.
type Worker struct {
	ID          string
	Pools       []string
	Version     string
	HeartbeatAt time.Time
	CreatedAt   time.Time
}
