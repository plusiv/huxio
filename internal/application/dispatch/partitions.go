package dispatch

import (
	"sync/atomic"

	"github.com/plusiv/huxio/internal/domain/entities"
)

// StaticPartitions owns every partition. It is the v1 single-worker
// configuration: correct for one worker per pool, and replaced by the lease
// manager when several workers share a pool.
type StaticPartitions struct {
	partitions []int16
}

// NewStaticPartitions returns a source owning the full fixed partition set.
func NewStaticPartitions() *StaticPartitions {
	partitions := make([]int16, entities.QueuePartitions)
	for i := range partitions {
		partitions[i] = int16(i)
	}
	return &StaticPartitions{partitions: partitions}
}

// Partitions implements PartitionSource.
func (s *StaticPartitions) Partitions() []int16 { return s.partitions }

// AtomicPartitions holds a partition set that changes as leases are gained and
// lost. Reads are lock-free pointer loads, because the delivery loop consults
// it on every claim.
type AtomicPartitions struct {
	value atomic.Pointer[[]int16]
}

// NewAtomicPartitions returns an empty set.
func NewAtomicPartitions() *AtomicPartitions {
	p := &AtomicPartitions{}
	empty := []int16{}
	p.value.Store(&empty)
	return p
}

// Store replaces the owned set.
func (p *AtomicPartitions) Store(partitions []int16) {
	owned := make([]int16, len(partitions))
	copy(owned, partitions)
	p.value.Store(&owned)
}

// Partitions implements PartitionSource.
func (p *AtomicPartitions) Partitions() []int16 {
	if owned := p.value.Load(); owned != nil {
		return *owned
	}
	return nil
}

var (
	_ PartitionSource = (*StaticPartitions)(nil)
	_ PartitionSource = (*AtomicPartitions)(nil)
)
