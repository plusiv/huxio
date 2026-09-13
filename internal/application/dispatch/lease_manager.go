package dispatch

import (
	"context"
	"time"

	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/plusiv/huxio/internal/utils"
	"github.com/rotisserie/eris"
)

// LeaseManagerOptions configures partition ownership.
type LeaseManagerOptions struct {
	WorkerID string
	Pool     string
	// Partitions is the fixed partition count. It must match every other
	// process: it is baked into every routed partition_key.
	Partitions int
	// TTL is how long a lease survives without a heartbeat. A lease older
	// than this is free for anyone to claim.
	TTL time.Duration
	// Heartbeat is how often ownership is refreshed and rebalanced.
	Heartbeat time.Duration
}

// LeaseManager keeps this worker's fair share of queue partitions leased.
// Workers own partitions rather than endpoints: there are 256 fixed
// partitions, and a rebalance is a row update.
type LeaseManager struct {
	leaseRepo       repositories.LeaseRepository
	ownedPartitions *AtomicPartitions
	// ownedGauge reports how many partitions this worker currently holds.
	ownedGauge Gauge
	opts       LeaseManagerOptions
}

// NewLeaseManager builds a manager. The returned PartitionSource is what the
// engine consults on every claim.
func NewLeaseManager(
	leaseRepo repositories.LeaseRepository,
	ownedGauge Gauge,
	opts LeaseManagerOptions,
) (*LeaseManager, error) {
	if leaseRepo == nil {
		return nil, eris.New("dispatch: a lease repository is required")
	}
	if opts.WorkerID == "" {
		return nil, eris.New("dispatch: a worker id is required to lease partitions")
	}
	if opts.Pool == "" {
		opts.Pool = "default"
	}
	if opts.Partitions <= 0 {
		opts.Partitions = entities.QueuePartitions
	}
	if opts.Heartbeat <= 0 {
		opts.Heartbeat = 5 * time.Second
	}
	if opts.TTL <= opts.Heartbeat {
		opts.TTL = 3 * opts.Heartbeat
	}
	return &LeaseManager{
		leaseRepo:       leaseRepo,
		ownedPartitions: NewAtomicPartitions(),
		ownedGauge:      ownedGauge,
		opts:            opts,
	}, nil
}

// Partitions is the source the engine reads on every claim.
func (m *LeaseManager) Partitions() PartitionSource { return m.ownedPartitions }

// Owned reports the partitions currently leased.
func (m *LeaseManager) Owned() []int16 { return m.ownedPartitions.Partitions() }

// Run keeps ownership current until ctx is cancelled, then releases every
// lease so a rebalance starts before this worker finishes draining.
func (m *LeaseManager) Run(ctx context.Context) error {
	log := logger.FromContext(ctx).With().Str("pool", m.opts.Pool).Logger()
	ctx = logger.Context(ctx, log)

	// Claim once before returning, so the engine has partitions to work on
	// immediately rather than after the first tick.
	m.rebalance(ctx)

	ticker := time.NewTicker(m.opts.Heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.release(ctx)
			return nil
		case <-ticker.C:
			m.rebalance(ctx)
		}
	}
}

// rebalance renews what this worker holds, sheds any excess, and claims up to
// its fair share.
func (m *LeaseManager) rebalance(ctx context.Context) {
	log := logger.FromContext(ctx)

	// Renewing first tells us which leases we still hold: a lease lost to
	// another worker mid-flight must stop being claimed from.
	owned := m.ownedPartitions.Partitions()
	if len(owned) > 0 {
		kept, err := m.leaseRepo.HeartbeatPartitions(ctx, m.opts.Pool, m.opts.WorkerID, owned)
		if err != nil {
			log.Warn().Err(err).Msg("partition heartbeat failed")
			return
		}
		if len(kept) != len(owned) {
			log.Info().
				Int("owned", len(owned)).
				Int("kept", len(kept)).
				Msg("lost partition leases to another worker")
		}
		owned = kept
		m.store(owned)
	}

	workers, err := m.leaseRepo.CountLiveWorkers(ctx, m.opts.Pool, m.opts.TTL)
	if err != nil {
		log.Warn().Err(err).Msg("failed to count live workers")
		return
	}
	// This worker may not have registered yet, and dividing by zero is worse
	// than assuming we are alone.
	workers = max(workers, 1)

	share := fairShare(m.opts.Partitions, workers)

	switch {
	case len(owned) > share:
		// New workers joined: give back the excess so they have something to
		// take. Releasing the tail keeps the kept set stable.
		excess := owned[share:]
		if err := m.leaseRepo.ReleasePartitions(ctx, m.opts.Pool, m.opts.WorkerID, excess); err != nil {
			log.Warn().Err(err).Msg("failed to release excess partitions")
			return
		}
		owned = owned[:share]
		m.store(owned)
		log.Info().Int("released", len(excess)).Int("owned", len(owned)).Msg("shed excess partitions")

	case len(owned) < share:
		claimed, err := m.leaseRepo.ClaimPartitions(ctx, m.opts.Pool, m.opts.WorkerID, m.opts.TTL, share)
		if err != nil {
			log.Warn().Err(err).Msg("failed to claim partitions")
			return
		}
		// The claim query prefers partitions this worker already holds, so the
		// result is the full owned set rather than only the new ones.
		if len(claimed) != len(owned) {
			log.Info().
				Int("owned", len(claimed)).
				Int("fair_share", share).
				Int("workers", workers).
				Msg("partition ownership changed")
		}
		m.store(claimed)
	}

	if m.ownedGauge != nil {
		m.ownedGauge.Set(float64(len(m.ownedPartitions.Partitions())))
	}
}

// release gives up every lease immediately. Doing this before drain finishes
// is what lets the rebalance start while this worker is still finishing its
// in-flight deliveries.
func (m *LeaseManager) release(ctx context.Context) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if err := m.leaseRepo.ReleasePartitions(releaseCtx, m.opts.Pool, m.opts.WorkerID, nil); err != nil {
		logger.FromContext(ctx).Warn().Err(err).Msg("failed to release partitions on shutdown")
	}
	m.store(nil)
	if m.ownedGauge != nil {
		m.ownedGauge.Set(0)
	}
}

func (m *LeaseManager) store(partitions []int16) {
	m.ownedPartitions.Store(utils.Unique(partitions))
}

// fairShare is ceil(partitions / workers): with 256 partitions it is close to
// even for every realistic worker count.
func fairShare(partitions, workers int) int {
	if workers <= 0 {
		return partitions
	}
	return (partitions + workers - 1) / workers
}
