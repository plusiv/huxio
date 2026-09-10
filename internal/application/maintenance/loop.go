// Package maintenance owns the periodic housekeeping: returning stuck tasks to
// the queue, reaping dead workers, creating and dropping time partitions, and
// purging expired idempotency keys.
//
// It runs inside worker processes and is guarded by a named lease so exactly
// one instance is active cluster-wide. It is deliberately not a separate
// deployment: that would make the install story worse for no benefit.
package maintenance

import (
	"context"
	"time"

	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/rotisserie/eris"
)

// LeaseName is the singleton lock this loop holds while it is the leader.
const LeaseName = "maintenance"

// Options configures the loop.
type Options struct {
	// OwnerID identifies this process in the named lease.
	OwnerID string
	// Pools are the queue pools whose depth and lag are reported.
	Pools []string

	// RescueInterval is how often locks held by dead workers are returned.
	RescueInterval time.Duration
	// PartitionInterval is how often time partitions are created and dropped.
	PartitionInterval time.Duration
	// StatsInterval is how often queue depth and lag are sampled.
	StatsInterval time.Duration
	// LeaseTTL is how long the leader lease survives without a heartbeat, and
	// how long a worker registry row survives.
	LeaseTTL time.Duration

	// PartitionsAhead is how many days of partitions to keep ready.
	PartitionsAhead int
	// MessageRetentionDays and AttemptRetentionDays decide which partitions
	// are dropped.
	MessageRetentionDays int
	AttemptRetentionDays int
}

// Metrics is the metric ports the loop reports through.
type Metrics struct {
	QueueLag   func(pool string, seconds float64)
	QueueDepth func(pool, state string, count float64)
}

// Hooks are optional per-tick callbacks, used for lane eviction and anything
// else a worker wants driven by the same timer.
type Hooks struct {
	// OnTick runs on every rescue tick, whether or not this process is the
	// leader: it is for process-local housekeeping.
	OnTick func(ctx context.Context)
}

// Loop is the maintenance runner.
type Loop struct {
	leaseRepo repositories.LeaseRepository
	queueRepo repositories.QueueRepository
	// partitionRepo owns the time-partition lifecycle and the bloat readings.
	partitionRepo   repositories.MaintenanceRepository
	idempotencyRepo repositories.IdempotencyRepository
	metrics         Metrics
	hooks           Hooks
	opts            Options

	leader bool
}

// Deps are the ports the loop needs.
type Deps struct {
	LeaseRepo       repositories.LeaseRepository
	QueueRepo       repositories.QueueRepository
	PartitionRepo   repositories.MaintenanceRepository
	IdempotencyRepo repositories.IdempotencyRepository
	Metrics         Metrics
	Hooks           Hooks
}

func New(deps Deps, opts Options) (*Loop, error) {
	if deps.LeaseRepo == nil || deps.QueueRepo == nil || deps.PartitionRepo == nil {
		return nil, eris.New("maintenance: missing a dependency")
	}
	if opts.OwnerID == "" {
		return nil, eris.New("maintenance: an owner id is required for the leader lease")
	}
	if opts.RescueInterval <= 0 {
		opts.RescueInterval = 30 * time.Second
	}
	if opts.PartitionInterval <= 0 {
		opts.PartitionInterval = time.Hour
	}
	if opts.StatsInterval <= 0 {
		opts.StatsInterval = 15 * time.Second
	}
	if opts.LeaseTTL <= 0 {
		opts.LeaseTTL = 30 * time.Second
	}
	if opts.PartitionsAhead <= 0 {
		opts.PartitionsAhead = 7
	}
	if opts.MessageRetentionDays <= 0 {
		opts.MessageRetentionDays = 90
	}
	if opts.AttemptRetentionDays <= 0 {
		opts.AttemptRetentionDays = 90
	}
	return &Loop{
		leaseRepo:       deps.LeaseRepo,
		queueRepo:       deps.QueueRepo,
		partitionRepo:   deps.PartitionRepo,
		idempotencyRepo: deps.IdempotencyRepo,
		metrics:         deps.Metrics,
		hooks:           deps.Hooks,
		opts:            opts,
	}, nil
}

// Run drives the loop until ctx is cancelled.
func (l *Loop) Run(ctx context.Context) error {
	log := logger.FromContext(ctx).With().Str("owner_id", l.opts.OwnerID).Logger()
	ctx = logger.Context(ctx, log)

	// Partitions must exist before anything is written, so the first pass runs
	// before the timers start.
	if l.acquireLeadership(ctx) {
		l.managePartitions(ctx)
	}

	rescue := time.NewTicker(l.opts.RescueInterval)
	defer rescue.Stop()
	partitions := time.NewTicker(l.opts.PartitionInterval)
	defer partitions.Stop()
	stats := time.NewTicker(l.opts.StatsInterval)
	defer stats.Stop()

	for {
		select {
		case <-ctx.Done():
			l.releaseLeadership(ctx)
			return nil

		case <-rescue.C:
			if l.hooks.OnTick != nil {
				l.hooks.OnTick(ctx)
			}
			if !l.acquireLeadership(ctx) {
				continue
			}
			l.rescueStuckTasks(ctx)
			l.reapDeadWorkers(ctx)
			l.purgeIdempotencyKeys(ctx)

		case <-partitions.C:
			if !l.acquireLeadership(ctx) {
				continue
			}
			l.managePartitions(ctx)

		case <-stats.C:
			// Queue depth and lag are read by every worker, not just the leader: the
			// numbers are the same and a missing leader must not blind the alerts.
			l.reportQueueStats(ctx)
		}
	}
}

// IsLeader reports whether this process currently holds the singleton lease.
func (l *Loop) IsLeader() bool { return l.leader }

// acquireLeadership takes or renews the named lease.
func (l *Loop) acquireLeadership(ctx context.Context) bool {
	acquired, err := l.leaseRepo.AcquireNamedLease(ctx, LeaseName, l.opts.OwnerID, l.opts.LeaseTTL)
	if err != nil {
		logger.FromContext(ctx).Warn().Err(err).Msg("failed to acquire the maintenance lease")
		l.leader = false
		return false
	}
	if acquired && !l.leader {
		logger.FromContext(ctx).Info().Msg("became the maintenance leader")
	}
	if !acquired && l.leader {
		logger.FromContext(ctx).Info().Msg("lost the maintenance leadership")
	}
	l.leader = acquired
	return acquired
}

func (l *Loop) releaseLeadership(ctx context.Context) {
	if !l.leader {
		return
	}
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if err := l.leaseRepo.ReleaseNamedLease(releaseCtx, LeaseName, l.opts.OwnerID); err != nil {
		logger.FromContext(ctx).Warn().Err(err).Msg("failed to release the maintenance lease")
	}
	l.leader = false
}

// rescueStuckTasks returns tasks whose holder died back to the queue. This is
// what makes delivery survive a worker dying mid-flight.
func (l *Loop) rescueStuckTasks(ctx context.Context) {
	rescued, err := l.queueRepo.RescueStuck(ctx)
	if err != nil {
		logger.FromContext(ctx).Error().Err(err).Msg("stuck task rescue failed")
		return
	}
	if rescued > 0 {
		logger.FromContext(ctx).Warn().Int64("tasks", rescued).Msg("returned stuck tasks to the queue")
	}
}

func (l *Loop) reapDeadWorkers(ctx context.Context) {
	reaped, err := l.leaseRepo.ReapDeadWorkers(ctx, l.opts.LeaseTTL)
	if err != nil {
		logger.FromContext(ctx).Warn().Err(err).Msg("failed to reap dead workers")
		return
	}
	if reaped > 0 {
		logger.FromContext(ctx).Info().Int64("workers", reaped).Msg("reaped dead workers")
	}
}

func (l *Loop) purgeIdempotencyKeys(ctx context.Context) {
	if l.idempotencyRepo == nil {
		return
	}
	purged, err := l.idempotencyRepo.PurgeExpired(ctx)
	if err != nil {
		logger.FromContext(ctx).Warn().Err(err).Msg("failed to purge expired idempotency keys")
		return
	}
	if purged > 0 {
		logger.FromContext(ctx).Debug().Int64("keys", purged).Msg("purged expired idempotency keys")
	}
}

// managePartitions keeps the daily partitions ahead of writes and drops the
// ones past retention. Dropping a partition is instant and produces no dead
// tuples, which is the entire reason for partitioning.
func (l *Loop) managePartitions(ctx context.Context) {
	log := logger.FromContext(ctx)

	for _, table := range []struct {
		name      string
		retention int
	}{
		{repositories.TableMessage, l.opts.MessageRetentionDays},
		{repositories.TableDeliveryAttempt, l.opts.AttemptRetentionDays},
	} {
		if _, err := l.partitionRepo.EnsureDailyPartitions(ctx, table.name, l.opts.PartitionsAhead); err != nil {
			// An insert into a missing partition fails outright, so this is the one
			// maintenance failure worth shouting about.
			log.Error().Err(err).Str("table", table.name).Msg("failed to create time partitions")
			continue
		}

		cutoff := time.Now().UTC().AddDate(0, 0, -table.retention)
		dropped, err := l.partitionRepo.DropPartitionsBefore(ctx, table.name, cutoff)
		if err != nil {
			log.Error().Err(err).Str("table", table.name).Msg("failed to drop expired partitions")
			continue
		}
		if len(dropped) > 0 {
			log.Info().
				Str("table", table.name).
				Int("dropped", len(dropped)).
				Time("cutoff", cutoff).
				Msg("dropped expired partitions")
		}
	}
}

// reportQueueStats samples depth and lag per pool. Queue lag is the worker
// autoscaling signal and the first SLO to break.
func (l *Loop) reportQueueStats(ctx context.Context) {
	stats, err := l.queueRepo.Stats(ctx, l.opts.Pools)
	if err != nil {
		logger.FromContext(ctx).Warn().Err(err).Msg("failed to read queue stats")
		return
	}
	if l.metrics.QueueLag == nil && l.metrics.QueueDepth == nil {
		return
	}

	for _, stat := range stats {
		if l.metrics.QueueLag != nil {
			l.metrics.QueueLag(stat.Pool, stat.OldestAge.Seconds())
		}
		if l.metrics.QueueDepth != nil {
			l.metrics.QueueDepth(stat.Pool, "ready", float64(stat.Ready))
			l.metrics.QueueDepth(stat.Pool, "delayed", float64(stat.Delayed))
			l.metrics.QueueDepth(stat.Pool, "locked", float64(stat.Locked))
		}
	}
}
