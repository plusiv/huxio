// Package worker is the inbound adapter for the worker role: it wires the
// delivery engines to the queue's notifications, registers the process, and
// orders shutdown so nothing durable is lost.
//
// Shutdown order, and why it is this order:
//
//  1. stop claiming        no new work enters this process
//  2. release leases       peers can start taking over immediately
//  3. finish in flight     requests already sent run to completion
//  4. flush attempts       the record of those requests reaches Postgres
//  5. deregister           peers stop counting this worker
//
// Steps 2 and 3 overlap on purpose: rebalancing is slower than draining, so
// starting it first means the handover is nearly done by the time this
// process exits.
package worker

import (
	"context"
	"strconv"
	"time"

	"github.com/plusiv/huxio/internal/application/dispatch"
	"github.com/plusiv/huxio/internal/application/maintenance"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/rotisserie/eris"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

// Pool pairs one pool's delivery engine with the lease manager that decides
// which partitions it may claim from.
type Pool struct {
	Name   string
	Engine *dispatch.Engine
	// LeaseManager is optional: without it the engine works every partition, which
	// is correct for a single worker per pool.
	LeaseManager *dispatch.LeaseManager
}

// Deps are the components the runner owns.
type Deps struct {
	Pools         []Pool
	AttemptWriter *dispatch.AttemptWriter
	// RegistryRepo records this process in worker_registry, which is how peers
	// count each other for the fair-share calculation.
	RegistryRepo repositories.LeaseRepository
	// Notifications delivers queue wakeups. Optional: the fallback poll works
	// without it.
	Notifications repositories.NotificationListener
	// MaintenanceLoop is optional and runs guarded by its own named lease, so
	// only one process in the cluster is actually doing the work.
	MaintenanceLoop *maintenance.Loop
}

// Options configures the runner.
type Options struct {
	WorkerID string
	Pools    []string
	Version  string
	// TaskChannel is the notification channel carrying queue wakeups.
	TaskChannel string
	// HeartbeatInterval refreshes the worker registry row.
	HeartbeatInterval time.Duration
	// LeaseTTL is how long a registry row survives without a heartbeat.
	LeaseTTL time.Duration
	// WakeupBuffer is the depth of the notification channel. A full buffer
	// drops wakeups, which costs latency, not deliveries: the fallback poll
	// still runs.
	WakeupBuffer int
}

// Runner owns the worker role's goroutines.
type Runner struct {
	deps Deps
	opts Options
}

func New(deps Deps, opts Options) (*Runner, error) {
	if len(deps.Pools) == 0 {
		return nil, eris.New("worker: at least one pool is required")
	}
	if deps.AttemptWriter == nil {
		return nil, eris.New("worker: an attempt writer is required")
	}
	if opts.WorkerID == "" {
		return nil, eris.New("worker: a worker id is required")
	}
	if opts.HeartbeatInterval <= 0 {
		opts.HeartbeatInterval = 5 * time.Second
	}
	if opts.LeaseTTL <= opts.HeartbeatInterval {
		opts.LeaseTTL = 3 * opts.HeartbeatInterval
	}
	if opts.WakeupBuffer <= 0 {
		opts.WakeupBuffer = 1024
	}
	return &Runner{deps: deps, opts: opts}, nil
}

// Run starts the worker role and blocks until ctx is cancelled and every
// shutdown step has completed.
func (r *Runner) Run(ctx context.Context) error {
	log := logger.FromContext(ctx).With().Str("worker_id", r.opts.WorkerID).Logger()
	ctx = logger.Context(ctx, log)

	// Registering before claiming anything means peers count this worker in
	// their fair share from the start, rather than briefly over-claiming.
	if err := r.register(ctx); err != nil {
		return err
	}

	// The writer outlives the engines: it is what makes the last attempts
	// durable, so it is stopped only after every delivery has finished.
	writerCtx, stopWriter := context.WithCancel(context.WithoutCancel(ctx))
	writerDone := make(chan error, 1)
	go func() { writerDone <- r.deps.AttemptWriter.Run(writerCtx) }()

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return r.heartbeat(groupCtx) })

	if r.deps.MaintenanceLoop != nil {
		group.Go(func() error { return r.deps.MaintenanceLoop.Run(groupCtx) })
	}

	for _, pool := range r.deps.Pools {
		wakeups := make(chan int16, r.opts.WakeupBuffer)

		if pool.LeaseManager != nil {
			// Releases its leases the moment ctx is cancelled, so peers begin
			// claiming while this worker is still draining.
			group.Go(func() error { return pool.LeaseManager.Run(groupCtx) })
		}
		if r.deps.Notifications != nil {
			group.Go(func() error { return r.listen(groupCtx, wakeups) })
		}
		group.Go(func() error { return pool.Engine.Run(groupCtx, wakeups) })
	}

	runErr := group.Wait()

	// Every delivery has finished, so what is still buffered describes real
	// HTTP requests. Stopping the writer without this flush would lose that
	// record on every single deploy.
	log.Info().Msg("flushing attempt writer")
	stopWriter()
	if err := <-writerDone; err != nil {
		log.Error().Err(err).Msg("attempt writer stopped with an error")
	}

	r.deregister(ctx, log)

	if runErr != nil && !eris.Is(runErr, context.Canceled) {
		return runErr
	}
	return nil
}

func (r *Runner) register(ctx context.Context) error {
	if r.deps.RegistryRepo == nil {
		return nil
	}
	err := r.deps.RegistryRepo.RegisterWorker(ctx, entities.Worker{
		ID:      r.opts.WorkerID,
		Pools:   r.opts.Pools,
		Version: r.opts.Version,
	})
	if err != nil {
		return eris.Wrap(err, "register worker")
	}
	return nil
}

func (r *Runner) deregister(ctx context.Context, log zerolog.Logger) {
	if r.deps.RegistryRepo == nil {
		return
	}
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if err := r.deps.RegistryRepo.DeregisterWorker(shutdownCtx, r.opts.WorkerID); err != nil {
		log.Warn().Err(err).Msg("failed to deregister worker")
	}
}

// heartbeat keeps the registry row alive, which is what lets other workers
// count live peers and work out their fair share of partitions.
func (r *Runner) heartbeat(ctx context.Context) error {
	if r.deps.RegistryRepo == nil {
		<-ctx.Done()
		return nil
	}

	ticker := time.NewTicker(r.opts.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.register(ctx); err != nil {
				logger.FromContext(ctx).Warn().Err(err).Msg("worker heartbeat failed")
			}
		}
	}
}

// listen forwards queue notifications into an engine's wakeup channel.
func (r *Runner) listen(ctx context.Context, wakeups chan<- int16) error {
	if r.opts.TaskChannel == "" {
		<-ctx.Done()
		return nil
	}

	return r.deps.Notifications.Listen(ctx, r.opts.TaskChannel, func(_ context.Context, payload string) {
		partition, err := strconv.Atoi(payload)
		if err != nil {
			return
		}
		select {
		case wakeups <- int16(partition):
		default:
			// The engine is busy; the fallback poll will pick this up.
		}
	})
}
