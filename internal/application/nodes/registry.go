// Package nodes keeps a process registered among its peers and reports how
// many are live. API nodes are stateless and load-balanced, so a per-tenant
// rate limit has to be enforced as limit/node_count on each of them.
// Approximate is fine: this is abuse protection, not billing.
package nodes

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/rotisserie/eris"
)

// PoolAPI is the pseudo-pool API nodes register under, so they can count each
// other without a second table.
const PoolAPI = "api"

// Options configures the registry.
type Options struct {
	NodeID  string
	Pool    string
	Version string
	// Heartbeat is how often the registry row is refreshed.
	Heartbeat time.Duration
	// TTL is how long a row survives without a heartbeat.
	TTL time.Duration
}

// Registry registers this process and caches the live peer count.
type Registry struct {
	leaseRepo repositories.LeaseRepository
	opts      Options

	count atomic.Int64
}

// New builds a registry that reports one node until the first count lands.
func New(leaseRepo repositories.LeaseRepository, opts Options) (*Registry, error) {
	if leaseRepo == nil {
		return nil, eris.New("nodes: a lease repository is required")
	}
	if opts.NodeID == "" {
		return nil, eris.New("nodes: a node id is required")
	}
	if opts.Pool == "" {
		opts.Pool = PoolAPI
	}
	if opts.Heartbeat <= 0 {
		opts.Heartbeat = 5 * time.Second
	}
	if opts.TTL <= opts.Heartbeat {
		opts.TTL = 3 * opts.Heartbeat
	}

	registry := &Registry{leaseRepo: leaseRepo, opts: opts}
	registry.count.Store(1)
	return registry, nil
}

// Nodes reports the live peer count. It never returns less than one, because
// dividing a limit by zero is worse than assuming we are alone.
func (r *Registry) Nodes() int {
	if count := r.count.Load(); count > 0 {
		return int(count)
	}
	return 1
}

// Run registers this process and refreshes both the row and the count until
// ctx is cancelled, then deregisters so peers stop dividing by a ghost.
func (r *Registry) Run(ctx context.Context) error {
	r.refresh(ctx)

	ticker := time.NewTicker(r.opts.Heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.deregister(ctx)
			return nil
		case <-ticker.C:
			r.refresh(ctx)
		}
	}
}

func (r *Registry) refresh(ctx context.Context) {
	log := logger.FromContext(ctx)

	err := r.leaseRepo.RegisterWorker(ctx, entities.Worker{
		ID:      r.opts.NodeID,
		Pools:   []string{r.opts.Pool},
		Version: r.opts.Version,
	})
	if err != nil {
		log.Warn().Err(err).Msg("failed to register this node")
		return
	}

	count, err := r.leaseRepo.CountLiveWorkers(ctx, r.opts.Pool, r.opts.TTL)
	if err != nil {
		log.Warn().Err(err).Msg("failed to count peer nodes")
		return
	}
	r.count.Store(int64(max(count, 1)))
}

func (r *Registry) deregister(ctx context.Context) {
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if err := r.leaseRepo.DeregisterWorker(shutdownCtx, r.opts.NodeID); err != nil {
		logger.FromContext(ctx).Warn().Err(err).Msg("failed to deregister this node")
	}
}
