package config

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/plusiv/huxio/internal/infrastructure/secrets"
	"github.com/rotisserie/eris"
)

// Gauge is the metric port the manager reports snapshot age through. It is
// satisfied by a Prometheus gauge, and by nothing else the application layer
// needs to know about.
type Gauge interface {
	Set(float64)
}

// ManagerOptions configures the snapshot manager.
type ManagerOptions struct {
	// Channel is the notification channel carrying invalidations.
	Channel string
	// RefreshInterval is the unconditional full refresh, the backstop for a
	// LISTEN that died silently on connection loss.
	RefreshInterval time.Duration
	// Debounce coalesces a burst of notifications into one reload.
	Debounce time.Duration
}

// Manager keeps a Snapshot current behind an atomic pointer. Reads are
// lock-free pointer loads; there is no per-delivery cache lookup and no Redis.
type Manager struct {
	snapshotRepo repositories.SnapshotRepository
	// notifications delivers the invalidations that trigger a reload.
	notifications repositories.NotificationListener
	sealer        *secrets.Sealer
	age           Gauge
	opts          ManagerOptions

	current atomic.Pointer[Snapshot]
	reload  chan struct{}
}

// DefaultChannel is the notification channel used for config invalidation.
const DefaultChannel = "huxio_config"

// NewManager builds a manager. Call Load before serving traffic and Run to
// keep the snapshot current.
func NewManager(
	snapshotRepo repositories.SnapshotRepository,
	notifications repositories.NotificationListener,
	sealer *secrets.Sealer,
	snapshotAge Gauge,
	opts ManagerOptions,
) *Manager {
	if opts.Channel == "" {
		opts.Channel = DefaultChannel
	}
	if opts.RefreshInterval <= 0 {
		opts.RefreshInterval = 60 * time.Second
	}
	if opts.Debounce <= 0 {
		opts.Debounce = 100 * time.Millisecond
	}
	return &Manager{
		snapshotRepo:  snapshotRepo,
		notifications: notifications,
		sealer:        sealer,
		age:           snapshotAge,
		opts:          opts,
		reload:        make(chan struct{}, 1),
	}
}

// Current returns the live snapshot. It is a pointer load, safe from any
// goroutine, and never nil after a successful Load.
func (m *Manager) Current() *Snapshot { return m.current.Load() }

// Load rebuilds the snapshot from the database and swaps it in.
func (m *Manager) Load(ctx context.Context) error {
	data, err := m.snapshotRepo.LoadSnapshot(ctx)
	if err != nil {
		return eris.Wrap(err, "load config snapshot")
	}

	snapshot, problems := BuildSnapshot(data, m.sealer)
	log := logger.FromContext(ctx)
	for _, problem := range problems {
		// One unreadable secret must not take the whole snapshot down: that
		// would stop delivery for every other tenant too.
		log.Error().Err(problem).Msg("skipping endpoint in config snapshot")
	}

	m.current.Store(snapshot)
	if m.age != nil {
		m.age.Set(0)
	}

	orgs, apps, endpoints, eventTypes := snapshot.Counts()
	log.Info().
		Int("organizations", orgs).
		Int("applications", apps).
		Int("endpoints", endpoints).
		Int("event_types", eventTypes).
		Int("skipped_endpoints", len(problems)).
		Msg("config snapshot loaded")
	return nil
}

// Invalidate asks for a reload without blocking the caller.
func (m *Manager) Invalidate() {
	select {
	case m.reload <- struct{}{}:
	default: // a reload is already pending; coalesce into it
	}
}

// Run keeps the snapshot current until ctx is cancelled: it reloads on
// notification, on the refresh interval, and on demand.
func (m *Manager) Run(ctx context.Context) error {
	go func() {
		if err := m.notifications.Listen(ctx, m.opts.Channel, func(_ context.Context, _ string) {
			m.Invalidate()
		}); err != nil {
			logger.FromContext(ctx).Error().Err(err).Msg("config listener stopped")
		}
	}()

	refresh := time.NewTicker(m.opts.RefreshInterval)
	defer refresh.Stop()
	ageTick := time.NewTicker(time.Second)
	defer ageTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ageTick.C:
			if m.age != nil {
				if snapshot := m.Current(); snapshot != nil {
					m.age.Set(snapshot.Age().Seconds())
				}
			}

		case <-refresh.C:
			m.reloadNow(ctx)

		case <-m.reload:
			// Coalesce a burst of notifications: a bulk endpoint import should
			// cost one reload, not one per row.
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(m.opts.Debounce):
			}
			drain(m.reload)
			m.reloadNow(ctx)
		}
	}
}

func (m *Manager) reloadNow(ctx context.Context) {
	if err := m.Load(ctx); err != nil {
		// The previous snapshot stays in place: stale config is far better than no
		// config, and the age metric will expose the staleness.
		logger.FromContext(ctx).Error().Err(err).Msg("config snapshot refresh failed")
	}
}

func drain(ch chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}
