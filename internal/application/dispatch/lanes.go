package dispatch

import (
	"sync"
	"time"

	"github.com/plusiv/huxio/internal/application/config"
)

// LaneManagerOptions configures the lane map.
type LaneManagerOptions struct {
	// Defaults are applied to every new lane.
	Defaults LaneOptions
	// IdleEviction drops an unused lane after this duration, unless it has
	// deliveries in flight or its breaker is not closed.
	IdleEviction time.Duration
	// Now lets a test drive the timers forward instead of sleeping.
	Now func() time.Time
}

// LaneManager owns this worker's lanes. Concurrent deliveries to the same
// endpoint share one lane.
type LaneManager struct {
	opts LaneManagerOptions

	mu    sync.RWMutex
	lanes map[string]*Lane
}

// NewLaneManager builds an empty manager.
func NewLaneManager(opts LaneManagerOptions) *LaneManager {
	if opts.IdleEviction <= 0 {
		opts.IdleEviction = 10 * time.Minute
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Defaults.Now == nil {
		opts.Defaults.Now = opts.Now
	}
	if opts.Defaults.Breaker.Now == nil {
		opts.Defaults.Breaker.Now = opts.Now
	}
	return &LaneManager{opts: opts, lanes: make(map[string]*Lane, 256)}
}

// For returns the lane for an endpoint, creating it on first use. An
// endpoint's own rate limit overrides the default.
func (m *LaneManager) For(ep *config.Endpoint) *Lane {
	m.mu.RLock()
	lane, ok := m.lanes[ep.ID]
	m.mu.RUnlock()
	if ok {
		return lane
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// Another goroutine may have created it while the write lock was awaited.
	if lane, ok = m.lanes[ep.ID]; ok {
		return lane
	}

	opts := m.opts.Defaults
	if ep.RateLimit != nil && *ep.RateLimit > 0 {
		opts.RateLimit = *ep.RateLimit
	}
	lane = NewLane(ep.ID, opts)
	m.lanes[ep.ID] = lane
	return lane
}

// EvictIdle removes lanes that have been idle past the threshold and have
// nothing in flight, and returns how many it dropped.
func (m *LaneManager) EvictIdle() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	evicted := 0
	for id, lane := range m.lanes {
		if lane.InFlight() > 0 {
			continue
		}
		// Keep open and half-open breakers so eviction does not reset the
		// cooldown or admit extra recovery probes.
		if lane.Breaker().State() != BreakerClosed {
			continue
		}
		if lane.IdleFor() < m.opts.IdleEviction {
			continue
		}
		delete(m.lanes, id)
		evicted++
	}
	return evicted
}

// LaneStats is a point-in-time view of one lane, for metrics and the admin
// endpoint.
type LaneStats struct {
	EndpointID   string
	Concurrency  int
	InFlight     int
	BreakerState BreakerState
	OpenFor      time.Duration
}

// Stats returns a snapshot of every lane.
func (m *LaneManager) Stats() []LaneStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make([]LaneStats, 0, len(m.lanes))
	for _, lane := range m.lanes {
		stats = append(stats, LaneStats{
			EndpointID:   lane.EndpointID(),
			Concurrency:  lane.Concurrency(),
			InFlight:     lane.InFlight(),
			BreakerState: lane.Breaker().State(),
			OpenFor:      lane.Breaker().OpenFor(),
		})
	}
	return stats
}

// BreakerCounts counts lanes per breaker state without adding an endpoint
// label, keeping the number of metric series bounded.
func (m *LaneManager) BreakerCounts() map[BreakerState]int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	counts := map[BreakerState]int{BreakerClosed: 0, BreakerOpen: 0, BreakerHalfOpen: 0}
	for _, lane := range m.lanes {
		counts[lane.Breaker().State()]++
	}
	return counts
}
