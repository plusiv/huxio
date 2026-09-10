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
	// IdleEviction drops a lane that has not been used for this long. The
	// cold-start cost of recreating one is a single allocation.
	IdleEviction time.Duration
	// Now lets a test drive the timers forward instead of sleeping.
	Now func() time.Time
}

// LaneManager owns this worker's lanes. Lanes live in a map guarded by an
// RWMutex, so the common case (an existing lane) is a read lock and a map
// lookup, with no allocation and no I/O.
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

// Get returns an existing lane, or nil.
func (m *LaneManager) Get(endpointID string) *Lane {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lanes[endpointID]
}

// Len reports how many lanes are live.
func (m *LaneManager) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.lanes)
}

// Drop removes a lane, used when an endpoint moves to another pool or worker.
func (m *LaneManager) Drop(endpointID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.lanes, endpointID)
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
		// A lane with an open breaker is kept: its state is the reason we are not
		// dialling a dead endpoint, and throwing it away would make us re-learn that
		// the hard way.
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

// BreakerCounts counts lanes per breaker state, which is the low-cardinality
// metric worth exporting. Per-endpoint gauges are sampled instead: labelling
// by endpoint takes down Prometheus before it takes down the service.
func (m *LaneManager) BreakerCounts() map[BreakerState]int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	counts := map[BreakerState]int{BreakerClosed: 0, BreakerOpen: 0, BreakerHalfOpen: 0}
	for _, lane := range m.lanes {
		counts[lane.Breaker().State()]++
	}
	return counts
}

// TotalInFlight sums deliveries in flight across every lane.
func (m *LaneManager) TotalInFlight() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	total := 0
	for _, lane := range m.lanes {
		total += lane.InFlight()
	}
	return total
}
