package middleware

import (
	"strconv"
	"sync"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/infrastructure/authctx"
	"golang.org/x/time/rate"
)

// NodeCounter reports how many API nodes are live, so a cluster-wide limit can
// be enforced locally as limit/nodes.
type NodeCounter interface {
	Nodes() int
}

// RateLimitOptions configures one limiter set.
type RateLimitOptions struct {
	// RequestsPerSecond is the cluster-wide limit per key.
	RequestsPerSecond int
	// Burst defaults to one second's worth of requests.
	Burst int
	// Nodes divides the limit across API nodes. Nil means this process is the
	// only one.
	Nodes NodeCounter
	// IdleEviction drops a key's limiter after this long without a request,
	// so the map does not grow with every tenant that ever called.
	IdleEviction time.Duration
}

// limiterSet is a keyed set of token-bucket limiters. Everything is in local
// memory: a network round trip to decide whether a request is allowed costs
// more than the request.
type limiterSet struct {
	opts RateLimitOptions

	mu        sync.Mutex
	limiters  map[string]*keyedLimiter
	lastSweep time.Time
}

type keyedLimiter struct {
	limiter  *rate.Limiter
	lastUsed time.Time
	// share is the per-node limit this limiter was built with, so a change in
	// node count re-rates it.
	share float64
}

func newLimiterSet(opts RateLimitOptions) *limiterSet {
	if opts.Burst <= 0 {
		opts.Burst = max(1, opts.RequestsPerSecond)
	}
	if opts.IdleEviction <= 0 {
		opts.IdleEviction = 10 * time.Minute
	}
	return &limiterSet{opts: opts, limiters: make(map[string]*keyedLimiter, 128), lastSweep: time.Now()}
}

// share is this node's slice of the cluster-wide limit.
func (s *limiterSet) share() float64 {
	nodes := 1
	if s.opts.Nodes != nil {
		nodes = max(s.opts.Nodes.Nodes(), 1)
	}
	share := float64(s.opts.RequestsPerSecond) / float64(nodes)
	// Never rate-limit a tenant to nothing because the cluster grew.
	return max(share, 1)
}

// allow reports whether a request for a key may proceed, and what to report in
// the rate limit headers.
func (s *limiterSet) allow(key string) (allowed bool, limit float64, remaining float64, reset time.Duration) {
	now := time.Now()
	share := s.share()

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.limiters[key]
	if !ok || entry.share != share {
		entry = &keyedLimiter{
			limiter: rate.NewLimiter(rate.Limit(share), max(1, int(share))),
			share:   share,
		}
		s.limiters[key] = entry
	}
	entry.lastUsed = now
	s.sweepLocked(now)

	reservation := entry.limiter.ReserveN(now, 1)
	if !reservation.OK() {
		return false, share, 0, time.Second
	}
	if delay := reservation.DelayFrom(now); delay > 0 {
		// Over budget: give the token back so a rejected request does not also
		// consume capacity.
		reservation.CancelAt(now)
		return false, share, 0, delay
	}
	return true, share, entry.limiter.TokensAt(now), 0
}

// sweepLocked drops idle limiters occasionally. The caller holds the mutex.
func (s *limiterSet) sweepLocked(now time.Time) {
	if now.Sub(s.lastSweep) < s.opts.IdleEviction {
		return
	}
	s.lastSweep = now
	for key, entry := range s.limiters {
		if now.Sub(entry.lastUsed) >= s.opts.IdleEviction {
			delete(s.limiters, key)
		}
	}
}

// RateLimitByOrg enforces the per-organization request rate. One tenant
// hammering the API must not consume another's share.
func RateLimitByOrg(opts RateLimitOptions) echo.MiddlewareFunc {
	set := newLimiterSet(opts)

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if opts.RequestsPerSecond <= 0 {
				return next(c)
			}
			org := authctx.OrgFrom(c.Request().Context())
			if org == "" {
				return next(c)
			}
			return enforce(c, next, set, org)
		}
	}
}

// RateLimitByApp enforces the per-application ingest rate, keyed by the
// application in the path.
func RateLimitByApp(param string, opts RateLimitOptions) echo.MiddlewareFunc {
	set := newLimiterSet(opts)

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if opts.RequestsPerSecond <= 0 {
				return next(c)
			}
			app := c.Param(param)
			if app == "" {
				return next(c)
			}
			// Keyed by tenant and application together: two tenants may use the same
			// tenant-assigned uid.
			return enforce(c, next, set, authctx.OrgFrom(c.Request().Context())+"/"+app)
		}
	}
}

func enforce(c *echo.Context, next echo.HandlerFunc, set *limiterSet, key string) error {
	allowed, limit, remaining, reset := set.allow(key)

	header := c.Response().Header()
	header.Set("RateLimit-Limit", strconv.Itoa(int(limit)))
	header.Set("RateLimit-Remaining", strconv.Itoa(int(remaining)))
	header.Set("RateLimit-Reset", strconv.Itoa(int(reset.Seconds()+0.5)))

	if !allowed {
		header.Set("Retry-After", strconv.Itoa(max(1, int(reset.Seconds()+0.5))))
		return apperrors.NewRateLimitError("rate limit exceeded")
	}
	return next(c)
}
