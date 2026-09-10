package deliveryhttp

import (
	"context"
	"net"
	"net/netip"
	"sync"
	"time"
)

// DNSCache caches resolver results in process. Go's resolver hits the system
// resolver on every dial, so caching removes a round trip from every cold
// delivery; it is a performance feature as much as an SSRF one.
type DNSCache struct {
	resolver Resolver
	ttl      time.Duration
	negTTL   time.Duration
	now      func() time.Time
	onHit    func()
	onMiss   func()

	mu      sync.RWMutex
	entries map[string]dnsEntry
}

// Resolver is the lookup the cache sits in front of.
type Resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

type dnsEntry struct {
	addrs     []netip.Addr
	err       error
	expiresAt time.Time
}

// DNSCacheOptions configures the cache.
type DNSCacheOptions struct {
	// Resolver defaults to the system resolver.
	Resolver Resolver
	// TTL is how long a successful lookup is reused.
	TTL time.Duration
	// NegativeTTL is how long a failure is remembered, so a dead hostname does
	// not cost a resolver round trip per delivery.
	NegativeTTL time.Duration
	// Now lets a test drive the timers forward instead of sleeping.
	Now func() time.Time
	// OnHit and OnMiss feed the cache metrics.
	OnHit  func()
	OnMiss func()
}

// NewDNSCache builds a cache with sane defaults.
func NewDNSCache(opts DNSCacheOptions) *DNSCache {
	cache := &DNSCache{
		resolver: opts.Resolver,
		ttl:      opts.TTL,
		negTTL:   opts.NegativeTTL,
		now:      opts.Now,
		onHit:    opts.OnHit,
		onMiss:   opts.OnMiss,
		entries:  make(map[string]dnsEntry, 1024),
	}
	if cache.resolver == nil {
		cache.resolver = net.DefaultResolver
	}
	if cache.ttl <= 0 {
		cache.ttl = 30 * time.Second
	}
	if cache.negTTL <= 0 {
		cache.negTTL = 5 * time.Second
	}
	if cache.now == nil {
		cache.now = time.Now
	}
	return cache
}

// Lookup resolves a host, serving from cache when the entry is fresh.
func (c *DNSCache) Lookup(ctx context.Context, host string) ([]netip.Addr, error) {
	// A literal address needs no resolver at all.
	if addr, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{addr}, nil
	}

	now := c.now()

	c.mu.RLock()
	entry, ok := c.entries[host]
	c.mu.RUnlock()
	if ok && now.Before(entry.expiresAt) {
		if c.onHit != nil {
			c.onHit()
		}
		return entry.addrs, entry.err
	}

	if c.onMiss != nil {
		c.onMiss()
	}

	addrs, err := c.resolver.LookupNetIP(ctx, "ip", host)

	ttl := c.ttl
	if err != nil {
		ttl = c.negTTL
	}
	c.mu.Lock()
	c.entries[host] = dnsEntry{addrs: addrs, err: err, expiresAt: now.Add(ttl)}
	c.mu.Unlock()

	return addrs, err
}

// Len reports how many hosts are cached, for tests and diagnostics.
func (c *DNSCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// Purge drops expired entries. The cache is small and bounded by the number of
// distinct endpoint hosts, so this runs on a timer rather than on every read.
func (c *DNSCache) Purge() int {
	now := c.now()

	c.mu.Lock()
	defer c.mu.Unlock()
	removed := 0
	for host, entry := range c.entries {
		if !now.Before(entry.expiresAt) {
			delete(c.entries, host)
			removed++
		}
	}
	return removed
}
