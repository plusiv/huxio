package deliveryhttp_test

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/adapters/outbound/deliveryhttp"
	"github.com/rotisserie/eris"
)

func TestDNSCacheServesFromCacheWithinTTL(t *testing.T) {
	t.Parallel()

	resolver := newFakeResolver()
	resolver.answers["example.test"] = []netip.Addr{netip.MustParseAddr("93.184.216.34")}

	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	var hits, misses int
	cache := deliveryhttp.NewDNSCache(deliveryhttp.DNSCacheOptions{
		Resolver: resolver,
		TTL:      30 * time.Second,
		Now:      func() time.Time { return now },
		OnHit:    func() { hits++ },
		OnMiss:   func() { misses++ },
	})

	ctx := context.Background()
	for range 5 {
		addrs, err := cache.Lookup(ctx, "example.test")
		if err != nil {
			t.Fatalf("Lookup: %v", err)
		}
		if len(addrs) != 1 {
			t.Fatalf("Lookup returned %d addresses", len(addrs))
		}
	}

	// One resolver round trip for five deliveries: this is the cold-path cost
	// the cache exists to remove.
	if resolver.lookups["example.test"] != 1 {
		t.Errorf("resolver called %d times, want 1", resolver.lookups["example.test"])
	}
	if misses != 1 || hits != 4 {
		t.Errorf("hits = %d, misses = %d; want 4 and 1", hits, misses)
	}

	// Past the TTL it resolves again.
	now = now.Add(31 * time.Second)
	if _, err := cache.Lookup(ctx, "example.test"); err != nil {
		t.Fatalf("Lookup after expiry: %v", err)
	}
	if resolver.lookups["example.test"] != 2 {
		t.Errorf("resolver called %d times after expiry, want 2", resolver.lookups["example.test"])
	}
}

func TestDNSCacheRemembersFailuresBriefly(t *testing.T) {
	t.Parallel()

	resolver := newFakeResolver()
	resolver.errs["missing.test"] = eris.New("no such host")

	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	cache := deliveryhttp.NewDNSCache(deliveryhttp.DNSCacheOptions{
		Resolver:    resolver,
		TTL:         30 * time.Second,
		NegativeTTL: 5 * time.Second,
		Now:         func() time.Time { return now },
	})

	ctx := context.Background()
	for range 3 {
		if _, err := cache.Lookup(ctx, "missing.test"); err == nil {
			t.Fatal("expected the lookup to fail")
		}
	}
	// A dead hostname must not cost a resolver round trip per delivery.
	if resolver.lookups["missing.test"] != 1 {
		t.Errorf("resolver called %d times, want 1", resolver.lookups["missing.test"])
	}

	// The negative TTL is deliberately shorter, so a fixed DNS record is
	// picked up quickly.
	now = now.Add(6 * time.Second)
	if _, err := cache.Lookup(ctx, "missing.test"); err == nil {
		t.Fatal("expected the lookup to fail")
	}
	if resolver.lookups["missing.test"] != 2 {
		t.Errorf("resolver called %d times after the negative TTL, want 2", resolver.lookups["missing.test"])
	}
}

func TestDNSCacheSkipsResolverForLiteralAddresses(t *testing.T) {
	t.Parallel()

	resolver := newFakeResolver()
	cache := deliveryhttp.NewDNSCache(deliveryhttp.DNSCacheOptions{Resolver: resolver})

	addrs, err := cache.Lookup(context.Background(), "93.184.216.34")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(addrs) != 1 || addrs[0].String() != "93.184.216.34" {
		t.Errorf("Lookup = %v", addrs)
	}
	if len(resolver.lookups) != 0 {
		t.Error("a literal address must not reach the resolver")
	}
}

func TestDNSCachePurge(t *testing.T) {
	t.Parallel()

	resolver := newFakeResolver()
	resolver.answers["a.test"] = []netip.Addr{netip.MustParseAddr("1.1.1.1")}
	resolver.answers["b.test"] = []netip.Addr{netip.MustParseAddr("1.0.0.1")}

	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	cache := deliveryhttp.NewDNSCache(deliveryhttp.DNSCacheOptions{
		Resolver: resolver,
		TTL:      10 * time.Second,
		Now:      func() time.Time { return now },
	})

	ctx := context.Background()
	if _, err := cache.Lookup(ctx, "a.test"); err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	now = now.Add(6 * time.Second)
	if _, err := cache.Lookup(ctx, "b.test"); err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if cache.Len() != 2 {
		t.Fatalf("cache holds %d entries, want 2", cache.Len())
	}

	// Only a.test has expired.
	now = now.Add(6 * time.Second)
	if removed := cache.Purge(); removed != 1 {
		t.Errorf("Purge removed %d entries, want 1", removed)
	}
	if cache.Len() != 1 {
		t.Errorf("cache holds %d entries after purge, want 1", cache.Len())
	}
}
