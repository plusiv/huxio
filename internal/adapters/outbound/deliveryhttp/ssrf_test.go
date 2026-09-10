package deliveryhttp_test

import (
	"context"
	"net/netip"
	"testing"

	"github.com/plusiv/huxio/internal/adapters/outbound/deliveryhttp"
)

func TestGuardBlocksInternalRanges(t *testing.T) {
	t.Parallel()

	guard, err := deliveryhttp.NewGuard(deliveryhttp.GuardOptions{})
	if err != nil {
		t.Fatalf("NewGuard: %v", err)
	}

	blocked := []struct {
		name string
		addr string
	}{
		{"loopback", "127.0.0.1"},
		{"loopback range", "127.13.14.15"},
		{"unspecified", "0.0.0.0"},
		{"this network", "0.1.2.3"},
		{"private 10", "10.0.0.1"},
		{"private 172", "172.16.5.4"},
		{"private 192", "192.168.1.1"},
		{"cgnat", "100.100.1.1"},
		{"link local", "169.254.1.1"},
		{"cloud metadata", "169.254.169.254"},
		{"multicast", "224.0.0.1"},
		{"broadcast", "255.255.255.255"},
		{"reserved", "240.0.0.1"},
		{"ipv6 loopback", "::1"},
		{"ipv6 unspecified", "::"},
		{"ipv6 unique local", "fc00::1"},
		{"ipv6 link local", "fe80::1"},
		{"ipv6 multicast", "ff02::1"},
		// The mapped forms are the ones people forget, and they are exactly
		// as dangerous as the plain ones.
		{"ipv4-mapped loopback", "::ffff:127.0.0.1"},
		{"ipv4-mapped metadata", "::ffff:169.254.169.254"},
		{"ipv4-mapped private", "::ffff:10.0.0.1"},
	}
	for _, tc := range blocked {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			addr := netip.MustParseAddr(tc.addr)
			if guard.Allow(addr) {
				t.Errorf("Allow(%s) = true, want blocked", tc.addr)
			}
		})
	}

	allowed := []string{"1.1.1.1", "8.8.8.8", "93.184.216.34", "2606:2800:220:1:248:1893:25c8:1946"}
	for _, raw := range allowed {
		if !guard.Allow(netip.MustParseAddr(raw)) {
			t.Errorf("Allow(%s) = false, want allowed", raw)
		}
	}

	if guard.Allow(netip.Addr{}) {
		t.Error("an invalid address must be blocked")
	}
}

func TestGuardAllowlist(t *testing.T) {
	t.Parallel()

	guard, err := deliveryhttp.NewGuard(deliveryhttp.GuardOptions{
		AllowSubnets: []string{"10.1.0.0/16"},
	})
	if err != nil {
		t.Fatalf("NewGuard: %v", err)
	}

	// An operator running internally opts one range back in, and only that one.
	if !guard.Allow(netip.MustParseAddr("10.1.2.3")) {
		t.Error("an allowlisted address must be permitted")
	}
	if guard.Allow(netip.MustParseAddr("10.2.2.3")) {
		t.Error("a private address outside the allowlist must stay blocked")
	}
	if guard.Allow(netip.MustParseAddr("127.0.0.1")) {
		t.Error("loopback must stay blocked")
	}

	if _, err := deliveryhttp.NewGuard(deliveryhttp.GuardOptions{AllowSubnets: []string{"not-a-cidr"}}); err == nil {
		t.Error("an unparseable allowlist entry must fail loudly, not be ignored")
	}
}

func TestGuardFilter(t *testing.T) {
	t.Parallel()

	guard, err := deliveryhttp.NewGuard(deliveryhttp.GuardOptions{})
	if err != nil {
		t.Fatalf("NewGuard: %v", err)
	}

	got := guard.Filter([]netip.Addr{
		netip.MustParseAddr("127.0.0.1"),
		netip.MustParseAddr("93.184.216.34"),
		netip.MustParseAddr("10.0.0.1"),
	})
	if len(got) != 1 || got[0].String() != "93.184.216.34" {
		t.Errorf("Filter = %v, want only the public address", got)
	}
	if len(guard.Filter(nil)) != 0 {
		t.Error("filtering nothing must return nothing")
	}
}

// fakeResolver returns scripted answers, and counts lookups so the cache can
// be observed.
type fakeResolver struct {
	answers map[string][]netip.Addr
	errs    map[string]error
	lookups map[string]int
}

func newFakeResolver() *fakeResolver {
	return &fakeResolver{
		answers: map[string][]netip.Addr{},
		errs:    map[string]error{},
		lookups: map[string]int{},
	}
}

func (r *fakeResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	r.lookups[host]++
	if err, ok := r.errs[host]; ok {
		return nil, err
	}
	return r.answers[host], nil
}
