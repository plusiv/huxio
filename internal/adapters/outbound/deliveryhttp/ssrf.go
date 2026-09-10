// Package deliveryhttp is the outbound HTTP adapter for webhook delivery: a
// tuned transport, an in-process DNS cache, and an SSRF guard that dials the
// address it checked.
package deliveryhttp

import (
	"net"
	"net/netip"

	"github.com/rotisserie/eris"
)

// ErrBlockedDestination is returned when every resolved address for a host is
// disallowed.
var ErrBlockedDestination = eris.New("destination address is not allowed")

// blockedPrefixes are refused by default. The cloud metadata endpoint
// (169.254.169.254) falls inside the link-local range and is the single most
// commonly exploited SSRF target.
var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),          // this host on this network
	netip.MustParsePrefix("10.0.0.0/8"),         // private
	netip.MustParsePrefix("100.64.0.0/10"),      // CGNAT
	netip.MustParsePrefix("127.0.0.0/8"),        // loopback
	netip.MustParsePrefix("169.254.0.0/16"),     // link-local, incl. metadata
	netip.MustParsePrefix("172.16.0.0/12"),      // private
	netip.MustParsePrefix("192.0.0.0/24"),       // IETF protocol assignments
	netip.MustParsePrefix("192.0.2.0/24"),       // documentation
	netip.MustParsePrefix("192.168.0.0/16"),     // private
	netip.MustParsePrefix("198.18.0.0/15"),      // benchmarking
	netip.MustParsePrefix("198.51.100.0/24"),    // documentation
	netip.MustParsePrefix("203.0.113.0/24"),     // documentation
	netip.MustParsePrefix("224.0.0.0/4"),        // multicast
	netip.MustParsePrefix("240.0.0.0/4"),        // reserved
	netip.MustParsePrefix("255.255.255.255/32"), // broadcast
	netip.MustParsePrefix("::/128"),             // unspecified
	netip.MustParsePrefix("::1/128"),            // loopback
	netip.MustParsePrefix("64:ff9b::/96"),       // NAT64
	netip.MustParsePrefix("100::/64"),           // discard-only
	netip.MustParsePrefix("2001:db8::/32"),      // documentation
	netip.MustParsePrefix("fc00::/7"),           // unique local
	netip.MustParsePrefix("fe80::/10"),          // link-local
	netip.MustParsePrefix("ff00::/8"),           // multicast
}

// Guard decides whether an address may be dialled.
type Guard struct {
	allowed []netip.Prefix
	// allowAll disables the guard entirely, for the benchmark harness and for
	// tests that dial loopback sinks.
	allowAll bool
}

// GuardOptions configures the guard.
type GuardOptions struct {
	// AllowSubnets are CIDRs to permit despite the default blocks, for operators
	// running internally.
	AllowSubnets []string
	// AllowPrivate disables the guard. Only the benchmark harness and tests
	// set it; it is never a production configuration.
	AllowPrivate bool
}

// NewGuard builds a guard, rejecting an unparseable allowlist rather than
// silently ignoring it.
func NewGuard(opts GuardOptions) (*Guard, error) {
	guard := &Guard{allowAll: opts.AllowPrivate}
	for _, raw := range opts.AllowSubnets {
		prefix, err := netip.ParsePrefix(raw)
		if err != nil {
			return nil, eris.Wrapf(err, "parse allowed subnet %q", raw)
		}
		guard.allowed = append(guard.allowed, prefix)
	}
	return guard, nil
}

// Allow reports whether an address may be dialled.
func (g *Guard) Allow(addr netip.Addr) bool {
	if g.allowAll {
		return true
	}
	if !addr.IsValid() {
		return false
	}

	// An IPv4-mapped IPv6 address must be judged as the IPv4 address it is,
	// or ::ffff:127.0.0.1 walks straight through the block list.
	if addr.Is4In6() {
		addr = addr.Unmap()
	}

	// An explicit allowlist entry wins, which is how an operator reaches an
	// internal endpoint on purpose.
	for _, prefix := range g.allowed {
		if prefix.Contains(addr) {
			return true
		}
	}

	if addr.IsLoopback() || addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() ||
		addr.IsInterfaceLocalMulticast() || addr.IsMulticast() || addr.IsUnspecified() {
		return false
	}
	for _, prefix := range blockedPrefixes {
		if prefix.Contains(addr) {
			return false
		}
	}
	return true
}

// AllowIP is the net.IP form, for callers holding a resolver result.
func (g *Guard) AllowIP(ip net.IP) bool {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	return g.Allow(addr)
}

// Filter returns the subset of addresses that may be dialled.
func (g *Guard) Filter(addrs []netip.Addr) []netip.Addr {
	allowed := make([]netip.Addr, 0, len(addrs))
	for _, addr := range addrs {
		if g.Allow(addr) {
			allowed = append(allowed, addr)
		}
	}
	return allowed
}
