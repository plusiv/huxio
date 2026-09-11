package deliveryhttp

import (
	"context"
	"net"
	"time"

	"github.com/rotisserie/eris"
)

// guardedDialer resolves a host once, validates the addresses, and dials one
// of the validated addresses directly. Resolving, validating and then letting
// the transport resolve again leaves a DNS rebinding window: the second
// lookup can return a private address.
type guardedDialer struct {
	guard  *Guard
	cache  *DNSCache
	dialer *net.Dialer
}

func newGuardedDialer(guard *Guard, cache *DNSCache, timeout time.Duration) *guardedDialer {
	return &guardedDialer{
		guard: guard,
		cache: cache,
		dialer: &net.Dialer{
			Timeout:   timeout,
			KeepAlive: 30 * time.Second,
		},
	}
}

// DialContext implements the transport's dialer.
func (d *guardedDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, eris.Wrapf(err, "split address %q", address)
	}

	addrs, err := d.cache.Lookup(ctx, host)
	if err != nil {
		return nil, eris.Wrapf(err, "resolve %q", host)
	}

	allowed := d.guard.Filter(addrs)
	if len(allowed) == 0 {
		return nil, eris.Wrapf(ErrBlockedDestination, "host %q", host)
	}

	var lastErr error
	for _, addr := range allowed {
		// Dial the address that was checked, by literal, so nothing can be
		// re-resolved between the check and the connect.
		target := net.JoinHostPort(addr.String(), port)
		conn, err := d.dialer.DialContext(ctx, network, target)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			break
		}
	}
	if lastErr == nil {
		lastErr = ErrBlockedDestination
	}
	return nil, lastErr
}
