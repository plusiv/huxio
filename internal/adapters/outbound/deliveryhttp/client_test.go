package deliveryhttp_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/adapters/outbound/deliveryhttp"
	"github.com/plusiv/huxio/internal/application/dispatch"
)

// newClient builds a client whose guard and resolver the test controls.
func newClient(t *testing.T, guardOpts deliveryhttp.GuardOptions, resolver deliveryhttp.Resolver, opts deliveryhttp.Options) *deliveryhttp.Client {
	t.Helper()

	guard, err := deliveryhttp.NewGuard(guardOpts)
	if err != nil {
		t.Fatalf("NewGuard: %v", err)
	}
	opts.Guard = guard
	opts.DNSCache = deliveryhttp.NewDNSCache(deliveryhttp.DNSCacheOptions{Resolver: resolver})

	client, err := deliveryhttp.New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(client.CloseIdleConnections)
	return client
}

// localClient allows loopback, for delivering to a test server.
func localClient(t *testing.T, opts deliveryhttp.Options) *deliveryhttp.Client {
	t.Helper()
	return newClient(t, deliveryhttp.GuardOptions{AllowPrivate: true}, net.DefaultResolver, opts)
}

func TestNewRequiresGuardAndCache(t *testing.T) {
	t.Parallel()

	if _, err := deliveryhttp.New(deliveryhttp.Options{}); err == nil {
		t.Error("a client without an SSRF guard must not be constructible")
	}
	guard, err := deliveryhttp.NewGuard(deliveryhttp.GuardOptions{})
	if err != nil {
		t.Fatalf("NewGuard: %v", err)
	}
	if _, err := deliveryhttp.New(deliveryhttp.Options{Guard: guard}); err == nil {
		t.Error("a client without a DNS cache must not be constructible")
	}
}

func TestDeliverSuccess(t *testing.T) {
	t.Parallel()

	var (
		gotHeaders http.Header
		gotBody    []byte
		gotMethod  string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotHeaders = r.Header.Clone()
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	client := localClient(t, deliveryhttp.Options{})
	result := client.Deliver(context.Background(), dispatch.DeliveryRequest{
		URL:     server.URL,
		Body:    []byte(`{"amount":100}`),
		Headers: map[string]string{"webhook-id": "msg_1", "X-Tenant": "acme"},
	})

	if result.Err != nil {
		t.Fatalf("Deliver: %v", result.Err)
	}
	if !result.Succeeded() || result.StatusCode != http.StatusOK {
		t.Errorf("result = %+v", result)
	}
	if result.Body != "ok" {
		t.Errorf("Body = %q", result.Body)
	}
	if result.Duration <= 0 {
		t.Error("Duration must be recorded")
	}
	if gotMethod != http.MethodPost {
		t.Errorf("method = %s, want POST", gotMethod)
	}
	if string(gotBody) != `{"amount":100}` {
		t.Errorf("body = %q", gotBody)
	}
	if gotHeaders.Get("webhook-id") != "msg_1" || gotHeaders.Get("X-Tenant") != "acme" {
		t.Errorf("headers = %v", gotHeaders)
	}
	if gotHeaders.Get("Content-Type") != "application/json" {
		t.Errorf("content type = %q", gotHeaders.Get("Content-Type"))
	}
	if !strings.HasPrefix(gotHeaders.Get("User-Agent"), "huxio/") {
		t.Errorf("user agent = %q", gotHeaders.Get("User-Agent"))
	}
}

func TestDeliverReusesConnections(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := localClient(t, deliveryhttp.Options{})
	ctx := context.Background()
	req := dispatch.DeliveryRequest{URL: server.URL, Body: []byte(`{}`)}

	if first := client.Deliver(ctx, req); first.ConnectionReused {
		t.Error("the first delivery cannot reuse a connection")
	}
	// The second delivery must reuse the connection, which is the difference
	// between ~5ms and ~80ms per delivery for a busy receiver.
	if second := client.Deliver(ctx, req); !second.ConnectionReused {
		t.Error("the second delivery to the same host must reuse the connection")
	}
}

func TestDeliverRecordsNon2xxAsAResult(t *testing.T) {
	t.Parallel()

	for _, status := range []int{400, 401, 404, 429, 500, 503} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte("nope"))
		}))

		client := localClient(t, deliveryhttp.Options{})
		result := client.Deliver(context.Background(), dispatch.DeliveryRequest{URL: server.URL, Body: []byte(`{}`)})
		server.Close()

		if result.Err != nil {
			t.Errorf("status %d produced a transport error: %v", status, result.Err)
		}
		if result.StatusCode != status || result.Succeeded() {
			t.Errorf("status %d: result = %+v", status, result)
		}
	}
}

func TestDeliverDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()

	var reachedTarget atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reachedTarget.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer redirector.Close()

	client := localClient(t, deliveryhttp.Options{})
	result := client.Deliver(context.Background(), dispatch.DeliveryRequest{URL: redirector.URL, Body: []byte(`{}`)})

	// Following a redirect to an arbitrary location would be an SSRF bypass:
	// the 3xx is the result.
	if result.StatusCode != http.StatusFound {
		t.Errorf("status = %d, want the 302 recorded as-is", result.StatusCode)
	}
	if result.Succeeded() {
		t.Error("a redirect is not a successful delivery")
	}
	if reachedTarget.Load() {
		t.Error("the redirect target must never be requested")
	}
}

func TestDeliverTruncatesAndDrainsLargeBodies(t *testing.T) {
	t.Parallel()

	const bodyLimit = 512
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		// A 5MB error page must not reach the database.
		_, _ = w.Write([]byte(strings.Repeat("x", 5<<20)))
	}))
	defer server.Close()

	client := localClient(t, deliveryhttp.Options{ResponseBodyLimit: bodyLimit})
	ctx := context.Background()
	req := dispatch.DeliveryRequest{URL: server.URL, Body: []byte(`{}`)}

	result := client.Deliver(ctx, req)
	if len(result.Body) != bodyLimit {
		t.Errorf("stored %d body bytes, want the %d byte limit", len(result.Body), bodyLimit)
	}

	// The body was drained rather than abandoned, so the connection returns
	// to the pool instead of leaking.
	if second := client.Deliver(ctx, req); !second.ConnectionReused {
		t.Error("an undrained response body leaks the connection")
	}
}

func TestDeliverTimeout(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer func() {
		close(release)
		server.Close()
	}()

	client := localClient(t, deliveryhttp.Options{})
	result := client.Deliver(context.Background(), dispatch.DeliveryRequest{
		URL:     server.URL,
		Body:    []byte(`{}`),
		Timeout: 50 * time.Millisecond,
	})

	if result.Err == nil {
		t.Fatal("expected a timeout error")
	}
	if !result.Timeout {
		t.Errorf("result = %+v, want Timeout set", result)
	}
	if result.StatusCode != 0 {
		t.Errorf("StatusCode = %d, want zero when no response arrived", result.StatusCode)
	}
}

func TestDeliverParsesRetryAfter(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	client := localClient(t, deliveryhttp.Options{})
	result := client.Deliver(context.Background(), dispatch.DeliveryRequest{URL: server.URL, Body: []byte(`{}`)})

	if result.RetryAfter == nil {
		t.Fatal("Retry-After must be parsed off the response")
	}
	if *result.RetryAfter != 2*time.Minute {
		t.Errorf("RetryAfter = %s, want 2m", *result.RetryAfter)
	}
}

func TestDeliverRefusesBlockedDestinations(t *testing.T) {
	t.Parallel()

	// Every one of these hostnames resolves into a blocked range.
	resolver := newFakeResolver()
	resolver.answers["loopback.test"] = []netip.Addr{netip.MustParseAddr("127.0.0.1")}
	resolver.answers["metadata.test"] = []netip.Addr{netip.MustParseAddr("169.254.169.254")}
	resolver.answers["private.test"] = []netip.Addr{netip.MustParseAddr("10.1.2.3")}
	resolver.answers["cgnat.test"] = []netip.Addr{netip.MustParseAddr("100.64.1.1")}
	resolver.answers["mapped.test"] = []netip.Addr{netip.MustParseAddr("::ffff:127.0.0.1")}
	resolver.answers["ula.test"] = []netip.Addr{netip.MustParseAddr("fc00::1")}

	client := newClient(t, deliveryhttp.GuardOptions{}, resolver, deliveryhttp.Options{})

	for host := range resolver.answers {
		result := client.Deliver(context.Background(), dispatch.DeliveryRequest{
			URL:  "https://" + host + "/hook",
			Body: []byte(`{}`),
		})
		if result.Err == nil {
			t.Errorf("%s: expected the delivery to be refused", host)
			continue
		}
		if !result.Refused {
			t.Errorf("%s: result = %+v, want Refused set", host, result)
		}
		if result.StatusCode != 0 {
			t.Errorf("%s: StatusCode = %d, want zero", host, result.StatusCode)
		}
	}
}

// TestDeliverResolvesExactlyOncePerDelivery pins the property that closes the
// rebinding window: the address that was checked is the address that is
// dialled, so the transport never gets to resolve the host a second time.
func TestDeliverResolvesExactlyOncePerDelivery(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	host, port := splitHostPort(t, server.Listener.Addr().String())
	serverAddr := netip.MustParseAddr(host)

	resolver := newFakeResolver()
	resolver.answers["once.test"] = []netip.Addr{serverAddr}

	guard, err := deliveryhttp.NewGuard(deliveryhttp.GuardOptions{AllowSubnets: []string{serverAddr.String() + "/32"}})
	if err != nil {
		t.Fatalf("NewGuard: %v", err)
	}
	// No caching at all, so any extra resolution would show up in the count.
	cache := deliveryhttp.NewDNSCache(deliveryhttp.DNSCacheOptions{
		Resolver: resolver, TTL: time.Nanosecond, NegativeTTL: time.Nanosecond,
	})
	client, err := deliveryhttp.New(deliveryhttp.Options{Guard: guard, DNSCache: cache})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.CloseIdleConnections()

	result := client.Deliver(context.Background(), dispatch.DeliveryRequest{
		URL:  "http://once.test:" + port + "/hook",
		Body: []byte(`{}`),
	})
	if result.Err != nil {
		t.Fatalf("Deliver: %v", result.Err)
	}
	if got := resolver.lookups["once.test"]; got != 1 {
		t.Errorf("resolver called %d times for one delivery; a second lookup is the rebinding window", got)
	}
}

// TestDeliverBlocksRebindingAfterCacheExpiry is the other half: once the
// cached answer expires, a record that has flipped to a private address is
// checked again and refused.
func TestDeliverBlocksRebindingAfterCacheExpiry(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	host, port := splitHostPort(t, server.Listener.Addr().String())
	serverAddr := netip.MustParseAddr(host)

	resolver := newFakeResolver()
	resolver.answers["rebind.test"] = []netip.Addr{serverAddr}

	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	guard, err := deliveryhttp.NewGuard(deliveryhttp.GuardOptions{AllowSubnets: []string{serverAddr.String() + "/32"}})
	if err != nil {
		t.Fatalf("NewGuard: %v", err)
	}
	cache := deliveryhttp.NewDNSCache(deliveryhttp.DNSCacheOptions{
		Resolver: resolver,
		TTL:      30 * time.Second,
		Now:      func() time.Time { return now },
	})
	client, err := deliveryhttp.New(deliveryhttp.Options{Guard: guard, DNSCache: cache})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.CloseIdleConnections()

	url := "http://rebind.test:" + port + "/hook"
	if first := client.Deliver(context.Background(), dispatch.DeliveryRequest{URL: url, Body: []byte(`{}`)}); first.Err != nil {
		t.Fatalf("first delivery: %v", first.Err)
	}

	// The record flips to an address inside the network, and the cached answer
	// expires.
	resolver.answers["rebind.test"] = []netip.Addr{netip.MustParseAddr("10.9.9.9")}
	now = now.Add(31 * time.Second)
	client.CloseIdleConnections()

	second := client.Deliver(context.Background(), dispatch.DeliveryRequest{URL: url, Body: []byte(`{}`)})
	if second.Err == nil {
		t.Fatal("a rebound hostname must not be delivered to")
	}
	if !second.Refused {
		t.Errorf("result = %+v, want Refused set", second)
	}
}

func splitHostPort(t *testing.T, address string) (host, port string) {
	t.Helper()
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("split %q: %v", address, err)
	}
	return host, port
}

func TestDeliverRefusesConnectionRefused(t *testing.T) {
	t.Parallel()

	// Bind and immediately close, so the port is almost certainly closed.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	client := localClient(t, deliveryhttp.Options{})
	result := client.Deliver(context.Background(), dispatch.DeliveryRequest{
		URL:  "http://" + addr + "/hook",
		Body: []byte(`{}`),
	})

	if result.Err == nil {
		t.Fatal("expected a connection error")
	}
	// A refused connection means the endpoint is absent, not slow: the lane
	// drops straight to one in flight for this.
	if !result.Refused {
		t.Errorf("result = %+v, want Refused set", result)
	}
}

func TestDeliverRejectsBadURL(t *testing.T) {
	t.Parallel()

	client := localClient(t, deliveryhttp.Options{})
	result := client.Deliver(context.Background(), dispatch.DeliveryRequest{
		URL:  "http://[::1]:namedport/hook",
		Body: []byte(`{}`),
	})
	if result.Err == nil {
		t.Error("an unparseable URL must not be delivered to")
	}
}
