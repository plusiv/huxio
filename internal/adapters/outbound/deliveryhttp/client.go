package deliveryhttp

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"syscall"
	"time"

	"github.com/plusiv/huxio/internal/application/dispatch"
	"github.com/plusiv/huxio/internal/domain/retry"
	"github.com/plusiv/huxio/internal/utils"
	"github.com/rotisserie/eris"
)

// UserAgent identifies our requests to receivers.
const UserAgent = "huxio/1.0"

// DefaultResponseBodyLimit is how much of a response body is read. An endpoint
// returning 50MB must not reach the database.
const DefaultResponseBodyLimit = 8 << 10

// Options configures the client.
type Options struct {
	// RequestTimeout is the default per-request deadline.
	RequestTimeout time.Duration
	// ResponseBodyLimit caps how much of a response body is stored.
	ResponseBodyLimit int
	// Guard and DNSCache are the SSRF guard and resolver cache.
	Guard    *Guard
	DNSCache *DNSCache
	// UserAgent overrides the outbound user agent.
	UserAgent string
	// TLSClientConfig overrides the transport's TLS settings. Production
	// leaves it nil and uses the system roots; the benchmark harness passes
	// the certificate its own sinks are serving.
	TLSClientConfig *tls.Config
}

// Client performs signed webhook deliveries over one shared transport. The
// defaults of net/http are wrong for this workload, most of all
// MaxIdleConnsPerHost, whose default of 2 is the classic Go webhook-sender
// footgun.
type Client struct {
	httpClient *http.Client
	timeout    time.Duration
	bodyLimit  int
	userAgent  string
}

// New builds a client. Guard and DNSCache are required: delivery must never
// dial an unchecked address.
func New(opts Options) (*Client, error) {
	if opts.Guard == nil {
		return nil, eris.New("deliveryhttp: an SSRF guard is required")
	}
	if opts.DNSCache == nil {
		return nil, eris.New("deliveryhttp: a DNS cache is required")
	}
	if opts.RequestTimeout <= 0 {
		opts.RequestTimeout = 30 * time.Second
	}
	if opts.ResponseBodyLimit <= 0 {
		opts.ResponseBodyLimit = DefaultResponseBodyLimit
	}
	if opts.UserAgent == "" {
		opts.UserAgent = UserAgent
	}

	transport := &http.Transport{
		DialContext:           newGuardedDialer(opts.Guard, opts.DNSCache, 10*time.Second).DialContext,
		MaxIdleConns:          20000,
		MaxIdleConnsPerHost:   64, // the default of 2 costs a handshake per delivery
		MaxConnsPerHost:       256,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ForceAttemptHTTP2:     true,
		DisableCompression:    true, // response bodies are never read for content
		TLSClientConfig:       opts.TLSClientConfig,
	}

	return &Client{
		httpClient: &http.Client{
			Transport: transport,
			// The deadline is per request, via context, so time can be attributed to
			// DNS, connect, TLS and first byte separately.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				// Following a redirect to an arbitrary location is an SSRF bypass. The 3xx
				// is recorded as the result instead.
				return http.ErrUseLastResponse
			},
		},
		timeout:   opts.RequestTimeout,
		bodyLimit: opts.ResponseBodyLimit,
		userAgent: opts.UserAgent,
	}, nil
}

// Deliver performs one request and reports what came back. A non-2xx response
// is a result, not an error; only a transport failure sets Err.
func (c *Client) Deliver(ctx context.Context, req dispatch.DeliveryRequest) dispatch.DeliveryResult {
	timeout := req.Timeout
	if timeout <= 0 {
		timeout = c.timeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var reused bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused },
	})

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, req.URL, bytes.NewReader(req.Body))
	if err != nil {
		return dispatch.DeliveryResult{Err: eris.Wrap(err, "build request"), Refused: true}
	}
	// GetBody lets HTTP/2 retry a request without sending an empty body.
	body := req.Body
	httpReq.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	httpReq.ContentLength = int64(len(req.Body))
	httpReq.Header.Set("content-type", "application/json")
	httpReq.Header.Set("user-agent", c.userAgent)
	for name, value := range req.Headers {
		httpReq.Header.Set(name, value)
	}

	started := time.Now()
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		elapsed := time.Since(started)
		return dispatch.DeliveryResult{
			Duration:         elapsed,
			Err:              err,
			Timeout:          isTimeout(err),
			Refused:          isRefused(err),
			ConnectionReused: reused,
		}
	}
	defer func() { _ = resp.Body.Close() }()

	// Read a bounded prefix, then drain: an unbounded read puts a 50MB error
	// page in the database, and an undrained body leaks the connection.
	truncated, readErr := io.ReadAll(io.LimitReader(resp.Body, int64(c.bodyLimit)))
	_, _ = io.Copy(io.Discard, resp.Body)

	result := dispatch.DeliveryResult{
		StatusCode:       resp.StatusCode,
		Body:             string(truncated),
		Duration:         time.Since(started),
		ConnectionReused: reused,
		RetryAfter:       retry.ParseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
	}
	if readErr != nil {
		result.Body = utils.Truncate(result.Body, c.bodyLimit)
	}
	return result
}

// CloseIdleConnections releases pooled connections, used on shutdown.
func (c *Client) CloseIdleConnections() { c.httpClient.CloseIdleConnections() }

func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var timeouter interface{ Timeout() bool }
	return errors.As(err, &timeouter) && timeouter.Timeout()
}

// isRefused reports the failures that mean "this endpoint is not there",
// as opposed to "this endpoint is slow": the lane drops straight to one
// in flight for these.
func isRefused(err error) bool {
	if errors.Is(err, ErrBlockedDestination) {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		var sysErr *os.SyscallError
		if errors.As(opErr.Err, &sysErr) {
			return errors.Is(sysErr.Err, syscall.ECONNREFUSED) ||
				errors.Is(sysErr.Err, syscall.EHOSTUNREACH) ||
				errors.Is(sysErr.Err, syscall.ENETUNREACH)
		}
		return !opErr.Timeout()
	}
	return false
}

var _ dispatch.DeliveryClient = (*Client)(nil)
