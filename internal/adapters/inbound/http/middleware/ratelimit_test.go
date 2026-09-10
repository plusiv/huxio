package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/adapters/inbound/http/handlers"
	appmiddleware "github.com/plusiv/huxio/internal/adapters/inbound/http/middleware"
	"github.com/plusiv/huxio/internal/infrastructure/authctx"
)

// fixedNodes reports a constant peer count.
type fixedNodes int

func (n fixedNodes) Nodes() int { return int(n) }

// newLimitedEcho builds an Echo instance whose single route is rate limited
// and whose requests are already authenticated as the supplied tenant.
func newLimitedEcho(t *testing.T, middleware echo.MiddlewareFunc, orgID string) *echo.Echo {
	t.Helper()

	e := echo.New()
	e.HTTPErrorHandler = handlers.CustomHTTPErrorHandler
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			req := c.Request()
			c.SetRequest(req.WithContext(authctx.Context(req.Context(), authctx.Principal{OrgID: orgID})))
			return next(c)
		}
	})
	e.GET("/probe/:app_id", func(c *echo.Context) error { return c.NoContent(http.StatusOK) }, middleware)
	return e
}

func call(e *echo.Echo, path string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	return recorder
}

func TestRateLimitByOrgRefusesOverBudget(t *testing.T) {
	t.Parallel()

	e := newLimitedEcho(t, appmiddleware.RateLimitByOrg(appmiddleware.RateLimitOptions{
		RequestsPerSecond: 2,
	}), "org_1")

	var allowed, throttled int
	var last *httptest.ResponseRecorder
	for range 20 {
		last = call(e, "/probe/app_1")
		switch last.Code {
		case http.StatusOK:
			allowed++
		case http.StatusTooManyRequests:
			throttled++
		default:
			t.Fatalf("unexpected status %d", last.Code)
		}
	}

	if allowed == 0 || allowed > 4 {
		t.Errorf("allowed %d of 20 requests against a 2/s limit", allowed)
	}
	if throttled == 0 {
		t.Fatal("nothing was throttled")
	}

	// Every response advertises the budget; a 429 also says when to retry.
	for _, header := range []string{"RateLimit-Limit", "RateLimit-Remaining", "RateLimit-Reset"} {
		if last.Header().Get(header) == "" {
			t.Errorf("missing %s header", header)
		}
	}
	if retry := last.Header().Get("Retry-After"); retry == "" {
		t.Error("a 429 must carry Retry-After")
	} else if seconds, err := strconv.Atoi(retry); err != nil || seconds < 1 {
		t.Errorf("Retry-After = %q, want at least one second", retry)
	}
}

func TestRateLimitIsPerTenant(t *testing.T) {
	t.Parallel()

	opts := appmiddleware.RateLimitOptions{RequestsPerSecond: 1}
	limiter := appmiddleware.RateLimitByOrg(opts)

	noisy := newLimitedEcho(t, limiter, "org_noisy")
	quiet := newLimitedEcho(t, limiter, "org_quiet")

	// Exhaust one tenant's budget.
	for range 10 {
		call(noisy, "/probe/app_1")
	}

	// The other tenant is unaffected: one tenant hammering the API must not
	// consume another's share.
	if got := call(quiet, "/probe/app_1"); got.Code != http.StatusOK {
		t.Errorf("the quiet tenant was throttled: %d", got.Code)
	}
}

func TestRateLimitByAppIsPerApplication(t *testing.T) {
	t.Parallel()

	e := newLimitedEcho(t, appmiddleware.RateLimitByApp("app_id", appmiddleware.RateLimitOptions{
		RequestsPerSecond: 1,
	}), "org_1")

	for range 10 {
		call(e, "/probe/app_busy")
	}
	if got := call(e, "/probe/app_quiet"); got.Code != http.StatusOK {
		t.Errorf("a burst on one application throttled another: %d", got.Code)
	}
}

func TestRateLimitDividesTheBudgetAcrossNodes(t *testing.T) {
	t.Parallel()

	// Ten nodes sharing a 20/s cluster limit means 2/s here.
	e := newLimitedEcho(t, appmiddleware.RateLimitByOrg(appmiddleware.RateLimitOptions{
		RequestsPerSecond: 20,
		Nodes:             fixedNodes(10),
	}), "org_1")

	allowed := 0
	for range 20 {
		if call(e, "/probe/app_1").Code == http.StatusOK {
			allowed++
		}
	}
	if allowed > 5 {
		t.Errorf("allowed %d requests; a 20/s cluster limit over 10 nodes is 2/s here", allowed)
	}

	limit := call(e, "/probe/app_1").Header().Get("RateLimit-Limit")
	if limit != "2" {
		t.Errorf("RateLimit-Limit = %q, want the per-node share of 2", limit)
	}
}

func TestRateLimitNeverThrottlesToNothing(t *testing.T) {
	t.Parallel()

	// A cluster far larger than the limit would divide the share below one
	// request per second, which would lock every tenant out.
	e := newLimitedEcho(t, appmiddleware.RateLimitByOrg(appmiddleware.RateLimitOptions{
		RequestsPerSecond: 5,
		Nodes:             fixedNodes(1000),
	}), "org_1")

	if got := call(e, "/probe/app_1"); got.Code != http.StatusOK {
		t.Errorf("status = %d; the per-node share must never fall below one request", got.Code)
	}
}

func TestRateLimitDisabledWhenUnset(t *testing.T) {
	t.Parallel()

	e := newLimitedEcho(t, appmiddleware.RateLimitByOrg(appmiddleware.RateLimitOptions{}), "org_1")

	for range 50 {
		if got := call(e, "/probe/app_1"); got.Code != http.StatusOK {
			t.Fatalf("status = %d; a zero limit means no limiting", got.Code)
		}
	}
}

func TestRateLimitRefillsOverTime(t *testing.T) {
	t.Parallel()

	e := newLimitedEcho(t, appmiddleware.RateLimitByOrg(appmiddleware.RateLimitOptions{
		RequestsPerSecond: 10,
	}), "org_1")

	for range 30 {
		call(e, "/probe/app_1")
	}
	if got := call(e, "/probe/app_1"); got.Code != http.StatusTooManyRequests {
		t.Fatalf("expected to be over budget, got %d", got.Code)
	}

	time.Sleep(150 * time.Millisecond)
	if got := call(e, "/probe/app_1"); got.Code != http.StatusOK {
		t.Errorf("status = %d after waiting; the bucket must refill", got.Code)
	}
}
