package integration_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/plusiv/huxio/configs"
	inboundhttp "github.com/plusiv/huxio/internal/adapters/inbound/http"
	"github.com/plusiv/huxio/internal/adapters/inbound/http/handlers"
	"github.com/plusiv/huxio/internal/application/usecases"
	"github.com/plusiv/huxio/internal/domain/entities"
)

func TestAPIAdminRoutes(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	appID := api.createApp(t, "Admin", nil)
	endpointID, _ := api.createEndpoint(t, appID, map[string]any{"url": "https://example.test/hook"})

	t.Run("rescue stuck", func(t *testing.T) {
		resp := api.do(t, request{Method: http.MethodPost, Path: "/api/v1/admin/rescue-stuck"})
		if resp.Status != http.StatusOK {
			t.Fatalf("rescue-stuck = %d %s", resp.Status, resp.Body)
		}
		var body struct {
			Rescued int64 `json:"rescued"`
		}
		resp.decode(t, &body)
		if body.Rescued != 0 {
			t.Errorf("rescued = %d with nothing stuck, want 0", body.Rescued)
		}
	})

	t.Run("queue stats", func(t *testing.T) {
		resp := api.do(t, request{Method: http.MethodGet, Path: "/api/v1/admin/queue"})
		if resp.Status != http.StatusOK {
			t.Fatalf("admin queue = %d %s", resp.Status, resp.Body)
		}
	})

	t.Run("partition leases", func(t *testing.T) {
		// Materialise the lease rows the way a worker would.
		if _, err := api.Leases.ClaimPartitions(t.Context(), "default", "wkr_admin", 15*time.Second, 4); err != nil {
			t.Fatalf("ClaimPartitions: %v", err)
		}

		resp := api.do(t, request{Method: http.MethodGet, Path: "/api/v1/admin/partitions?pool=default"})
		if resp.Status != http.StatusOK {
			t.Fatalf("admin partitions = %d %s", resp.Status, resp.Body)
		}
		var leases []struct {
			PartitionKey int16   `json:"partitionKey"`
			Pool         string  `json:"pool"`
			OwnerID      *string `json:"ownerId"`
		}
		resp.decode(t, &leases)
		if len(leases) != entities.QueuePartitions {
			t.Fatalf("returned %d leases, want %d", len(leases), entities.QueuePartitions)
		}
		owned := 0
		for _, lease := range leases {
			if lease.OwnerID != nil && *lease.OwnerID == "wkr_admin" {
				owned++
			}
		}
		if owned != 4 {
			t.Errorf("%d partitions shown as owned, want 4", owned)
		}
	})

	t.Run("move pool", func(t *testing.T) {
		resp := api.do(t, request{
			Method: http.MethodPost, Path: "/api/v1/admin/pool",
			Body: map[string]any{"appId": appID, "endpointId": endpointID, "pool": configs.QuarantinePool},
		})
		if resp.Status != http.StatusNoContent {
			t.Fatalf("move pool = %d %s", resp.Status, resp.Body)
		}
		if got := api.endpointRow(t, endpointID).Pool; got != configs.QuarantinePool {
			t.Errorf("pool = %q, want the quarantine pool", got)
		}

		// And back out again, which is how an operator releases an endpoint
		// its owner has fixed.
		if back := api.do(t, request{
			Method: http.MethodPost, Path: "/api/v1/admin/pool",
			Body: map[string]any{"appId": appID, "endpointId": endpointID, "pool": configs.DefaultPool},
		}); back.Status != http.StatusNoContent {
			t.Fatalf("move pool back = %d %s", back.Status, back.Body)
		}
		if got := api.endpointRow(t, endpointID).Pool; got != configs.DefaultPool {
			t.Errorf("pool = %q, want the default pool", got)
		}
	})

	t.Run("re-enable endpoint", func(t *testing.T) {
		now := time.Now().UTC()
		if err := api.Endpoints.SetDisabled(t.Context(), endpointID, &now); err != nil {
			t.Fatalf("SetDisabled: %v", err)
		}
		if err := api.Endpoints.SetFirstFailure(t.Context(), endpointID, &now); err != nil {
			t.Fatalf("SetFirstFailure: %v", err)
		}

		resp := api.do(t, request{
			Method: http.MethodPost, Path: "/api/v1/admin/endpoint/enable",
			Body: map[string]any{"appId": appID, "endpointId": endpointID},
		})
		if resp.Status != http.StatusNoContent {
			t.Fatalf("re-enable = %d %s", resp.Status, resp.Body)
		}

		ep := api.endpointRow(t, endpointID)
		if ep.DisabledAt != nil || ep.FirstFailureAt != nil {
			t.Errorf("re-enable must clear both timestamps, got %v / %v", ep.DisabledAt, ep.FirstFailureAt)
		}
	})

	t.Run("validation", func(t *testing.T) {
		for _, body := range []map[string]any{
			{"appId": appID, "endpointId": endpointID},
			{"appId": appID, "pool": "default"},
			{"endpointId": endpointID, "pool": "default"},
		} {
			if resp := api.do(t, request{Method: http.MethodPost, Path: "/api/v1/admin/pool", Body: body}); resp.Status != http.StatusUnprocessableEntity {
				t.Errorf("move pool %v = %d %s, want 422", body, resp.Status, resp.Body)
			}
		}
	})

	t.Run("portal tokens are refused", func(t *testing.T) {
		portal := api.portalToken(t, appID)
		for _, req := range []request{
			{Method: http.MethodPost, Path: "/api/v1/admin/rescue-stuck", Token: portal},
			{Method: http.MethodGet, Path: "/api/v1/admin/partitions", Token: portal},
			{
				Method: http.MethodPost, Path: "/api/v1/admin/pool", Token: portal,
				Body: map[string]any{"appId": appID, "endpointId": endpointID, "pool": "quarantine"},
			},
		} {
			resp := api.do(t, req)
			if resp.Status != http.StatusForbidden {
				t.Errorf("%s %s with a portal token = %d %s, want 403", req.Method, req.Path, resp.Status, resp.Body)
			}
		}
	})
}

func TestAPIRateLimiting(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)

	// A router of its own with a limit low enough to trip deterministically.
	limited := inboundhttp.NewRouter(inboundhttp.RouterDeps{
		Config: inboundhttp.RouterConfig{
			MaxPayloadBytes:  testMaxPayloadBytes,
			CORSAllowOrigins: []string{"*"},
			RateLimitOrgRPS:  2,
			RateLimitAppRPS:  2,
			Pools:            []string{"default"},
		},
		Metrics:            api.Metrics,
		TokenParser:        api.Tokens,
		IdempotencyRepo:    api.Idempotency,
		HealthHandler:      handlers.NewHealthHandler(api.Store, api.Config, "test"),
		ApplicationHandler: handlers.NewApplicationHandler(usecases.NewApplicationUseCase(api.Applications, api.Snapshots)),
		EndpointHandler: handlers.NewEndpointHandler(
			usecases.NewEndpointUseCase(api.Endpoints, api.Sealer, api.Snapshots),
			usecases.NewApplicationUseCase(api.Applications, api.Snapshots),
		),
		EventTypeHandler:   handlers.NewEventTypeHandler(usecases.NewEventTypeUseCase(api.EventTypes, api.Snapshots)),
		MessageHandler:     handlers.NewMessageHandler(nil, nil, api.Metrics),
		AttemptHandler:     handlers.NewAttemptHandler(nil),
		PortalTokenHandler: handlers.NewPortalHandler(nil),
		AdminHandler:       handlers.NewAdminHandler(nil, nil),
		StreamHandler:      handlers.NewStreamHandler(nil),
	})

	// Burst through the budget: the limiter allows a couple, then refuses.
	var (
		allowed   int
		throttled int
		headers   http.Header
	)
	for range 20 {
		recorder := api.doAgainst(t, limited, request{Method: http.MethodGet, Path: "/api/v1/app"})
		switch recorder.Status {
		case http.StatusOK:
			allowed++
		case http.StatusTooManyRequests:
			throttled++
			headers = recorder.Header
		default:
			t.Fatalf("unexpected status %d: %s", recorder.Status, recorder.Body)
		}
	}

	if allowed == 0 {
		t.Fatal("the rate limiter refused everything")
	}
	if throttled == 0 {
		t.Fatalf("20 requests against a 2/s limit produced no 429s (allowed %d)", allowed)
	}

	// The client is told what the limit is and when to come back.
	for _, header := range []string{"RateLimit-Limit", "RateLimit-Remaining", "RateLimit-Reset", "Retry-After"} {
		if headers.Get(header) == "" {
			t.Errorf("a 429 must carry %s", header)
		}
	}

	// Another tenant has its own budget: one noisy tenant must not throttle
	// everyone else.
	other := api.doAgainst(t, limited, request{
		Method: http.MethodGet, Path: "/api/v1/app", Token: api.otherOrgToken(t),
	})
	if other.Status != http.StatusOK {
		t.Errorf("another tenant was throttled by the first tenant's traffic: %d %s", other.Status, other.Body)
	}
}
