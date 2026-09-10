package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	inboundhttp "github.com/plusiv/huxio/internal/adapters/inbound/http"
	"github.com/plusiv/huxio/internal/adapters/inbound/http/handlers"
	"github.com/plusiv/huxio/internal/adapters/inbound/http/portal"
	"github.com/plusiv/huxio/internal/adapters/outbound/persistence/postgres"
	"github.com/plusiv/huxio/internal/application/config"
	"github.com/plusiv/huxio/internal/application/usecases"
	"github.com/plusiv/huxio/internal/infrastructure/auth"
	"github.com/plusiv/huxio/internal/infrastructure/ids"
	"github.com/plusiv/huxio/internal/infrastructure/payload"
	"github.com/plusiv/huxio/internal/infrastructure/secrets"
	"github.com/plusiv/huxio/internal/infrastructure/telemetry"
)

// apiEnv is a fully wired API on top of an isolated database, exercised
// through the real router so middleware order and auth are covered too.
type apiEnv struct {
	*testEnv

	Router  http.Handler
	Tokens  *auth.Manager
	Config  *config.Manager
	Metrics *telemetry.Metrics
	Sealer  *secrets.Sealer

	OrgID string
	Token string
}

const (
	testMaxPayloadBytes = 4096
	// High enough that the functional tests never trip it; the rate limit
	// test builds its own router instead.
	testRateLimitRPS = 10000
)

func newAPIEnv(t *testing.T) *apiEnv {
	t.Helper()

	env := newTestEnv(t)
	ctx := context.Background()

	key, err := secrets.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	sealer, err := secrets.NewSealer([]string{key})
	if err != nil {
		t.Fatalf("NewSealer: %v", err)
	}
	codec, err := payload.NewCodec(512)
	if err != nil {
		t.Fatalf("NewCodec: %v", err)
	}
	t.Cleanup(codec.Close)

	tokens, err := auth.NewManager(auth.Config{Algorithm: "HS256", Secret: "integration-secret", Issuer: "huxio"})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	metrics := telemetry.New()
	configManager := config.NewManager(env.Snapshots, postgres.NewListener(env.Pool), sealer, metrics.ConfigSnapshotAge,
		config.ManagerOptions{
			Channel: postgres.ConfigNotifyChannel,
			// Short, so a test that relies on the snapshot catching up does not wait a
			// minute for it.
			RefreshInterval: 250 * time.Millisecond,
			Debounce:        20 * time.Millisecond,
		})
	if err := configManager.Load(ctx); err != nil {
		t.Fatalf("config Load: %v", err)
	}

	// The manager runs for the life of the test, as it does in every real
	// process: an invalidation with nobody draining it never reloads, and a
	// worker that defers work until the snapshot catches up would wait forever.
	managerCtx, stopManager := context.WithCancel(context.Background())
	managerDone := make(chan error, 1)
	go func() { managerDone <- configManager.Run(managerCtx) }()
	t.Cleanup(func() {
		stopManager()
		select {
		case <-managerDone:
		case <-time.After(5 * time.Second):
			t.Error("the config manager did not stop")
		}
	})

	ingestUC := usecases.NewIngestUseCase(env.Store, env.Messages, env.Queue, configManager, codec,
		usecases.IngestConfig{MaxPayloadBytes: testMaxPayloadBytes, DefaultRetentionDays: 90, Pool: "default"},
		usecases.WithApplicationFallback(env.Applications, configManager))
	messageUC := usecases.NewMessageUseCase(env.Messages, configManager, env.Applications, configManager)
	appUC := usecases.NewApplicationUseCase(env.Applications, env.Snapshots)
	endpointUC := usecases.NewEndpointUseCase(env.Endpoints, sealer, env.Snapshots)
	eventTypeUC := usecases.NewEventTypeUseCase(env.EventTypes, env.Snapshots)

	attemptUC := usecases.NewAttemptUseCase(
		env.Attempts, env.Endpoints, env.Messages, env.Queue,
		configManager, env.Applications, configManager, "default",
	)
	portalUC := usecases.NewPortalUseCase(tokens, configManager, env.Applications, configManager,
		"http://portal.test", time.Hour)
	adminUC := usecases.NewAdminUseCase(env.Queue, env.Leases, env.Endpoints,
		configManager, env.Applications, configManager, env.Snapshots)

	portalUI, err := portal.New(endpointUC, attemptUC, eventTypeUC, ingestUC, false)
	if err != nil {
		t.Fatalf("portal.New: %v", err)
	}

	health := handlers.NewHealthHandler(env.Store, configManager, "test")
	router := inboundhttp.NewRouter(inboundhttp.RouterDeps{
		Config: inboundhttp.RouterConfig{
			MaxPayloadBytes:  testMaxPayloadBytes,
			CORSAllowOrigins: []string{"*"},
			RateLimitOrgRPS:  testRateLimitRPS,
			RateLimitAppRPS:  testRateLimitRPS,
			Pools:            []string{"default"},
		},
		Metrics:            metrics,
		TokenParser:        tokens,
		IdempotencyRepo:    env.Idempotency,
		HealthHandler:      health,
		ApplicationHandler: handlers.NewApplicationHandler(appUC),
		EndpointHandler:    handlers.NewEndpointHandler(endpointUC, appUC),
		EventTypeHandler:   handlers.NewEventTypeHandler(eventTypeUC),
		MessageHandler:     handlers.NewMessageHandler(ingestUC, messageUC, metrics),
		AttemptHandler:     handlers.NewAttemptHandler(attemptUC),
		PortalTokenHandler: handlers.NewPortalHandler(portalUC),
		AdminHandler:       handlers.NewAdminHandler(adminUC, []string{"default"}),
		StreamHandler:      handlers.NewStreamHandler(attemptUC),
		PortalUI:           portalUI,
	})

	org := env.seedOrg(t, "Integration tenant")
	token, _, err := tokens.IssueOrgToken(org.ID, time.Hour)
	if err != nil {
		t.Fatalf("IssueOrgToken: %v", err)
	}

	api := &apiEnv{
		testEnv: env,
		Router:  router,
		Tokens:  tokens,
		Config:  configManager,
		Metrics: metrics,
		Sealer:  sealer,
		OrgID:   org.ID,
		Token:   token,
	}
	// Pick up the seeded organization.
	api.reloadConfig(t)
	return api
}

// reloadConfig rebuilds the snapshot, standing in for the LISTEN-driven reload
// a running process would do.
func (e *apiEnv) reloadConfig(t *testing.T) {
	t.Helper()
	if err := e.Config.Load(context.Background()); err != nil {
		t.Fatalf("reload config snapshot: %v", err)
	}
}

// portalToken mints a token scoped to one application.
func (e *apiEnv) portalToken(t *testing.T, appID string) string {
	t.Helper()
	token, _, err := e.Tokens.IssuePortalToken(appID, e.OrgID, time.Hour)
	if err != nil {
		t.Fatalf("IssuePortalToken: %v", err)
	}
	return token
}

// otherOrgToken mints a token for a different tenant.
func (e *apiEnv) otherOrgToken(t *testing.T) string {
	t.Helper()
	token, _, err := e.Tokens.IssueOrgToken(ids.New(ids.PrefixOrganization), time.Hour)
	if err != nil {
		t.Fatalf("IssueOrgToken: %v", err)
	}
	return token
}

// request is one HTTP call against the router.
type request struct {
	Method  string
	Path    string
	Body    any
	Token   string
	Headers map[string]string
	// NoAuth omits the Authorization header entirely.
	NoAuth bool
}

// response is the recorded reply, with helpers for reading JSON out of it.
type response struct {
	Status int
	Body   []byte
	Header http.Header
}

// decode unmarshals the response body into target.
func (r response) decode(t *testing.T, target any) {
	t.Helper()
	if err := json.Unmarshal(r.Body, target); err != nil {
		t.Fatalf("decode response %q: %v", r.Body, err)
	}
}

// field reads one top-level field out of a JSON object response.
func (r response) field(t *testing.T, name string) any {
	t.Helper()
	var object map[string]any
	r.decode(t, &object)
	return object[name]
}

// do performs a request against the router.
func (e *apiEnv) do(t *testing.T, req request) response {
	t.Helper()

	var body io.Reader
	if req.Body != nil {
		switch typed := req.Body.(type) {
		case string:
			body = bytes.NewBufferString(typed)
		case []byte:
			body = bytes.NewBuffer(typed)
		default:
			encoded, err := json.Marshal(typed)
			if err != nil {
				t.Fatalf("encode request body: %v", err)
			}
			body = bytes.NewBuffer(encoded)
		}
	}

	httpReq := httptest.NewRequest(req.Method, req.Path, body)
	if req.Body != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}
	if !req.NoAuth {
		token := req.Token
		if token == "" {
			token = e.Token
		}
		httpReq.Header.Set("Authorization", "Bearer "+token)
	}
	for name, value := range req.Headers {
		httpReq.Header.Set(name, value)
	}

	recorder := httptest.NewRecorder()
	e.Router.ServeHTTP(recorder, httpReq)

	return response{
		Status: recorder.Code,
		Body:   recorder.Body.Bytes(),
		Header: recorder.Header(),
	}
}

// createApp creates an application through the API and returns its id.
func (e *apiEnv) createApp(t *testing.T, name string, uid *string) string {
	t.Helper()
	body := map[string]any{"name": name}
	if uid != nil {
		body["uid"] = *uid
	}
	resp := e.do(t, request{Method: http.MethodPost, Path: "/api/v1/app", Body: body})
	if resp.Status != http.StatusCreated {
		t.Fatalf("create application: %d %s", resp.Status, resp.Body)
	}
	id, _ := resp.field(t, "id").(string)
	if id == "" {
		t.Fatalf("create application returned no id: %s", resp.Body)
	}
	e.reloadConfig(t)
	return id
}

// createEndpointWithoutReload creates an endpoint and deliberately does not
// refresh the config snapshot, so tests can exercise what a worker does with a
// snapshot that predates the endpoint.
func (e *apiEnv) createEndpointWithoutReload(t *testing.T, appID string, body map[string]any) string {
	t.Helper()

	resp := e.do(t, request{
		Method: http.MethodPost,
		Path:   "/api/v1/app/" + appID + "/endpoint",
		Body:   body,
	})
	if resp.Status != http.StatusCreated {
		t.Fatalf("create endpoint: %d %s", resp.Status, resp.Body)
	}
	id, _ := resp.field(t, "id").(string)
	return id
}

// createEndpoint creates an endpoint through the API and returns its id and
// plaintext secret.
func (e *apiEnv) createEndpoint(t *testing.T, appID string, body map[string]any) (string, string) {
	t.Helper()
	resp := e.do(t, request{
		Method: http.MethodPost,
		Path:   "/api/v1/app/" + appID + "/endpoint",
		Body:   body,
	})
	if resp.Status != http.StatusCreated {
		t.Fatalf("create endpoint: %d %s", resp.Status, resp.Body)
	}
	var decoded struct {
		ID     string `json:"id"`
		Secret string `json:"secret"`
	}
	resp.decode(t, &decoded)
	e.reloadConfig(t)
	return decoded.ID, decoded.Secret
}

// doAgainst performs a request against a specific handler, for tests that
// build their own router.
func (e *apiEnv) doAgainst(t *testing.T, handler http.Handler, req request) response {
	t.Helper()

	original := e.Router
	e.Router = handler
	defer func() { e.Router = original }()

	return e.do(t, req)
}
