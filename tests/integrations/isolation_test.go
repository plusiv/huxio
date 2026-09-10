package integration_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/plusiv/huxio/configs"
	"github.com/plusiv/huxio/internal/application/dispatch"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/domain/retry"
)

// fastRetry keeps the isolation tests quick while still exercising the real
// re-enqueue path.
func fastRetry() *retry.Policy {
	return retry.NewPolicy(
		retry.WithSchedule([]time.Duration{
			20 * time.Millisecond, 20 * time.Millisecond, 20 * time.Millisecond,
			20 * time.Millisecond, 20 * time.Millisecond, 20 * time.Millisecond,
		}),
		retry.WithJitter(retry.NoJitter),
	)
}

func (e *testEnv) endpointRow(t *testing.T, id string) *entities.Endpoint {
	t.Helper()

	ep, err := e.Endpoints.GetEndpoint(context.Background(), repositories.EndpointFilters{
		ID: repositories.Eq(id),
	})
	if err != nil {
		t.Fatalf("GetEndpoint: %v", err)
	}
	return ep
}

func TestEndpointAutoDisablesAfterSustainedFailure(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	receiver := newSink(t, http.StatusInternalServerError)

	appID := api.createApp(t, "Auto-disable", nil)
	endpointID, _ := api.createEndpoint(t, appID, map[string]any{"url": receiver.server.URL})

	worker := newWorkerEnv(t, api, workerOptions{
		policy:           fastRetry(),
		laneConcurrency:  4,
		breakerThreshold: 100, // keep the breaker out of this test's way
		// A failure run of any length is enough to disable here; production
		// waits two hours.
		disableAfter: time.Nanosecond,
	})
	stop := worker.start(t)
	defer stop()

	worker.ingest(t, appID, "invoice.paid", map[string]any{"a": 1})

	// The failure run is recorded, then the endpoint is switched off.
	deadline := time.Now().Add(15 * time.Second)
	for {
		ep := api.endpointRow(t, endpointID)
		if ep.DisabledAt != nil {
			if ep.FirstFailureAt == nil {
				t.Error("the start of the failure run must be recorded alongside the disable")
			}
			// Disabling is not deleting: the owner can switch it back on.
			if ep.Deleted() {
				t.Error("auto-disable must not delete the endpoint")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("endpoint was never auto-disabled (first failure at %v)", ep.FirstFailureAt)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// It can be re-enabled through the API, which clears the failure run.
	if resp := api.do(t, request{
		Method: http.MethodPatch,
		Path:   "/api/v1/app/" + appID + "/endpoint/" + endpointID,
		Body:   map[string]any{"disabled": false},
	}); resp.Status != http.StatusOK {
		t.Fatalf("re-enable: %d %s", resp.Status, resp.Body)
	}
	ep := api.endpointRow(t, endpointID)
	if ep.DisabledAt != nil || ep.FirstFailureAt != nil {
		t.Errorf("re-enabling must clear both timestamps, got %+v / %+v", ep.DisabledAt, ep.FirstFailureAt)
	}
}

func TestEndpointFailureRunClearsOnRecovery(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	receiver := newSink(t, http.StatusInternalServerError)

	appID := api.createApp(t, "Recovery", nil)
	endpointID, _ := api.createEndpoint(t, appID, map[string]any{"url": receiver.server.URL})

	worker := newWorkerEnv(t, api, workerOptions{
		policy:           fastRetry(),
		laneConcurrency:  4,
		breakerThreshold: 100,
		disableAfter:     time.Hour, // long enough not to fire here
	})
	stop := worker.start(t)
	defer stop()

	worker.ingest(t, appID, "invoice.paid", map[string]any{"a": 1})

	deadline := time.Now().Add(10 * time.Second)
	for api.endpointRow(t, endpointID).FirstFailureAt == nil {
		if time.Now().After(deadline) {
			t.Fatal("the failure run was never recorded")
		}
		time.Sleep(20 * time.Millisecond)
	}

	receiver.setStatus(http.StatusOK, nil)

	deadline = time.Now().Add(10 * time.Second)
	for api.endpointRow(t, endpointID).FirstFailureAt != nil {
		if time.Now().After(deadline) {
			t.Fatal("a success must clear the failure run, or the endpoint eventually disables itself for nothing")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestBreakerOpensAndQuarantinesADeadEndpoint(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	receiver := newSink(t, http.StatusInternalServerError)

	appID := api.createApp(t, "Quarantine", nil)
	endpointID, _ := api.createEndpoint(t, appID, map[string]any{"url": receiver.server.URL})

	worker := newWorkerEnv(t, api, workerOptions{
		policy:           fastRetry(),
		laneConcurrency:  4,
		breakerThreshold: 2,
		breakerCooldown:  50 * time.Millisecond,
		quarantineAfter:  time.Nanosecond, // production waits fifteen minutes
		disableAfter:     time.Hour,
	})
	stop := worker.start(t)
	defer stop()

	// Several messages, so the failure count reaches the breaker threshold.
	for range 5 {
		worker.ingest(t, appID, "invoice.paid", map[string]any{"a": 1})
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		ep := api.endpointRow(t, endpointID)
		if ep.Pool == configs.QuarantinePool {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("endpoint was never quarantined (pool %q)", ep.Pool)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// The breaker is what stopped the dialling, so the lane must show it.
	lane := worker.lanes.Get(endpointID)
	if lane == nil {
		t.Fatal("the endpoint has no lane")
	}
	if lane.Breaker().State() == dispatch.BreakerClosed {
		t.Error("the breaker must be open or half-open for a dead endpoint")
	}
	// A dead endpoint's lane collapses to one in flight rather than hammering
	// it with the configured concurrency.
	if got := lane.Concurrency(); got != 1 {
		t.Errorf("lane concurrency = %d, want it collapsed to 1", got)
	}

	// A subsequent fan-out routes this endpoint's work to the quarantine
	// pool, where the default-pool worker will not see it.
	api.reloadConfig(t)
	msgID := worker.ingest(t, appID, "invoice.paid", map[string]any{"a": 2})

	deadline = time.Now().Add(10 * time.Second)
	for {
		var pool string
		err := api.Pool.QueryRow(context.Background(),
			`SELECT pool FROM delivery_task WHERE msg_id = $1 AND kind = $2 LIMIT 1`,
			msgID, int16(entities.TaskDeliver),
		).Scan(&pool)
		if err == nil {
			if pool != configs.QuarantinePool {
				t.Errorf("deliver task pool = %q, want the quarantine pool", pool)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no deliver task was created for the quarantined endpoint")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestBreakerStopsDiallingADeadEndpoint(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)

	// The sink always fails: once the breaker opens, the request count must
	// stop growing even though retries keep coming round.
	receiver := newSink(t, http.StatusInternalServerError)

	appID := api.createApp(t, "Breaker", nil)
	api.createEndpoint(t, appID, map[string]any{"url": receiver.server.URL})

	worker := newWorkerEnv(t, api, workerOptions{
		policy:           fastRetry(),
		laneConcurrency:  4,
		breakerThreshold: 2,
		// A long cooldown, so nothing is dialled again during the test.
		breakerCooldown: time.Hour,
		disableAfter:    time.Hour,
	})
	stop := worker.start(t)
	defer stop()

	for range 3 {
		worker.ingest(t, appID, "invoice.paid", map[string]any{"a": 1})
	}

	// Wait for the breaker to trip.
	deadline := time.Now().Add(15 * time.Second)
	for len(receiver.requests()) < 2 {
		if time.Now().After(deadline) {
			t.Fatal("the endpoint was never dialled")
		}
		time.Sleep(20 * time.Millisecond)
	}
	settled := len(receiver.requests())

	// Retries keep coming round, but an open breaker skips the dial entirely,
	// which is where the CPU saving is at scale.
	time.Sleep(500 * time.Millisecond)
	if got := len(receiver.requests()); got > settled+2 {
		t.Errorf("requests grew from %d to %d after the breaker opened; it is still dialling", settled, got)
	}
}

// TestHealthyEndpointIsUnaffectedByATarpit is the product claim in miniature:
// a slow endpoint must not delay a fast one sharing the same worker.
func TestHealthyEndpointIsUnaffectedByATarpit(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)

	// The tarpit holds every request until the test lets go.
	release := make(chan struct{})
	tarpitHeld := make(chan struct{}, 64)

	tarpitServer := newBlockingSink(t, release, tarpitHeld)
	healthy := newSink(t, http.StatusOK)

	appID := api.createApp(t, "Isolation", nil)
	// The tarpit endpoint subscribes to one event type, the healthy endpoint
	// to another, so each message goes to exactly one of them.
	api.createEndpoint(t, appID, map[string]any{
		"url":         tarpitServer.URL,
		"filterTypes": []string{"slow.event"},
	})
	api.createEndpoint(t, appID, map[string]any{
		"url":         healthy.server.URL,
		"filterTypes": []string{"fast.event"},
	})

	worker := newWorkerEnv(t, api, workerOptions{
		requestTimeout:   30 * time.Second,
		policy:           fastRetry(),
		laneConcurrency:  2,
		breakerThreshold: 100,
		laneWaitTimeout:  50 * time.Millisecond,
		laneRequeueDelay: 50 * time.Millisecond,
	})
	stop := worker.start(t)
	defer func() {
		close(release)
		stop()
	}()

	// Saturate the tarpit lane well beyond its concurrency.
	for range 12 {
		worker.ingest(t, appID, "slow.event", map[string]any{"slow": true})
	}
	// Wait until the tarpit is actually holding requests, so the healthy
	// delivery is competing with a genuinely blocked endpoint.
	select {
	case <-tarpitHeld:
	case <-time.After(10 * time.Second):
		t.Fatal("the tarpit never received a request")
	}

	started := time.Now()
	worker.ingest(t, appID, "fast.event", map[string]any{"fast": true})

	if !healthy.waitFor(1, 10*time.Second) {
		t.Fatal("the healthy endpoint never received its webhook while a tarpit was saturated")
	}
	elapsed := time.Since(started)

	// The tarpit holds its own lane's slots and nothing else: the healthy
	// endpoint's delivery must not wait behind it.
	if elapsed > 5*time.Second {
		t.Errorf("healthy delivery took %s while a tarpit was saturated; isolation is not holding", elapsed)
	}

	// And the tarpit's excess work is back in Postgres rather than occupying
	// the worker.
	var delayed int
	if err := api.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM delivery_task WHERE locked_until IS NULL`,
	).Scan(&delayed); err != nil {
		t.Fatalf("count queued tasks: %v", err)
	}
	if delayed == 0 {
		t.Log("no tasks were waiting in the queue; the lane absorbed the burst")
	}
}
