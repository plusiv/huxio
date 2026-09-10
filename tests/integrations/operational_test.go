package integration_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/application/dispatch"
	"github.com/plusiv/huxio/internal/application/usecases"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/domain/retry"
)

// newOperationalWorker builds a worker whose engine publishes operational
// webhooks, and an operational use case wired to the same ingest path.
func newOperationalWorker(t *testing.T, api *apiEnv, opts workerOptions) *workerEnv {
	t.Helper()

	worker := newWorkerEnv(t, api, opts)

	ingestUC := usecases.NewIngestUseCase(api.Store, api.Messages, api.Queue, api.Config, worker.codec,
		usecases.IngestConfig{MaxPayloadBytes: testMaxPayloadBytes, DefaultRetentionDays: 90, Pool: "default"},
		usecases.WithApplicationFallback(api.Applications, api.Config))
	appUC := usecases.NewApplicationUseCase(api.Applications, api.Snapshots)
	operational := usecases.NewOperationalUseCase(ingestUC, appUC)

	worker.engine = rebuildEngine(t, api, worker, engineOverrides{
		operational:  operational,
		policy:       opts.policy,
		failingAt:    opts.failingNotifyAttempt,
		disableAfter: opts.disableAfter,
	})
	return worker
}

func TestOperationalWebhooksReportAFailingEndpoint(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	broken := newSink(t, http.StatusInternalServerError)
	// The operational endpoint receives the notifications about the broken one.
	ops := newSink(t, http.StatusOK)

	appID := api.createApp(t, "Operational", nil)
	api.createEndpoint(t, appID, map[string]any{
		"url":         broken.server.URL,
		"filterTypes": []string{"invoice.paid"},
	})

	// The reserved operational application is created on first emit, so the
	// tenant subscribes to it up front by creating it here and attaching an
	// endpoint.
	opsAppResp := api.do(t, request{
		Method: http.MethodPost, Path: "/api/v1/app",
		Body: map[string]any{"name": "Operational webhooks", "uid": usecases.OperationalAppUID},
	})
	if opsAppResp.Status != http.StatusCreated {
		t.Fatalf("create operational app: %d %s", opsAppResp.Status, opsAppResp.Body)
	}
	opsAppID, _ := opsAppResp.field(t, "id").(string)
	api.createEndpoint(t, opsAppID, map[string]any{
		"url": ops.server.URL,
		"filterTypes": []string{
			dispatch.EventAttemptFailing,
			dispatch.EventAttemptRecovered,
			dispatch.EventAttemptExhausted,
		},
	})

	worker := newOperationalWorker(t, api, workerOptions{
		policy: retry.NewPolicy(
			// Six delays, so the fourth attempt lands quickly.
			retry.WithSchedule([]time.Duration{
				10 * time.Millisecond, 10 * time.Millisecond, 10 * time.Millisecond,
				10 * time.Millisecond, 10 * time.Millisecond,
			}),
			retry.WithJitter(retry.NoJitter),
		),
		laneConcurrency:      4,
		breakerThreshold:     100,
		failingNotifyAttempt: 4,
		disableAfter:         time.Hour,
	})
	stop := worker.start(t)
	defer stop()

	worker.ingest(t, appID, "invoice.paid", map[string]any{"a": 1})

	// The failing notice fires partway through the schedule, and the exhausted
	// notice when the attempts run out.
	if !ops.waitFor(2, 20*time.Second) {
		t.Fatalf("operational endpoint received %d notifications, want the failing and exhausted notices", len(ops.requests()))
	}

	seen := map[string]int{}
	for _, req := range ops.requests() {
		body := string(req.Body)
		for _, event := range []string{
			dispatch.EventAttemptFailing,
			dispatch.EventAttemptExhausted,
			dispatch.EventAttemptRecovered,
		} {
			if strings.Contains(body, event) {
				seen[event]++
			}
		}
	}
	if seen[dispatch.EventAttemptFailing] != 1 {
		t.Errorf("failing notices = %d, want exactly one per outage", seen[dispatch.EventAttemptFailing])
	}
	if seen[dispatch.EventAttemptExhausted] == 0 {
		t.Error("no exhausted notice was sent")
	}

	// The notifications are ordinary signed webhooks with their own delivery
	// log, not a second delivery mechanism.
	attempts, err := api.Attempts.GetAttempts(context.Background(), repositories.AttemptFilters{
		AppID: repositories.Eq(opsAppID),
	}, repositories.CursorPagination{Limit: 20})
	if err != nil {
		t.Fatalf("GetAttempts: %v", err)
	}
	if len(attempts.Items) == 0 {
		t.Error("operational webhooks must appear in the delivery log like any other message")
	}
	for _, req := range ops.requests() {
		if req.Headers.Get("webhook-signature") == "" {
			t.Error("operational webhooks must be signed")
		}
	}
}

func TestOperationalWebhookReportsRecovery(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	flaky := newSink(t, http.StatusInternalServerError)
	ops := newSink(t, http.StatusOK)

	appID := api.createApp(t, "Recovery notice", nil)
	api.createEndpoint(t, appID, map[string]any{
		"url":         flaky.server.URL,
		"filterTypes": []string{"invoice.paid"},
	})

	opsAppResp := api.do(t, request{
		Method: http.MethodPost, Path: "/api/v1/app",
		Body: map[string]any{"name": "Operational webhooks", "uid": usecases.OperationalAppUID},
	})
	opsAppID, _ := opsAppResp.field(t, "id").(string)
	api.createEndpoint(t, opsAppID, map[string]any{
		"url":         ops.server.URL,
		"filterTypes": []string{dispatch.EventAttemptFailing, dispatch.EventAttemptRecovered},
	})

	worker := newOperationalWorker(t, api, workerOptions{
		policy: retry.NewPolicy(
			retry.WithSchedule([]time.Duration{
				10 * time.Millisecond, 10 * time.Millisecond, 10 * time.Millisecond,
				10 * time.Millisecond, time.Second, time.Second,
			}),
			retry.WithJitter(retry.NoJitter),
		),
		laneConcurrency:      4,
		breakerThreshold:     100,
		failingNotifyAttempt: 4,
		disableAfter:         time.Hour,
	})
	stop := worker.start(t)
	defer stop()

	worker.ingest(t, appID, "invoice.paid", map[string]any{"a": 1})

	// Wait for the failing notice, then let the endpoint recover.
	deadline := time.Now().Add(20 * time.Second)
	for !containsEvent(ops.requests(), dispatch.EventAttemptFailing) {
		if time.Now().After(deadline) {
			t.Fatal("the failing notice never arrived")
		}
		time.Sleep(50 * time.Millisecond)
	}
	flaky.setStatus(http.StatusOK, nil)

	deadline = time.Now().Add(20 * time.Second)
	for !containsEvent(ops.requests(), dispatch.EventAttemptRecovered) {
		if time.Now().After(deadline) {
			t.Fatal("the recovery notice never arrived")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func containsEvent(requests []receivedRequest, event string) bool {
	for _, req := range requests {
		if strings.Contains(string(req.Body), event) {
			return true
		}
	}
	return false
}
