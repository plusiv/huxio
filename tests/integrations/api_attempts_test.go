package integration_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/utils"
)

func TestAPIAttemptListingAndFilters(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	appID := api.createApp(t, "Attempts", nil)
	endpointID, _ := api.createEndpoint(t, appID, map[string]any{"url": "https://example.test/hook"})
	otherEndpoint, _ := api.createEndpoint(t, appID, map[string]any{"url": "https://example.test/other"})

	app, err := api.Applications.GetApplication(context.Background(), repositories.ApplicationFilters{
		ID: repositories.Eq(appID),
	})
	if err != nil {
		t.Fatalf("GetApplication: %v", err)
	}
	msg := api.seedMessage(t, app, "invoice.paid")

	// One success and one permanent failure to the first endpoint, and a
	// pending retry to the second.
	api.seedAttempt(t, msg, api.endpointRow(t, endpointID))
	api.seedAttempt(t, msg, api.endpointRow(t, endpointID), func(a *entities.DeliveryAttempt) {
		a.Status = entities.AttemptFailed
		a.ResponseStatusCode = 500
	})
	api.seedAttempt(t, msg, api.endpointRow(t, otherEndpoint), func(a *entities.DeliveryAttempt) {
		a.Status = entities.AttemptPendingRetry
		a.ResponseStatusCode = 429
		a.NextAttemptAt = utils.Ptr(time.Now().UTC().Add(time.Minute))
	})

	type listed struct {
		Data []struct {
			ID           string `json:"id"`
			EndpointID   string `json:"endpointId"`
			Status       int    `json:"status"`
			StatusText   string `json:"statusText"`
			ResponseCode int    `json:"responseStatusCode"`
		} `json:"data"`
		Done bool `json:"done"`
	}

	tests := []struct {
		name  string
		path  string
		count int
		check func(t *testing.T, body listed)
	}{
		{
			name:  "by message",
			path:  "/api/v1/app/" + appID + "/attempt/msg/" + msg.ID,
			count: 3,
		},
		{
			name:  "by endpoint",
			path:  "/api/v1/app/" + appID + "/attempt/endpoint/" + endpointID,
			count: 2,
			check: func(t *testing.T, body listed) {
				for _, item := range body.Data {
					if item.EndpointID != endpointID {
						t.Errorf("attempt for endpoint %s leaked into another endpoint's log", item.EndpointID)
					}
				}
			},
		},
		{
			name:  "by message and endpoint",
			path:  "/api/v1/app/" + appID + "/msg/" + msg.ID + "/endpoint/" + otherEndpoint + "/attempt",
			count: 1,
		},
		{
			name:  "status filter by name",
			path:  "/api/v1/app/" + appID + "/attempt/msg/" + msg.ID + "?status=fail",
			count: 1,
			check: func(t *testing.T, body listed) {
				if body.Data[0].StatusText != "fail" {
					t.Errorf("statusText = %q", body.Data[0].StatusText)
				}
			},
		},
		{
			name:  "status filter by number",
			path:  "/api/v1/app/" + appID + "/attempt/msg/" + msg.ID + "?status=0",
			count: 1,
		},
		{
			name:  "status code class filter",
			path:  "/api/v1/app/" + appID + "/attempt/msg/" + msg.ID + "?statusCodeClass=4",
			count: 1,
			check: func(t *testing.T, body listed) {
				if body.Data[0].ResponseCode != 429 {
					t.Errorf("responseStatusCode = %d", body.Data[0].ResponseCode)
				}
			},
		},
		{
			name:  "future window excludes everything",
			path:  "/api/v1/app/" + appID + "/attempt/msg/" + msg.ID + "?after=" + time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
			count: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := api.do(t, request{Method: http.MethodGet, Path: tc.path})
			if resp.Status != http.StatusOK {
				t.Fatalf("GET %s = %d %s", tc.path, resp.Status, resp.Body)
			}
			var body listed
			resp.decode(t, &body)
			if len(body.Data) != tc.count {
				t.Fatalf("returned %d attempts, want %d (%s)", len(body.Data), tc.count, resp.Body)
			}
			if tc.check != nil {
				tc.check(t, body)
			}
		})
	}

	t.Run("invalid status is rejected", func(t *testing.T) {
		resp := api.do(t, request{
			Method: http.MethodGet,
			Path:   "/api/v1/app/" + appID + "/attempt/msg/" + msg.ID + "?status=maybe",
		})
		if resp.Status != http.StatusUnprocessableEntity {
			t.Errorf("status = %d %s, want 422", resp.Status, resp.Body)
		}
	})

	t.Run("another tenant sees nothing", func(t *testing.T) {
		resp := api.do(t, request{
			Method: http.MethodGet,
			Path:   "/api/v1/app/" + appID + "/attempt/msg/" + msg.ID,
			Token:  api.otherOrgToken(t),
		})
		if resp.Status != http.StatusNotFound {
			t.Errorf("status = %d %s, want 404", resp.Status, resp.Body)
		}
	})
}

func TestAPIEndpointStats(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	appID := api.createApp(t, "Stats", nil)
	endpointID, _ := api.createEndpoint(t, appID, map[string]any{"url": "https://example.test/hook"})

	app, err := api.Applications.GetApplication(context.Background(), repositories.ApplicationFilters{
		ID: repositories.Eq(appID),
	})
	if err != nil {
		t.Fatalf("GetApplication: %v", err)
	}
	msg := api.seedMessage(t, app, "invoice.paid")
	ep := api.endpointRow(t, endpointID)

	api.seedAttempt(t, msg, ep)
	api.seedAttempt(t, msg, ep)
	api.seedAttempt(t, msg, ep, func(a *entities.DeliveryAttempt) { a.Status = entities.AttemptFailed })
	api.seedAttempt(t, msg, ep, func(a *entities.DeliveryAttempt) { a.Status = entities.AttemptPendingRetry })

	resp := api.do(t, request{
		Method: http.MethodGet,
		Path:   "/api/v1/app/" + appID + "/endpoint/" + endpointID + "/stats",
	})
	if resp.Status != http.StatusOK {
		t.Fatalf("GET stats = %d %s", resp.Status, resp.Body)
	}

	var stats struct {
		Success int64 `json:"success"`
		Pending int64 `json:"pending"`
		Fail    int64 `json:"fail"`
	}
	resp.decode(t, &stats)
	if stats.Success != 2 || stats.Fail != 1 || stats.Pending != 1 {
		t.Errorf("stats = %+v, want 2 success, 1 fail, 1 pending", stats)
	}
}

func TestAPIResendQueuesOneManualDelivery(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	receiver := newSink(t, http.StatusOK)

	appID := api.createApp(t, "Resend", nil)
	endpointID, _ := api.createEndpoint(t, appID, map[string]any{"url": receiver.server.URL})

	worker := newWorkerEnv(t, api, workerOptions{})
	msgID := worker.ingest(t, appID, "invoice.paid", map[string]any{"a": 1})

	stop := worker.start(t)
	defer stop()

	if !receiver.waitFor(1, 10*time.Second) {
		t.Fatal("the first delivery never arrived")
	}
	api.waitForAttempts(t, msgID, 1, 10*time.Second)

	// Ask for it again by hand.
	resp := api.do(t, request{
		Method: http.MethodPost,
		Path:   "/api/v1/app/" + appID + "/msg/" + msgID + "/endpoint/" + endpointID + "/resend",
	})
	if resp.Status != http.StatusAccepted {
		t.Fatalf("resend = %d %s, want 202", resp.Status, resp.Body)
	}

	if !receiver.waitFor(2, 10*time.Second) {
		t.Fatal("the resent delivery never arrived")
	}
	attempts := api.waitForAttempts(t, msgID, 2, 10*time.Second)

	var manual bool
	for _, attempt := range attempts {
		if attempt.TriggerType == entities.TriggerManual {
			manual = true
		}
	}
	if !manual {
		t.Error("the resent delivery must be recorded as a manual trigger, so it is not retried on failure")
	}

	// An unknown message is a 404, not a silently queued nothing.
	if unknown := api.do(t, request{
		Method: http.MethodPost,
		Path:   "/api/v1/app/" + appID + "/msg/msg_unknown/endpoint/" + endpointID + "/resend",
	}); unknown.Status != http.StatusNotFound {
		t.Errorf("resend of an unknown message = %d %s, want 404", unknown.Status, unknown.Body)
	}
}

func TestAPIRecoverReplaysWhatFailed(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	receiver := newSink(t, http.StatusInternalServerError)

	appID := api.createApp(t, "Recover", nil)
	endpointID, _ := api.createEndpoint(t, appID, map[string]any{"url": receiver.server.URL})

	// One attempt only, so the failure is terminal and recoverable.
	policy := fastRetry()
	worker := newWorkerEnv(t, api, workerOptions{policy: policy})
	stop := worker.start(t)
	defer stop()

	since := time.Now().UTC().Add(-time.Minute)
	msgID := worker.ingest(t, appID, "invoice.paid", map[string]any{"a": 1})

	// Wait for the retries to be exhausted, leaving a terminal failure.
	deadline := time.Now().Add(20 * time.Second)
	for {
		attempts := api.waitForAttempts(t, msgID, 1, 10*time.Second)
		terminal := false
		for _, attempt := range attempts {
			if attempt.Status == entities.AttemptFailed {
				terminal = true
			}
		}
		if terminal {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the delivery never failed permanently")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// The endpoint recovers, and its owner asks for everything that failed.
	receiver.setStatus(http.StatusOK, nil)
	before := len(receiver.requests())

	resp := api.do(t, request{
		Method: http.MethodPost,
		Path:   "/api/v1/app/" + appID + "/endpoint/" + endpointID + "/recover",
		Body:   map[string]any{"since": since.Format(time.RFC3339)},
	})
	if resp.Status != http.StatusAccepted {
		t.Fatalf("recover = %d %s, want 202", resp.Status, resp.Body)
	}
	var recovered struct {
		Enqueued  int  `json:"enqueued"`
		Truncated bool `json:"truncated"`
	}
	resp.decode(t, &recovered)
	if recovered.Enqueued != 1 {
		t.Fatalf("recover enqueued %d deliveries, want 1", recovered.Enqueued)
	}

	if !receiver.waitFor(before+1, 10*time.Second) {
		t.Fatal("the recovered delivery never arrived")
	}

	// A window in the future recovers nothing.
	empty := api.do(t, request{
		Method: http.MethodPost,
		Path:   "/api/v1/app/" + appID + "/endpoint/" + endpointID + "/recover",
		Body:   map[string]any{"since": time.Now().UTC().Add(time.Hour).Format(time.RFC3339)},
	})
	empty.decode(t, &recovered)
	if recovered.Enqueued != 0 {
		t.Errorf("a future window recovered %d deliveries", recovered.Enqueued)
	}

	// And a nonsensical window is rejected rather than quietly ignored.
	if bad := api.do(t, request{
		Method: http.MethodPost,
		Path:   "/api/v1/app/" + appID + "/endpoint/" + endpointID + "/recover",
		Body: map[string]any{
			"since": time.Now().UTC().Format(time.RFC3339),
			"until": time.Now().UTC().Add(-time.Hour).Format(time.RFC3339),
		},
	}); bad.Status != http.StatusUnprocessableEntity {
		t.Errorf("inverted window = %d %s, want 422", bad.Status, bad.Body)
	}
}

func TestAPIBulkReplayAcrossEndpoints(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	first := newSink(t, http.StatusInternalServerError)
	second := newSink(t, http.StatusInternalServerError)

	appID := api.createApp(t, "Replay", nil)
	api.createEndpoint(t, appID, map[string]any{"url": first.server.URL})
	api.createEndpoint(t, appID, map[string]any{"url": second.server.URL})

	worker := newWorkerEnv(t, api, workerOptions{policy: fastRetry()})
	stop := worker.start(t)
	defer stop()

	since := time.Now().UTC().Add(-time.Minute)
	msgID := worker.ingest(t, appID, "invoice.paid", map[string]any{"a": 1})

	// Both endpoints fail out.
	deadline := time.Now().Add(20 * time.Second)
	for {
		result, err := api.Attempts.GetAttempts(context.Background(), repositories.AttemptFilters{
			MsgID:  repositories.Eq(msgID),
			Status: repositories.Eq(entities.AttemptFailed),
		}, repositories.CursorPagination{Limit: 10})
		if err != nil {
			t.Fatalf("GetAttempts: %v", err)
		}
		if len(result.Items) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of 2 deliveries failed permanently", len(result.Items))
		}
		time.Sleep(50 * time.Millisecond)
	}

	first.setStatus(http.StatusOK, nil)
	second.setStatus(http.StatusOK, nil)
	firstBefore, secondBefore := len(first.requests()), len(second.requests())

	resp := api.do(t, request{
		Method: http.MethodPost,
		Path:   "/api/v1/app/" + appID + "/replay",
		Body:   map[string]any{"since": since.Format(time.RFC3339)},
	})
	if resp.Status != http.StatusAccepted {
		t.Fatalf("replay = %d %s, want 202", resp.Status, resp.Body)
	}
	var replayed struct {
		Enqueued int `json:"enqueued"`
	}
	resp.decode(t, &replayed)
	if replayed.Enqueued != 2 {
		t.Fatalf("replay enqueued %d deliveries, want one per failed endpoint", replayed.Enqueued)
	}

	if !first.waitFor(firstBefore+1, 10*time.Second) || !second.waitFor(secondBefore+1, 10*time.Second) {
		t.Error("both endpoints must receive their replayed delivery")
	}
}

func TestAPIPortalAccessMintsAScopedToken(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	appID := api.createApp(t, "Portal", nil)
	otherApp := api.createApp(t, "Someone else", nil)
	endpointID, _ := api.createEndpoint(t, appID, map[string]any{"url": "https://example.test/hook"})

	resp := api.do(t, request{
		Method: http.MethodPost,
		Path:   "/api/v1/app/" + appID + "/portal-access",
		Body:   map[string]any{},
	})
	if resp.Status != http.StatusOK {
		t.Fatalf("portal-access = %d %s", resp.Status, resp.Body)
	}

	var access struct {
		URL    string    `json:"url"`
		Token  string    `json:"token"`
		Expiry time.Time `json:"expiry"`
	}
	resp.decode(t, &access)
	if access.Token == "" || access.URL == "" {
		t.Fatalf("portal access = %+v", access)
	}
	if access.Expiry.Before(time.Now()) {
		t.Errorf("expiry %s is already past", access.Expiry)
	}

	// The minted token works on its own application.
	if own := api.do(t, request{
		Method: http.MethodGet,
		Path:   "/api/v1/app/" + appID + "/endpoint/" + endpointID,
		Token:  access.Token,
	}); own.Status != http.StatusOK {
		t.Errorf("the portal token cannot read its own endpoint: %d %s", own.Status, own.Body)
	}
	// And nowhere else.
	if crossed := api.do(t, request{
		Method: http.MethodGet,
		Path:   "/api/v1/app/" + otherApp + "/endpoint",
		Token:  access.Token,
	}); crossed.Status != http.StatusForbidden {
		t.Errorf("the portal token reached another application: %d %s", crossed.Status, crossed.Body)
	}
	// A portal token may not mint another portal token.
	if escalated := api.do(t, request{
		Method: http.MethodPost,
		Path:   "/api/v1/app/" + appID + "/portal-access",
		Body:   map[string]any{},
		Token:  access.Token,
	}); escalated.Status != http.StatusForbidden {
		t.Errorf("a portal token minted another token: %d %s", escalated.Status, escalated.Body)
	}

	// An absurd lifetime is refused: a leaked portal token is the worst bug
	// this project can ship, so the blast radius stays capped.
	if long := api.do(t, request{
		Method: http.MethodPost,
		Path:   "/api/v1/app/" + appID + "/portal-access",
		Body:   map[string]any{"expiry": 999999},
	}); long.Status != http.StatusUnprocessableEntity {
		t.Errorf("a 999999s lifetime = %d %s, want 422", long.Status, long.Body)
	}
}
