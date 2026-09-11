package integration_test

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestAPIAttemptStreamTailsNewAttempts exercises the portal's headline
// feature: a live tail of deliveries as they happen.
func TestAPIAttemptStreamTailsNewAttempts(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	receiver := newSink(t, http.StatusOK)

	appID := api.createApp(t, "Stream", nil)
	api.createEndpoint(t, appID, map[string]any{"url": receiver.server.URL})

	worker := newWorkerEnv(t, api, workerOptions{})
	stopWorker := worker.start(t)
	defer stopWorker()

	// The stream must be served by a real server: httptest.NewRecorder buffers,
	// and a live tail that only appears at the end is no tail.
	server := httptest.NewServer(api.Router)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		server.URL+"/api/v1/app/"+appID+"/attempt/stream", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+api.Token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stream = %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("content type = %q, want text/event-stream", got)
	}
	// Proxies buffer by default, which would hold every event until the
	// connection closed.
	if resp.Header.Get("X-Accel-Buffering") != "no" {
		t.Error("the stream must ask proxies not to buffer it")
	}

	events := make(chan string, 8)
	go func() {
		defer close(events)
		scanner := bufio.NewScanner(resp.Body)
		var current strings.Builder
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				if current.Len() > 0 {
					events <- current.String()
					current.Reset()
				}
				continue
			}
			current.WriteString(line)
			current.WriteString("\n")
		}
	}()

	// Ingest after the stream is open: only new attempts are tailed.
	msgID := worker.ingest(t, appID, "invoice.paid", map[string]any{"a": 1})

	deadline := time.After(15 * time.Second)
	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatal("the stream closed before delivering an attempt")
			}
			if strings.HasPrefix(event, ": keep-alive") {
				continue
			}
			if !strings.Contains(event, "event: attempt") {
				t.Fatalf("unexpected event: %q", event)
			}
			if !strings.Contains(event, msgID) {
				t.Fatalf("event does not mention the delivered message: %q", event)
			}
			if !strings.Contains(event, `"statusText":"success"`) {
				t.Errorf("event = %q, want a successful attempt", event)
			}
			return
		case <-deadline:
			t.Fatal("no attempt event arrived on the stream")
		}
	}
}

func TestAPIAttemptStreamRequiresScopedToken(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	appID := api.createApp(t, "Stream auth", nil)
	otherApp := api.createApp(t, "Other", nil)

	// A portal token for another application must not open the tail: a leaked
	// portal token reaching another tenant's delivery log is the worst bug
	// this project can ship.
	resp := api.do(t, request{
		Method: http.MethodGet,
		Path:   "/api/v1/app/" + otherApp + "/attempt/stream",
		Token:  api.portalToken(t, appID),
	})
	if resp.Status != http.StatusForbidden {
		t.Errorf("cross-application stream = %d %s, want 403", resp.Status, resp.Body)
	}

	unauthenticated := api.do(t, request{
		Method: http.MethodGet,
		Path:   "/api/v1/app/" + appID + "/attempt/stream",
		NoAuth: true,
	})
	if unauthenticated.Status != http.StatusUnauthorized {
		t.Errorf("unauthenticated stream = %d, want 401", unauthenticated.Status)
	}
}
