package integration_test

import (
	"context"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/adapters/inbound/http/portal"
	"github.com/plusiv/huxio/internal/domain/repositories"
)

// portalClient is a browser-like client for the portal: it keeps cookies and
// does not follow redirects automatically, so each hop can be asserted.
type portalClient struct {
	t      *testing.T
	server *httptest.Server
	http   *http.Client
}

func newPortalClient(t *testing.T, api *apiEnv) *portalClient {
	t.Helper()

	server := httptest.NewServer(api.Router)
	t.Cleanup(server.Close)

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar: %v", err)
	}
	return &portalClient{
		t:      t,
		server: server,
		http: &http.Client{
			Jar: jar,
			// Redirects are followed by hand so the token-to-cookie hop is visible to
			// the tests.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

// get performs a GET and returns the status, body and headers.
func (p *portalClient) get(path string) (int, string, http.Header) {
	p.t.Helper()

	resp, err := p.http.Get(p.server.URL + path)
	if err != nil {
		p.t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()

	body := readAll(p.t, resp)
	return resp.StatusCode, body, resp.Header
}

// follow performs a GET and follows one redirect, returning the final page.
func (p *portalClient) follow(path string) (int, string) {
	p.t.Helper()

	status, body, header := p.get(path)
	for range 3 {
		if status != http.StatusSeeOther && status != http.StatusFound {
			return status, body
		}
		next := header.Get("Location")
		if next == "" {
			return status, body
		}
		status, body, header = p.get(next)
	}
	return status, body
}

// post submits a form and returns the status and the redirect target.
func (p *portalClient) post(path string, form url.Values) (int, string) {
	p.t.Helper()

	resp, err := p.http.PostForm(p.server.URL+path, form)
	if err != nil {
		p.t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	_ = readAll(p.t, resp)

	return resp.StatusCode, resp.Header.Get("Location")
}

// enter exchanges a portal token for a session, as the sender's backend
// handing a customer over does.
func (p *portalClient) enter(token string) {
	p.t.Helper()

	status, _, header := p.get("/portal?token=" + url.QueryEscape(token))
	if status != http.StatusSeeOther {
		p.t.Fatalf("entering the portal = %d, want a redirect that drops the token from the URL", status)
	}
	if location := header.Get("Location"); strings.Contains(location, "token") {
		p.t.Errorf("the redirect target still carries the token: %s", location)
	}

	var found bool
	for _, cookie := range header["Set-Cookie"] {
		if !strings.HasPrefix(cookie, portal.SessionCookie+"=") {
			continue
		}
		found = true
		// A credential in a cookie that JavaScript can read, or that is sent
		// cross-site, is a credential waiting to leak.
		if !strings.Contains(cookie, "HttpOnly") {
			p.t.Error("the session cookie must be HttpOnly")
		}
		if !strings.Contains(cookie, "SameSite=Strict") {
			p.t.Error("the session cookie must be SameSite=Strict")
		}
		if !strings.Contains(cookie, "Path=/portal") {
			p.t.Error("the session cookie must be scoped to /portal")
		}
	}
	if !found {
		p.t.Fatal("entering the portal did not set a session cookie")
	}
}

func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()

	var builder strings.Builder
	buffer := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buffer)
		builder.Write(buffer[:n])
		if err != nil {
			break
		}
	}
	return builder.String()
}

// portalToken mints a portal token through the API, as a sender would.
func (e *apiEnv) mintPortalToken(t *testing.T, appID string) string {
	t.Helper()

	resp := e.do(t, request{
		Method: http.MethodPost,
		Path:   "/api/v1/app/" + appID + "/portal-access",
		Body:   map[string]any{},
	})
	if resp.Status != http.StatusOK {
		t.Fatalf("portal-access = %d %s", resp.Status, resp.Body)
	}
	token, _ := resp.field(t, "token").(string)
	if token == "" {
		t.Fatal("portal-access returned no token")
	}
	return token
}

func TestPortalEndpointLifecycle(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	receiver := newSink(t, http.StatusOK)

	appID := api.createApp(t, "Portal tenant", nil)
	if resp := api.do(t, request{
		Method: http.MethodPost, Path: "/api/v1/event-type",
		Body: map[string]any{"name": "invoice.paid", "description": "An invoice was paid"},
	}); resp.Status != http.StatusCreated {
		t.Fatalf("create event type: %d %s", resp.Status, resp.Body)
	}
	api.reloadConfig(t)

	client := newPortalClient(t, api)
	client.enter(api.mintPortalToken(t, appID))

	// The empty state tells the customer what to do.
	status, body := client.follow("/portal")
	if status != http.StatusOK {
		t.Fatalf("GET /portal = %d", status)
	}
	if !strings.Contains(body, "No endpoints yet") {
		t.Errorf("the empty state is missing: %s", firstLines(body))
	}
	// The event-type picker is built from the tenant's catalogue.
	if !strings.Contains(body, "invoice.paid") {
		t.Error("the event type picker must offer the tenant's event types")
	}

	// Add an endpoint through the form.
	code, location := client.post("/portal/endpoints", url.Values{
		"url":         {receiver.server.URL},
		"description": {"My integration"},
		"eventTypes":  {"invoice.paid"},
	})
	if code != http.StatusSeeOther {
		t.Fatalf("creating an endpoint = %d, want a redirect", code)
	}
	if !strings.Contains(location, "flash=") {
		t.Errorf("the redirect should carry a confirmation: %s", location)
	}

	status, body = client.follow("/portal")
	if status != http.StatusOK {
		t.Fatalf("GET /portal = %d", status)
	}
	if !strings.Contains(body, receiver.server.URL) || !strings.Contains(body, "My integration") {
		t.Errorf("the new endpoint is not listed: %s", firstLines(body))
	}

	// Find it, then open its page.
	endpoints, err := api.Endpoints.GetEndpoints(context.Background(),
		repositories.EndpointFilters{AppID: repositories.Eq(appID)},
		repositories.CursorPagination{Limit: 10})
	if err != nil {
		t.Fatalf("GetEndpoints: %v", err)
	}
	if len(endpoints.Items) != 1 {
		t.Fatalf("expected one endpoint, found %d", len(endpoints.Items))
	}
	endpointID := endpoints.Items[0].ID

	status, body = client.follow("/portal/endpoints/" + endpointID)
	if status != http.StatusOK {
		t.Fatalf("GET endpoint page = %d", status)
	}
	for _, want := range []string{"Signing secret", "Send a test event", "Recent deliveries", "delivered"} {
		if !strings.Contains(body, want) {
			t.Errorf("the endpoint page is missing %q", want)
		}
	}
	// The secret is not on the page until it is asked for.
	if strings.Contains(body, "whsec_") {
		t.Error("the secret must not be rendered until the viewer reveals it")
	}

	// Reveal it.
	code, location = client.post("/portal/endpoints/"+endpointID+"/secret", url.Values{"action": {"reveal"}})
	if code != http.StatusSeeOther {
		t.Fatalf("reveal = %d", code)
	}
	status, body = client.follow(location)
	if status != http.StatusOK || !strings.Contains(body, "whsec_") {
		t.Errorf("the revealed secret is not on the page: %d %s", status, firstLines(body))
	}

	// Rotate it: the page shows a new secret and says the old one still works.
	before := api.endpointRow(t, endpointID).Secret.Sealed
	code, location = client.post("/portal/endpoints/"+endpointID+"/secret", url.Values{"action": {"rotate"}})
	if code != http.StatusSeeOther {
		t.Fatalf("rotate = %d", code)
	}
	status, body = client.follow(location)
	if status != http.StatusOK {
		t.Fatalf("page after rotation = %d", status)
	}
	if !strings.Contains(body, "24 hours") {
		t.Error("the rotation message must explain the overlap window")
	}
	after := api.endpointRow(t, endpointID)
	if string(after.Secret.Sealed) == string(before) {
		t.Error("rotation did not change the stored secret")
	}
	if len(after.OldSecrets) != 1 {
		t.Errorf("OldSecrets = %d, want the superseded secret kept", len(after.OldSecrets))
	}

	// Update the settings.
	code, _ = client.post("/portal/endpoints/"+endpointID, url.Values{
		"url":         {receiver.server.URL + "/updated"},
		"description": {"Renamed"},
		"eventTypes":  {"invoice.paid"},
	})
	if code != http.StatusSeeOther {
		t.Fatalf("update = %d", code)
	}
	if got := api.endpointRow(t, endpointID); got.URL != receiver.server.URL+"/updated" || got.Description != "Renamed" {
		t.Errorf("update did not apply: %+v", got)
	}

	// Disable and enable.
	if code, _ = client.post("/portal/endpoints/"+endpointID, url.Values{"action": {"disable"}}); code != http.StatusSeeOther {
		t.Fatalf("disable = %d", code)
	}
	if !api.endpointRow(t, endpointID).Disabled() {
		t.Error("disable did not take effect")
	}
	if code, _ = client.post("/portal/endpoints/"+endpointID, url.Values{"action": {"enable"}}); code != http.StatusSeeOther {
		t.Fatalf("enable = %d", code)
	}
	if api.endpointRow(t, endpointID).Disabled() {
		t.Error("enable did not take effect")
	}

	// Delete it.
	if code, _ = client.post("/portal/endpoints/"+endpointID+"/delete", nil); code != http.StatusSeeOther {
		t.Fatalf("delete = %d", code)
	}
	status, body = client.follow("/portal")
	if status != http.StatusOK || !strings.Contains(body, "No endpoints yet") {
		t.Errorf("the endpoint was not removed from the list: %s", firstLines(body))
	}
}

func TestPortalTestEventAndDeliveryLog(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	receiver := newSink(t, http.StatusOK)

	appID := api.createApp(t, "Portal delivery", nil)
	endpointID, _ := api.createEndpoint(t, appID, map[string]any{"url": receiver.server.URL})

	worker := newWorkerEnv(t, api, workerOptions{})
	stop := worker.start(t)
	defer stop()

	client := newPortalClient(t, api)
	client.enter(api.mintPortalToken(t, appID))

	// The two-minute demo: send a sample event from the portal and watch it
	// arrive.
	code, location := client.post("/portal/endpoints/"+endpointID+"/test", url.Values{
		"eventType": {"test.event"},
		"payload":   {`{"from":"the portal"}`},
	})
	if code != http.StatusSeeOther {
		t.Fatalf("test event = %d", code)
	}
	if !strings.Contains(location, "flash=") {
		t.Errorf("the test send should confirm itself: %s", location)
	}

	if !receiver.waitFor(1, 15*time.Second) {
		t.Fatal("the test event never reached the endpoint")
	}
	if got := receiver.requests()[0]; !strings.Contains(string(got.Body), `"from":"the portal"`) {
		t.Errorf("the endpoint received %s", got.Body)
	}

	// It appears in the log, both on the endpoint page and in the log view.
	deadline := time.Now().Add(15 * time.Second)
	for {
		status, body := client.follow("/portal/endpoints/" + endpointID)
		if status == http.StatusOK && strings.Contains(body, "delivered") && strings.Contains(body, "msg_") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the delivery never appeared in the endpoint's log")
		}
		time.Sleep(200 * time.Millisecond)
	}

	status, body := client.follow("/portal/log")
	if status != http.StatusOK {
		t.Fatalf("GET /portal/log = %d", status)
	}
	if !strings.Contains(body, "msg_") {
		t.Errorf("the log view is empty: %s", firstLines(body))
	}
	// The status filter is offered and accepted.
	status, body = client.follow("/portal/log?status=success")
	if status != http.StatusOK || !strings.Contains(body, "delivered") {
		t.Errorf("the filtered log = %d: %s", status, firstLines(body))
	}
	if status, _ := client.follow("/portal/log?status=nonsense"); status != http.StatusUnprocessableEntity {
		t.Errorf("an unknown status filter = %d, want 422", status)
	}

	// A bad payload is rejected with a message rather than a stack trace.
	code, location = client.post("/portal/endpoints/"+endpointID+"/test", url.Values{
		"payload": {"not json"},
	})
	if code != http.StatusSeeOther || !strings.Contains(location, "flashKind=bad") {
		t.Errorf("a malformed payload = %d %s, want a rejection flash", code, location)
	}
}

func TestPortalResendAndRecover(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	receiver := newSink(t, http.StatusOK)

	appID := api.createApp(t, "Portal replay", nil)
	endpointID, _ := api.createEndpoint(t, appID, map[string]any{"url": receiver.server.URL})

	worker := newWorkerEnv(t, api, workerOptions{})
	stop := worker.start(t)
	defer stop()

	msgID := worker.ingest(t, appID, "invoice.paid", map[string]any{"a": 1})
	if !receiver.waitFor(1, 15*time.Second) {
		t.Fatal("the first delivery never arrived")
	}
	api.waitForAttempts(t, msgID, 1, 15*time.Second)

	client := newPortalClient(t, api)
	client.enter(api.mintPortalToken(t, appID))

	// Resend from the log.
	if code, _ := client.post("/portal/endpoints/"+endpointID+"/resend", url.Values{"msgId": {msgID}}); code != http.StatusSeeOther {
		t.Fatalf("resend = %d", code)
	}
	if !receiver.waitFor(2, 15*time.Second) {
		t.Fatal("the resent delivery never arrived")
	}

	// Recover reports how much it queued, even when that is nothing.
	code, location := client.post("/portal/endpoints/"+endpointID+"/recover", url.Values{"hours": {"24"}})
	if code != http.StatusSeeOther {
		t.Fatalf("recover = %d", code)
	}
	if !strings.Contains(location, "flash=") {
		t.Errorf("recover should report what it did: %s", location)
	}
	status, body := client.follow(location)
	if status != http.StatusOK || !strings.Contains(body, "replaying") {
		t.Errorf("recover message missing from the page: %d %s", status, firstLines(body))
	}
}

// TestPortalSessionIsConfinedToItsApplication covers the worst bug this
// project could ship: one tenant's customer reading another's deliveries.
// The portal takes the application from the session token, so there is no
// request a session can make that names another one.
func TestPortalSessionIsConfinedToItsApplication(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)

	appA := api.createApp(t, "Tenant A", nil)
	appB := api.createApp(t, "Tenant B", nil)
	endpointA, _ := api.createEndpoint(t, appA, map[string]any{"url": "https://example.test/a"})
	endpointB, _ := api.createEndpoint(t, appB, map[string]any{"url": "https://example.test/b"})

	client := newPortalClient(t, api)
	client.enter(api.mintPortalToken(t, appA))

	// Its own endpoint is visible.
	if status, body := client.follow("/portal/endpoints/" + endpointA); status != http.StatusOK {
		t.Fatalf("own endpoint = %d: %s", status, firstLines(body))
	}
	// The other application's endpoint is not reachable by id, because the
	// application is never taken from the URL.
	if status, _ := client.follow("/portal/endpoints/" + endpointB); status != http.StatusNotFound {
		t.Errorf("another application's endpoint = %d, want 404", status)
	}
	// Neither is its delivery log.
	status, body := client.follow("/portal/log?endpointId=" + endpointB)
	if status == http.StatusOK && strings.Contains(body, "https://example.test/b") {
		t.Error("the log view leaked another application's endpoint")
	}

	// Nor can it act on it.
	for _, path := range []string{
		"/portal/endpoints/" + endpointB,
		"/portal/endpoints/" + endpointB + "/secret",
		"/portal/endpoints/" + endpointB + "/delete",
		"/portal/endpoints/" + endpointB + "/resend",
		"/portal/endpoints/" + endpointB + "/recover",
	} {
		code, location := client.post(path, url.Values{"action": {"reveal"}, "msgId": {"msg_x"}, "hours": {"1"}})
		switch code {
		case http.StatusNotFound:
			// The endpoint does not exist as far as this session is concerned.
		case http.StatusSeeOther:
			if !strings.Contains(location, "flashKind=bad") {
				t.Errorf("POST %s redirected without an error: %s", path, location)
			}
		default:
			t.Errorf("POST %s = %d, want a refusal", path, code)
		}
	}

	// The other application's endpoint is untouched.
	if got := api.endpointRow(t, endpointB); got.Deleted() || got.Disabled() {
		t.Error("a portal session modified another application's endpoint")
	}
}

func TestPortalRejectsWrongTokens(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	appID := api.createApp(t, "Portal auth", nil)

	client := newPortalClient(t, api)

	// No token at all.
	if status, _, _ := client.get("/portal"); status != http.StatusUnauthorized {
		t.Errorf("no token = %d, want 401", status)
	}
	// Garbage.
	if status, _, _ := client.get("/portal?token=nonsense"); status != http.StatusUnauthorized {
		t.Errorf("a garbage token = %d, want 401", status)
	}
	// An organization token must not open the portal: it would carry the
	// whole tenant's data into a customer-facing page.
	if status, _, _ := client.get("/portal?token=" + url.QueryEscape(api.Token)); status != http.StatusForbidden {
		t.Errorf("an org token = %d, want 403", status)
	}
	// An expired portal token.
	expired, _, err := api.Tokens.IssuePortalToken(appID, api.OrgID, time.Millisecond)
	if err != nil {
		t.Fatalf("IssuePortalToken: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if status, _, _ := client.get("/portal?token=" + url.QueryEscape(expired)); status != http.StatusUnauthorized {
		t.Errorf("an expired token = %d, want 401", status)
	}

	// Signing out clears the session.
	client.enter(api.mintPortalToken(t, appID))
	if status, _ := client.follow("/portal"); status != http.StatusOK {
		t.Fatal("the session did not start")
	}
	if code, _ := client.post("/portal/sign-out", nil); code != http.StatusSeeOther {
		t.Errorf("sign out = %d", code)
	}
	if status, _, _ := client.get("/portal"); status != http.StatusUnauthorized {
		t.Errorf("after signing out = %d, want 401", status)
	}
}

func TestPortalLiveTailStreamsDeliveries(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	receiver := newSink(t, http.StatusOK)

	appID := api.createApp(t, "Portal tail", nil)
	api.createEndpoint(t, appID, map[string]any{"url": receiver.server.URL})

	worker := newWorkerEnv(t, api, workerOptions{})
	stop := worker.start(t)
	defer stop()

	client := newPortalClient(t, api)
	client.enter(api.mintPortalToken(t, appID))

	// The page carries the EventSource wiring: no framework, no bundle.
	status, body := client.follow("/portal/tail")
	if status != http.StatusOK {
		t.Fatalf("GET /portal/tail = %d", status)
	}
	if !strings.Contains(body, "new EventSource('/portal/stream')") {
		t.Errorf("the tail page is missing its stream wiring: %s", firstLines(body))
	}

	// Open the stream, then deliver something.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, client.server.URL+"/portal/stream", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	for _, cookie := range client.http.Jar.Cookies(mustParseURL(t, client.server.URL+"/portal")) {
		req.AddCookie(cookie)
	}

	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stream = %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("content type = %q", got)
	}

	events := make(chan string, 8)
	go func() {
		defer close(events)
		buffer := make([]byte, 2048)
		var accumulated strings.Builder
		for {
			n, err := resp.Body.Read(buffer)
			if n > 0 {
				accumulated.Write(buffer[:n])
				if strings.Contains(accumulated.String(), "event: attempt") {
					events <- accumulated.String()
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	msgID := worker.ingest(t, appID, "invoice.paid", map[string]any{"a": 1})

	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("the stream closed without delivering an event")
		}
		if !strings.Contains(event, msgID) {
			t.Errorf("the event does not mention the delivered message: %q", event)
		}
		if !strings.Contains(event, `"statusText":"delivered"`) {
			t.Errorf("event = %q, want a delivered attempt", event)
		}
	case <-ctx.Done():
		t.Fatal("no attempt event arrived on the portal stream")
	}
}

func TestPortalStreamRequiresASession(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	client := newPortalClient(t, api)

	if status, _, _ := client.get("/portal/stream"); status != http.StatusUnauthorized {
		t.Errorf("an unauthenticated stream = %d, want 401", status)
	}
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()

	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return parsed
}

// firstLines trims a rendered page for a readable failure message.
func firstLines(body string) string {
	lines := strings.Split(body, "\n")
	var kept []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "<style") || strings.HasPrefix(trimmed, "-") {
			continue
		}
		kept = append(kept, trimmed)
		if len(kept) == 25 {
			break
		}
	}
	return strings.Join(kept, "\n")
}
