package integration_test

import (
	"net/http"
	"strings"
	"testing"
)

func TestAPIAuthentication(t *testing.T) {
	t.Parallel()
	env := newAPIEnv(t)

	tests := []struct {
		name    string
		request request
		want    int
	}{
		{"no header", request{Method: http.MethodGet, Path: "/api/v1/app", NoAuth: true}, http.StatusUnauthorized},
		{"garbage token", request{Method: http.MethodGet, Path: "/api/v1/app", Token: "nonsense"}, http.StatusUnauthorized},
		{"valid token", request{Method: http.MethodGet, Path: "/api/v1/app"}, http.StatusOK},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if resp := env.do(t, tc.request); resp.Status != tc.want {
				t.Errorf("status = %d %s, want %d", resp.Status, resp.Body, tc.want)
			}
		})
	}

	t.Run("wrong scheme", func(t *testing.T) {
		resp := env.do(t, request{
			Method: http.MethodGet, Path: "/api/v1/app", NoAuth: true,
			Headers: map[string]string{"Authorization": "Basic dXNlcjpwYXNz"},
		})
		if resp.Status != http.StatusUnauthorized {
			t.Errorf("status = %d, want 401", resp.Status)
		}
	})
}

func TestAPITrailingSlashesAreAccepted(t *testing.T) {
	t.Parallel()
	env := newAPIEnv(t)

	// The compatible SDKs use trailing slashes; humans and curl usually don't.
	for _, path := range []string{"/api/v1/app", "/api/v1/app/", "/api/v1/event-type/"} {
		resp := env.do(t, request{Method: http.MethodGet, Path: path})
		if resp.Status != http.StatusOK {
			t.Errorf("GET %s = %d %s, want 200", path, resp.Status, resp.Body)
		}
	}
}

func TestAPIApplicationCRUD(t *testing.T) {
	t.Parallel()
	env := newAPIEnv(t)

	created := env.do(t, request{
		Method: http.MethodPost, Path: "/api/v1/app",
		Body: map[string]any{"name": "Acme", "uid": "cust_1", "rateLimit": 100, "metadata": map[string]any{"tier": "gold"}},
	})
	if created.Status != http.StatusCreated {
		t.Fatalf("POST /app = %d %s", created.Status, created.Body)
	}
	appID, _ := created.field(t, "id").(string)

	// Duplicate uid inside the tenant conflicts.
	if dupe := env.do(t, request{
		Method: http.MethodPost, Path: "/api/v1/app",
		Body: map[string]any{"name": "Duplicate", "uid": "cust_1"},
	}); dupe.Status != http.StatusConflict {
		t.Errorf("duplicate uid = %d %s, want 409", dupe.Status, dupe.Body)
	}

	// Readable by id and by uid.
	for _, key := range []string{appID, "cust_1"} {
		resp := env.do(t, request{Method: http.MethodGet, Path: "/api/v1/app/" + key})
		if resp.Status != http.StatusOK {
			t.Fatalf("GET /app/%s = %d %s", key, resp.Status, resp.Body)
		}
		if got := resp.field(t, "id"); got != appID {
			t.Errorf("GET /app/%s returned id %v", key, got)
		}
	}

	patched := env.do(t, request{
		Method: http.MethodPatch, Path: "/api/v1/app/" + appID,
		Body: map[string]any{"name": "Acme Renamed"},
	})
	if patched.Status != http.StatusOK {
		t.Fatalf("PATCH /app = %d %s", patched.Status, patched.Body)
	}
	if got := patched.field(t, "name"); got != "Acme Renamed" {
		t.Errorf("name = %v", got)
	}

	listed := env.do(t, request{Method: http.MethodGet, Path: "/api/v1/app?limit=10"})
	var list struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
		Done bool `json:"done"`
	}
	listed.decode(t, &list)
	if len(list.Data) != 1 || !list.Done {
		t.Errorf("listing = %+v", list)
	}

	if deleted := env.do(t, request{Method: http.MethodDelete, Path: "/api/v1/app/" + appID}); deleted.Status != http.StatusNoContent {
		t.Fatalf("DELETE /app = %d %s", deleted.Status, deleted.Body)
	}
	if gone := env.do(t, request{Method: http.MethodGet, Path: "/api/v1/app/" + appID}); gone.Status != http.StatusNotFound {
		t.Errorf("GET after delete = %d, want 404", gone.Status)
	}

	// Another tenant sees nothing of ours.
	if crossed := env.do(t, request{Method: http.MethodGet, Path: "/api/v1/app?limit=10", Token: env.otherOrgToken(t)}); crossed.Status == http.StatusOK {
		var theirs struct {
			Data []any `json:"data"`
		}
		crossed.decode(t, &theirs)
		if len(theirs.Data) != 0 {
			t.Errorf("another tenant saw %d applications", len(theirs.Data))
		}
	} else {
		t.Errorf("cross-tenant list = %d %s", crossed.Status, crossed.Body)
	}
}

func TestAPIEndpointCRUDAndSecrets(t *testing.T) {
	t.Parallel()
	env := newAPIEnv(t)

	appID := env.createApp(t, "Endpoints", nil)
	endpointID, secret := env.createEndpoint(t, appID, map[string]any{
		"url":         "https://example.test/hook",
		"description": "Primary",
		"filterTypes": []string{"invoice.paid"},
		"channels":    []string{"ch_acme"},
		"headers":     map[string]string{"X-Tenant": "acme"},
	})
	if !strings.HasPrefix(secret, "whsec_") {
		t.Errorf("secret = %q, want the whsec_ prefix", secret)
	}

	fetched := env.do(t, request{Method: http.MethodGet, Path: "/api/v1/app/" + appID + "/endpoint/" + endpointID})
	if fetched.Status != http.StatusOK {
		t.Fatalf("GET endpoint = %d %s", fetched.Status, fetched.Body)
	}
	// The secret must never appear on a plain read.
	if strings.Contains(string(fetched.Body), "whsec_") {
		t.Errorf("endpoint response leaked a secret: %s", fetched.Body)
	}

	revealed := env.do(t, request{Method: http.MethodGet, Path: "/api/v1/app/" + appID + "/endpoint/" + endpointID + "/secret"})
	if revealed.Status != http.StatusOK {
		t.Fatalf("GET secret = %d %s", revealed.Status, revealed.Body)
	}
	if got := revealed.field(t, "key"); got != secret {
		t.Errorf("revealed key = %v, want the create-time secret", got)
	}

	if rotated := env.do(t, request{
		Method: http.MethodPost, Path: "/api/v1/app/" + appID + "/endpoint/" + endpointID + "/secret/rotate",
		Body: map[string]any{},
	}); rotated.Status != http.StatusNoContent {
		t.Fatalf("rotate secret = %d %s", rotated.Status, rotated.Body)
	}
	afterRotate := env.do(t, request{Method: http.MethodGet, Path: "/api/v1/app/" + appID + "/endpoint/" + endpointID + "/secret"})
	if got := afterRotate.field(t, "key"); got == secret {
		t.Error("rotation must install a different secret")
	}

	headers := env.do(t, request{
		Method: http.MethodPatch, Path: "/api/v1/app/" + appID + "/endpoint/" + endpointID + "/headers",
		Body: map[string]any{"headers": map[string]string{"X-Tenant": "acme-2", "X-New": "1"}},
	})
	if headers.Status != http.StatusOK {
		t.Fatalf("PATCH headers = %d %s", headers.Status, headers.Body)
	}
	var headerBody struct {
		Headers map[string]string `json:"headers"`
	}
	headers.decode(t, &headerBody)
	if headerBody.Headers["X-Tenant"] != "acme-2" || headerBody.Headers["X-New"] != "1" {
		t.Errorf("headers = %v", headerBody.Headers)
	}

	// Signature headers can never be overridden.
	if forged := env.do(t, request{
		Method: http.MethodPatch, Path: "/api/v1/app/" + appID + "/endpoint/" + endpointID + "/headers",
		Body: map[string]any{"headers": map[string]string{"webhook-signature": "v1,forged"}},
	}); forged.Status != http.StatusUnprocessableEntity {
		t.Errorf("forged signature header = %d %s, want 422", forged.Status, forged.Body)
	}

	disabled := env.do(t, request{
		Method: http.MethodPatch, Path: "/api/v1/app/" + appID + "/endpoint/" + endpointID,
		Body: map[string]any{"disabled": true},
	})
	if disabled.Status != http.StatusOK {
		t.Fatalf("disable endpoint = %d %s", disabled.Status, disabled.Body)
	}
	if got := disabled.field(t, "disabled"); got != true {
		t.Errorf("disabled = %v", got)
	}

	if deleted := env.do(t, request{
		Method: http.MethodDelete, Path: "/api/v1/app/" + appID + "/endpoint/" + endpointID,
	}); deleted.Status != http.StatusNoContent {
		t.Fatalf("DELETE endpoint = %d %s", deleted.Status, deleted.Body)
	}
	if gone := env.do(t, request{
		Method: http.MethodGet, Path: "/api/v1/app/" + appID + "/endpoint/" + endpointID,
	}); gone.Status != http.StatusNotFound {
		t.Errorf("GET after delete = %d, want 404", gone.Status)
	}
}

func TestAPIEndpointValidation(t *testing.T) {
	t.Parallel()
	env := newAPIEnv(t)

	appID := env.createApp(t, "Validation", nil)
	path := "/api/v1/app/" + appID + "/endpoint"

	tests := []struct {
		name string
		body map[string]any
		want int
	}{
		{"missing url", map[string]any{"description": "no url"}, http.StatusUnprocessableEntity},
		{"non-http scheme", map[string]any{"url": "ftp://example.test/x"}, http.StatusUnprocessableEntity},
		{"unsupported secret type", map[string]any{"url": "https://example.test/x", "secretType": "rsa"}, http.StatusUnprocessableEntity},
		{"secret without prefix", map[string]any{"url": "https://example.test/x", "secret": "plain"}, http.StatusUnprocessableEntity},
		{"reserved header", map[string]any{"url": "https://example.test/x", "headers": map[string]string{"webhook-id": "forged"}}, http.StatusUnprocessableEntity},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if resp := env.do(t, request{Method: http.MethodPost, Path: path, Body: tc.body}); resp.Status != tc.want {
				t.Errorf("status = %d %s, want %d", resp.Status, resp.Body, tc.want)
			}
		})
	}
}

func TestAPIPortalTokenIsConfinedToItsApplication(t *testing.T) {
	t.Parallel()
	env := newAPIEnv(t)

	appA := env.createApp(t, "Portal app", nil)
	appB := env.createApp(t, "Someone else", nil)
	endpointA, _ := env.createEndpoint(t, appA, map[string]any{"url": "https://example.test/a"})
	endpointB, _ := env.createEndpoint(t, appB, map[string]any{"url": "https://example.test/b"})

	portal := env.portalToken(t, appA)

	// Its own application: allowed.
	if resp := env.do(t, request{
		Method: http.MethodGet, Path: "/api/v1/app/" + appA + "/endpoint/" + endpointA, Token: portal,
	}); resp.Status != http.StatusOK {
		t.Fatalf("portal token on its own app = %d %s", resp.Status, resp.Body)
	}
	if resp := env.do(t, request{
		Method: http.MethodPost, Path: "/api/v1/app/" + appA + "/endpoint", Token: portal,
		Body: map[string]any{"url": "https://example.test/portal-created"},
	}); resp.Status != http.StatusCreated {
		t.Errorf("portal token creating an endpoint = %d %s", resp.Status, resp.Body)
	}

	// Another application in the same tenant: forbidden. This is the bug that
	// would be worst to ship, so it is asserted on every shape of route.
	forbidden := []request{
		{Method: http.MethodGet, Path: "/api/v1/app/" + appB + "/endpoint/" + endpointB, Token: portal},
		{Method: http.MethodGet, Path: "/api/v1/app/" + appB + "/endpoint", Token: portal},
		{Method: http.MethodGet, Path: "/api/v1/app/" + appB + "/msg", Token: portal},
		{Method: http.MethodGet, Path: "/api/v1/app/" + appB, Token: portal},
		{Method: http.MethodGet, Path: "/api/v1/app/" + appB + "/endpoint/" + endpointB + "/secret", Token: portal},
		{
			Method: http.MethodPost, Path: "/api/v1/app/" + appB + "/msg", Token: portal,
			Body: map[string]any{"eventType": "invoice.paid", "payload": map[string]any{"a": 1}},
		},
	}
	for _, req := range forbidden {
		resp := env.do(t, req)
		if resp.Status != http.StatusForbidden {
			t.Errorf("%s %s with a foreign portal token = %d %s, want 403", req.Method, req.Path, resp.Status, resp.Body)
		}
	}

	// Tenant-wide configuration is off limits to portal tokens entirely.
	tenantWide := []request{
		{Method: http.MethodGet, Path: "/api/v1/app", Token: portal},
		{Method: http.MethodPost, Path: "/api/v1/app", Token: portal, Body: map[string]any{"name": "sneaky"}},
		{Method: http.MethodPost, Path: "/api/v1/event-type", Token: portal, Body: map[string]any{"name": "sneaky.event"}},
		{Method: http.MethodDelete, Path: "/api/v1/app/" + appA, Token: portal},
	}
	for _, req := range tenantWide {
		resp := env.do(t, req)
		if resp.Status != http.StatusForbidden {
			t.Errorf("%s %s with a portal token = %d %s, want 403", req.Method, req.Path, resp.Status, resp.Body)
		}
	}
}

func TestAPIEventTypeCRUD(t *testing.T) {
	t.Parallel()
	env := newAPIEnv(t)

	created := env.do(t, request{
		Method: http.MethodPost, Path: "/api/v1/event-type",
		Body: map[string]any{"name": "invoice.paid", "description": "An invoice was paid"},
	})
	if created.Status != http.StatusCreated {
		t.Fatalf("POST /event-type = %d %s", created.Status, created.Body)
	}

	if dupe := env.do(t, request{
		Method: http.MethodPost, Path: "/api/v1/event-type",
		Body: map[string]any{"name": "invoice.paid"},
	}); dupe.Status != http.StatusConflict {
		t.Errorf("duplicate name = %d %s, want 409", dupe.Status, dupe.Body)
	}

	if bad := env.do(t, request{
		Method: http.MethodPost, Path: "/api/v1/event-type",
		Body: map[string]any{"name": "invoice paid"},
	}); bad.Status != http.StatusUnprocessableEntity {
		t.Errorf("invalid name = %d %s, want 422", bad.Status, bad.Body)
	}

	updated := env.do(t, request{
		Method: http.MethodPut, Path: "/api/v1/event-type/invoice.paid",
		Body: map[string]any{"description": "Updated"},
	})
	if updated.Status != http.StatusOK {
		t.Fatalf("PUT /event-type = %d %s", updated.Status, updated.Body)
	}

	if archived := env.do(t, request{Method: http.MethodDelete, Path: "/api/v1/event-type/invoice.paid"}); archived.Status != http.StatusNoContent {
		t.Fatalf("DELETE /event-type = %d %s", archived.Status, archived.Body)
	}

	// Archived types are hidden by default but still readable, because historical
	// messages reference them by name.
	live := env.do(t, request{Method: http.MethodGet, Path: "/api/v1/event-type"})
	var liveList struct {
		Data []any `json:"data"`
	}
	live.decode(t, &liveList)
	if len(liveList.Data) != 0 {
		t.Errorf("live listing returned %d archived items", len(liveList.Data))
	}

	withArchived := env.do(t, request{Method: http.MethodGet, Path: "/api/v1/event-type?includeArchived=true"})
	var archivedList struct {
		Data []struct {
			Name     string `json:"name"`
			Archived bool   `json:"archived"`
		} `json:"data"`
	}
	withArchived.decode(t, &archivedList)
	if len(archivedList.Data) != 1 || !archivedList.Data[0].Archived {
		t.Errorf("archived listing = %+v", archivedList)
	}

	single := env.do(t, request{Method: http.MethodGet, Path: "/api/v1/event-type/invoice.paid"})
	if single.Status != http.StatusOK {
		t.Errorf("GET archived event type = %d %s, want 200", single.Status, single.Body)
	}
}

func TestAPIHealthAndMetrics(t *testing.T) {
	t.Parallel()
	env := newAPIEnv(t)

	// Health and readiness are unauthenticated: a probe has no token.
	live := env.do(t, request{Method: http.MethodGet, Path: "/api/v1/health", NoAuth: true})
	if live.Status != http.StatusOK {
		t.Fatalf("GET /health = %d %s", live.Status, live.Body)
	}
	if got := live.field(t, "status"); got != "ok" {
		t.Errorf("status = %v", got)
	}

	ready := env.do(t, request{Method: http.MethodGet, Path: "/api/v1/health/ready", NoAuth: true})
	if ready.Status != http.StatusOK {
		t.Fatalf("GET /health/ready = %d %s", ready.Status, ready.Body)
	}
	if got := ready.field(t, "database"); got != "ok" {
		t.Errorf("database = %v", got)
	}

	// Ingest something so the per-tenant histogram has an observation.
	appID := env.createApp(t, "Metrics", nil)
	env.do(t, request{
		Method: http.MethodPost, Path: "/api/v1/app/" + appID + "/msg",
		Body: map[string]any{"eventType": "invoice.paid", "payload": map[string]any{"a": 1}},
	})

	metrics := env.do(t, request{Method: http.MethodGet, Path: "/metrics", NoAuth: true})
	if metrics.Status != http.StatusOK {
		t.Fatalf("GET /metrics = %d", metrics.Status)
	}
	body := string(metrics.Body)
	for _, want := range []string{
		"huxio_ingest_duration_seconds",
		"huxio_api_request_duration_seconds",
		"huxio_config_snapshot_age_seconds",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics is missing %s", want)
		}
	}
	// The isolation claim is unprovable without a per-tenant label.
	if !strings.Contains(body, `org="`+env.OrgID+`"`) {
		t.Error("ingest duration must be labelled by organization")
	}
}
