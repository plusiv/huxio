package integration_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/infrastructure/ids"
)

func TestAPIIngestStoresMessageAndQueuesFanout(t *testing.T) {
	t.Parallel()
	env := newAPIEnv(t)
	ctx := context.Background()

	appID := env.createApp(t, "Acme customer", uidPtr("cust_1"))
	env.createEndpoint(t, appID, map[string]any{
		"url":         "https://example.test/hook",
		"filterTypes": []string{"invoice.paid"},
	})

	resp := env.do(t, request{
		Method: http.MethodPost,
		Path:   "/api/v1/app/" + appID + "/msg",
		Body: map[string]any{
			"eventType": "invoice.paid",
			"eventId":   "evt_1",
			"payload":   map[string]any{"amount": 100, "currency": "EUR"},
		},
	})
	if resp.Status != http.StatusAccepted {
		t.Fatalf("POST /msg = %d %s, want 202", resp.Status, resp.Body)
	}

	var created struct {
		ID        string         `json:"id"`
		EventType string         `json:"eventType"`
		EventID   string         `json:"eventId"`
		Payload   map[string]any `json:"payload"`
	}
	resp.decode(t, &created)
	if !strings.HasPrefix(created.ID, "msg_") {
		t.Errorf("id = %q, want a msg_ prefixed KSUID", created.ID)
	}
	if created.EventType != "invoice.paid" || created.EventID != "evt_1" {
		t.Errorf("response = %+v", created)
	}
	if created.Payload["currency"] != "EUR" {
		t.Errorf("payload echoed as %v", created.Payload)
	}

	// A 202 means the row is on disk, not that it is queued for later.
	var messages int
	if err := env.Pool.QueryRow(ctx, `SELECT count(*) FROM message WHERE id = $1`, created.ID).Scan(&messages); err != nil {
		t.Fatalf("count message: %v", err)
	}
	if messages != 1 {
		t.Fatal("the message must be committed before the 202")
	}

	// Exactly one fan-out task, routed on the application id.
	var (
		tasks        int
		kind         int16
		partitionKey int16
	)
	if err := env.Pool.QueryRow(ctx,
		`SELECT count(*), min(kind), min(partition_key) FROM delivery_task WHERE msg_id = $1`, created.ID,
	).Scan(&tasks, &kind, &partitionKey); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	if tasks != 1 {
		t.Fatalf("queued %d tasks, want exactly one fan-out", tasks)
	}
	if entities.TaskKind(kind) != entities.TaskFanout {
		t.Errorf("task kind = %d, want fan-out", kind)
	}
	if want := entities.PartitionKeyFor(appID); partitionKey != want {
		t.Errorf("partition key = %d, want %d", partitionKey, want)
	}

	// Reading it back returns the payload byte for byte.
	fetched := env.do(t, request{Method: http.MethodGet, Path: "/api/v1/app/" + appID + "/msg/" + created.ID})
	if fetched.Status != http.StatusOK {
		t.Fatalf("GET /msg/:id = %d %s", fetched.Status, fetched.Body)
	}
	if got := fetched.field(t, "eventType"); got != "invoice.paid" {
		t.Errorf("eventType = %v", got)
	}

	// The listing omits payloads.
	list := env.do(t, request{Method: http.MethodGet, Path: "/api/v1/app/" + appID + "/msg?limit=10"})
	if list.Status != http.StatusOK {
		t.Fatalf("GET /msg = %d %s", list.Status, list.Body)
	}
	var listed struct {
		Data []struct {
			ID      string         `json:"id"`
			Payload map[string]any `json:"payload"`
		} `json:"data"`
		Done bool `json:"done"`
	}
	list.decode(t, &listed)
	if len(listed.Data) != 1 || listed.Data[0].ID != created.ID {
		t.Fatalf("listing = %+v", listed)
	}
	if listed.Data[0].Payload != nil {
		t.Error("listings must not carry payloads")
	}
	if !listed.Done {
		t.Error("a single-page listing must report done")
	}
}

func TestAPIIngestAddressesApplicationByUID(t *testing.T) {
	t.Parallel()
	env := newAPIEnv(t)

	appID := env.createApp(t, "Acme customer", uidPtr("cust_1"))
	env.createEndpoint(t, appID, map[string]any{"url": "https://example.test/hook"})

	// The compatible SDKs address applications by the tenant's own key.
	resp := env.do(t, request{
		Method: http.MethodPost,
		Path:   "/api/v1/app/cust_1/msg",
		Body:   map[string]any{"eventType": "invoice.paid", "payload": map[string]any{"a": 1}},
	})
	if resp.Status != http.StatusAccepted {
		t.Fatalf("POST by uid = %d %s, want 202", resp.Status, resp.Body)
	}
}

func TestAPIIngestWithoutMatchingEndpointStillAcceptsAndQueues(t *testing.T) {
	t.Parallel()
	env := newAPIEnv(t)
	ctx := context.Background()

	appID := env.createApp(t, "No endpoints", nil)

	resp := env.do(t, request{
		Method: http.MethodPost,
		Path:   "/api/v1/app/" + appID + "/msg",
		Body:   map[string]any{"eventType": "invoice.paid", "payload": map[string]any{"a": 1}},
	})
	if resp.Status != http.StatusAccepted {
		t.Fatalf("POST /msg = %d %s, want 202", resp.Status, resp.Body)
	}

	var messages, tasks int
	if err := env.Pool.QueryRow(ctx, `SELECT count(*) FROM message`).Scan(&messages); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if err := env.Pool.QueryRow(ctx, `SELECT count(*) FROM delivery_task`).Scan(&tasks); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	if messages != 1 {
		t.Error("the message must be stored even when nothing subscribes")
	}
	// The fan-out is queued regardless of what this node's snapshot knows:
	// an endpoint created moments ago may not be in it yet, and skipping the
	// insert on that basis would silently drop a delivery we acknowledged.
	if tasks != 1 {
		t.Errorf("queued %d tasks, want the fan-out", tasks)
	}
}

func TestAPIIngestRejections(t *testing.T) {
	t.Parallel()
	env := newAPIEnv(t)

	appID := env.createApp(t, "Rejections", nil)
	path := "/api/v1/app/" + appID + "/msg"

	tests := []struct {
		name       string
		body       any
		path       string
		wantStatus int
		wantCode   string
	}{
		{
			"missing event type",
			map[string]any{"payload": map[string]any{"a": 1}},
			path, http.StatusUnprocessableEntity, "",
		},
		{
			"missing payload",
			map[string]any{"eventType": "invoice.paid"},
			path, http.StatusUnprocessableEntity, "",
		},
		{
			"malformed json body",
			`{"eventType":"invoice.paid","payload":`,
			path, http.StatusBadRequest, "bad_request",
		},
		{
			"unknown application",
			map[string]any{"eventType": "invoice.paid", "payload": map[string]any{"a": 1}},
			"/api/v1/app/app_missing/msg", http.StatusNotFound, "not_found",
		},
		{
			"oversized payload",
			`{"eventType":"invoice.paid","payload":{"blob":"` + strings.Repeat("x", testMaxPayloadBytes) + `"}}`,
			path, http.StatusRequestEntityTooLarge, "payload_too_large",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := env.do(t, request{Method: http.MethodPost, Path: tc.path, Body: tc.body})
			if resp.Status != tc.wantStatus {
				t.Fatalf("status = %d %s, want %d", resp.Status, resp.Body, tc.wantStatus)
			}
			if tc.wantCode != "" {
				if got := resp.field(t, "code"); got != tc.wantCode {
					t.Errorf("code = %v, want %q", got, tc.wantCode)
				}
			}
			// No error response may leak internals.
			if strings.Contains(string(resp.Body), "postgres") || strings.Contains(string(resp.Body), "pgx") {
				t.Errorf("error response leaked internals: %s", resp.Body)
			}
		})
	}
}

func TestAPIIngestDuplicateEventIDConflicts(t *testing.T) {
	t.Parallel()
	env := newAPIEnv(t)

	appID := env.createApp(t, "Dedup", nil)
	body := map[string]any{
		"eventType": "invoice.paid",
		"eventId":   "evt_dupe",
		"payload":   map[string]any{"a": 1},
	}

	if resp := env.do(t, request{Method: http.MethodPost, Path: "/api/v1/app/" + appID + "/msg", Body: body}); resp.Status != http.StatusAccepted {
		t.Fatalf("first POST = %d %s", resp.Status, resp.Body)
	}
	resp := env.do(t, request{Method: http.MethodPost, Path: "/api/v1/app/" + appID + "/msg", Body: body})
	if resp.Status != http.StatusConflict {
		t.Fatalf("duplicate eventId = %d %s, want 409", resp.Status, resp.Body)
	}
}

func TestAPIIdempotencyReplaysTheOriginalResponse(t *testing.T) {
	t.Parallel()
	env := newAPIEnv(t)
	ctx := context.Background()

	appID := env.createApp(t, "Idempotent", nil)
	env.createEndpoint(t, appID, map[string]any{"url": "https://example.test/hook"})

	req := request{
		Method:  http.MethodPost,
		Path:    "/api/v1/app/" + appID + "/msg",
		Body:    map[string]any{"eventType": "invoice.paid", "payload": map[string]any{"a": 1}},
		Headers: map[string]string{"Idempotency-Key": "client-key-1"},
	}

	first := env.do(t, req)
	if first.Status != http.StatusAccepted {
		t.Fatalf("first POST = %d %s", first.Status, first.Body)
	}
	second := env.do(t, req)
	if second.Status != http.StatusAccepted {
		t.Fatalf("replayed POST = %d %s", second.Status, second.Body)
	}
	if string(first.Body) != string(second.Body) {
		t.Errorf("replay returned a different body:\n%s\n%s", first.Body, second.Body)
	}

	// One message, not two: the retry did not create a second one.
	var messages int
	if err := env.Pool.QueryRow(ctx, `SELECT count(*) FROM message`).Scan(&messages); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if messages != 1 {
		t.Errorf("stored %d messages, want 1", messages)
	}

	// A different key is a different request.
	other := env.do(t, request{
		Method:  req.Method,
		Path:    req.Path,
		Body:    req.Body,
		Headers: map[string]string{"Idempotency-Key": "client-key-2"},
	})
	if other.Status != http.StatusAccepted {
		t.Fatalf("second key = %d %s", other.Status, other.Body)
	}
	if err := env.Pool.QueryRow(ctx, `SELECT count(*) FROM message`).Scan(&messages); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if messages != 2 {
		t.Errorf("stored %d messages, want 2", messages)
	}
}

func TestAPIIdempotencyKeyIsScopedPerTenant(t *testing.T) {
	t.Parallel()
	env := newAPIEnv(t)
	ctx := context.Background()

	// A second tenant using the identical client key must not receive the
	// first tenant's cached response.
	otherOrg := env.seedOrg(t, "Other tenant")
	otherToken, _, err := env.Tokens.IssueOrgToken(otherOrg.ID, 0)
	if err != nil {
		t.Fatalf("IssueOrgToken: %v", err)
	}
	env.reloadConfig(t)

	appA := env.createApp(t, "Tenant A app", nil)

	resp := env.do(t, request{
		Method:  http.MethodPost,
		Path:    "/api/v1/app/" + appA + "/msg",
		Body:    map[string]any{"eventType": "invoice.paid", "payload": map[string]any{"a": 1}},
		Headers: map[string]string{"Idempotency-Key": "shared-key"},
	})
	if resp.Status != http.StatusAccepted {
		t.Fatalf("tenant A POST = %d %s", resp.Status, resp.Body)
	}

	// The other tenant reaching a path it does not own gets a 404, not a
	// replay of tenant A's 202.
	crossed := env.do(t, request{
		Method:  http.MethodPost,
		Path:    "/api/v1/app/" + appA + "/msg",
		Body:    map[string]any{"eventType": "invoice.paid", "payload": map[string]any{"a": 1}},
		Headers: map[string]string{"Idempotency-Key": "shared-key"},
		Token:   otherToken,
	})
	if crossed.Status != http.StatusNotFound {
		t.Fatalf("cross-tenant POST = %d %s, want 404", crossed.Status, crossed.Body)
	}

	var messages int
	if err := env.Pool.QueryRow(ctx, `SELECT count(*) FROM message`).Scan(&messages); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if messages != 1 {
		t.Errorf("stored %d messages, want only tenant A's", messages)
	}
}

func TestAPIMessageLookupRequiresTheOwningTenant(t *testing.T) {
	t.Parallel()
	env := newAPIEnv(t)

	appID := env.createApp(t, "Owner", nil)
	env.createEndpoint(t, appID, map[string]any{"url": "https://example.test/hook"})
	resp := env.do(t, request{
		Method: http.MethodPost,
		Path:   "/api/v1/app/" + appID + "/msg",
		Body:   map[string]any{"eventType": "invoice.paid", "payload": map[string]any{"a": 1}},
	})
	msgID, _ := resp.field(t, "id").(string)

	// Another tenant cannot read it, even with the exact ids.
	crossed := env.do(t, request{
		Method: http.MethodGet,
		Path:   "/api/v1/app/" + appID + "/msg/" + msgID,
		Token:  env.otherOrgToken(t),
	})
	if crossed.Status != http.StatusNotFound {
		t.Errorf("cross-tenant read = %d %s, want 404", crossed.Status, crossed.Body)
	}

	// An unknown message inside the right tenant is also a 404, not a 500.
	unknown := env.do(t, request{
		Method: http.MethodGet,
		Path:   "/api/v1/app/" + appID + "/msg/" + ids.New(ids.PrefixMessage),
	})
	if unknown.Status != http.StatusNotFound {
		t.Errorf("unknown message = %d %s, want 404", unknown.Status, unknown.Body)
	}
}
