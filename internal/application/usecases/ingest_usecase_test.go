package usecases_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/plusiv/huxio/internal/application/usecases"
	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/ids"
	"github.com/plusiv/huxio/internal/utils"
	"github.com/rotisserie/eris"
)

type ingestFixture struct {
	uc       *usecases.IngestUseCase
	tx       *fakeTx
	messages *fakeMessageRepo
	queue    *fakeQueue
}

func newIngestFixture(t *testing.T, data *repositories.SnapshotData, cfg usecases.IngestConfig) *ingestFixture {
	t.Helper()

	sealer, err := testSealer()
	if err != nil {
		t.Fatalf("testSealer: %v", err)
	}
	snapshot, err := buildSnapshot(sealer, data)
	if err != nil {
		t.Fatalf("buildSnapshot: %v", err)
	}
	codec, err := testCodec()
	if err != nil {
		t.Fatalf("testCodec: %v", err)
	}
	t.Cleanup(codec.Close)

	f := &ingestFixture{tx: &fakeTx{}, messages: newFakeMessageRepo(), queue: &fakeQueue{}}
	f.uc = usecases.NewIngestUseCase(f.tx, f.messages, f.queue, staticSnapshot{snapshot}, codec, cfg)
	return f
}

// oneEndpointApp is the common shape: one tenant, one application addressable
// by uid, one endpoint subscribed to invoice.paid.
func oneEndpointApp() *repositories.SnapshotData {
	return &repositories.SnapshotData{
		Organizations: []*entities.Organization{{ID: "org_1", Name: "Acme"}},
		Applications:  []*entities.Application{{ID: "app_1", OrgID: "org_1", UID: utils.Ptr("cust_1")}},
		Endpoints: []*entities.Endpoint{
			{ID: "ep_1", AppID: "app_1", OrgID: "org_1", URL: "https://example.test/hook", EventTypes: []string{"invoice.paid"}},
		},
	}
}

func TestIngestStoresMessageAndQueuesOneFanout(t *testing.T) {
	t.Parallel()

	f := newIngestFixture(t, oneEndpointApp(), usecases.IngestConfig{})

	result, err := f.uc.Ingest(context.Background(), usecases.IngestInput{
		OrgID:      "org_1",
		AppIDOrUID: "cust_1", // addressed by the tenant-assigned uid
		EventType:  "invoice.paid",
		Payload:    json.RawMessage(`{"amount":100}`),
	})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if result.MatchedEndpoints != 1 {
		t.Errorf("MatchedEndpoints = %d, want 1", result.MatchedEndpoints)
	}

	stored := f.messages.lastCreated()
	if stored == nil {
		t.Fatal("no message was stored")
	}
	if stored.AppID != "app_1" || stored.OrgID != "org_1" {
		t.Errorf("message scoped to %s/%s", stored.OrgID, stored.AppID)
	}
	if stored.PayloadSize != len(`{"amount":100}`) {
		t.Errorf("PayloadSize = %d, want the uncompressed length", stored.PayloadSize)
	}

	// created_at must equal the timestamp embedded in the id, or a lookup by
	// id cannot find the right time partition.
	fromID, err := ids.TimeOf(stored.ID)
	if err != nil {
		t.Fatalf("TimeOf: %v", err)
	}
	if !fromID.Equal(stored.CreatedAt) {
		t.Errorf("created_at %s does not match the id timestamp %s", stored.CreatedAt, fromID)
	}

	// Exactly one enqueue, whatever the endpoint count, routed on app id.
	tasks := f.queue.tasks()
	if len(tasks) != 1 {
		t.Fatalf("enqueued %d tasks, want exactly one fan-out", len(tasks))
	}
	if tasks[0].Kind != entities.TaskFanout {
		t.Errorf("task kind = %v, want fan-out", tasks[0].Kind)
	}
	if want := entities.PartitionKeyFor("app_1"); tasks[0].PartitionKey != want {
		t.Errorf("partition key = %d, want crc32(app_id) %% 256 = %d", tasks[0].PartitionKey, want)
	}
	if tasks[0].MsgID != stored.ID || !tasks[0].MsgCreatedAt.Equal(stored.CreatedAt) {
		t.Error("the queued task must carry both message key columns")
	}

	// Both writes happened in one unit of work, and the wakeup came after it.
	if f.tx.calls != 1 {
		t.Errorf("transactions = %d, want 1", f.tx.calls)
	}
	if got := f.queue.notifications(); len(got) != 1 || got[0] != tasks[0].PartitionKey {
		t.Errorf("notifications = %v, want the fan-out partition", got)
	}
}

func TestIngestManyEndpointsStillCostsOneEnqueue(t *testing.T) {
	t.Parallel()

	data := oneEndpointApp()
	for i := range 200 {
		data.Endpoints = append(data.Endpoints, &entities.Endpoint{
			ID:    "ep_bulk_" + string(rune('a'+i%26)) + string(rune('a'+i/26)),
			AppID: "app_1", OrgID: "org_1", URL: "https://example.test/bulk",
		})
	}
	f := newIngestFixture(t, data, usecases.IngestConfig{})

	result, err := f.uc.Ingest(context.Background(), usecases.IngestInput{
		OrgID: "org_1", AppIDOrUID: "app_1", EventType: "invoice.paid",
		Payload: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if result.MatchedEndpoints != 201 {
		t.Errorf("MatchedEndpoints = %d, want every endpoint", result.MatchedEndpoints)
	}
	if got := len(f.queue.tasks()); got != 1 {
		t.Errorf("enqueued %d tasks for 201 endpoints; the request path must cost one", got)
	}
}

func TestIngestWithNoMatchingEndpointStillQueuesTheFanout(t *testing.T) {
	t.Parallel()

	f := newIngestFixture(t, oneEndpointApp(), usecases.IngestConfig{})

	result, err := f.uc.Ingest(context.Background(), usecases.IngestInput{
		OrgID: "org_1", AppIDOrUID: "app_1",
		EventType: "customer.created", // nothing in this snapshot subscribes
		Payload:   json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if result.MatchedEndpoints != 0 {
		t.Errorf("MatchedEndpoints = %d, want 0", result.MatchedEndpoints)
	}
	if f.messages.lastCreated() == nil {
		t.Error("the message must be stored")
	}
	// The match is advisory: an endpoint created moments ago may not be in
	// this node's snapshot yet, so the fan-out is queued regardless and the
	// worker decides. Skipping it here would silently drop that delivery.
	tasks := f.queue.tasks()
	if len(tasks) != 1 || tasks[0].Kind != entities.TaskFanout {
		t.Errorf("enqueued %+v, want one fan-out task", tasks)
	}
}

func TestIngestChannelFiltering(t *testing.T) {
	t.Parallel()

	data := oneEndpointApp()
	data.Endpoints = append(data.Endpoints, &entities.Endpoint{
		ID: "ep_channel", AppID: "app_1", OrgID: "org_1",
		URL: "https://example.test/channel", Channels: []string{"ch_acme"},
	})
	f := newIngestFixture(t, data, usecases.IngestConfig{})

	tests := []struct {
		name     string
		channels []string
		want     int
	}{
		{"no channels skips the channelled endpoint", nil, 1},
		{"matching channel includes it", []string{"ch_acme"}, 2},
		{"other channel excludes it", []string{"ch_other"}, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := f.uc.Ingest(context.Background(), usecases.IngestInput{
				OrgID: "org_1", AppIDOrUID: "app_1", EventType: "invoice.paid",
				Payload: json.RawMessage(`{}`), Channels: tc.channels,
			})
			if err != nil {
				t.Fatalf("Ingest: %v", err)
			}
			if result.MatchedEndpoints != tc.want {
				t.Errorf("MatchedEndpoints = %d, want %d", result.MatchedEndpoints, tc.want)
			}
		})
	}
}

func TestIngestValidation(t *testing.T) {
	t.Parallel()

	f := newIngestFixture(t, oneEndpointApp(), usecases.IngestConfig{MaxPayloadBytes: 64})

	tests := []struct {
		name string
		in   usecases.IngestInput
		kind apperrors.ErrorKind
	}{
		{
			"unknown application",
			usecases.IngestInput{OrgID: "org_1", AppIDOrUID: "app_missing", EventType: "invoice.paid", Payload: json.RawMessage(`{}`)},
			apperrors.KindNotFound,
		},
		{
			"application in another tenant",
			usecases.IngestInput{OrgID: "org_other", AppIDOrUID: "app_1", EventType: "invoice.paid", Payload: json.RawMessage(`{}`)},
			apperrors.KindNotFound,
		},
		{
			"missing event type",
			usecases.IngestInput{OrgID: "org_1", AppIDOrUID: "app_1", Payload: json.RawMessage(`{}`)},
			apperrors.KindValidation,
		},
		{
			"empty payload",
			usecases.IngestInput{OrgID: "org_1", AppIDOrUID: "app_1", EventType: "invoice.paid"},
			apperrors.KindValidation,
		},
		{
			"malformed json",
			usecases.IngestInput{OrgID: "org_1", AppIDOrUID: "app_1", EventType: "invoice.paid", Payload: json.RawMessage(`{"a":`)},
			apperrors.KindValidation,
		},
		{
			"oversized payload",
			usecases.IngestInput{OrgID: "org_1", AppIDOrUID: "app_1", EventType: "invoice.paid", Payload: json.RawMessage(`{"a":"` + strings.Repeat("x", 200) + `"}`)},
			apperrors.KindPayloadTooLarge,
		},
		{
			"absurd retention",
			usecases.IngestInput{OrgID: "org_1", AppIDOrUID: "app_1", EventType: "invoice.paid", Payload: json.RawMessage(`{}`), RetentionDays: utils.Ptr(4000)},
			apperrors.KindValidation,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := f.uc.Ingest(context.Background(), tc.in)
			var appErr *apperrors.AppError
			if !eris.As(err, &appErr) {
				t.Fatalf("error = %v, want an AppError", err)
			}
			if appErr.Kind != tc.kind {
				t.Errorf("Kind = %q, want %q", appErr.Kind, tc.kind)
			}
		})
	}
}

func TestIngestMapsDuplicateEventIDToConflict(t *testing.T) {
	t.Parallel()

	f := newIngestFixture(t, oneEndpointApp(), usecases.IngestConfig{})
	f.messages.createErr = eris.Wrap(repositories.ErrConflict, "insert message")

	_, err := f.uc.Ingest(context.Background(), usecases.IngestInput{
		OrgID: "org_1", AppIDOrUID: "app_1", EventType: "invoice.paid",
		Payload: json.RawMessage(`{}`), EventID: utils.Ptr("evt_1"),
	})
	var appErr *apperrors.AppError
	if !eris.As(err, &appErr) || appErr.Kind != apperrors.KindConflict {
		t.Fatalf("error = %v, want a conflict", err)
	}
	if got := f.queue.notifications(); len(got) != 0 {
		t.Error("a failed ingest must not send a wakeup")
	}
}

func TestIngestDoesNotAcknowledgeWhenCommitFails(t *testing.T) {
	t.Parallel()

	f := newIngestFixture(t, oneEndpointApp(), usecases.IngestConfig{})
	f.tx.commitErr = eris.New("commit failed")

	_, err := f.uc.Ingest(context.Background(), usecases.IngestInput{
		OrgID: "org_1", AppIDOrUID: "app_1", EventType: "invoice.paid",
		Payload: json.RawMessage(`{}`),
	})
	var appErr *apperrors.AppError
	if !eris.As(err, &appErr) || appErr.Kind != apperrors.KindInternal {
		t.Fatalf("error = %v, want an internal error", err)
	}
	// Durability before acknowledgement: no 202 without a commit, and no wakeup
	// for rows that were never committed.
	if got := f.queue.notifications(); len(got) != 0 {
		t.Error("a failed commit must not send a wakeup")
	}
}

func TestIngestSurvivesNotifyFailure(t *testing.T) {
	t.Parallel()

	f := newIngestFixture(t, oneEndpointApp(), usecases.IngestConfig{})
	f.queue.notifyErr = eris.New("notify failed")

	// A lost wakeup costs latency, not a delivery: workers also poll.
	if _, err := f.uc.Ingest(context.Background(), usecases.IngestInput{
		OrgID: "org_1", AppIDOrUID: "app_1", EventType: "invoice.paid",
		Payload: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("Ingest must not fail because a notification did: %v", err)
	}
}

func TestIngestRoundTripsPayloadThroughStorage(t *testing.T) {
	t.Parallel()

	f := newIngestFixture(t, oneEndpointApp(), usecases.IngestConfig{})
	raw := json.RawMessage(`{"nested":{"list":[1,2,3]},"text":"` + strings.Repeat("compress me ", 100) + `"}`)

	result, err := f.uc.Ingest(context.Background(), usecases.IngestInput{
		OrgID: "org_1", AppIDOrUID: "app_1", EventType: "invoice.paid", Payload: raw,
	})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	stored := f.messages.lastCreated()
	if len(stored.Payload) >= len(raw) {
		t.Errorf("stored %d bytes for a %d byte payload; it should have compressed", len(stored.Payload), len(raw))
	}

	loaded, err := f.uc.LoadPayload(context.Background(), result.Message.ID, result.Message.CreatedAt)
	if err != nil {
		t.Fatalf("LoadPayload: %v", err)
	}
	if string(loaded) != string(raw) {
		t.Error("payload did not survive the storage round trip byte for byte")
	}

	if _, err := f.uc.LoadPayload(context.Background(), "msg_unknown", result.Message.CreatedAt); err == nil {
		t.Error("LoadPayload must fail for an unknown message")
	}
}

// fakeInvalidatorCounter records snapshot invalidation requests.
type fakeInvalidatorCounter struct{ calls int }

func (f *fakeInvalidatorCounter) Invalidate() { f.calls++ }

// emptySnapshot serves a snapshot that knows nothing, standing in for a node
// whose snapshot has not caught up yet.
func emptySnapshot(t *testing.T) staticSnapshot {
	t.Helper()
	sealer, err := testSealer()
	if err != nil {
		t.Fatalf("testSealer: %v", err)
	}
	snapshot, err := buildSnapshot(sealer, &repositories.SnapshotData{})
	if err != nil {
		t.Fatalf("buildSnapshot: %v", err)
	}
	return staticSnapshot{snapshot}
}

func TestIngestFallsBackToTheDatabaseWhenTheSnapshotIsStale(t *testing.T) {
	t.Parallel()

	// The application exists in the database but not yet in this node's snapshot:
	// exactly the state right after "create app, then send".
	apps := newFakeApplicationRepo()
	if err := apps.CreateApplication(context.Background(), &entities.Application{
		ID: "app_1", OrgID: "org_1", UID: utils.Ptr("cust_1"),
	}); err != nil {
		t.Fatalf("CreateApplication: %v", err)
	}

	codec, err := testCodec()
	if err != nil {
		t.Fatalf("testCodec: %v", err)
	}
	t.Cleanup(codec.Close)

	tx := &fakeTx{}
	messages := newFakeMessageRepo()
	queue := &fakeQueue{}
	invalidator := &fakeInvalidatorCounter{}

	uc := usecases.NewIngestUseCase(tx, messages, queue, emptySnapshot(t), codec, usecases.IngestConfig{},
		usecases.WithApplicationFallback(apps, invalidator))

	for _, key := range []string{"app_1", "cust_1"} {
		result, err := uc.Ingest(context.Background(), usecases.IngestInput{
			OrgID: "org_1", AppIDOrUID: key, EventType: "invoice.paid",
			Payload: json.RawMessage(`{"a":1}`),
		})
		if err != nil {
			t.Fatalf("Ingest(%q): %v", key, err)
		}
		// The snapshot cannot match endpoints yet, so the count is zero, but
		// the fan-out is still queued for the worker to expand.
		if result.MatchedEndpoints != 0 {
			t.Errorf("MatchedEndpoints = %d", result.MatchedEndpoints)
		}
		if len(queue.tasks()) == 0 {
			t.Error("a stale snapshot must not stop the fan-out being queued")
		}
	}
	if messages.lastCreated() == nil {
		t.Error("the message must be stored")
	}
	// The stale snapshot is asked to catch up rather than being consulted
	// through the database on every subsequent request.
	if invalidator.calls == 0 {
		t.Error("a snapshot miss must trigger a reload")
	}

	// A genuinely unknown application is still a 404, and one in another
	// tenant is not reachable through the fallback either.
	for _, in := range []usecases.IngestInput{
		{OrgID: "org_1", AppIDOrUID: "app_missing", EventType: "invoice.paid", Payload: json.RawMessage(`{}`)},
		{OrgID: "org_other", AppIDOrUID: "app_1", EventType: "invoice.paid", Payload: json.RawMessage(`{}`)},
	} {
		if _, err := uc.Ingest(context.Background(), in); kindOf(t, err) != apperrors.KindNotFound {
			t.Errorf("Ingest(%+v) = %v, want not found", in, err)
		}
	}
}

func TestIngestWithoutFallbackStillRequiresTheSnapshot(t *testing.T) {
	t.Parallel()

	codec, err := testCodec()
	if err != nil {
		t.Fatalf("testCodec: %v", err)
	}
	t.Cleanup(codec.Close)

	uc := usecases.NewIngestUseCase(&fakeTx{}, newFakeMessageRepo(), &fakeQueue{}, emptySnapshot(t), codec, usecases.IngestConfig{})

	if _, err := uc.Ingest(context.Background(), usecases.IngestInput{
		OrgID: "org_1", AppIDOrUID: "app_1", EventType: "invoice.paid", Payload: json.RawMessage(`{}`),
	}); kindOf(t, err) != apperrors.KindNotFound {
		t.Errorf("error = %v, want not found", err)
	}
}
