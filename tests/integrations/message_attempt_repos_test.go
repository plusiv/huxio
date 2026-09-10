package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/ids"
	"github.com/plusiv/huxio/internal/utils"
	"github.com/rotisserie/eris"
)

func TestMessageRepoStoresPayloadOpaquely(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	org := env.seedOrg(t, "Messages")
	app := env.seedApp(t, org.ID, "customer", nil)

	raw := []byte(`{"amount":100,"currency":"EUR"}`)
	msg := env.seedMessage(t, app, "invoice.paid", func(m *entities.Message) {
		m.Payload = raw
		m.PayloadSize = len(raw)
		m.Channels = []string{"ch_acme"}
		m.UID = uidPtr("evt_1")
	})

	// The partition holding a row is derivable from its id alone.
	derived, err := ids.TimeOf(msg.ID)
	if err != nil {
		t.Fatalf("TimeOf: %v", err)
	}
	if !derived.Equal(msg.CreatedAt) {
		t.Fatalf("created_at %s does not match the id timestamp %s", msg.CreatedAt, derived)
	}

	payload, err := env.Messages.GetPayload(ctx, msg.ID, derived)
	if err != nil {
		t.Fatalf("GetPayload: %v", err)
	}
	if string(payload) != string(raw) {
		t.Errorf("payload round-tripped as %q, want the bytes exactly as stored", payload)
	}

	if _, err := env.Messages.GetPayload(ctx, "msg_missing", derived); !eris.Is(err, repositories.ErrNotFound) {
		t.Errorf("expected ErrNotFound for an unknown message, got %v", err)
	}
}

func TestMessageRepoUIDIsUniquePerApplication(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	org := env.seedOrg(t, "Dedup")
	app := env.seedApp(t, org.ID, "customer", nil)
	first := env.seedMessage(t, app, "invoice.paid", func(m *entities.Message) { m.UID = uidPtr("evt_dupe") })

	clash := &entities.Message{
		ID:          ids.New(ids.PrefixMessage),
		CreatedAt:   first.CreatedAt,
		OrgID:       app.OrgID,
		AppID:       app.ID,
		EventType:   "invoice.paid",
		UID:         uidPtr("evt_dupe"),
		Payload:     []byte(`{}`),
		PayloadSize: 2,
		ExpiresAt:   first.ExpiresAt,
	}
	if err := env.Messages.CreateMessage(ctx, clash); !eris.Is(err, repositories.ErrConflict) {
		t.Fatalf("expected ErrConflict on a duplicate event id, got %v", err)
	}
}

func TestMessageRepoListFiltersAndOmitsPayload(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	org := env.seedOrg(t, "Listing")
	app := env.seedApp(t, org.ID, "customer", nil)
	env.seedMessage(t, app, "invoice.paid", func(m *entities.Message) { m.Channels = []string{"ch_acme"} })
	env.seedMessage(t, app, "invoice.void")

	byType, err := env.Messages.GetMessages(ctx, repositories.MessageFilters{
		AppID:     repositories.Eq(app.ID),
		EventType: repositories.Eq("invoice.paid"),
	}, repositories.CursorPagination{Limit: 10})
	if err != nil {
		t.Fatalf("GetMessages: %v", err)
	}
	if len(byType.Items) != 1 {
		t.Fatalf("event type filter returned %d messages", len(byType.Items))
	}
	if byType.Items[0].Payload != nil {
		t.Error("listings must not carry payloads")
	}

	byChannel, err := env.Messages.GetMessages(ctx, repositories.MessageFilters{
		AppID:   repositories.Eq(app.ID),
		Channel: utils.Ptr("ch_acme"),
	}, repositories.CursorPagination{Limit: 10})
	if err != nil {
		t.Fatalf("GetMessages(channel): %v", err)
	}
	if len(byChannel.Items) != 1 {
		t.Errorf("channel filter returned %d messages", len(byChannel.Items))
	}

	window, err := env.Messages.GetMessages(ctx, repositories.MessageFilters{
		AppID:     repositories.Eq(app.ID),
		CreatedAt: &repositories.Filter[time.Time]{GTE: utils.Ptr(time.Now().UTC().Add(time.Hour))},
	}, repositories.CursorPagination{Limit: 10})
	if err != nil {
		t.Fatalf("GetMessages(window): %v", err)
	}
	if len(window.Items) != 0 {
		t.Errorf("future window returned %d messages", len(window.Items))
	}
}

func TestAttemptRepoCopyAndFilters(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	org := env.seedOrg(t, "Attempts")
	app := env.seedApp(t, org.ID, "customer", nil)
	ep := env.seedEndpoint(t, app, "https://example.test/hook")
	other := env.seedEndpoint(t, app, "https://example.test/other")
	msg := env.seedMessage(t, app, "invoice.paid")

	now := time.Now().UTC()
	batch := make([]entities.DeliveryAttempt, 0, 3)
	for i, spec := range []struct {
		endpointID string
		status     entities.AttemptStatus
		code       int16
	}{
		{ep.ID, entities.AttemptSucceeded, 200},
		{ep.ID, entities.AttemptFailed, 500},
		{other.ID, entities.AttemptPendingRetry, 429},
	} {
		id, err := ids.NewAt(ids.PrefixAttempt, now.Add(time.Duration(i)*time.Second))
		if err != nil {
			t.Fatalf("generate attempt id: %v", err)
		}
		createdAt, err := ids.TimeOf(id)
		if err != nil {
			t.Fatalf("decode attempt id: %v", err)
		}
		batch = append(batch, entities.DeliveryAttempt{
			ID:                 id,
			CreatedAt:          createdAt,
			OrgID:              org.ID,
			AppID:              app.ID,
			MsgID:              msg.ID,
			EndpointID:         spec.endpointID,
			URL:                "https://example.test/hook",
			Status:             spec.status,
			ResponseStatusCode: spec.code,
			ResponseBody:       "body",
			ResponseDurationMS: 10,
			AttemptNumber:      int16(i + 1),
			TriggerType:        entities.TriggerScheduled,
		})
	}

	copied, err := env.Attempts.CopyAttempts(ctx, batch)
	if err != nil {
		t.Fatalf("CopyAttempts: %v", err)
	}
	if copied != int64(len(batch)) {
		t.Fatalf("CopyAttempts copied %d rows, want %d", copied, len(batch))
	}
	if n, err := env.Attempts.CopyAttempts(ctx, nil); err != nil || n != 0 {
		t.Errorf("CopyAttempts(nil) = %d, %v", n, err)
	}

	// "Everything that failed to this endpoint", the replay headline feature.
	failed, err := env.Attempts.GetAttempts(ctx, repositories.AttemptFilters{
		EndpointID: repositories.Eq(ep.ID),
		Status:     repositories.Eq(entities.AttemptFailed),
	}, repositories.CursorPagination{Limit: 10})
	if err != nil {
		t.Fatalf("GetAttempts(failed): %v", err)
	}
	if len(failed.Items) != 1 || failed.Items[0].ResponseStatusCode != 500 {
		t.Fatalf("failed filter returned %+v", failed.Items)
	}

	byClass, err := env.Attempts.GetAttempts(ctx, repositories.AttemptFilters{
		MsgID:           repositories.Eq(msg.ID),
		StatusCodeClass: utils.Ptr(4),
	}, repositories.CursorPagination{Limit: 10})
	if err != nil {
		t.Fatalf("GetAttempts(statusCodeClass): %v", err)
	}
	if len(byClass.Items) != 1 || byClass.Items[0].ResponseStatusCode != 429 {
		t.Errorf("status class filter returned %+v", byClass.Items)
	}

	// Newest first, so the portal's log reads chronologically downward.
	all, err := env.Attempts.GetAttempts(ctx, repositories.AttemptFilters{
		MsgID: repositories.Eq(msg.ID),
	}, repositories.CursorPagination{Limit: 10})
	if err != nil {
		t.Fatalf("GetAttempts: %v", err)
	}
	if len(all.Items) != 3 {
		t.Fatalf("expected 3 attempts, got %d", len(all.Items))
	}
	for i := 1; i < len(all.Items); i++ {
		if all.Items[i-1].CreatedAt.Before(all.Items[i].CreatedAt) {
			t.Error("attempts must be returned newest first")
		}
	}

	single, err := env.Attempts.GetAttempt(ctx, repositories.AttemptFilters{ID: repositories.Eq(batch[0].ID)})
	if err != nil {
		t.Fatalf("GetAttempt: %v", err)
	}
	if single.TriggerType != entities.TriggerScheduled || single.Status != entities.AttemptSucceeded {
		t.Errorf("attempt round-tripped as %+v", single)
	}
}
