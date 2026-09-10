package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/plusiv/huxio/configs"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/infrastructure/ids"
	"github.com/plusiv/huxio/internal/utils"
)

// seedOrg inserts a tenant.
func (e *testEnv) seedOrg(t *testing.T, name string) *entities.Organization {
	t.Helper()
	org := &entities.Organization{
		ID:        ids.New(ids.PrefixOrganization),
		Name:      name,
		CreatedAt: time.Now().UTC(),
	}
	org.UpdatedAt = org.CreatedAt
	if err := e.Organizations.CreateOrganization(context.Background(), org); err != nil {
		t.Fatalf("seed organization: %v", err)
	}
	return org
}

// seedApp inserts an application under an organization.
func (e *testEnv) seedApp(t *testing.T, orgID, name string, uid *string) *entities.Application {
	t.Helper()
	app := &entities.Application{
		ID:        ids.New(ids.PrefixApplication),
		OrgID:     orgID,
		UID:       uid,
		Name:      name,
		CreatedAt: time.Now().UTC(),
	}
	app.UpdatedAt = app.CreatedAt
	if err := e.Applications.CreateApplication(context.Background(), app); err != nil {
		t.Fatalf("seed application: %v", err)
	}
	return app
}

// seedEndpoint inserts an endpoint with a placeholder sealed secret.
func (e *testEnv) seedEndpoint(t *testing.T, app *entities.Application, url string, mutate ...func(*entities.Endpoint)) *entities.Endpoint {
	t.Helper()
	ep := &entities.Endpoint{
		ID:         ids.New(ids.PrefixEndpoint),
		AppID:      app.ID,
		OrgID:      app.OrgID,
		URL:        url,
		Version:    1,
		Secret:     entities.SealedSecret{Sealed: []byte("sealed-secret")},
		SecretType: entities.SecretTypeHMAC256,
		Pool:       configs.DefaultPool,
		CreatedAt:  time.Now().UTC(),
	}
	ep.UpdatedAt = ep.CreatedAt
	for _, m := range mutate {
		m(ep)
	}
	if err := e.Endpoints.CreateEndpoint(context.Background(), ep); err != nil {
		t.Fatalf("seed endpoint: %v", err)
	}
	return ep
}

// seedEventType inserts an event type.
func (e *testEnv) seedEventType(t *testing.T, orgID, name string) *entities.EventType {
	t.Helper()
	et := &entities.EventType{
		OrgID:       orgID,
		Name:        name,
		Description: name + " events",
		CreatedAt:   time.Now().UTC(),
	}
	et.UpdatedAt = et.CreatedAt
	if err := e.EventTypes.CreateEventType(context.Background(), et); err != nil {
		t.Fatalf("seed event type: %v", err)
	}
	return et
}

// seedMessage inserts a message whose created_at equals the timestamp embedded
// in its id, as ingest does.
func (e *testEnv) seedMessage(t *testing.T, app *entities.Application, eventType string, mutate ...func(*entities.Message)) *entities.Message {
	t.Helper()
	now := time.Now().UTC()
	id, err := ids.NewAt(ids.PrefixMessage, now)
	if err != nil {
		t.Fatalf("generate message id: %v", err)
	}
	createdAt, err := ids.TimeOf(id)
	if err != nil {
		t.Fatalf("decode message id: %v", err)
	}
	msg := &entities.Message{
		ID:          id,
		CreatedAt:   createdAt,
		OrgID:       app.OrgID,
		AppID:       app.ID,
		EventType:   eventType,
		Payload:     []byte(`{"hello":"world"}`),
		PayloadSize: len(`{"hello":"world"}`),
		ExpiresAt:   createdAt.AddDate(0, 0, 90),
	}
	for _, m := range mutate {
		m(msg)
	}
	if err := e.Messages.CreateMessage(context.Background(), msg); err != nil {
		t.Fatalf("seed message: %v", err)
	}
	return msg
}

// seedAttempt inserts one delivery attempt through the batch writer path.
func (e *testEnv) seedAttempt(t *testing.T, msg *entities.Message, ep *entities.Endpoint, mutate ...func(*entities.DeliveryAttempt)) entities.DeliveryAttempt {
	t.Helper()
	now := time.Now().UTC()
	id, err := ids.NewAt(ids.PrefixAttempt, now)
	if err != nil {
		t.Fatalf("generate attempt id: %v", err)
	}
	attempt := entities.DeliveryAttempt{
		ID: id,
		// Real time, not the id's second-resolution timestamp: the engine does the
		// same so that "the latest attempt" is well defined.
		CreatedAt:          now,
		OrgID:              msg.OrgID,
		AppID:              msg.AppID,
		MsgID:              msg.ID,
		EndpointID:         ep.ID,
		URL:                ep.URL,
		Status:             entities.AttemptSucceeded,
		ResponseStatusCode: 200,
		ResponseBody:       "ok",
		ResponseDurationMS: 12,
		AttemptNumber:      1,
		TriggerType:        entities.TriggerScheduled,
	}
	for _, m := range mutate {
		m(&attempt)
	}
	if _, err := e.Attempts.CopyAttempts(context.Background(), []entities.DeliveryAttempt{attempt}); err != nil {
		t.Fatalf("seed attempt: %v", err)
	}
	return attempt
}

// uidPtr is a readability helper for optional tenant-assigned identifiers.
func uidPtr(v string) *string { return utils.Ptr(v) }

// seedMessageTx inserts a message using the transaction carried by ctx.
func (e *testEnv) seedMessageTx(t *testing.T, ctx context.Context, app *entities.Application) *entities.Message {
	t.Helper()
	id, err := ids.NewAt(ids.PrefixMessage, time.Now().UTC())
	if err != nil {
		t.Fatalf("generate message id: %v", err)
	}
	createdAt, err := ids.TimeOf(id)
	if err != nil {
		t.Fatalf("decode message id: %v", err)
	}
	msg := &entities.Message{
		ID:          id,
		CreatedAt:   createdAt,
		OrgID:       app.OrgID,
		AppID:       app.ID,
		EventType:   "invoice.paid",
		Payload:     []byte(`{}`),
		PayloadSize: 2,
		ExpiresAt:   createdAt.AddDate(0, 0, 90),
	}
	if err := e.Messages.CreateMessage(ctx, msg); err != nil {
		t.Fatalf("seed message in transaction: %v", err)
	}
	return msg
}
