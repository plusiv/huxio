package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/plusiv/huxio/configs"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/ids"
	"github.com/plusiv/huxio/internal/utils"
	"github.com/rotisserie/eris"
)

func TestOrganizationRepoCRUD(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	org := env.seedOrg(t, "Acme")

	got, err := env.Organizations.GetOrganization(ctx, repositories.OrganizationFilters{ID: repositories.Eq(org.ID)})
	if err != nil {
		t.Fatalf("GetOrganization: %v", err)
	}
	if got.Name != "Acme" {
		t.Errorf("Name = %q", got.Name)
	}

	got.Name = "Acme Renamed"
	got.UpdatedAt = time.Now().UTC()
	if err := env.Organizations.UpdateOrganization(ctx, got); err != nil {
		t.Fatalf("UpdateOrganization: %v", err)
	}

	if err := env.Organizations.DeleteOrganization(ctx, org.ID); err != nil {
		t.Fatalf("DeleteOrganization: %v", err)
	}
	if _, err := env.Organizations.GetOrganization(ctx, repositories.OrganizationFilters{ID: repositories.Eq(org.ID)}); !eris.Is(err, repositories.ErrNotFound) {
		t.Errorf("expected ErrNotFound after soft delete, got %v", err)
	}
	// A second delete finds nothing live to delete.
	if err := env.Organizations.DeleteOrganization(ctx, org.ID); !eris.Is(err, repositories.ErrNotFound) {
		t.Errorf("expected ErrNotFound on repeated delete, got %v", err)
	}
	// The row is still there, which is the point of a soft delete.
	deleted, err := env.Organizations.GetOrganization(ctx, repositories.OrganizationFilters{
		ID:        repositories.Eq(org.ID),
		DeletedAt: repositories.Eq(true),
	})
	if err != nil {
		t.Fatalf("GetOrganization(deleted): %v", err)
	}
	if deleted.DeletedAt == nil {
		t.Error("deleted_at must be stamped, not the row removed")
	}
}

func TestApplicationRepoUIDIsUniquePerOrg(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	orgA := env.seedOrg(t, "A")
	orgB := env.seedOrg(t, "B")

	env.seedApp(t, orgA.ID, "customer-1", uidPtr("cust_1"))

	// Same uid in another organization is fine.
	env.seedApp(t, orgB.ID, "customer-1", uidPtr("cust_1"))

	// Same uid in the same organization conflicts.
	dupe := &entities.Application{
		ID:        ids.New(ids.PrefixApplication),
		OrgID:     orgA.ID,
		UID:       uidPtr("cust_1"),
		Name:      "duplicate",
		CreatedAt: time.Now().UTC(),
	}
	dupe.UpdatedAt = dupe.CreatedAt
	if err := env.Applications.CreateApplication(ctx, dupe); !eris.Is(err, repositories.ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}

	byUID, err := env.Applications.GetApplication(ctx, repositories.ApplicationFilters{
		OrgID: repositories.Eq(orgA.ID),
		UID:   repositories.Eq("cust_1"),
	})
	if err != nil {
		t.Fatalf("GetApplication by uid: %v", err)
	}
	if byUID.OrgID != orgA.ID {
		t.Errorf("uid lookup crossed tenants: %s", byUID.OrgID)
	}
}

func TestApplicationRepoCursorPagination(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	org := env.seedOrg(t, "Paginated")
	const total = 7
	for i := range total {
		env.seedApp(t, org.ID, "app", uidPtr("uid_"+string(rune('a'+i))))
	}

	seen := make(map[string]struct{}, total)
	page := repositories.CursorPagination{Limit: 3}
	pages := 0
	for {
		result, err := env.Applications.GetApplications(ctx, repositories.ApplicationFilters{
			OrgID: repositories.Eq(org.ID),
		}, page)
		if err != nil {
			t.Fatalf("GetApplications: %v", err)
		}
		pages++
		for _, app := range result.Items {
			if _, dupe := seen[app.ID]; dupe {
				t.Fatalf("application %s returned on two pages", app.ID)
			}
			seen[app.ID] = struct{}{}
		}
		if !result.HasMore {
			if result.NextCursor != nil {
				t.Error("last page must not carry a next cursor")
			}
			break
		}
		if result.NextCursor == nil {
			t.Fatal("HasMore without a next cursor")
		}
		page.Cursor = *result.NextCursor
		if pages > total {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(seen) != total {
		t.Errorf("saw %d applications across %d pages, want %d", len(seen), pages, total)
	}
}

func TestEventTypeRepoArchiveAndRevive(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	org := env.seedOrg(t, "Events")
	env.seedEventType(t, org.ID, "invoice.paid")

	// Creating the same live name twice conflicts.
	dupe := &entities.EventType{OrgID: org.ID, Name: "invoice.paid", CreatedAt: time.Now().UTC()}
	dupe.UpdatedAt = dupe.CreatedAt
	if err := env.EventTypes.CreateEventType(ctx, dupe); !eris.Is(err, repositories.ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}

	if err := env.EventTypes.ArchiveEventType(ctx, org.ID, "invoice.paid"); err != nil {
		t.Fatalf("ArchiveEventType: %v", err)
	}
	if _, err := env.EventTypes.GetEventType(ctx, repositories.EventTypeFilters{
		OrgID: repositories.Eq(org.ID), Name: repositories.Eq("invoice.paid"),
	}); !eris.Is(err, repositories.ErrNotFound) {
		t.Errorf("archived event type must not be returned by default, got %v", err)
	}

	// It is still readable, because historical messages reference it by name.
	archived, err := env.EventTypes.GetEventType(ctx, repositories.EventTypeFilters{
		OrgID: repositories.Eq(org.ID), Name: repositories.Eq("invoice.paid"), IncludeArchived: true,
	})
	if err != nil {
		t.Fatalf("GetEventType(IncludeArchived): %v", err)
	}
	if !archived.Archived() {
		t.Error("expected the archived flag to be set")
	}

	// Re-creating an archived name revives the row rather than conflicting.
	revived := &entities.EventType{OrgID: org.ID, Name: "invoice.paid", Description: "revived", CreatedAt: time.Now().UTC()}
	revived.UpdatedAt = revived.CreatedAt
	if err := env.EventTypes.CreateEventType(ctx, revived); err != nil {
		t.Fatalf("revive event type: %v", err)
	}
	live, err := env.EventTypes.GetEventType(ctx, repositories.EventTypeFilters{
		OrgID: repositories.Eq(org.ID), Name: repositories.Eq("invoice.paid"),
	})
	if err != nil {
		t.Fatalf("GetEventType after revive: %v", err)
	}
	if live.Description != "revived" {
		t.Errorf("Description = %q, want the new description", live.Description)
	}
}

func TestEndpointRepoLifecycleAndFilters(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	org := env.seedOrg(t, "Endpoints")
	app := env.seedApp(t, org.ID, "customer", uidPtr("cust"))

	ep := env.seedEndpoint(t, app, "https://example.test/hook", func(e *entities.Endpoint) {
		e.UID = uidPtr("ep_main")
		e.EventTypes = []string{"invoice.paid"}
		e.Channels = []string{"ch_acme"}
		e.Headers = []byte(`{"X-Custom":"1"}`)
	})

	got, err := env.Endpoints.GetEndpoint(ctx, repositories.EndpointFilters{ID: repositories.Eq(ep.ID)})
	if err != nil {
		t.Fatalf("GetEndpoint: %v", err)
	}
	if len(got.EventTypes) != 1 || got.EventTypes[0] != "invoice.paid" {
		t.Errorf("EventTypes = %v", got.EventTypes)
	}
	if len(got.Channels) != 1 || got.Channels[0] != "ch_acme" {
		t.Errorf("Channels = %v", got.Channels)
	}
	if string(got.Secret.Sealed) != "sealed-secret" {
		t.Errorf("Secret round-tripped as %q", got.Secret.Sealed)
	}
	if got.Pool != configs.DefaultPool {
		t.Errorf("Pool = %q", got.Pool)
	}

	// An endpoint with no filter list wants every event type; NULL must not
	// come back as an empty slice, or "all" would read as "none".
	bare := env.seedEndpoint(t, app, "https://example.test/all")
	fetched, err := env.Endpoints.GetEndpoint(ctx, repositories.EndpointFilters{ID: repositories.Eq(bare.ID)})
	if err != nil {
		t.Fatalf("GetEndpoint(bare): %v", err)
	}
	if fetched.EventTypes != nil || fetched.Channels != nil {
		t.Errorf("expected null filters, got %v / %v", fetched.EventTypes, fetched.Channels)
	}
	if !fetched.WantsEventType("anything") {
		t.Error("an endpoint without filters must accept every event type")
	}

	// Health state transitions.
	disabledAt := time.Now().UTC()
	if err := env.Endpoints.SetFirstFailure(ctx, ep.ID, &disabledAt); err != nil {
		t.Fatalf("SetFirstFailure: %v", err)
	}
	if err := env.Endpoints.SetDisabled(ctx, ep.ID, &disabledAt); err != nil {
		t.Fatalf("SetDisabled: %v", err)
	}
	if err := env.Endpoints.SetPool(ctx, ep.ID, configs.QuarantinePool); err != nil {
		t.Fatalf("SetPool: %v", err)
	}

	quarantined, err := env.Endpoints.GetEndpoints(ctx, repositories.EndpointFilters{
		AppID:    repositories.Eq(app.ID),
		Pool:     repositories.Eq(configs.QuarantinePool),
		Disabled: repositories.Eq(true),
	}, repositories.CursorPagination{})
	if err != nil {
		t.Fatalf("GetEndpoints(quarantined): %v", err)
	}
	if len(quarantined.Items) != 1 || quarantined.Items[0].ID != ep.ID {
		t.Fatalf("quarantine filter returned %d endpoints", len(quarantined.Items))
	}
	if quarantined.Items[0].Deliverable() {
		t.Error("a disabled endpoint must not be deliverable")
	}

	// Re-enabling clears the timestamps rather than deleting the row.
	if err := env.Endpoints.SetDisabled(ctx, ep.ID, nil); err != nil {
		t.Fatalf("SetDisabled(nil): %v", err)
	}
	if err := env.Endpoints.SetFirstFailure(ctx, ep.ID, nil); err != nil {
		t.Fatalf("SetFirstFailure(nil): %v", err)
	}
	reenabled, err := env.Endpoints.GetEndpoint(ctx, repositories.EndpointFilters{ID: repositories.Eq(ep.ID)})
	if err != nil {
		t.Fatalf("GetEndpoint after re-enable: %v", err)
	}
	if reenabled.Disabled() || reenabled.FirstFailureAt != nil {
		t.Error("re-enabling must clear disabled_at and first_failure_at")
	}
}

func TestEndpointRepoRotateSecretKeepsOverlap(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	org := env.seedOrg(t, "Rotation")
	app := env.seedApp(t, org.ID, "customer", nil)
	ep := env.seedEndpoint(t, app, "https://example.test/hook")

	expires := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Millisecond)
	err := env.Endpoints.RotateSecret(ctx, ep.ID,
		entities.SealedSecret{Sealed: []byte("new-secret")},
		[]entities.SealedSecret{{Sealed: []byte("sealed-secret"), ExpiresAt: utils.Ptr(expires)}},
	)
	if err != nil {
		t.Fatalf("RotateSecret: %v", err)
	}

	got, err := env.Endpoints.GetEndpoint(ctx, repositories.EndpointFilters{ID: repositories.Eq(ep.ID)})
	if err != nil {
		t.Fatalf("GetEndpoint: %v", err)
	}
	if string(got.Secret.Sealed) != "new-secret" {
		t.Errorf("Secret = %q", got.Secret.Sealed)
	}
	if len(got.OldSecrets) != 1 || string(got.OldSecrets[0].Sealed) != "sealed-secret" {
		t.Fatalf("OldSecrets = %+v", got.OldSecrets)
	}
	if got.OldSecrets[0].ExpiresAt == nil || !got.OldSecrets[0].ExpiresAt.Equal(expires) {
		t.Errorf("rotation window expiry = %v, want %v", got.OldSecrets[0].ExpiresAt, expires)
	}
	// Both signatures must be emitted during the overlap.
	if len(got.ActiveSecrets(time.Now().UTC())) != 2 {
		t.Error("expected the current and the rotated-out secret to be active")
	}
}

func TestSnapshotRepoLoadsEverythingLive(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	org := env.seedOrg(t, "Snapshot")
	app := env.seedApp(t, org.ID, "customer", uidPtr("cust"))
	env.seedEventType(t, org.ID, "invoice.paid")
	live := env.seedEndpoint(t, app, "https://example.test/live")
	gone := env.seedEndpoint(t, app, "https://example.test/gone")
	if err := env.Endpoints.DeleteEndpoint(ctx, gone.ID); err != nil {
		t.Fatalf("DeleteEndpoint: %v", err)
	}

	data, err := env.Snapshots.LoadSnapshot(ctx)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if len(data.Organizations) != 1 || len(data.Applications) != 1 || len(data.EventTypes) != 1 {
		t.Fatalf("snapshot = %d orgs, %d apps, %d event types",
			len(data.Organizations), len(data.Applications), len(data.EventTypes))
	}
	if len(data.Endpoints) != 1 || data.Endpoints[0].ID != live.ID {
		t.Errorf("snapshot must contain only live endpoints, got %d", len(data.Endpoints))
	}

	if err := env.Snapshots.NotifyConfigChanged(ctx, org.ID); err != nil {
		t.Fatalf("NotifyConfigChanged: %v", err)
	}
}
