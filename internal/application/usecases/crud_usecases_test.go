package usecases_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/application/usecases"
	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/utils"
	"github.com/rotisserie/eris"
)

func kindOf(t *testing.T, err error) apperrors.ErrorKind {
	t.Helper()
	var appErr *apperrors.AppError
	if !eris.As(err, &appErr) {
		t.Fatalf("error = %v, want an AppError", err)
	}
	return appErr.Kind
}

func TestApplicationUseCaseCRUD(t *testing.T) {
	t.Parallel()

	repo := newFakeApplicationRepo()
	invalidator := &fakeInvalidator{}
	uc := usecases.NewApplicationUseCase(repo, invalidator)
	ctx := context.Background()

	app, err := uc.CreateApplication(ctx, usecases.CreateApplicationInput{
		OrgID: "org_1", Name: "Acme customer", UID: utils.Ptr("cust_1"),
		Metadata: json.RawMessage(`{"tier":"gold"}`),
	})
	if err != nil {
		t.Fatalf("CreateApplication: %v", err)
	}
	if !strings.HasPrefix(app.ID, "app_") {
		t.Errorf("ID = %q, want an app_ prefixed KSUID", app.ID)
	}
	if invalidator.count() != 1 {
		t.Error("creating an application must invalidate the config snapshot")
	}

	// Addressable by both id and tenant-assigned uid.
	for _, key := range []string{app.ID, "cust_1"} {
		got, err := uc.GetApplication(ctx, "org_1", key)
		if err != nil {
			t.Fatalf("GetApplication(%q): %v", key, err)
		}
		if got.ID != app.ID {
			t.Errorf("GetApplication(%q) = %s", key, got.ID)
		}
	}

	updated, err := uc.UpdateApplication(ctx, "org_1", app.ID, usecases.UpdateApplicationInput{
		Name:      utils.Ptr("Renamed"),
		RateLimit: utils.Ptr(500),
	})
	if err != nil {
		t.Fatalf("UpdateApplication: %v", err)
	}
	if updated.Name != "Renamed" || utils.Deref(updated.RateLimit, 0) != 500 {
		t.Errorf("update produced %+v", updated)
	}

	list, err := uc.ListApplications(ctx, usecases.ListApplicationsInput{OrgID: "org_1", Limit: 10})
	if err != nil {
		t.Fatalf("ListApplications: %v", err)
	}
	if len(list.Items) != 1 {
		t.Errorf("ListApplications returned %d items", len(list.Items))
	}

	if err := uc.DeleteApplication(ctx, "org_1", app.ID); err != nil {
		t.Fatalf("DeleteApplication: %v", err)
	}
	if _, err := uc.GetApplication(ctx, "org_1", app.ID); kindOf(t, err) != apperrors.KindNotFound {
		t.Errorf("GetApplication after delete = %v", err)
	}
}

func TestApplicationUseCaseValidationAndConflicts(t *testing.T) {
	t.Parallel()

	repo := newFakeApplicationRepo()
	uc := usecases.NewApplicationUseCase(repo, &fakeInvalidator{})
	ctx := context.Background()

	if _, err := uc.CreateApplication(ctx, usecases.CreateApplicationInput{OrgID: "org_1", Name: "  "}); kindOf(t, err) != apperrors.KindValidation {
		t.Errorf("blank name = %v, want a validation error", err)
	}
	if _, err := uc.CreateApplication(ctx, usecases.CreateApplicationInput{
		OrgID: "org_1", Name: "ok", Metadata: json.RawMessage(`{oops`),
	}); kindOf(t, err) != apperrors.KindValidation {
		t.Errorf("bad metadata = %v, want a validation error", err)
	}
	if _, err := uc.CreateApplication(ctx, usecases.CreateApplicationInput{
		OrgID: "org_1", Name: "ok", RateLimit: utils.Ptr(0),
	}); kindOf(t, err) != apperrors.KindValidation {
		t.Errorf("zero rate limit = %v, want a validation error", err)
	}

	if _, err := uc.CreateApplication(ctx, usecases.CreateApplicationInput{
		OrgID: "org_1", Name: "first", UID: utils.Ptr("cust_1"),
	}); err != nil {
		t.Fatalf("CreateApplication: %v", err)
	}
	if _, err := uc.CreateApplication(ctx, usecases.CreateApplicationInput{
		OrgID: "org_1", Name: "second", UID: utils.Ptr("cust_1"),
	}); kindOf(t, err) != apperrors.KindConflict {
		t.Errorf("duplicate uid = %v, want a conflict", err)
	}
	// The same uid is free in another tenant.
	if _, err := uc.CreateApplication(ctx, usecases.CreateApplicationInput{
		OrgID: "org_2", Name: "other tenant", UID: utils.Ptr("cust_1"),
	}); err != nil {
		t.Errorf("uid must be unique per tenant only: %v", err)
	}

	// A tenant cannot reach another tenant's application.
	other, err := uc.CreateApplication(ctx, usecases.CreateApplicationInput{OrgID: "org_2", Name: "theirs"})
	if err != nil {
		t.Fatalf("CreateApplication: %v", err)
	}
	if _, err := uc.GetApplication(ctx, "org_1", other.ID); kindOf(t, err) != apperrors.KindNotFound {
		t.Errorf("cross-tenant read = %v, want not found", err)
	}
}

func newEndpointUseCase(t *testing.T) (*usecases.EndpointUseCase, *fakeEndpointRepo, *fakeInvalidator) {
	t.Helper()
	sealer, err := testSealer()
	if err != nil {
		t.Fatalf("testSealer: %v", err)
	}
	repo := newFakeEndpointRepo()
	invalidator := &fakeInvalidator{}
	return usecases.NewEndpointUseCase(repo, sealer, invalidator), repo, invalidator
}

func TestEndpointUseCaseCreateGeneratesSealedSecret(t *testing.T) {
	t.Parallel()

	uc, repo, invalidator := newEndpointUseCase(t)
	ctx := context.Background()

	ep, secret, err := uc.CreateEndpoint(ctx, usecases.CreateEndpointInput{
		OrgID: "org_1", AppID: "app_1",
		URL:        "https://example.test/hook",
		EventTypes: []string{"invoice.paid", "invoice.paid"},
		Headers:    map[string]string{"X-Tenant": "acme"},
	})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}
	if !strings.HasPrefix(secret, usecases.SecretPrefix) {
		t.Errorf("secret = %q, want the whsec_ prefix receivers expect", secret)
	}
	if len(ep.EventTypes) != 1 {
		t.Errorf("EventTypes = %v, want duplicates removed", ep.EventTypes)
	}
	// The secret must never be stored in the clear.
	stored := repo.get(ep.ID)
	if strings.Contains(string(stored.Secret.Sealed), secret) {
		t.Error("the stored secret must be sealed, not plaintext")
	}
	if invalidator.count() != 1 {
		t.Error("creating an endpoint must invalidate the config snapshot")
	}

	revealed, err := uc.RevealSecret(ctx, "app_1", ep.ID)
	if err != nil {
		t.Fatalf("RevealSecret: %v", err)
	}
	if revealed != secret {
		t.Errorf("RevealSecret = %q, want the secret returned at create time", revealed)
	}
}

func TestEndpointUseCaseCreateValidation(t *testing.T) {
	t.Parallel()

	uc, _, _ := newEndpointUseCase(t)
	ctx := context.Background()

	tests := []struct {
		name string
		in   usecases.CreateEndpointInput
		kind apperrors.ErrorKind
	}{
		{"missing url", usecases.CreateEndpointInput{OrgID: "org_1", AppID: "app_1"}, apperrors.KindValidation},
		{"bad scheme", usecases.CreateEndpointInput{OrgID: "org_1", AppID: "app_1", URL: "ftp://example.test/x"}, apperrors.KindValidation},
		{"no host", usecases.CreateEndpointInput{OrgID: "org_1", AppID: "app_1", URL: "https:///path"}, apperrors.KindValidation},
		{
			"signature header override",
			usecases.CreateEndpointInput{OrgID: "org_1", AppID: "app_1", URL: "https://example.test/x",
				Headers: map[string]string{"webhook-signature": "v1,forged"}},
			apperrors.KindValidation,
		},
		{
			"invalid header name",
			usecases.CreateEndpointInput{OrgID: "org_1", AppID: "app_1", URL: "https://example.test/x",
				Headers: map[string]string{"Bad Header": "1"}},
			apperrors.KindValidation,
		},
		{
			"unsupported secret type",
			usecases.CreateEndpointInput{OrgID: "org_1", AppID: "app_1", URL: "https://example.test/x",
				SecretType: utils.Ptr(entities.SecretType("rsa"))},
			apperrors.KindValidation,
		},
		{
			"secret without prefix",
			usecases.CreateEndpointInput{OrgID: "org_1", AppID: "app_1", URL: "https://example.test/x", Secret: "plain"},
			apperrors.KindValidation,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, _, err := uc.CreateEndpoint(ctx, tc.in); kindOf(t, err) != tc.kind {
				t.Errorf("error = %v, want %s", err, tc.kind)
			}
		})
	}
}

func TestEndpointUseCaseDisableIsNotDelete(t *testing.T) {
	t.Parallel()

	uc, repo, _ := newEndpointUseCase(t)
	ctx := context.Background()

	ep, _, err := uc.CreateEndpoint(ctx, usecases.CreateEndpointInput{
		OrgID: "org_1", AppID: "app_1", URL: "https://example.test/hook",
	})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}
	// Simulate a failure run recorded by the delivery engine.
	if err := repo.SetFirstFailure(ctx, ep.ID, utils.Ptr(ep.CreatedAt)); err != nil {
		t.Fatalf("SetFirstFailure: %v", err)
	}

	disabled, err := uc.SetDisabled(ctx, "app_1", ep.ID, true)
	if err != nil {
		t.Fatalf("SetDisabled(true): %v", err)
	}
	if !disabled.Disabled() || disabled.Deleted() {
		t.Error("disabling must set disabled_at and leave deleted_at alone")
	}

	reenabled, err := uc.SetDisabled(ctx, "app_1", ep.ID, false)
	if err != nil {
		t.Fatalf("SetDisabled(false): %v", err)
	}
	if reenabled.Disabled() {
		t.Error("re-enabling must clear disabled_at")
	}
	if repo.get(ep.ID).FirstFailureAt != nil {
		t.Error("re-enabling must restart the failure window")
	}

	if err := uc.DeleteEndpoint(ctx, "app_1", ep.ID); err != nil {
		t.Fatalf("DeleteEndpoint: %v", err)
	}
	if _, err := uc.GetEndpoint(ctx, "app_1", ep.ID); kindOf(t, err) != apperrors.KindNotFound {
		t.Errorf("GetEndpoint after delete = %v", err)
	}
}

func TestEndpointUseCaseRotateSecretKeepsOverlap(t *testing.T) {
	t.Parallel()

	uc, repo, _ := newEndpointUseCase(t)
	ctx := context.Background()

	ep, original, err := uc.CreateEndpoint(ctx, usecases.CreateEndpointInput{
		OrgID: "org_1", AppID: "app_1", URL: "https://example.test/hook",
	})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}
	originalSealed := repo.get(ep.ID).Secret.Sealed

	rotated, err := uc.RotateSecret(ctx, "app_1", ep.ID, "")
	if err != nil {
		t.Fatalf("RotateSecret: %v", err)
	}
	if rotated == original {
		t.Error("rotation must install a different secret")
	}

	stored := repo.get(ep.ID)
	if len(stored.OldSecrets) != 1 {
		t.Fatalf("OldSecrets = %d, want the superseded secret retained", len(stored.OldSecrets))
	}
	if string(stored.OldSecrets[0].Sealed) != string(originalSealed) {
		t.Error("the retired secret must be the one that was current")
	}
	if stored.OldSecrets[0].ExpiresAt == nil {
		t.Fatal("the retired secret needs an expiry, or it never ages out")
	}
	if window := stored.OldSecrets[0].ExpiresAt.Sub(stored.UpdatedAt); window < 23*time.Hour {
		t.Errorf("rotation window = %s, want roughly 24h", window)
	}
	// Both keys sign during the overlap so receivers can roll over.
	if len(stored.ActiveSecrets(time.Now().UTC())) != 2 {
		t.Error("expected the new and the retired secret to be active")
	}

	if _, err := uc.RotateSecret(ctx, "app_1", ep.ID, "not-prefixed"); kindOf(t, err) != apperrors.KindValidation {
		t.Error("a caller-supplied secret must carry the whsec_ prefix")
	}
}

func TestEndpointUseCasePatchHeaders(t *testing.T) {
	t.Parallel()

	uc, _, _ := newEndpointUseCase(t)
	ctx := context.Background()

	ep, _, err := uc.CreateEndpoint(ctx, usecases.CreateEndpointInput{
		OrgID: "org_1", AppID: "app_1", URL: "https://example.test/hook",
		Headers: map[string]string{"X-One": "1", "X-Two": "2"},
	})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}

	got, err := uc.PatchHeaders(ctx, "app_1", ep.ID, map[string]string{
		"X-One":   "updated",
		"X-Two":   "", // empty value removes
		"X-Three": "3",
	})
	if err != nil {
		t.Fatalf("PatchHeaders: %v", err)
	}
	if got["X-One"] != "updated" || got["X-Three"] != "3" {
		t.Errorf("headers = %v", got)
	}
	if _, present := got["X-Two"]; present {
		t.Error("an empty value must remove the header")
	}

	if _, err := uc.PatchHeaders(ctx, "app_1", ep.ID, map[string]string{"webhook-id": "forged"}); kindOf(t, err) != apperrors.KindValidation {
		t.Error("signature headers must not be patchable")
	}
}

func TestEventTypeUseCaseLifecycle(t *testing.T) {
	t.Parallel()

	repo := newFakeEventTypeRepo()
	invalidator := &fakeInvalidator{}
	uc := usecases.NewEventTypeUseCase(repo, invalidator)
	ctx := context.Background()

	if _, err := uc.CreateEventType(ctx, usecases.CreateEventTypeInput{
		OrgID: "org_1", Name: "invoice.paid", Description: "An invoice was paid",
	}); err != nil {
		t.Fatalf("CreateEventType: %v", err)
	}
	if _, err := uc.CreateEventType(ctx, usecases.CreateEventTypeInput{
		OrgID: "org_1", Name: "invoice.paid",
	}); kindOf(t, err) != apperrors.KindConflict {
		t.Error("a live event type name must be unique per tenant")
	}

	updated, err := uc.UpdateEventType(ctx, "org_1", "invoice.paid", usecases.UpdateEventTypeInput{
		Description: utils.Ptr("Updated"),
		Schemas:     json.RawMessage(`{"1":{"type":"object"}}`),
	})
	if err != nil {
		t.Fatalf("UpdateEventType: %v", err)
	}
	if updated.Description != "Updated" {
		t.Errorf("Description = %q", updated.Description)
	}

	if err := uc.ArchiveEventType(ctx, "org_1", "invoice.paid"); err != nil {
		t.Fatalf("ArchiveEventType: %v", err)
	}
	// Archived types stay readable: historical messages reference them.
	archived, err := uc.GetEventType(ctx, "org_1", "invoice.paid")
	if err != nil {
		t.Fatalf("GetEventType after archive: %v", err)
	}
	if !archived.Archived() {
		t.Error("expected the event type to be archived")
	}
	if _, err := uc.UpdateEventType(ctx, "org_1", "invoice.paid", usecases.UpdateEventTypeInput{
		Description: utils.Ptr("nope"),
	}); kindOf(t, err) != apperrors.KindBusiness {
		t.Error("an archived event type must not be updatable")
	}

	live, err := uc.ListEventTypes(ctx, usecases.ListEventTypesInput{OrgID: "org_1"})
	if err != nil {
		t.Fatalf("ListEventTypes: %v", err)
	}
	if len(live.Items) != 0 {
		t.Errorf("live listing returned %d archived items", len(live.Items))
	}
	all, err := uc.ListEventTypes(ctx, usecases.ListEventTypesInput{OrgID: "org_1", IncludeArchived: true})
	if err != nil {
		t.Fatalf("ListEventTypes(IncludeArchived): %v", err)
	}
	if len(all.Items) != 1 {
		t.Errorf("archived listing returned %d items", len(all.Items))
	}

	if err := uc.UnarchiveEventType(ctx, "org_1", "invoice.paid"); err != nil {
		t.Fatalf("UnarchiveEventType: %v", err)
	}
	if got, err := uc.GetEventType(ctx, "org_1", "invoice.paid"); err != nil || got.Archived() {
		t.Errorf("unarchive left %+v, %v", got, err)
	}
}

func TestEventTypeUseCaseNameValidation(t *testing.T) {
	t.Parallel()

	uc := usecases.NewEventTypeUseCase(newFakeEventTypeRepo(), &fakeInvalidator{})
	ctx := context.Background()

	for _, name := range []string{"", "  ", "invoice paid", "invoice/paid", strings.Repeat("a", 300)} {
		if _, err := uc.CreateEventType(ctx, usecases.CreateEventTypeInput{OrgID: "org_1", Name: name}); kindOf(t, err) != apperrors.KindValidation {
			t.Errorf("name %q = %v, want a validation error", name, err)
		}
	}
	if _, err := uc.CreateEventType(ctx, usecases.CreateEventTypeInput{
		OrgID: "org_1", Name: "invoice.paid", Schemas: json.RawMessage(`{oops`),
	}); kindOf(t, err) != apperrors.KindValidation {
		t.Error("malformed schemas must be rejected")
	}
}
