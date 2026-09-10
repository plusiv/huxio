package usecases_test

import (
	"context"
	"strings"
	"testing"

	"github.com/plusiv/huxio/internal/application/usecases"
	"github.com/plusiv/huxio/internal/domain/apperrors"
)

func TestOrganizationUseCaseCRUD(t *testing.T) {
	t.Parallel()

	repo := newFakeOrganizationRepo()
	invalidator := &fakeInvalidator{}
	uc := usecases.NewOrganizationUseCase(repo, invalidator)
	ctx := context.Background()

	org, err := uc.CreateOrganization(ctx, usecases.CreateOrganizationInput{Name: "Acme"})
	if err != nil {
		t.Fatalf("CreateOrganization: %v", err)
	}
	if !strings.HasPrefix(org.ID, "org_") {
		t.Fatalf("id = %q, want an org_ prefix", org.ID)
	}
	if org.CreatedAt.IsZero() || !org.CreatedAt.Equal(org.UpdatedAt) {
		t.Fatalf("timestamps = %v / %v, want both set and equal", org.CreatedAt, org.UpdatedAt)
	}
	// A new tenant must reach every running process's snapshot.
	if invalidator.count() != 1 {
		t.Fatalf("invalidations = %d, want 1", invalidator.count())
	}

	fetched, err := uc.GetOrganization(ctx, org.ID)
	if err != nil {
		t.Fatalf("GetOrganization: %v", err)
	}
	if fetched.Name != "Acme" {
		t.Fatalf("name = %q, want Acme", fetched.Name)
	}

	if _, err := uc.CreateOrganization(ctx, usecases.CreateOrganizationInput{Name: "  "}); err == nil {
		t.Fatal("CreateOrganization with a blank name succeeded")
	} else if kind := kindOf(t, err); kind != apperrors.KindValidation {
		t.Fatalf("kind = %v, want validation", kind)
	}

	if _, err := uc.GetOrganization(ctx, "org_missing"); err == nil {
		t.Fatal("GetOrganization for a missing tenant succeeded")
	} else if kind := kindOf(t, err); kind != apperrors.KindNotFound {
		t.Fatalf("kind = %v, want not found", kind)
	}

	if _, err := uc.CreateOrganization(ctx, usecases.CreateOrganizationInput{Name: "Globex"}); err != nil {
		t.Fatalf("CreateOrganization: %v", err)
	}

	all, err := uc.ListOrganizations(ctx, usecases.ListOrganizationsInput{})
	if err != nil {
		t.Fatalf("ListOrganizations: %v", err)
	}
	if len(all.Items) != 2 {
		t.Fatalf("listed %d tenants, want 2", len(all.Items))
	}

	matching, err := uc.ListOrganizations(ctx, usecases.ListOrganizationsInput{Search: "acm"})
	if err != nil {
		t.Fatalf("ListOrganizations with a search: %v", err)
	}
	if len(matching.Items) != 1 || matching.Items[0].Name != "Acme" {
		t.Fatalf("search returned %v, want just Acme", matching.Items)
	}
}

// A failed invalidation must not fail the create: the row is already written,
// and the backstop refresh picks it up.
func TestOrganizationUseCaseSurvivesAFailedInvalidation(t *testing.T) {
	t.Parallel()

	uc := usecases.NewOrganizationUseCase(
		newFakeOrganizationRepo(),
		&fakeInvalidator{err: context.DeadlineExceeded},
	)

	if _, err := uc.CreateOrganization(context.Background(),
		usecases.CreateOrganizationInput{Name: "Acme"}); err != nil {
		t.Fatalf("CreateOrganization: %v", err)
	}
}
