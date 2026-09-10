package usecases

import (
	"context"
	"time"

	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/ids"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/plusiv/huxio/internal/utils"
	"github.com/rotisserie/eris"
)

// CreateOrganizationInput is a create request.
type CreateOrganizationInput struct {
	Name string
}

// ListOrganizationsInput is a list request.
type ListOrganizationsInput struct {
	Search string
	Cursor string
	Limit  int
}

// OrganizationUseCase owns tenant lifecycle. There is no HTTP surface for it:
// creating a tenant is an operator action and every API token is already
// scoped to one, so `huxio org` is the only caller today. It exists as a use
// case anyway so the rules live in the application layer rather than in a
// cobra command, and so an admin route can be added without moving them.
type OrganizationUseCase struct {
	orgRepo     repositories.OrganizationRepository
	invalidator ConfigInvalidator
}

// NewOrganizationUseCase builds the use case.
func NewOrganizationUseCase(
	orgRepo repositories.OrganizationRepository,
	invalidator ConfigInvalidator,
) *OrganizationUseCase {
	return &OrganizationUseCase{orgRepo: orgRepo, invalidator: invalidator}
}

// CreateOrganization inserts a tenant and publishes a config invalidation so
// every running process reloads its snapshot instead of waiting for the
// backstop refresh.
func (uc *OrganizationUseCase) CreateOrganization(
	ctx context.Context,
	in CreateOrganizationInput,
) (*entities.Organization, error) {
	if utils.IsBlank(in.Name) {
		return nil, apperrors.NewValidationError("name is required")
	}

	now := time.Now().UTC()
	org := &entities.Organization{
		ID:        ids.New(ids.PrefixOrganization),
		Name:      in.Name,
		CreatedAt: now,
		UpdatedAt: now,
	}
	// Tenant names are labels, not identifiers: the schema has no unique index
	// on them, so there is no conflict case to map here.
	if err := uc.orgRepo.CreateOrganization(ctx, org); err != nil {
		return nil, apperrors.NewInternalError(err)
	}
	uc.invalidate(ctx, org.ID)
	return org, nil
}

// GetOrganization resolves a tenant by id.
func (uc *OrganizationUseCase) GetOrganization(ctx context.Context, id string) (*entities.Organization, error) {
	org, err := uc.orgRepo.GetOrganization(ctx, repositories.OrganizationFilters{
		ID: repositories.Eq(id),
	})
	if err != nil {
		if eris.Is(err, repositories.ErrNotFound) {
			return nil, apperrors.NewNotFoundError("organization")
		}
		return nil, apperrors.NewInternalError(err)
	}
	return org, nil
}

// ListOrganizations returns a cursor-paginated page.
func (uc *OrganizationUseCase) ListOrganizations(
	ctx context.Context,
	in ListOrganizationsInput,
) (repositories.CursorResult[*entities.Organization], error) {
	filters := repositories.OrganizationFilters{}
	if search := utils.NilIfBlank(in.Search); search != nil {
		filters.Name = &repositories.Filter[string]{IContains: search}
	}

	result, err := uc.orgRepo.GetOrganizations(ctx, filters, repositories.CursorPagination{
		Cursor: in.Cursor, Limit: in.Limit,
	})
	if err != nil {
		return result, apperrors.NewInternalError(err)
	}
	return result, nil
}

// invalidate publishes a config change. A failure is logged and swallowed: the
// write already succeeded, and the backstop refresh will pick it up.
func (uc *OrganizationUseCase) invalidate(ctx context.Context, orgID string) {
	if uc.invalidator == nil {
		return
	}
	if err := uc.invalidator.NotifyConfigChanged(ctx, orgID); err != nil {
		logger.FromContext(ctx).Warn().Err(err).Msg("config invalidation failed")
	}
}
