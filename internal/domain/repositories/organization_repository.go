package repositories

import (
	"context"

	"github.com/plusiv/huxio/internal/domain/entities"
)

// OrganizationFilters holds the predicates for organization queries.
type OrganizationFilters struct {
	ID        *Filter[string]
	Name      *Filter[string]
	DeletedAt *Filter[bool]
}

// OrganizationRepository is the port for tenant persistence.
type OrganizationRepository interface {
	CreateOrganization(ctx context.Context, org *entities.Organization) error
	UpdateOrganization(ctx context.Context, org *entities.Organization) error
	DeleteOrganization(ctx context.Context, id string) error

	// GetOrganization returns ErrNotFound when no live record matches.
	GetOrganization(ctx context.Context, filters OrganizationFilters, order ...OrderBy) (*entities.Organization, error)
	GetOrganizations(ctx context.Context, filters OrganizationFilters, cursor CursorPagination, order ...OrderBy) (CursorResult[*entities.Organization], error)
}
