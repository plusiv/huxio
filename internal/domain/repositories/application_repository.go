package repositories

import (
	"context"

	"github.com/plusiv/huxio/internal/domain/entities"
)

// ApplicationFilters holds the predicates for application queries.
type ApplicationFilters struct {
	ID    *Filter[string]
	OrgID *Filter[string]
	UID   *Filter[string]
	Name  *Filter[string]
}

// ApplicationRepository is the port for application persistence.
type ApplicationRepository interface {
	CreateApplication(ctx context.Context, app *entities.Application) error
	UpdateApplication(ctx context.Context, app *entities.Application) error
	DeleteApplication(ctx context.Context, id string) error

	GetApplication(ctx context.Context, filters ApplicationFilters, order ...OrderBy) (*entities.Application, error)
	GetApplications(ctx context.Context, filters ApplicationFilters, cursor CursorPagination, order ...OrderBy) (CursorResult[*entities.Application], error)
}
