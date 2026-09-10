package repositories

import (
	"context"
	"time"

	"github.com/plusiv/huxio/internal/domain/entities"
)

// EndpointFilters holds the predicates for endpoint queries.
type EndpointFilters struct {
	ID                 *Filter[string]
	AppID              *Filter[string]
	OrgID              *Filter[string]
	UID                *Filter[string]
	URL                *Filter[string]
	Pool               *Filter[string]
	Disabled           *Filter[bool]
	FirstFailureBefore *time.Time
}

// EndpointRepository is the port for endpoint persistence.
type EndpointRepository interface {
	CreateEndpoint(ctx context.Context, ep *entities.Endpoint) error
	UpdateEndpoint(ctx context.Context, ep *entities.Endpoint) error
	DeleteEndpoint(ctx context.Context, id string) error

	GetEndpoint(ctx context.Context, filters EndpointFilters, order ...OrderBy) (*entities.Endpoint, error)
	GetEndpoints(ctx context.Context, filters EndpointFilters, cursor CursorPagination, order ...OrderBy) (CursorResult[*entities.Endpoint], error)

	// SetPool moves an endpoint between worker pools, which is how a failing
	// endpoint is quarantined without touching healthy capacity.
	SetPool(ctx context.Context, id, pool string) error
	// SetDisabled switches an endpoint off or back on.
	SetDisabled(ctx context.Context, id string, at *time.Time) error
	// SetFirstFailure records or clears the start of the current failure run.
	SetFirstFailure(ctx context.Context, id string, at *time.Time) error
	// RotateSecret replaces the current secret, keeping the previous one for
	// the duration of the overlap window.
	RotateSecret(ctx context.Context, id string, secret entities.SealedSecret, old []entities.SealedSecret) error
}
