package repositories

import (
	"context"

	"github.com/plusiv/huxio/internal/domain/entities"
)

// EventTypeFilters holds the predicates for event type queries.
type EventTypeFilters struct {
	OrgID *Filter[string]
	Name  *Filter[string]
	// IncludeArchived keeps soft-deleted rows in the result. Historical messages
	// reference retired event types by name, so the portal has to be able to show
	// them.
	IncludeArchived bool
}

// EventTypeRepository is the port for event type persistence.
type EventTypeRepository interface {
	CreateEventType(ctx context.Context, et *entities.EventType) error
	UpdateEventType(ctx context.Context, et *entities.EventType) error
	// ArchiveEventType soft-deletes; event types are never hard-deleted.
	ArchiveEventType(ctx context.Context, orgID, name string) error
	UnarchiveEventType(ctx context.Context, orgID, name string) error

	GetEventType(ctx context.Context, filters EventTypeFilters, order ...OrderBy) (*entities.EventType, error)
	GetEventTypes(ctx context.Context, filters EventTypeFilters, cursor CursorPagination, order ...OrderBy) (CursorResult[*entities.EventType], error)
}
