package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/rotisserie/eris"
)

const eventTypeColumns = `org_id, name, description, schemas, created_at, updated_at, deleted_at`

var eventTypeSortColumns = map[string]string{
	"name":      "name",
	"createdAt": "created_at",
}

// EventTypeRepo is the pgx implementation of repositories.EventTypeRepository.
type EventTypeRepo struct {
	store *Store
}

// NewEventTypeRepo builds the repository.
func NewEventTypeRepo(store *Store) *EventTypeRepo { return &EventTypeRepo{store: store} }

// CreateEventType inserts an event type. Re-creating a previously archived
// name revives the row, because historical messages still reference it.
func (r *EventTypeRepo) CreateEventType(ctx context.Context, et *entities.EventType) error {
	const query = `
		INSERT INTO event_type (org_id, name, description, schemas, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $5)
		ON CONFLICT (org_id, name) DO UPDATE
		SET description = EXCLUDED.description,
		    schemas     = EXCLUDED.schemas,
		    updated_at  = EXCLUDED.updated_at,
		    deleted_at  = NULL
		WHERE event_type.deleted_at IS NOT NULL`
	tag, err := r.store.Querier(ctx).Exec(ctx, query,
		et.OrgID, et.Name, et.Description, jsonbArg(et.Schemas), et.CreatedAt,
	)
	if err != nil {
		return wrapWriteError(err, "insert event type")
	}
	if tag.RowsAffected() == 0 {
		return eris.Wrap(repositories.ErrConflict, "event type already exists")
	}
	return nil
}

// UpdateEventType writes the mutable fields of an event type.
func (r *EventTypeRepo) UpdateEventType(ctx context.Context, et *entities.EventType) error {
	const query = `
		UPDATE event_type
		SET    description = $3, schemas = $4, updated_at = $5
		WHERE  org_id = $1 AND name = $2 AND deleted_at IS NULL`
	tag, err := r.store.Querier(ctx).Exec(ctx, query,
		et.OrgID, et.Name, et.Description, jsonbArg(et.Schemas), et.UpdatedAt,
	)
	if err != nil {
		return wrapWriteError(err, "update event type")
	}
	if tag.RowsAffected() == 0 {
		return repositories.ErrNotFound
	}
	return nil
}

// ArchiveEventType soft-deletes an event type. Deleting one is always soft:
// historical messages reference it by name and the portal has to be able to
// show what they were.
func (r *EventTypeRepo) ArchiveEventType(ctx context.Context, orgID, name string) error {
	return r.setDeletedAt(ctx, orgID, name, timePtr(time.Now().UTC()), "deleted_at IS NULL")
}

// UnarchiveEventType restores a retired event type.
func (r *EventTypeRepo) UnarchiveEventType(ctx context.Context, orgID, name string) error {
	return r.setDeletedAt(ctx, orgID, name, nil, "deleted_at IS NOT NULL")
}

func (r *EventTypeRepo) setDeletedAt(ctx context.Context, orgID, name string, at *time.Time, predicate string) error {
	query := `
		UPDATE event_type
		SET    deleted_at = $3, updated_at = now()
		WHERE  org_id = $1 AND name = $2 AND ` + predicate
	tag, err := r.store.Querier(ctx).Exec(ctx, query, orgID, name, at)
	if err != nil {
		return wrapWriteError(err, "update event type lifecycle")
	}
	if tag.RowsAffected() == 0 {
		return repositories.ErrNotFound
	}
	return nil
}

// GetEventType returns one event type, or ErrNotFound.
func (r *EventTypeRepo) GetEventType(
	ctx context.Context,
	filters repositories.EventTypeFilters,
	order ...repositories.OrderBy,
) (*entities.EventType, error) {
	b := applyEventTypeFilters(filters)
	query := `SELECT ` + eventTypeColumns + ` FROM event_type ` + b.Clause() + ` ` +
		OrderClause(order, eventTypeSortColumns, "name ASC") + ` LIMIT 1`

	et, err := scanEventType(r.store.Querier(ctx).QueryRow(ctx, query, b.Args()...))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, repositories.ErrNotFound
		}
		return nil, eris.Wrap(err, "select event type")
	}
	return et, nil
}

// GetEventTypes returns a cursor-paginated page of event types.
func (r *EventTypeRepo) GetEventTypes(
	ctx context.Context,
	filters repositories.EventTypeFilters,
	page repositories.CursorPagination,
	order ...repositories.OrderBy,
) (repositories.CursorResult[*entities.EventType], error) {
	var empty repositories.CursorResult[*entities.EventType]

	b := applyEventTypeFilters(filters)
	limit := ApplyKeysetCursor(b, page, "name", "name", false, nil)
	query := `SELECT ` + eventTypeColumns + ` FROM event_type ` + b.Clause() + ` ` +
		OrderClause(order, eventTypeSortColumns, "name ASC") + ` LIMIT ` + b.Arg(limit+1)

	rows, err := r.store.Querier(ctx).Query(ctx, query, b.Args()...)
	if err != nil {
		return empty, eris.Wrap(err, "select event types")
	}
	defer rows.Close()

	items, err := collectRows(rows, scanEventType)
	if err != nil {
		return empty, eris.Wrap(err, "scan event types")
	}
	return BuildCursorResult(items, limit,
		func(e *entities.EventType) string { return e.Name },
		func(e *entities.EventType) string { return e.Name },
	), nil
}

func applyEventTypeFilters(filters repositories.EventTypeFilters) *QueryBuilder {
	b := NewQueryBuilder()
	ApplyFilter(b, filters.OrgID, "org_id")
	ApplyFilter(b, filters.Name, "name")
	if !filters.IncludeArchived {
		b.Where("deleted_at IS NULL")
	}
	return b
}

func scanEventType(row pgx.Row) (*entities.EventType, error) {
	var (
		et      entities.EventType
		schemas []byte
	)
	if err := row.Scan(
		&et.OrgID, &et.Name, &et.Description, &schemas,
		&et.CreatedAt, &et.UpdatedAt, &et.DeletedAt,
	); err != nil {
		return nil, err
	}
	et.Schemas = schemas
	return &et, nil
}

func timePtr(t time.Time) *time.Time { return &t }
