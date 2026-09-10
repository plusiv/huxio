package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/rotisserie/eris"
)

const applicationColumns = `id, org_id, uid, name, rate_limit, metadata, created_at, updated_at, deleted_at`

var applicationSortColumns = map[string]string{
	"id":        "id",
	"name":      "name",
	"createdAt": "created_at",
}

// ApplicationRepo is the pgx implementation of repositories.ApplicationRepository.
type ApplicationRepo struct {
	store *Store
}

// NewApplicationRepo builds the repository.
func NewApplicationRepo(store *Store) *ApplicationRepo { return &ApplicationRepo{store: store} }

// CreateApplication inserts an application.
func (r *ApplicationRepo) CreateApplication(ctx context.Context, app *entities.Application) error {
	const query = `
		INSERT INTO application (id, org_id, uid, name, rate_limit, metadata, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, COALESCE($6, '{}'::jsonb), $7, $7)`
	_, err := r.store.Querier(ctx).Exec(ctx, query,
		app.ID, app.OrgID, app.UID, app.Name, app.RateLimit, jsonbArg(app.Metadata), app.CreatedAt,
	)
	if err != nil {
		return wrapWriteError(err, "insert application")
	}
	return nil
}

// UpdateApplication writes the mutable fields of an application.
func (r *ApplicationRepo) UpdateApplication(ctx context.Context, app *entities.Application) error {
	const query = `
		UPDATE application
		SET    uid = $2, name = $3, rate_limit = $4, metadata = COALESCE($5, '{}'::jsonb), updated_at = $6
		WHERE  id = $1 AND deleted_at IS NULL`
	tag, err := r.store.Querier(ctx).Exec(ctx, query,
		app.ID, app.UID, app.Name, app.RateLimit, jsonbArg(app.Metadata), app.UpdatedAt,
	)
	if err != nil {
		return wrapWriteError(err, "update application")
	}
	if tag.RowsAffected() == 0 {
		return repositories.ErrNotFound
	}
	return nil
}

// DeleteApplication soft-deletes an application.
func (r *ApplicationRepo) DeleteApplication(ctx context.Context, id string) error {
	return softDelete(ctx, r.store, "application", "id", id)
}

// GetApplication returns one application, or ErrNotFound.
func (r *ApplicationRepo) GetApplication(
	ctx context.Context,
	filters repositories.ApplicationFilters,
	order ...repositories.OrderBy,
) (*entities.Application, error) {
	b := applyApplicationFilters(filters)
	query := `SELECT ` + applicationColumns + ` FROM application ` + b.Clause() + ` ` +
		OrderClause(order, applicationSortColumns, "id ASC") + ` LIMIT 1`

	app, err := scanApplication(r.store.Querier(ctx).QueryRow(ctx, query, b.Args()...))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, repositories.ErrNotFound
		}
		return nil, eris.Wrap(err, "select application")
	}
	return app, nil
}

// GetApplications returns a cursor-paginated page of applications.
func (r *ApplicationRepo) GetApplications(
	ctx context.Context,
	filters repositories.ApplicationFilters,
	page repositories.CursorPagination,
	order ...repositories.OrderBy,
) (repositories.CursorResult[*entities.Application], error) {
	var empty repositories.CursorResult[*entities.Application]

	b := applyApplicationFilters(filters)
	limit := ApplyKeysetCursor(b, page, "id", "id", false, nil)
	query := `SELECT ` + applicationColumns + ` FROM application ` + b.Clause() + ` ` +
		OrderClause(order, applicationSortColumns, "id ASC") + ` LIMIT ` + b.Arg(limit+1)

	rows, err := r.store.Querier(ctx).Query(ctx, query, b.Args()...)
	if err != nil {
		return empty, eris.Wrap(err, "select applications")
	}
	defer rows.Close()

	items, err := collectRows(rows, scanApplication)
	if err != nil {
		return empty, eris.Wrap(err, "scan applications")
	}
	return BuildCursorResult(items, limit,
		func(a *entities.Application) string { return a.ID },
		func(a *entities.Application) string { return a.ID },
	), nil
}

func applyApplicationFilters(filters repositories.ApplicationFilters) *QueryBuilder {
	b := NewQueryBuilder()
	ApplyFilter(b, filters.ID, "id")
	ApplyFilter(b, filters.OrgID, "org_id")
	ApplyFilter(b, filters.UID, "uid")
	ApplyFilter(b, filters.Name, "name")
	b.Where("deleted_at IS NULL")
	return b
}

func scanApplication(row pgx.Row) (*entities.Application, error) {
	var (
		app      entities.Application
		metadata []byte
	)
	if err := row.Scan(
		&app.ID, &app.OrgID, &app.UID, &app.Name, &app.RateLimit,
		&metadata, &app.CreatedAt, &app.UpdatedAt, &app.DeletedAt,
	); err != nil {
		return nil, err
	}
	app.Metadata = metadata
	return &app, nil
}

// jsonbArg passes nil rather than an empty byte slice so COALESCE can apply
// the column default.
func jsonbArg(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	return raw
}
