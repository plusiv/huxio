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

const organizationColumns = `id, name, created_at, updated_at, deleted_at`

var organizationSortColumns = map[string]string{
	"id":        "id",
	"name":      "name",
	"createdAt": "created_at",
}

// OrganizationRepo is the pgx implementation of repositories.OrganizationRepository.
type OrganizationRepo struct {
	store *Store
}

// NewOrganizationRepo builds the repository.
func NewOrganizationRepo(store *Store) *OrganizationRepo { return &OrganizationRepo{store: store} }

// CreateOrganization inserts a tenant.
func (r *OrganizationRepo) CreateOrganization(ctx context.Context, org *entities.Organization) error {
	const query = `
		INSERT INTO organization (id, name, created_at, updated_at)
		VALUES ($1, $2, $3, $3)`
	if _, err := r.store.Querier(ctx).Exec(ctx, query, org.ID, org.Name, org.CreatedAt); err != nil {
		return wrapWriteError(err, "insert organization")
	}
	return nil
}

// UpdateOrganization renames a tenant.
func (r *OrganizationRepo) UpdateOrganization(ctx context.Context, org *entities.Organization) error {
	const query = `
		UPDATE organization
		SET    name = $2, updated_at = $3
		WHERE  id = $1 AND deleted_at IS NULL`
	tag, err := r.store.Querier(ctx).Exec(ctx, query, org.ID, org.Name, org.UpdatedAt)
	if err != nil {
		return wrapWriteError(err, "update organization")
	}
	if tag.RowsAffected() == 0 {
		return repositories.ErrNotFound
	}
	return nil
}

// DeleteOrganization soft-deletes a tenant.
func (r *OrganizationRepo) DeleteOrganization(ctx context.Context, id string) error {
	return softDelete(ctx, r.store, "organization", "id", id)
}

// GetOrganization returns one tenant, or ErrNotFound.
func (r *OrganizationRepo) GetOrganization(
	ctx context.Context,
	filters repositories.OrganizationFilters,
	order ...repositories.OrderBy,
) (*entities.Organization, error) {
	b := r.applyFilters(filters)
	query := `SELECT ` + organizationColumns + ` FROM organization ` + b.Clause() + ` ` +
		OrderClause(order, organizationSortColumns, "id ASC") + ` LIMIT 1`

	row := r.store.Querier(ctx).QueryRow(ctx, query, b.Args()...)
	org, err := scanOrganization(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, repositories.ErrNotFound
		}
		return nil, eris.Wrap(err, "select organization")
	}
	return org, nil
}

// GetOrganizations returns a cursor-paginated page of tenants.
func (r *OrganizationRepo) GetOrganizations(
	ctx context.Context,
	filters repositories.OrganizationFilters,
	page repositories.CursorPagination,
	order ...repositories.OrderBy,
) (repositories.CursorResult[*entities.Organization], error) {
	var empty repositories.CursorResult[*entities.Organization]

	b := r.applyFilters(filters)
	limit := ApplyKeysetCursor(b, page, "id", "id", false, nil)
	query := `SELECT ` + organizationColumns + ` FROM organization ` + b.Clause() + ` ` +
		OrderClause(order, organizationSortColumns, "id ASC") +
		` LIMIT ` + b.Arg(limit+1)

	rows, err := r.store.Querier(ctx).Query(ctx, query, b.Args()...)
	if err != nil {
		return empty, eris.Wrap(err, "select organizations")
	}
	defer rows.Close()

	items, err := collectRows(rows, scanOrganization)
	if err != nil {
		return empty, eris.Wrap(err, "scan organizations")
	}
	return BuildCursorResult(items, limit,
		func(o *entities.Organization) string { return o.ID },
		func(o *entities.Organization) string { return o.ID },
	), nil
}

func (r *OrganizationRepo) applyFilters(filters repositories.OrganizationFilters) *QueryBuilder {
	b := NewQueryBuilder()
	ApplyFilter(b, filters.ID, "id")
	ApplyFilter(b, filters.Name, "name")
	if filters.DeletedAt.IsZero() {
		b.Where("deleted_at IS NULL")
	} else {
		ApplyBoolStateFilter(b, filters.DeletedAt, "deleted_at")
	}
	return b
}

func scanOrganization(row pgx.Row) (*entities.Organization, error) {
	var org entities.Organization
	if err := row.Scan(&org.ID, &org.Name, &org.CreatedAt, &org.UpdatedAt, &org.DeletedAt); err != nil {
		return nil, err
	}
	return &org, nil
}

// softDelete stamps deleted_at on a live row, returning ErrNotFound when there
// is nothing live to delete. Nothing in this system is hard-deleted except
// queue rows and whole time partitions.
func softDelete(ctx context.Context, store *Store, table, idColumn, id string) error {
	query := `UPDATE ` + table + ` SET deleted_at = $2, updated_at = $2 WHERE ` + idColumn + ` = $1 AND deleted_at IS NULL`
	tag, err := store.Querier(ctx).Exec(ctx, query, id, time.Now().UTC())
	if err != nil {
		return wrapWriteError(err, "soft delete "+table)
	}
	if tag.RowsAffected() == 0 {
		return repositories.ErrNotFound
	}
	return nil
}
