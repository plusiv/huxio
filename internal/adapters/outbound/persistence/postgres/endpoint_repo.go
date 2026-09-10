package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/rotisserie/eris"
)

const endpointColumns = `id, app_id, org_id, uid, url, description, version, rate_limit,
	event_types, channels, headers, secret_enc, secret_type, old_secrets_enc,
	pool, first_failure_at, disabled_at, created_at, updated_at, deleted_at`

var endpointSortColumns = map[string]string{
	"id":        "id",
	"url":       "url",
	"createdAt": "created_at",
}

// EndpointRepo is the pgx implementation of repositories.EndpointRepository.
type EndpointRepo struct {
	store *Store
}

// NewEndpointRepo builds the repository.
func NewEndpointRepo(store *Store) *EndpointRepo { return &EndpointRepo{store: store} }

// CreateEndpoint inserts an endpoint with its sealed signing secret.
func (r *EndpointRepo) CreateEndpoint(ctx context.Context, ep *entities.Endpoint) error {
	oldSecrets, err := marshalSealedSecrets(ep.OldSecrets)
	if err != nil {
		return err
	}
	const query = `
		INSERT INTO endpoint (
			id, app_id, org_id, uid, url, description, version, rate_limit,
			event_types, channels, headers, secret_enc, secret_type, old_secrets_enc,
			pool, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $16)`
	_, err = r.store.Querier(ctx).Exec(ctx, query,
		ep.ID, ep.AppID, ep.OrgID, ep.UID, ep.URL, ep.Description, ep.Version, ep.RateLimit,
		nilIfEmpty(ep.EventTypes), nilIfEmpty(ep.Channels), jsonbArg(ep.Headers),
		ep.Secret.Sealed, string(ep.SecretType), oldSecrets, ep.Pool, ep.CreatedAt,
	)
	if err != nil {
		return wrapWriteError(err, "insert endpoint")
	}
	return nil
}

// UpdateEndpoint writes the mutable fields of an endpoint. Secret rotation and
// the health-state transitions have dedicated methods.
func (r *EndpointRepo) UpdateEndpoint(ctx context.Context, ep *entities.Endpoint) error {
	const query = `
		UPDATE endpoint
		SET    uid = $2, url = $3, description = $4, version = $5, rate_limit = $6,
		       event_types = $7, channels = $8, headers = $9, updated_at = $10
		WHERE  id = $1 AND deleted_at IS NULL`
	tag, err := r.store.Querier(ctx).Exec(ctx, query,
		ep.ID, ep.UID, ep.URL, ep.Description, ep.Version, ep.RateLimit,
		nilIfEmpty(ep.EventTypes), nilIfEmpty(ep.Channels), jsonbArg(ep.Headers), ep.UpdatedAt,
	)
	if err != nil {
		return wrapWriteError(err, "update endpoint")
	}
	if tag.RowsAffected() == 0 {
		return repositories.ErrNotFound
	}
	return nil
}

// DeleteEndpoint soft-deletes an endpoint. Unlike disabling, this is final.
func (r *EndpointRepo) DeleteEndpoint(ctx context.Context, id string) error {
	return softDelete(ctx, r.store, "endpoint", "id", id)
}

// SetPool moves an endpoint between worker pools.
func (r *EndpointRepo) SetPool(ctx context.Context, id, pool string) error {
	return r.updateColumn(ctx, id, "pool", pool)
}

// SetDisabled switches an endpoint off (non-nil at) or back on (nil).
func (r *EndpointRepo) SetDisabled(ctx context.Context, id string, at *time.Time) error {
	return r.updateColumn(ctx, id, "disabled_at", at)
}

// SetFirstFailure records or clears the start of the current failure run.
func (r *EndpointRepo) SetFirstFailure(ctx context.Context, id string, at *time.Time) error {
	return r.updateColumn(ctx, id, "first_failure_at", at)
}

// RotateSecret installs a new signing secret, keeping the superseded ones for
// the duration of their overlap window so both signatures are emitted.
func (r *EndpointRepo) RotateSecret(
	ctx context.Context,
	id string,
	secret entities.SealedSecret,
	old []entities.SealedSecret,
) error {
	encoded, err := marshalSealedSecrets(old)
	if err != nil {
		return err
	}
	const query = `
		UPDATE endpoint
		SET    secret_enc = $2, old_secrets_enc = $3, updated_at = now()
		WHERE  id = $1 AND deleted_at IS NULL`
	tag, err := r.store.Querier(ctx).Exec(ctx, query, id, secret.Sealed, encoded)
	if err != nil {
		return wrapWriteError(err, "rotate endpoint secret")
	}
	if tag.RowsAffected() == 0 {
		return repositories.ErrNotFound
	}
	return nil
}

// GetEndpoint returns one endpoint, or ErrNotFound.
func (r *EndpointRepo) GetEndpoint(
	ctx context.Context,
	filters repositories.EndpointFilters,
	order ...repositories.OrderBy,
) (*entities.Endpoint, error) {
	b := applyEndpointFilters(filters)
	query := `SELECT ` + endpointColumns + ` FROM endpoint ` + b.Clause() + ` ` +
		OrderClause(order, endpointSortColumns, "id ASC") + ` LIMIT 1`

	ep, err := scanEndpoint(r.store.Querier(ctx).QueryRow(ctx, query, b.Args()...))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, repositories.ErrNotFound
		}
		return nil, eris.Wrap(err, "select endpoint")
	}
	return ep, nil
}

// GetEndpoints returns a cursor-paginated page of endpoints.
func (r *EndpointRepo) GetEndpoints(
	ctx context.Context,
	filters repositories.EndpointFilters,
	page repositories.CursorPagination,
	order ...repositories.OrderBy,
) (repositories.CursorResult[*entities.Endpoint], error) {
	var empty repositories.CursorResult[*entities.Endpoint]

	b := applyEndpointFilters(filters)
	limit := ApplyKeysetCursor(b, page, "id", "id", false, nil)
	query := `SELECT ` + endpointColumns + ` FROM endpoint ` + b.Clause() + ` ` +
		OrderClause(order, endpointSortColumns, "id ASC") + ` LIMIT ` + b.Arg(limit+1)

	rows, err := r.store.Querier(ctx).Query(ctx, query, b.Args()...)
	if err != nil {
		return empty, eris.Wrap(err, "select endpoints")
	}
	defer rows.Close()

	items, err := collectRows(rows, scanEndpoint)
	if err != nil {
		return empty, eris.Wrap(err, "scan endpoints")
	}
	return BuildCursorResult(items, limit,
		func(e *entities.Endpoint) string { return e.ID },
		func(e *entities.Endpoint) string { return e.ID },
	), nil
}

func (r *EndpointRepo) updateColumn(ctx context.Context, id, column string, value any) error {
	query := `UPDATE endpoint SET ` + column + ` = $2, updated_at = now() WHERE id = $1 AND deleted_at IS NULL`
	tag, err := r.store.Querier(ctx).Exec(ctx, query, id, value)
	if err != nil {
		return wrapWriteError(err, "update endpoint "+column)
	}
	if tag.RowsAffected() == 0 {
		return repositories.ErrNotFound
	}
	return nil
}

func applyEndpointFilters(filters repositories.EndpointFilters) *QueryBuilder {
	b := NewQueryBuilder()
	ApplyFilter(b, filters.ID, "id")
	ApplyFilter(b, filters.AppID, "app_id")
	ApplyFilter(b, filters.OrgID, "org_id")
	ApplyFilter(b, filters.UID, "uid")
	ApplyFilter(b, filters.URL, "url")
	ApplyFilter(b, filters.Pool, "pool")
	ApplyBoolStateFilter(b, filters.Disabled, "disabled_at")
	if filters.FirstFailureBefore != nil {
		b.Wheref("first_failure_at IS NOT NULL AND first_failure_at < %s", *filters.FirstFailureBefore)
	}
	b.Where("deleted_at IS NULL")
	return b
}

func scanEndpoint(row pgx.Row) (*entities.Endpoint, error) {
	var (
		ep         entities.Endpoint
		headers    []byte
		secretType string
		oldSecrets []byte
	)
	if err := row.Scan(
		&ep.ID, &ep.AppID, &ep.OrgID, &ep.UID, &ep.URL, &ep.Description, &ep.Version, &ep.RateLimit,
		&ep.EventTypes, &ep.Channels, &headers, &ep.Secret.Sealed, &secretType, &oldSecrets,
		&ep.Pool, &ep.FirstFailureAt, &ep.DisabledAt, &ep.CreatedAt, &ep.UpdatedAt, &ep.DeletedAt,
	); err != nil {
		return nil, err
	}
	ep.Headers = headers
	ep.SecretType = entities.SecretType(secretType)

	parsed, err := unmarshalSealedSecrets(oldSecrets)
	if err != nil {
		return nil, err
	}
	ep.OldSecrets = parsed
	return &ep, nil
}

func marshalSealedSecrets(secrets []entities.SealedSecret) (any, error) {
	if len(secrets) == 0 {
		return nil, nil
	}
	raw, err := json.Marshal(secrets)
	if err != nil {
		return nil, eris.Wrap(err, "encode rotated secrets")
	}
	return raw, nil
}

func unmarshalSealedSecrets(raw []byte) ([]entities.SealedSecret, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var secrets []entities.SealedSecret
	if err := json.Unmarshal(raw, &secrets); err != nil {
		return nil, eris.Wrap(err, "decode rotated secrets")
	}
	return secrets, nil
}

// nilIfEmpty stores NULL rather than an empty array, because a null filter
// means "every event type" while an empty array would mean "none".
func nilIfEmpty(values []string) any {
	if len(values) == 0 {
		return nil
	}
	return values
}
