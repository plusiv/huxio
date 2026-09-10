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

// IdempotencyRepo is the pgx implementation of
// repositories.IdempotencyRepository.
type IdempotencyRepo struct {
	store *Store
}

// NewIdempotencyRepo builds the repository.
func NewIdempotencyRepo(store *Store) *IdempotencyRepo { return &IdempotencyRepo{store: store} }

// Begin inserts an in-progress row, which doubles as a short-lived lock so two
// concurrent duplicates are safe. It reports true when the caller won the race
// and should handle the request.
func (r *IdempotencyRepo) Begin(
	ctx context.Context,
	key string,
	lockTTL time.Duration,
) (*entities.IdempotencyRecord, bool, error) {
	const claim = `
		INSERT INTO idempotency_key (key, status, expires_at)
		VALUES ($1, $2, now() + ($3 * interval '1 second'))
		ON CONFLICT (key) DO UPDATE
		SET status = $2, response = NULL, status_code = NULL,
		    expires_at = now() + ($3 * interval '1 second'), created_at = now()
		WHERE idempotency_key.expires_at < now()
		RETURNING key`

	var claimed string
	err := r.store.Querier(ctx).
		QueryRow(ctx, claim, key, int16(entities.IdempotencyInProgress), lockTTL.Seconds()).
		Scan(&claimed)
	switch {
	case err == nil:
		return nil, true, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return nil, false, eris.Wrap(err, "begin idempotent request")
	}

	// A live record already exists: complete rows replay their response, and
	// in-progress ones produce a 409.
	existing, err := r.get(ctx, key)
	if err != nil {
		return nil, false, err
	}
	return existing, false, nil
}

// Complete stores the response to replay for subsequent duplicates.
func (r *IdempotencyRepo) Complete(
	ctx context.Context,
	key string,
	statusCode int16,
	response []byte,
	ttl time.Duration,
) error {
	const query = `
		UPDATE idempotency_key
		SET    status = $2, status_code = $3, response = $4,
		       expires_at = now() + ($5 * interval '1 second')
		WHERE  key = $1`
	tag, err := r.store.Querier(ctx).Exec(ctx, query,
		key, int16(entities.IdempotencyComplete), statusCode, response, ttl.Seconds(),
	)
	if err != nil {
		return eris.Wrap(err, "complete idempotent request")
	}
	if tag.RowsAffected() == 0 {
		return repositories.ErrNotFound
	}
	return nil
}

// Abort removes an in-progress row so a failed request can be retried
// immediately rather than after the lock expires.
func (r *IdempotencyRepo) Abort(ctx context.Context, key string) error {
	const query = `DELETE FROM idempotency_key WHERE key = $1 AND status = $2`
	if _, err := r.store.Querier(ctx).Exec(ctx, query, key, int16(entities.IdempotencyInProgress)); err != nil {
		return eris.Wrap(err, "abort idempotent request")
	}
	return nil
}

// PurgeExpired deletes records past their expiry.
func (r *IdempotencyRepo) PurgeExpired(ctx context.Context) (int64, error) {
	tag, err := r.store.Querier(ctx).Exec(ctx, `DELETE FROM idempotency_key WHERE expires_at < now()`)
	if err != nil {
		return 0, eris.Wrap(err, "purge expired idempotency keys")
	}
	return tag.RowsAffected(), nil
}

func (r *IdempotencyRepo) get(ctx context.Context, key string) (*entities.IdempotencyRecord, error) {
	const query = `
		SELECT key, status, response, status_code, expires_at, created_at
		FROM   idempotency_key
		WHERE  key = $1`

	var (
		record entities.IdempotencyRecord
		status int16
	)
	err := r.store.Querier(ctx).QueryRow(ctx, query, key).Scan(
		&record.Key, &status, &record.Response, &record.StatusCode,
		&record.ExpiresAt, &record.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, repositories.ErrNotFound
		}
		return nil, eris.Wrap(err, "select idempotency key")
	}
	record.Status = entities.IdempotencyStatus(status)
	return &record, nil
}
