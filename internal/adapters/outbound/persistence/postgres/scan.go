package postgres

import (
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/rotisserie/eris"
)

// uniqueViolation is the Postgres error code for a unique constraint failure.
const uniqueViolation = "23505"

// wrapWriteError translates driver errors into domain sentinels so the
// application layer never inspects a pgconn type.
func wrapWriteError(err error, context string) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if eris.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		return eris.Wrap(repositories.ErrConflict, context)
	}
	return eris.Wrap(err, context)
}

// collectAndClose scans every row and closes the result set, for callers that
// do not otherwise need the rows handle.
func collectAndClose[T any](rows pgx.Rows, scan func(pgx.Row) (T, error)) ([]T, error) {
	defer rows.Close()
	return collectRows(rows, scan)
}

// collectRows scans every row with the supplied row scanner. It exists because
// the entity scanners take pgx.Row, which pgx.Rows also satisfies.
func collectRows[T any](rows pgx.Rows, scan func(pgx.Row) (T, error)) ([]T, error) {
	out := make([]T, 0, 16)
	for rows.Next() {
		item, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
