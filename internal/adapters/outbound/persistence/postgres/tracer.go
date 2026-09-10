package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
)

// queryTracer logs failed and slow queries through the context-scoped zerolog
// logger, mirroring the reference project's slow-query logging.
type queryTracer struct {
	slowThreshold time.Duration
}

type traceStartKey struct{}

type traceStart struct {
	at  time.Time
	sql string
}

func newQueryTracer(slowThreshold time.Duration) pgx.QueryTracer {
	return &queryTracer{slowThreshold: slowThreshold}
}

// isExpectedQueryError reports whether an error is a normal business outcome
// rather than a fault.
func isExpectedQueryError(err error) bool {
	if errors.Is(err, pgx.ErrNoRows) {
		return true
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == uniqueViolation
}

// TraceQueryStart implements pgx.QueryTracer.
func (t *queryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, traceStartKey{}, traceStart{at: time.Now(), sql: data.SQL})
}

// TraceQueryEnd implements pgx.QueryTracer.
func (t *queryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	start, ok := ctx.Value(traceStartKey{}).(traceStart)
	if !ok {
		return
	}
	elapsed := time.Since(start.at)
	log := logger.FromContext(ctx)

	switch {
	case data.Err != nil && isExpectedQueryError(data.Err):
		// "No rows" and unique violations are ordinary outcomes: a lookup that
		// misses and a conflict the caller turns into a 404 or a 409. Logging
		// them as errors trains operators to ignore the error log.
		log.Debug().Err(data.Err).Str("sql", start.sql).Dur("elapsed", elapsed).Msg("query rejected")
	case data.Err != nil:
		log.Error().Err(data.Err).Str("sql", start.sql).Dur("elapsed", elapsed).Msg("query failed")
	case elapsed >= t.slowThreshold:
		log.Warn().Str("sql", start.sql).Dur("elapsed", elapsed).Msg("slow query")
	default:
		log.Trace().Str("sql", start.sql).Dur("elapsed", elapsed).Msg("query")
	}
}
