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

const attemptColumns = `id, created_at, org_id, app_id, msg_id, endpoint_id, url, status,
	response_status_code, response_body, response_duration_ms, attempt_number,
	trigger_type, next_attempt_at, deleted_at`

var attemptCopyColumns = []string{
	"id", "created_at", "org_id", "app_id", "msg_id", "endpoint_id", "url", "status",
	"response_status_code", "response_body", "response_duration_ms", "attempt_number",
	"trigger_type", "next_attempt_at",
}

var attemptSortColumns = map[string]string{
	"createdAt": "created_at",
	"id":        "id",
}

// AttemptRepo is the pgx implementation of repositories.AttemptRepository.
type AttemptRepo struct {
	store *Store
}

// NewAttemptRepo builds the repository.
func NewAttemptRepo(store *Store) *AttemptRepo { return &AttemptRepo{store: store} }

// CopyAttempts bulk-inserts a batch with COPY, collapsing hundreds of round
// trips into one. This is the largest single throughput win in the design,
// which is why there is deliberately no single-row insert here.
func (r *AttemptRepo) CopyAttempts(ctx context.Context, attempts []entities.DeliveryAttempt) (int64, error) {
	if len(attempts) == 0 {
		return 0, nil
	}
	rows := make([][]any, len(attempts))
	for i, a := range attempts {
		rows[i] = []any{
			a.ID, a.CreatedAt, a.OrgID, a.AppID, a.MsgID, a.EndpointID, a.URL, int16(a.Status),
			a.ResponseStatusCode, a.ResponseBody, a.ResponseDurationMS, a.AttemptNumber,
			int16(a.TriggerType), a.NextAttemptAt,
		}
	}
	copied, err := r.store.Querier(ctx).CopyFrom(
		ctx,
		pgx.Identifier{"delivery_attempt"},
		attemptCopyColumns,
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return copied, eris.Wrap(err, "copy delivery attempts")
	}
	return copied, nil
}

// GetAttempt returns one attempt, or ErrNotFound.
func (r *AttemptRepo) GetAttempt(
	ctx context.Context,
	filters repositories.AttemptFilters,
	order ...repositories.OrderBy,
) (*entities.DeliveryAttempt, error) {
	b := applyAttemptFilters(filters)
	query := `SELECT ` + attemptColumns + ` FROM delivery_attempt ` + b.Clause() + ` ` +
		OrderClause(order, attemptSortColumns, "created_at DESC, id DESC") + ` LIMIT 1`

	attempt, err := scanAttempt(r.store.Querier(ctx).QueryRow(ctx, query, b.Args()...))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, repositories.ErrNotFound
		}
		return nil, eris.Wrap(err, "select delivery attempt")
	}
	return attempt, nil
}

// GetAttempts returns a cursor-paginated page of attempts.
func (r *AttemptRepo) GetAttempts(
	ctx context.Context,
	filters repositories.AttemptFilters,
	page repositories.CursorPagination,
	order ...repositories.OrderBy,
) (repositories.CursorResult[*entities.DeliveryAttempt], error) {
	var empty repositories.CursorResult[*entities.DeliveryAttempt]

	b := applyAttemptFilters(filters)
	limit := ApplyKeysetCursor(b, page, "created_at", "id", true, TimeCursorValue)
	query := `SELECT ` + attemptColumns + ` FROM delivery_attempt ` + b.Clause() + ` ` +
		OrderClause(order, attemptSortColumns, "created_at DESC, id DESC") + ` LIMIT ` + b.Arg(limit+1)

	rows, err := r.store.Querier(ctx).Query(ctx, query, b.Args()...)
	if err != nil {
		return empty, eris.Wrap(err, "select delivery attempts")
	}
	defer rows.Close()

	items, err := collectRows(rows, scanAttempt)
	if err != nil {
		return empty, eris.Wrap(err, "scan delivery attempts")
	}
	return BuildCursorResult(items, limit,
		func(a *entities.DeliveryAttempt) string { return a.ID },
		func(a *entities.DeliveryAttempt) string { return a.CreatedAt.UTC().Format(time.RFC3339Nano) },
	), nil
}

func applyAttemptFilters(filters repositories.AttemptFilters) *QueryBuilder {
	b := NewQueryBuilder()
	ApplyFilter(b, filters.ID, "id")
	ApplyFilter(b, filters.OrgID, "org_id")
	ApplyFilter(b, filters.AppID, "app_id")
	ApplyFilter(b, filters.MsgID, "msg_id")
	ApplyFilter(b, filters.EndpointID, "endpoint_id")
	ApplyFilter(b, filters.CreatedAt, "created_at")
	if !filters.Status.IsZero() {
		ApplyFilter(b, castFilter(filters.Status, func(s entities.AttemptStatus) int16 { return int16(s) }), "status")
	}
	if !filters.TriggerType.IsZero() {
		ApplyFilter(b, castFilter(filters.TriggerType, func(t entities.TriggerType) int16 { return int16(t) }), "trigger_type")
	}
	if filters.StatusCodeClass != nil {
		// statusCodeClass=4 matches 400-499, mapping onto the same index as a
		// plain status filter.
		lower := int16(*filters.StatusCodeClass * 100)
		b.Wheref("response_status_code >= %s AND response_status_code < %s", lower, lower+100)
	}
	b.Where("deleted_at IS NULL")
	return b
}

func scanAttempt(row pgx.Row) (*entities.DeliveryAttempt, error) {
	var (
		attempt entities.DeliveryAttempt
		status  int16
		trigger int16
	)
	if err := row.Scan(
		&attempt.ID, &attempt.CreatedAt, &attempt.OrgID, &attempt.AppID, &attempt.MsgID,
		&attempt.EndpointID, &attempt.URL, &status, &attempt.ResponseStatusCode,
		&attempt.ResponseBody, &attempt.ResponseDurationMS, &attempt.AttemptNumber,
		&trigger, &attempt.NextAttemptAt, &attempt.DeletedAt,
	); err != nil {
		return nil, err
	}
	attempt.Status = entities.AttemptStatus(status)
	attempt.TriggerType = entities.TriggerType(trigger)
	return &attempt, nil
}

// castFilter converts a domain-typed filter into one whose values bind as the
// stored column type.
func castFilter[T, U comparable](in *repositories.Filter[T], convert func(T) U) *repositories.Filter[U] {
	if in.IsZero() {
		return nil
	}
	out := &repositories.Filter[U]{Exists: in.Exists}
	if in.Is != nil {
		v := convert(*in.Is)
		out.Is = &v
	}
	if in.IsNot != nil {
		v := convert(*in.IsNot)
		out.IsNot = &v
	}
	for _, v := range in.In {
		out.In = append(out.In, convert(v))
	}
	for _, v := range in.NotIn {
		out.NotIn = append(out.NotIn, convert(v))
	}
	if in.GT != nil {
		v := convert(*in.GT)
		out.GT = &v
	}
	if in.GTE != nil {
		v := convert(*in.GTE)
		out.GTE = &v
	}
	if in.LT != nil {
		v := convert(*in.LT)
		out.LT = &v
	}
	if in.LTE != nil {
		v := convert(*in.LTE)
		out.LTE = &v
	}
	return out
}

// ListFailedDeliveries returns the deliveries whose most recent attempt
// failed. The latest-attempt semantics matter: a message that failed once and
// then succeeded on retry must not be replayed.
func (r *AttemptRepo) ListFailedDeliveries(
	ctx context.Context,
	filters repositories.AttemptFilters,
	limit int,
) ([]repositories.FailedDelivery, error) {
	if limit <= 0 {
		limit = 1000
	}

	b := applyAttemptFilters(filters)
	query := `
		WITH latest AS (
		    SELECT DISTINCT ON (msg_id, endpoint_id)
		           msg_id, endpoint_id, status
		    FROM   delivery_attempt
		    ` + b.Clause() + `
		    ORDER  BY msg_id, endpoint_id, created_at DESC, id DESC
		)
		SELECT msg_id, endpoint_id
		FROM   latest
		WHERE  status = ` + b.Arg(int16(entities.AttemptFailed)) + `
		ORDER  BY msg_id
		LIMIT  ` + b.Arg(limit)

	rows, err := r.store.Querier(ctx).Query(ctx, query, b.Args()...)
	if err != nil {
		return nil, eris.Wrap(err, "select failed deliveries")
	}
	defer rows.Close()

	failed := make([]repositories.FailedDelivery, 0, min(limit, 128))
	for rows.Next() {
		var delivery repositories.FailedDelivery
		if err := rows.Scan(&delivery.MsgID, &delivery.EndpointID); err != nil {
			return nil, eris.Wrap(err, "scan failed delivery")
		}
		failed = append(failed, delivery)
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "iterate failed deliveries")
	}
	return failed, nil
}

// Stats counts attempts by outcome.
func (r *AttemptRepo) Stats(
	ctx context.Context,
	filters repositories.AttemptFilters,
) (repositories.AttemptStats, error) {
	var stats repositories.AttemptStats

	b := applyAttemptFilters(filters)
	query := `
		SELECT count(*) FILTER (WHERE status = ` + b.Arg(int16(entities.AttemptSucceeded)) + `),
		       count(*) FILTER (WHERE status = ` + b.Arg(int16(entities.AttemptPendingRetry)) + `),
		       count(*) FILTER (WHERE status = ` + b.Arg(int16(entities.AttemptFailed)) + `)
		FROM   delivery_attempt ` + b.Clause()

	err := r.store.Querier(ctx).QueryRow(ctx, query, b.Args()...).
		Scan(&stats.Succeeded, &stats.Pending, &stats.Failed)
	if err != nil {
		return stats, eris.Wrap(err, "select attempt stats")
	}
	return stats, nil
}
