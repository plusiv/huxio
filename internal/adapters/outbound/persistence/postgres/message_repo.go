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

const messageColumns = `id, created_at, org_id, app_id, event_type, uid, channels,
	payload, payload_size, expires_at, deleted_at`

const messageColumnsNoPayload = `id, created_at, org_id, app_id, event_type, uid, channels,
	NULL::bytea AS payload, payload_size, expires_at, deleted_at`

var messageSortColumns = map[string]string{
	"createdAt": "created_at",
	"id":        "id",
}

// MessageRepo is the pgx implementation of repositories.MessageRepository.
type MessageRepo struct {
	store *Store
}

// NewMessageRepo builds the repository.
func NewMessageRepo(store *Store) *MessageRepo { return &MessageRepo{store: store} }

// CreateMessage inserts one message. created_at is supplied by the caller and
// equals the timestamp embedded in the message's KSUID, so the time partition
// holding a row is always derivable from its id.
func (r *MessageRepo) CreateMessage(ctx context.Context, msg *entities.Message) error {
	const query = `
		INSERT INTO message (id, created_at, org_id, app_id, event_type, uid, channels,
		                     payload, payload_size, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`
	_, err := r.store.Querier(ctx).Exec(ctx, query,
		msg.ID, msg.CreatedAt, msg.OrgID, msg.AppID, msg.EventType, msg.UID,
		nilIfEmpty(msg.Channels), msg.Payload, msg.PayloadSize, msg.ExpiresAt,
	)
	if err != nil {
		return wrapWriteError(err, "insert message")
	}
	return nil
}

// GetMessage returns one message including its payload, or ErrNotFound.
func (r *MessageRepo) GetMessage(
	ctx context.Context,
	filters repositories.MessageFilters,
	order ...repositories.OrderBy,
) (*entities.Message, error) {
	b := applyMessageFilters(filters)
	query := `SELECT ` + messageColumns + ` FROM message ` + b.Clause() + ` ` +
		OrderClause(order, messageSortColumns, "created_at DESC, id DESC") + ` LIMIT 1`

	msg, err := scanMessage(r.store.Querier(ctx).QueryRow(ctx, query, b.Args()...))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, repositories.ErrNotFound
		}
		return nil, eris.Wrap(err, "select message")
	}
	return msg, nil
}

// GetMessages returns a cursor-paginated page of messages without payloads:
// listings are for the portal and API index views, and payloads would dominate
// the response size.
func (r *MessageRepo) GetMessages(
	ctx context.Context,
	filters repositories.MessageFilters,
	page repositories.CursorPagination,
	order ...repositories.OrderBy,
) (repositories.CursorResult[*entities.Message], error) {
	var empty repositories.CursorResult[*entities.Message]

	b := applyMessageFilters(filters)
	limit := ApplyKeysetCursor(b, page, "created_at", "id", true, TimeCursorValue)
	query := `SELECT ` + messageColumnsNoPayload + ` FROM message ` + b.Clause() + ` ` +
		OrderClause(order, messageSortColumns, "created_at DESC, id DESC") + ` LIMIT ` + b.Arg(limit+1)

	rows, err := r.store.Querier(ctx).Query(ctx, query, b.Args()...)
	if err != nil {
		return empty, eris.Wrap(err, "select messages")
	}
	defer rows.Close()

	items, err := collectRows(rows, scanMessage)
	if err != nil {
		return empty, eris.Wrap(err, "scan messages")
	}
	return BuildCursorResult(items, limit,
		func(m *entities.Message) string { return m.ID },
		func(m *entities.Message) string { return m.CreatedAt.UTC().Format(time.RFC3339Nano) },
	), nil
}

// GetPayload reads just the stored payload bytes. Both key columns are passed
// so Postgres prunes to the single day partition holding the row.
func (r *MessageRepo) GetPayload(ctx context.Context, id string, createdAt time.Time) ([]byte, error) {
	const query = `SELECT payload FROM message WHERE id = $1 AND created_at = $2 AND deleted_at IS NULL`

	var payload []byte
	if err := r.store.Querier(ctx).QueryRow(ctx, query, id, createdAt).Scan(&payload); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, repositories.ErrNotFound
		}
		return nil, eris.Wrap(err, "select message payload")
	}
	return payload, nil
}

func applyMessageFilters(filters repositories.MessageFilters) *QueryBuilder {
	b := NewQueryBuilder()
	ApplyFilter(b, filters.ID, "id")
	ApplyFilter(b, filters.AppID, "app_id")
	ApplyFilter(b, filters.OrgID, "org_id")
	ApplyFilter(b, filters.EventType, "event_type")
	ApplyFilter(b, filters.UID, "uid")
	ApplyFilter(b, filters.CreatedAt, "created_at")
	if filters.Channel != nil {
		b.Wheref("%s = ANY(channels)", *filters.Channel)
	}
	b.Where("deleted_at IS NULL")
	return b
}

func scanMessage(row pgx.Row) (*entities.Message, error) {
	var msg entities.Message
	if err := row.Scan(
		&msg.ID, &msg.CreatedAt, &msg.OrgID, &msg.AppID, &msg.EventType, &msg.UID,
		&msg.Channels, &msg.Payload, &msg.PayloadSize, &msg.ExpiresAt, &msg.DeletedAt,
	); err != nil {
		return nil, err
	}
	return &msg, nil
}
