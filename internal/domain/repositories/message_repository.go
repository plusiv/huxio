package repositories

import (
	"context"
	"time"

	"github.com/plusiv/huxio/internal/domain/entities"
)

// MessageFilters holds the predicates for message queries. Time-partitioned
// reads should always carry a created-at window so Postgres can prune
// partitions; a lookup by id alone scans every partition.
type MessageFilters struct {
	ID        *Filter[string]
	AppID     *Filter[string]
	OrgID     *Filter[string]
	EventType *Filter[string]
	UID       *Filter[string]
	Channel   *string
	CreatedAt *Filter[time.Time]
}

// MessageRepository is the port for message persistence.
type MessageRepository interface {
	// CreateMessage inserts one message. Ingest calls it inside the same
	// transaction as the queue insert.
	CreateMessage(ctx context.Context, msg *entities.Message) error

	// GetMessage returns ErrNotFound when no live record matches. Pass the
	// created-at window (derivable from the KSUID) to prune partitions.
	GetMessage(ctx context.Context, filters MessageFilters, order ...OrderBy) (*entities.Message, error)
	GetMessages(ctx context.Context, filters MessageFilters, cursor CursorPagination, order ...OrderBy) (CursorResult[*entities.Message], error)
	// GetPayload reads just the stored payload bytes for one message.
	GetPayload(ctx context.Context, id string, createdAt time.Time) ([]byte, error)
}
