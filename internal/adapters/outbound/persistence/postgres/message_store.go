package postgres

import (
	"context"
	"time"

	"github.com/plusiv/huxio/internal/application/dispatch"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
)

// MessageStore adapts the message repository to the narrow port the delivery
// engine needs, adding the created-at window that prunes the read to one day
// partition.
type MessageStore struct {
	messages repositories.MessageRepository
}

// NewMessageStore builds the adapter.
func NewMessageStore(messages repositories.MessageRepository) *MessageStore {
	return &MessageStore{messages: messages}
}

// LoadMessage reads one message, payload included: the fan-out is the one
// caller, and the payload it reads here seeds the per-process cache that the
// deliver tasks it queues then read from.
func (s *MessageStore) LoadMessage(ctx context.Context, id string, createdAt time.Time) (*entities.Message, error) {
	day := createdAt.UTC().Truncate(24 * time.Hour)
	nextDay := day.AddDate(0, 0, 1)

	return s.messages.GetMessage(ctx, repositories.MessageFilters{
		ID:        repositories.Eq(id),
		CreatedAt: &repositories.Filter[time.Time]{GTE: &day, LT: &nextDay},
	})
}

// LoadPayload reads the stored payload bytes.
func (s *MessageStore) LoadPayload(ctx context.Context, id string, createdAt time.Time) ([]byte, error) {
	return s.messages.GetPayload(ctx, id, createdAt)
}

var _ dispatch.MessageStore = (*MessageStore)(nil)
