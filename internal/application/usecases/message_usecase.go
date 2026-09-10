package usecases

import (
	"context"
	"time"

	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/ids"
	"github.com/rotisserie/eris"
)

// GetMessageInput identifies one message.
type GetMessageInput struct {
	OrgID      string
	AppIDOrUID string
	MsgID      string
}

// ListMessagesInput is a message listing request.
type ListMessagesInput struct {
	OrgID      string
	AppIDOrUID string
	EventTypes []string
	Channel    *string
	Before     *time.Time
	After      *time.Time
	Cursor     string
	Limit      int
}

// MessageUseCase serves message reads. Reads resolve the application through
// the config snapshot so a tenant can never address another tenant's data.
type MessageUseCase struct {
	messageRepo repositories.MessageRepository
	resolver    appResolver
}

// NewMessageUseCase builds the use case. The application repository is the
// fallback for a config snapshot that has not caught up yet.
func NewMessageUseCase(
	messageRepo repositories.MessageRepository,
	snapshotProvider SnapshotProvider,
	appRepo repositories.ApplicationRepository,
	invalidator SnapshotInvalidator,
) *MessageUseCase {
	return &MessageUseCase{
		messageRepo: messageRepo,
		resolver:    newAppResolver(snapshotProvider, appRepo, invalidator),
	}
}

// GetMessage returns one message without its payload.
func (uc *MessageUseCase) GetMessage(ctx context.Context, in GetMessageInput) (*entities.Message, error) {
	app, err := uc.resolver.resolve(ctx, in.OrgID, in.AppIDOrUID)
	if err != nil {
		return nil, err
	}

	filters := repositories.MessageFilters{
		AppID: repositories.Eq(app.ID),
		ID:    repositories.Eq(in.MsgID),
	}
	// The created-at window comes from the id itself, so Postgres prunes to
	// one day partition instead of scanning every one of them.
	if createdAt, err := ids.TimeOf(in.MsgID); err == nil {
		day := createdAt.Truncate(24 * time.Hour)
		filters.CreatedAt = &repositories.Filter[time.Time]{
			GTE: &day,
			LT:  timePointer(day.AddDate(0, 0, 1)),
		}
	}

	msg, err := uc.messageRepo.GetMessage(ctx, filters)
	if err != nil {
		if eris.Is(err, repositories.ErrNotFound) {
			return nil, apperrors.NewNotFoundError("message")
		}
		return nil, apperrors.NewInternalError(err)
	}
	return msg, nil
}

// ListMessages returns a cursor-paginated page.
func (uc *MessageUseCase) ListMessages(
	ctx context.Context,
	in ListMessagesInput,
) (repositories.CursorResult[*entities.Message], error) {
	var empty repositories.CursorResult[*entities.Message]

	app, err := uc.resolver.resolve(ctx, in.OrgID, in.AppIDOrUID)
	if err != nil {
		return empty, err
	}

	filters := repositories.MessageFilters{AppID: repositories.Eq(app.ID), Channel: in.Channel}
	if len(in.EventTypes) > 0 {
		filters.EventType = repositories.AnyOf(in.EventTypes...)
	}
	if in.Before != nil || in.After != nil {
		filters.CreatedAt = &repositories.Filter[time.Time]{LT: in.Before, GTE: in.After}
	}

	result, err := uc.messageRepo.GetMessages(ctx, filters, repositories.CursorPagination{
		Cursor: in.Cursor, Limit: in.Limit,
	})
	if err != nil {
		return empty, apperrors.NewInternalError(err)
	}
	return result, nil
}

func timePointer(t time.Time) *time.Time { return &t }
