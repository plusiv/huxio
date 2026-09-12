package usecases

import (
	"context"
	"time"

	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/ids"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/plusiv/huxio/internal/utils"
	"github.com/rotisserie/eris"
)

// MaxReplayBatch bounds one replay or recover call. A tenant with a month of
// failures should not be able to enqueue all of it in one request.
const MaxReplayBatch = 5000

// AttemptFilterInput is the filter set shared by every attempt query.
type AttemptFilterInput struct {
	Status          *entities.AttemptStatus
	StatusCodeClass *int
	EventTypes      []string
	Channel         *string
	Before          *time.Time
	After           *time.Time
}

// ListAttemptsInput is an attempt listing request. MsgID and EndpointID may be
// combined, which is what the per-message-per-endpoint route does.
type ListAttemptsInput struct {
	OrgID      string
	AppIDOrUID string
	MsgID      string
	EndpointID string
	Filters    AttemptFilterInput
	Cursor     string
	Limit      int
}

// ResendInput is a single manual resend.
type ResendInput struct {
	OrgID      string
	AppIDOrUID string
	MsgID      string
	EndpointID string
}

// RecoverInput replays everything that failed to one endpoint in a window.
type RecoverInput struct {
	OrgID      string
	AppIDOrUID string
	EndpointID string
	Since      time.Time
	Until      *time.Time
}

// ReplayInput replays by filter, across endpoints.
type ReplayInput struct {
	OrgID       string
	AppIDOrUID  string
	EndpointIDs []string
	EventTypes  []string
	Since       time.Time
	Until       *time.Time
}

// ReplayResult reports how much work a replay enqueued.
type ReplayResult struct {
	Enqueued int
	// Truncated reports that the batch cap was hit, so the caller should
	// narrow the window and call again.
	Truncated bool
}

// EndpointStats is the endpoint stats response.
type EndpointStats struct {
	Succeeded int64 `json:"success"`
	Pending   int64 `json:"pending"`
	Failed    int64 `json:"fail"`
}

// AttemptUseCase serves the delivery log and the replay paths.
type AttemptUseCase struct {
	attemptRepo  repositories.AttemptRepository
	endpointRepo repositories.EndpointRepository
	messageRepo  repositories.MessageRepository
	queueRepo    repositories.QueueRepository
	resolver     appResolver
	// queuePool is the worker pool replayed deliveries are enqueued into, not
	// a connection pool.
	queuePool string
}

// NewAttemptUseCase builds the use case.
func NewAttemptUseCase(
	attemptRepo repositories.AttemptRepository,
	endpointRepo repositories.EndpointRepository,
	messageRepo repositories.MessageRepository,
	queueRepo repositories.QueueRepository,
	snapshotProvider SnapshotProvider,
	appRepo repositories.ApplicationRepository,
	invalidator SnapshotInvalidator,
	queuePool string,
) *AttemptUseCase {
	return &AttemptUseCase{
		attemptRepo:  attemptRepo,
		endpointRepo: endpointRepo,
		messageRepo:  messageRepo,
		queueRepo:    queueRepo,
		resolver:     newAppResolver(snapshotProvider, appRepo, invalidator),
		queuePool:    utils.Fallback(queuePool, "default"),
	}
}

// ListAttempts returns a cursor-paginated page of the delivery log.
func (uc *AttemptUseCase) ListAttempts(
	ctx context.Context,
	in ListAttemptsInput,
) (repositories.CursorResult[*entities.DeliveryAttempt], error) {
	var empty repositories.CursorResult[*entities.DeliveryAttempt]

	app, err := uc.resolver.resolve(ctx, in.OrgID, in.AppIDOrUID)
	if err != nil {
		return empty, err
	}

	filters := repositories.AttemptFilters{
		AppID:           repositories.Eq(app.ID),
		StatusCodeClass: in.Filters.StatusCodeClass,
	}
	if in.MsgID != "" {
		filters.MsgID = repositories.Eq(in.MsgID)
	}
	if in.EndpointID != "" {
		endpointID, err := uc.resolveEndpoint(ctx, app.ID, in.EndpointID)
		if err != nil {
			return empty, err
		}
		filters.EndpointID = repositories.Eq(endpointID)
	}
	if in.Filters.Status != nil {
		filters.Status = repositories.Eq(*in.Filters.Status)
	}
	if in.Filters.Before != nil || in.Filters.After != nil {
		filters.CreatedAt = &repositories.Filter[time.Time]{LT: in.Filters.Before, GTE: in.Filters.After}
	}

	result, err := uc.attemptRepo.GetAttempts(ctx, filters, repositories.CursorPagination{
		Cursor: in.Cursor, Limit: in.Limit,
	})
	if err != nil {
		return empty, apperrors.NewInternalError(err)
	}
	return result, nil
}

// Stats counts attempts for one endpoint over a window.
func (uc *AttemptUseCase) Stats(
	ctx context.Context,
	orgID, appIDOrUID, endpointIDOrUID string,
	since *time.Time,
) (EndpointStats, error) {
	app, err := uc.resolver.resolve(ctx, orgID, appIDOrUID)
	if err != nil {
		return EndpointStats{}, err
	}
	endpointID, err := uc.resolveEndpoint(ctx, app.ID, endpointIDOrUID)
	if err != nil {
		return EndpointStats{}, err
	}

	filters := repositories.AttemptFilters{
		AppID:      repositories.Eq(app.ID),
		EndpointID: repositories.Eq(endpointID),
	}
	if since != nil {
		filters.CreatedAt = &repositories.Filter[time.Time]{GTE: since}
	}

	stats, err := uc.attemptRepo.Stats(ctx, filters)
	if err != nil {
		return EndpointStats{}, apperrors.NewInternalError(err)
	}
	return EndpointStats{Succeeded: stats.Succeeded, Pending: stats.Pending, Failed: stats.Failed}, nil
}

// Resend queues one more delivery of a message to an endpoint. A manual
// resend gets exactly one attempt: it is not retried on failure, because the
// person who asked for it is watching and will ask again.
func (uc *AttemptUseCase) Resend(ctx context.Context, in ResendInput) error {
	app, err := uc.resolver.resolve(ctx, in.OrgID, in.AppIDOrUID)
	if err != nil {
		return err
	}
	endpointID, err := uc.resolveEndpoint(ctx, app.ID, in.EndpointID)
	if err != nil {
		return err
	}

	createdAt, err := uc.messageCreatedAt(ctx, app.ID, in.MsgID)
	if err != nil {
		return err
	}

	return uc.enqueueReplays(ctx, app, []repositories.FailedDelivery{
		{MsgID: in.MsgID, EndpointID: endpointID},
	}, createdAt, entities.TriggerManual)
}

// Recover replays everything that failed to one endpoint since a point in
// time. This is the headline feature the partial failed-attempt index exists
// to serve.
func (uc *AttemptUseCase) Recover(ctx context.Context, in RecoverInput) (ReplayResult, error) {
	app, err := uc.resolver.resolve(ctx, in.OrgID, in.AppIDOrUID)
	if err != nil {
		return ReplayResult{}, err
	}
	endpointID, err := uc.resolveEndpoint(ctx, app.ID, in.EndpointID)
	if err != nil {
		return ReplayResult{}, err
	}
	if err := validateReplayWindow(in.Since, in.Until); err != nil {
		return ReplayResult{}, err
	}

	filters := repositories.AttemptFilters{
		AppID:      repositories.Eq(app.ID),
		EndpointID: repositories.Eq(endpointID),
		CreatedAt:  &repositories.Filter[time.Time]{GTE: &in.Since, LT: in.Until},
	}
	return uc.replay(ctx, app, filters)
}

// Replay replays by filter across endpoints and event types.
func (uc *AttemptUseCase) Replay(ctx context.Context, in ReplayInput) (ReplayResult, error) {
	app, err := uc.resolver.resolve(ctx, in.OrgID, in.AppIDOrUID)
	if err != nil {
		return ReplayResult{}, err
	}
	if err := validateReplayWindow(in.Since, in.Until); err != nil {
		return ReplayResult{}, err
	}

	filters := repositories.AttemptFilters{
		AppID:     repositories.Eq(app.ID),
		CreatedAt: &repositories.Filter[time.Time]{GTE: &in.Since, LT: in.Until},
	}
	if len(in.EndpointIDs) > 0 {
		resolved := make([]string, 0, len(in.EndpointIDs))
		for _, idOrUID := range in.EndpointIDs {
			endpointID, err := uc.resolveEndpoint(ctx, app.ID, idOrUID)
			if err != nil {
				return ReplayResult{}, err
			}
			resolved = append(resolved, endpointID)
		}
		filters.EndpointID = repositories.AnyOf(resolved...)
	}

	return uc.replay(ctx, app, filters)
}

// replay expands the failed deliveries matching filters into new tasks.
func (uc *AttemptUseCase) replay(
	ctx context.Context,
	app AppRef,
	filters repositories.AttemptFilters,
) (ReplayResult, error) {
	failed, err := uc.attemptRepo.ListFailedDeliveries(ctx, filters, MaxReplayBatch+1)
	if err != nil {
		return ReplayResult{}, apperrors.NewInternalError(err)
	}

	result := ReplayResult{}
	if len(failed) > MaxReplayBatch {
		failed = failed[:MaxReplayBatch]
		result.Truncated = true
	}
	if len(failed) == 0 {
		return result, nil
	}

	// Group by message so the created-at each task needs is derived once.
	byMessage := make(map[string][]repositories.FailedDelivery, len(failed))
	for _, delivery := range failed {
		byMessage[delivery.MsgID] = append(byMessage[delivery.MsgID], delivery)
	}

	for msgID, deliveries := range byMessage {
		createdAt, err := ids.TimeOf(msgID)
		if err != nil {
			// An id we did not mint: skip it rather than guess its partition.
			logger.FromContext(ctx).Warn().Str("msg_id", msgID).Msg("skipping replay of an unparseable message id")
			continue
		}
		if err := uc.enqueueReplays(ctx, app, deliveries, createdAt, entities.TriggerBulkReplay); err != nil {
			return result, err
		}
		result.Enqueued += len(deliveries)
	}
	return result, nil
}

// enqueueReplays inserts deliver tasks straight into the queue, skipping
// fan-out: the endpoints are already known.
func (uc *AttemptUseCase) enqueueReplays(
	ctx context.Context,
	app AppRef,
	deliveries []repositories.FailedDelivery,
	msgCreatedAt time.Time,
	trigger entities.TriggerType,
) error {
	now := time.Now().UTC()
	tasks := make([]repositories.EnqueueTask, 0, len(deliveries))

	for _, delivery := range deliveries {
		tasks = append(tasks, repositories.EnqueueTask{
			PartitionKey: entities.PartitionKeyFor(delivery.EndpointID),
			Pool:         uc.queuePool,
			Kind:         entities.TaskDeliver,
			OrgID:        app.OrgID,
			AppID:        app.ID,
			MsgID:        delivery.MsgID,
			MsgCreatedAt: msgCreatedAt,
			EndpointID:   utils.Ptr(delivery.EndpointID),
			TriggerType:  trigger,
			VisibleAt:    now,
		})
	}

	if err := uc.queueRepo.Enqueue(ctx, tasks); err != nil {
		return apperrors.NewInternalError(err)
	}
	// One wakeup for every partition the replay landed in, after the insert.
	if err := uc.queueRepo.Notify(ctx, repositories.DistinctPartitions(tasks)); err != nil {
		logger.FromContext(ctx).Warn().Err(err).Msg("replay wakeup notification failed")
	}
	return nil
}

// messageCreatedAt derives a message's partition key column from its id, and
// confirms the message exists inside the application.
func (uc *AttemptUseCase) messageCreatedAt(ctx context.Context, appID, msgID string) (time.Time, error) {
	createdAt, err := ids.TimeOf(msgID)
	if err != nil {
		return time.Time{}, apperrors.NewNotFoundError("message")
	}

	day := createdAt.Truncate(24 * time.Hour)
	nextDay := day.AddDate(0, 0, 1)
	msg, err := uc.messageRepo.GetMessage(ctx, repositories.MessageFilters{
		AppID:     repositories.Eq(appID),
		ID:        repositories.Eq(msgID),
		CreatedAt: &repositories.Filter[time.Time]{GTE: &day, LT: &nextDay},
	})
	if err != nil {
		if eris.Is(err, repositories.ErrNotFound) {
			return time.Time{}, apperrors.NewNotFoundError("message")
		}
		return time.Time{}, apperrors.NewInternalError(err)
	}
	return msg.CreatedAt, nil
}

// resolveEndpoint maps an id-or-uid onto an endpoint id inside one
// application, which is also what stops a caller addressing another
// application's endpoint.
func (uc *AttemptUseCase) resolveEndpoint(ctx context.Context, appID, idOrUID string) (string, error) {
	ep, err := uc.endpointRepo.GetEndpoint(ctx, repositories.EndpointFilters{
		AppID: repositories.Eq(appID),
		ID:    repositories.Eq(idOrUID),
	})
	if err == nil {
		return ep.ID, nil
	}
	if !eris.Is(err, repositories.ErrNotFound) {
		return "", apperrors.NewInternalError(err)
	}

	ep, err = uc.endpointRepo.GetEndpoint(ctx, repositories.EndpointFilters{
		AppID: repositories.Eq(appID),
		UID:   repositories.Eq(idOrUID),
	})
	if err != nil {
		if eris.Is(err, repositories.ErrNotFound) {
			return "", apperrors.NewNotFoundError("endpoint")
		}
		return "", apperrors.NewInternalError(err)
	}
	return ep.ID, nil
}

func validateReplayWindow(since time.Time, until *time.Time) error {
	if since.IsZero() {
		return apperrors.NewValidationError("since is required")
	}
	if until != nil && !until.After(since) {
		return apperrors.NewValidationError("until must be after since")
	}
	return nil
}
