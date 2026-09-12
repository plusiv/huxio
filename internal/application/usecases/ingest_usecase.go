package usecases

import (
	"context"
	"encoding/json"
	"time"

	"github.com/plusiv/huxio/internal/application/config"
	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/ids"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/plusiv/huxio/internal/utils"
	"github.com/rotisserie/eris"
)

// IngestConfig carries the ingest limits.
type IngestConfig struct {
	// MaxPayloadBytes rejects oversized payloads with 413.
	MaxPayloadBytes int64
	// DefaultRetentionDays sets expires_at when the request does not.
	DefaultRetentionDays int
	// Pool is the worker pool fan-out tasks are enqueued into.
	Pool string
}

// IngestInput is one accepted message request.
type IngestInput struct {
	OrgID string
	// AppIDOrUID accepts either the native application id or the tenant-assigned
	// uid, which the compatible SDKs rely on.
	AppIDOrUID string
	EventType  string
	// EventID is the tenant's optional deduplication key.
	EventID  *string
	Payload  json.RawMessage
	Channels []string
	// RetentionDays overrides the configured default for this message.
	RetentionDays *int
}

// IngestResult reports what was accepted.
type IngestResult struct {
	Message *entities.Message
	// MatchedEndpoints is how many endpoints matched at ingest time. It is
	// advisory: the worker re-evaluates matching when it expands the fan-out,
	// against a snapshot that may have moved on.
	MatchedEndpoints int
	// Payload is the caller's original JSON, echoed back in the response
	// without ever being unmarshalled.
	Payload json.RawMessage
}

// IngestUseCase accepts messages. It is the only write path that must be fast:
// it makes the message durable, queues exactly one fan-out task in the same
// transaction, and returns. It never makes an outbound HTTP request and never
// splits the two writes.
type IngestUseCase struct {
	txManager        repositories.TxManager
	messageRepo      repositories.MessageRepository
	queueRepo        repositories.QueueRepository
	snapshotProvider SnapshotProvider
	payloadCodec     PayloadCodec
	resolver         appResolver
	cfg              IngestConfig
}

// NewIngestUseCase builds the use case.
func NewIngestUseCase(
	txManager repositories.TxManager,
	messageRepo repositories.MessageRepository,
	queueRepo repositories.QueueRepository,
	snapshotProvider SnapshotProvider,
	payloadCodec PayloadCodec,
	cfg IngestConfig,
	opts ...IngestOption,
) *IngestUseCase {
	if cfg.MaxPayloadBytes <= 0 {
		cfg.MaxPayloadBytes = 1 << 20
	}
	if cfg.DefaultRetentionDays <= 0 {
		cfg.DefaultRetentionDays = 90
	}
	if cfg.Pool == "" {
		cfg.Pool = "default"
	}
	uc := &IngestUseCase{
		txManager: txManager, messageRepo: messageRepo, queueRepo: queueRepo,
		snapshotProvider: snapshotProvider, payloadCodec: payloadCodec, cfg: cfg,
		resolver: newAppResolver(snapshotProvider, nil, nil),
	}
	for _, opt := range opts {
		opt(uc)
	}
	return uc
}

// IngestOption customises the use case.
type IngestOption func(*IngestUseCase)

// WithApplicationFallback lets ingest resolve an application from the database
// when the config snapshot has not caught up yet, which is what makes
// "create an application, then immediately send to it" work.
func WithApplicationFallback(appRepo repositories.ApplicationRepository, invalidator SnapshotInvalidator) IngestOption {
	return func(uc *IngestUseCase) {
		uc.resolver = newAppResolver(uc.snapshotProvider, appRepo, invalidator)
	}
}

// Ingest stores a message and queues its fan-out. The returned result is only
// produced after the transaction commits, so a 202 always means the message is
// on disk.
func (uc *IngestUseCase) Ingest(ctx context.Context, in IngestInput) (*IngestResult, error) {
	app, err := uc.resolver.resolve(ctx, in.OrgID, in.AppIDOrUID)
	if err != nil {
		return nil, err
	}

	if err := uc.validate(in); err != nil {
		return nil, err
	}

	// The id carries the creation time, and created_at is stored as exactly
	// that value, so the partition holding this row is derivable from the id.
	now := time.Now().UTC()
	msgID, err := ids.NewAt(ids.PrefixMessage, now)
	if err != nil {
		return nil, apperrors.NewInternalError(err)
	}
	createdAt, err := ids.TimeOf(msgID)
	if err != nil {
		return nil, apperrors.NewInternalError(err)
	}

	retentionDays := utils.Deref(in.RetentionDays, uc.cfg.DefaultRetentionDays)
	msg := &entities.Message{
		ID:          msgID,
		CreatedAt:   createdAt,
		OrgID:       app.OrgID,
		AppID:       app.ID,
		EventType:   in.EventType,
		UID:         in.EventID,
		Channels:    in.Channels,
		Payload:     uc.payloadCodec.Encode(in.Payload),
		PayloadSize: len(in.Payload),
		ExpiresAt:   createdAt.AddDate(0, 0, retentionDays),
	}

	// Advisory only: the match count goes back to the caller and into metrics,
	// and never decides whether to enqueue.
	//
	// Skipping the enqueue when nothing matches would save an insert, but it
	// assumes this node's snapshot is complete, and it is not: an endpoint
	// created on another node moments ago may not have reached us yet. We
	// would drop a delivery after telling the sender 202. The fan-out is
	// always queued, and the worker re-checks matching against a snapshot
	// that is newer than this one; it drops an empty fan-out in microseconds.
	var matched []*config.Endpoint
	if snapshot := uc.snapshotProvider.Current(); snapshot != nil {
		matched = snapshot.MatchingEndpoints(app.ID, in.EventType, in.Channels)
	}
	partitionKey := entities.PartitionKeyFor(app.ID)

	err = uc.txManager.WithinTransaction(ctx, func(txCtx context.Context) error {
		if err := uc.messageRepo.CreateMessage(txCtx, msg); err != nil {
			return err
		}
		// One enqueue per message, whatever the endpoint count. The expansion
		// happens in a worker, where latency does not matter.
		return uc.queueRepo.Enqueue(txCtx, []repositories.EnqueueTask{{
			PartitionKey: partitionKey,
			Pool:         uc.cfg.Pool,
			Kind:         entities.TaskFanout,
			OrgID:        app.OrgID,
			AppID:        app.ID,
			MsgID:        msg.ID,
			MsgCreatedAt: msg.CreatedAt,
			VisibleAt:    now,
		}})
	})
	if err != nil {
		if eris.Is(err, repositories.ErrConflict) {
			return nil, apperrors.NewConflictError("a message with this eventId already exists")
		}
		return nil, apperrors.NewInternalError(err)
	}

	// Best effort, and after the commit rather than inside it: workers also
	// poll, so a lost wakeup costs latency, not a delivery, while a NOTIFY
	// inside the transaction would serialize every ingest commit behind one
	// fsync (see the queue repository's Notify).
	if err := uc.queueRepo.Notify(ctx, []int16{partitionKey}); err != nil {
		logger.FromContext(ctx).Warn().Err(err).
			Int16("partition_key", partitionKey).
			Msg("queue wakeup notification failed")
	}

	return &IngestResult{Message: msg, MatchedEndpoints: len(matched), Payload: in.Payload}, nil
}

func (uc *IngestUseCase) validate(in IngestInput) error {
	if utils.IsBlank(in.EventType) {
		return apperrors.NewValidationError("eventType is required")
	}
	if int64(len(in.Payload)) > uc.cfg.MaxPayloadBytes {
		return apperrors.NewPayloadTooLargeError("payload exceeds the maximum size")
	}
	if len(in.Payload) == 0 {
		return apperrors.NewValidationError("payload is required")
	}
	// Validated once here, then stored and sent as opaque bytes.
	if !json.Valid(in.Payload) {
		return apperrors.NewValidationError("payload must be well-formed JSON")
	}
	if in.RetentionDays != nil && (*in.RetentionDays <= 0 || *in.RetentionDays > 365) {
		return apperrors.NewValidationError("payloadRetentionPeriod must be between 1 and 365 days")
	}
	return nil
}

// LoadPayload reads and decompresses a stored payload, for the read API and
// for resend.
func (uc *IngestUseCase) LoadPayload(ctx context.Context, msgID string, createdAt time.Time) ([]byte, error) {
	stored, err := uc.messageRepo.GetPayload(ctx, msgID, createdAt)
	if err != nil {
		if eris.Is(err, repositories.ErrNotFound) {
			return nil, apperrors.NewNotFoundError("message")
		}
		return nil, apperrors.NewInternalError(err)
	}
	raw, err := uc.payloadCodec.Decode(stored)
	if err != nil {
		return nil, apperrors.NewInternalError(err)
	}
	return raw, nil
}

// Snapshot exposes the current snapshot for handlers that need to resolve an
// application without a second lookup.
func (uc *IngestUseCase) Snapshot() *config.Snapshot { return uc.snapshotProvider.Current() }
