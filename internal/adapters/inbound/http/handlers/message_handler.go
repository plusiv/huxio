package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/application/usecases"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/infrastructure/telemetry"
	"github.com/plusiv/huxio/internal/utils"
)

// MessageHandler serves the ingest and message-read routes.
type MessageHandler struct {
	ingestUseCase  *usecases.IngestUseCase
	messageUseCase *usecases.MessageUseCase
	metrics        *telemetry.Metrics
}

// NewMessageHandler builds the handler.
func NewMessageHandler(
	ingestUseCase *usecases.IngestUseCase,
	messageUseCase *usecases.MessageUseCase,
	metrics *telemetry.Metrics,
) *MessageHandler {
	return &MessageHandler{
		ingestUseCase:  ingestUseCase,
		messageUseCase: messageUseCase,
		metrics:        metrics,
	}
}

// createMessageRequest is the ingest payload.
type createMessageRequest struct {
	EventType              string          `json:"eventType"              validate:"required,max=256"`
	EventID                *string         `json:"eventId"                validate:"omitempty,max=256"`
	Payload                json.RawMessage `json:"payload"                validate:"required"`
	Channels               []string        `json:"channels"               validate:"omitempty,max=10,dive,max=128"`
	PayloadRetentionPeriod *int            `json:"payloadRetentionPeriod" validate:"omitempty,min=1,max=365"`
}

// messageResponse is the wire shape of a message.
type messageResponse struct {
	ID        string          `json:"id"`
	Timestamp time.Time       `json:"timestamp"`
	EventType string          `json:"eventType"`
	EventID   *string         `json:"eventId,omitempty"`
	Channels  []string        `json:"channels,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

func newMessageResponse(msg *entities.Message, payload json.RawMessage) messageResponse {
	return messageResponse{
		ID:        msg.ID,
		Timestamp: msg.CreatedAt,
		EventType: msg.EventType,
		EventID:   msg.UID,
		Channels:  msg.Channels,
		Payload:   payload,
	}
}

// CreateMessage accepts a message. A 202 is returned only after the
// transaction commits, so it always means the message is on disk.
func (h *MessageHandler) CreateMessage(c *echo.Context) error {
	principal, err := principal(c)
	if err != nil {
		return err
	}

	var req createMessageRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	started := time.Now()
	result, err := h.ingestUseCase.Ingest(c.Request().Context(), usecases.IngestInput{
		OrgID:         principal.OrgID,
		AppIDOrUID:    c.Param("app_id"),
		EventType:     req.EventType,
		EventID:       req.EventID,
		Payload:       req.Payload,
		Channels:      req.Channels,
		RetentionDays: req.PayloadRetentionPeriod,
	})
	if err != nil {
		return err
	}
	h.metrics.IngestDuration.WithLabelValues(principal.OrgID).Observe(time.Since(started).Seconds())

	return c.JSON(http.StatusAccepted, newMessageResponse(result.Message, result.Payload))
}

// GetMessage returns one message including its payload.
func (h *MessageHandler) GetMessage(c *echo.Context) error {
	principal, err := principal(c)
	if err != nil {
		return err
	}

	msg, err := h.messageUseCase.GetMessage(c.Request().Context(), usecases.GetMessageInput{
		OrgID:      principal.OrgID,
		AppIDOrUID: c.Param("app_id"),
		MsgID:      c.Param("msg_id"),
	})
	if err != nil {
		return err
	}

	payload, err := h.ingestUseCase.LoadPayload(c.Request().Context(), msg.ID, msg.CreatedAt)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, newMessageResponse(msg, payload))
}

// ListMessages returns a cursor-paginated page of messages without payloads.
func (h *MessageHandler) ListMessages(c *echo.Context) error {
	principal, err := principal(c)
	if err != nil {
		return err
	}

	page := pagination(c)
	result, err := h.messageUseCase.ListMessages(c.Request().Context(), usecases.ListMessagesInput{
		OrgID:      principal.OrgID,
		AppIDOrUID: c.Param("app_id"),
		EventTypes: c.QueryParams()["eventTypes"],
		Channel:    utils.NilIfBlank(c.QueryParam("channel")),
		Before:     parseTimeQuery(c, "before"),
		After:      parseTimeQuery(c, "after"),
		Cursor:     page.Cursor,
		Limit:      page.Limit,
	})
	if err != nil {
		return err
	}

	items := utils.Map(result.Items, func(msg *entities.Message) messageResponse {
		return newMessageResponse(msg, nil)
	})
	return c.JSON(http.StatusOK, CursorResponse[messageResponse]{
		Data:         items,
		Iterator:     result.NextCursor,
		PrevIterator: result.PrevCursor,
		Done:         !result.HasMore,
	})
}

// parseTimeQuery reads an RFC3339 timestamp query parameter.
func parseTimeQuery(c *echo.Context, name string) *time.Time {
	raw := c.QueryParam(name)
	if raw == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil
	}
	return utils.Ptr(parsed.UTC())
}
