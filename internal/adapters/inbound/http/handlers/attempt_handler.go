package handlers

import (
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/application/usecases"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/utils"
)

// AttemptHandler serves the delivery log, resend, recover and replay.
type AttemptHandler struct {
	attemptUseCase *usecases.AttemptUseCase
}

// NewAttemptHandler builds the handler.
func NewAttemptHandler(attemptUseCase *usecases.AttemptUseCase) *AttemptHandler {
	return &AttemptHandler{attemptUseCase: attemptUseCase}
}

// attemptResponse is the wire shape of one delivery attempt.
type attemptResponse struct {
	ID           string     `json:"id"`
	Timestamp    time.Time  `json:"timestamp"`
	MsgID        string     `json:"msgId"`
	EndpointID   string     `json:"endpointId"`
	URL          string     `json:"url"`
	Status       int        `json:"status"`
	StatusText   string     `json:"statusText"`
	ResponseCode int        `json:"responseStatusCode"`
	Response     string     `json:"response"`
	DurationMS   int32      `json:"responseDurationMs"`
	Attempt      int16      `json:"attemptNumber"`
	TriggerType  int        `json:"triggerType"`
	NextAttempt  *time.Time `json:"nextAttempt,omitempty"`
}

func newAttemptResponse(attempt *entities.DeliveryAttempt) attemptResponse {
	return attemptResponse{
		ID:           attempt.ID,
		Timestamp:    attempt.CreatedAt,
		MsgID:        attempt.MsgID,
		EndpointID:   attempt.EndpointID,
		URL:          attempt.URL,
		Status:       int(attempt.Status),
		StatusText:   attemptStatusText(attempt.Status),
		ResponseCode: int(attempt.ResponseStatusCode),
		Response:     attempt.ResponseBody,
		DurationMS:   attempt.ResponseDurationMS,
		Attempt:      attempt.AttemptNumber,
		TriggerType:  int(attempt.TriggerType),
		NextAttempt:  attempt.NextAttemptAt,
	}
}

func attemptStatusText(status entities.AttemptStatus) string {
	switch status {
	case entities.AttemptSucceeded:
		return "success"
	case entities.AttemptPendingRetry:
		return "pending"
	default:
		return "fail"
	}
}

type recoverRequest struct {
	Since *time.Time `json:"since" validate:"required"`
	Until *time.Time `json:"until"`
}

type replayRequest struct {
	Since       *time.Time `json:"since"       validate:"required"`
	Until       *time.Time `json:"until"`
	EndpointIDs []string   `json:"endpointIds" validate:"omitempty,max=100,dive,max=256"`
	EventTypes  []string   `json:"eventTypes"  validate:"omitempty,max=100,dive,max=256"`
}

type replayResponse struct {
	Enqueued  int  `json:"enqueued"`
	Truncated bool `json:"truncated"`
}

// ListByMessage returns every attempt made for one message.
func (h *AttemptHandler) ListByMessage(c *echo.Context) error {
	return h.list(c, c.Param("msg_id"), "")
}

// ListByEndpoint returns every attempt made to one endpoint.
func (h *AttemptHandler) ListByEndpoint(c *echo.Context) error {
	return h.list(c, "", c.Param("endpoint_id"))
}

// ListByMessageAndEndpoint returns the attempts for one message and endpoint
// pair, which is what the portal shows when someone opens a delivery.
func (h *AttemptHandler) ListByMessageAndEndpoint(c *echo.Context) error {
	return h.list(c, c.Param("msg_id"), c.Param("endpoint_id"))
}

func (h *AttemptHandler) list(c *echo.Context, msgID, endpointID string) error {
	principal, err := principal(c)
	if err != nil {
		return err
	}

	filters, err := attemptFilters(c)
	if err != nil {
		return err
	}

	page := pagination(c)
	result, err := h.attemptUseCase.ListAttempts(c.Request().Context(), usecases.ListAttemptsInput{
		OrgID:      principal.OrgID,
		AppIDOrUID: c.Param("app_id"),
		MsgID:      msgID,
		EndpointID: endpointID,
		Filters:    filters,
		Cursor:     page.Cursor,
		Limit:      page.Limit,
	})
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, CursorResponse[attemptResponse]{
		Data:         utils.Map(result.Items, newAttemptResponse),
		Iterator:     result.NextCursor,
		PrevIterator: result.PrevCursor,
		Done:         !result.HasMore,
	})
}

// Resend queues one more delivery of a message to an endpoint.
func (h *AttemptHandler) Resend(c *echo.Context) error {
	principal, err := principal(c)
	if err != nil {
		return err
	}

	err = h.attemptUseCase.Resend(c.Request().Context(), usecases.ResendInput{
		OrgID:      principal.OrgID,
		AppIDOrUID: c.Param("app_id"),
		MsgID:      c.Param("msg_id"),
		EndpointID: c.Param("endpoint_id"),
	})
	if err != nil {
		return err
	}
	return c.NoContent(http.StatusAccepted)
}

// Recover replays everything that failed to one endpoint since a point in
// time.
func (h *AttemptHandler) Recover(c *echo.Context) error {
	principal, err := principal(c)
	if err != nil {
		return err
	}

	var req recoverRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	result, err := h.attemptUseCase.Recover(c.Request().Context(), usecases.RecoverInput{
		OrgID:      principal.OrgID,
		AppIDOrUID: c.Param("app_id"),
		EndpointID: c.Param("endpoint_id"),
		Since:      utils.Deref(req.Since, time.Time{}),
		Until:      req.Until,
	})
	if err != nil {
		return err
	}
	return c.JSON(http.StatusAccepted, replayResponse{Enqueued: result.Enqueued, Truncated: result.Truncated})
}

// Replay replays by filter across endpoints, which is broader than the
// per-endpoint recover call.
func (h *AttemptHandler) Replay(c *echo.Context) error {
	principal, err := principal(c)
	if err != nil {
		return err
	}

	var req replayRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	result, err := h.attemptUseCase.Replay(c.Request().Context(), usecases.ReplayInput{
		OrgID:       principal.OrgID,
		AppIDOrUID:  c.Param("app_id"),
		EndpointIDs: req.EndpointIDs,
		EventTypes:  req.EventTypes,
		Since:       utils.Deref(req.Since, time.Time{}),
		Until:       req.Until,
	})
	if err != nil {
		return err
	}
	return c.JSON(http.StatusAccepted, replayResponse{Enqueued: result.Enqueued, Truncated: result.Truncated})
}

// Stats reports attempt counts for one endpoint.
func (h *AttemptHandler) Stats(c *echo.Context) error {
	principal, err := principal(c)
	if err != nil {
		return err
	}

	stats, err := h.attemptUseCase.Stats(c.Request().Context(),
		principal.OrgID, c.Param("app_id"), c.Param("endpoint_id"),
		parseTimeQuery(c, "since"),
	)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, stats)
}

// attemptFilters reads the shared filter set off the query string.
func attemptFilters(c *echo.Context) (usecases.AttemptFilterInput, error) {
	filters := usecases.AttemptFilterInput{
		StatusCodeClass: intQuery(c, "statusCodeClass"),
		EventTypes:      c.QueryParams()["eventTypes"],
		Channel:         utils.NilIfBlank(c.QueryParam("channel")),
		Before:          parseTimeQuery(c, "before"),
		After:           parseTimeQuery(c, "after"),
	}

	if raw := c.QueryParam("status"); raw != "" {
		status, err := parseAttemptStatus(raw)
		if err != nil {
			return filters, err
		}
		filters.Status = &status
	}
	return filters, nil
}
