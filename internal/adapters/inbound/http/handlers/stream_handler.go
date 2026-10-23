package handlers

import (
	"encoding/json"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/adapters/inbound/http/attemptstream"
	"github.com/plusiv/huxio/internal/application/usecases"
	"github.com/plusiv/huxio/internal/domain/entities"
)

// StreamHandler serves the live attempt tail.
type StreamHandler struct {
	attemptUseCase *usecases.AttemptUseCase
}

// NewStreamHandler builds the handler.
func NewStreamHandler(attemptUseCase *usecases.AttemptUseCase) *StreamHandler {
	return &StreamHandler{attemptUseCase: attemptUseCase}
}

// StreamAttempts writes new delivery attempts as server-sent events until the
// client goes away.
func (h *StreamHandler) StreamAttempts(c *echo.Context) error {
	principal, err := principal(c)
	if err != nil {
		return err
	}

	filters, err := attemptFilters(c)
	if err != nil {
		return err
	}

	input := usecases.ListAttemptsInput{
		OrgID:      principal.OrgID,
		AppIDOrUID: c.Param("app_id"),
		EndpointID: c.QueryParam("endpointId"),
		MsgID:      c.QueryParam("msgId"),
		Filters:    filters,
		Limit:      50,
	}

	attemptstream.Serve(c.Request().Context(), c.Response(), h.attemptUseCase, input,
		func(attempt *entities.DeliveryAttempt) ([]byte, error) {
			return json.Marshal(newAttemptResponse(attempt))
		})
	return nil
}
