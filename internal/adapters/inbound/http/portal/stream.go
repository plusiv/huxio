package portal

import (
	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/adapters/inbound/http/attemptstream"
	"github.com/plusiv/huxio/internal/application/usecases"
)

// Stream writes new delivery attempts for the session's application.
func (h *Handler) Stream(c *echo.Context) error {
	session, err := sessionFrom(c)
	if err != nil {
		return err
	}

	input := usecases.ListAttemptsInput{
		OrgID:      session.OrgID,
		AppIDOrUID: session.AppID,
		EndpointID: c.QueryParam("endpointId"),
		Limit:      50,
	}
	attemptstream.Serve(c.Request().Context(), c.Response(), h.attemptUseCase, input, jsonEvent)
	return nil
}
