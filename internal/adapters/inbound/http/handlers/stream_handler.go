package handlers

import (
	"fmt"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/application/usecases"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
)

// StreamPollInterval is how often the live tail looks for new attempts. It
// polls rather than listening because attempts are written in batches: a
// notification per attempt would undo the batching this system is built on.
const StreamPollInterval = time.Second

// StreamKeepAlive keeps proxies from closing an idle stream.
const StreamKeepAlive = 20 * time.Second

// StreamHandler serves the live attempt tail, the portal's headline feature.
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

	response := c.Response()
	header := response.Header()
	header.Set(echo.HeaderContentType, "text/event-stream")
	header.Set(echo.HeaderCacheControl, "no-cache")
	header.Set("Connection", "keep-alive")
	// Nginx buffers responses by default, which would hold every event until
	// the connection closed.
	header.Set("X-Accel-Buffering", "no")
	response.WriteHeader(http.StatusOK)

	controller := http.NewResponseController(response)
	_ = controller.Flush()

	ctx := c.Request().Context()
	input := usecases.ListAttemptsInput{
		OrgID:      principal.OrgID,
		AppIDOrUID: c.Param("app_id"),
		EndpointID: c.QueryParam("endpointId"),
		MsgID:      c.QueryParam("msgId"),
		Filters:    filters,
		Limit:      50,
	}

	// Only attempts newer than the connection are streamed: the log itself is
	// paginated, and replaying history down a live tail would be surprising.
	since := time.Now().UTC()
	ticker := time.NewTicker(StreamPollInterval)
	defer ticker.Stop()
	keepAlive := time.NewTicker(StreamKeepAlive)
	defer keepAlive.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-keepAlive.C:
			if _, err := fmt.Fprint(response, ": keep-alive\n\n"); err != nil {
				return nil
			}
			_ = controller.Flush()

		case <-ticker.C:
			input.Filters.After = &since
			result, err := h.attemptUseCase.ListAttempts(ctx, input)
			if err != nil {
				logger.FromContext(ctx).Warn().Err(err).Msg("attempt stream query failed")
				continue
			}

			// The listing is newest first; the tail reads better oldest first.
			for i := len(result.Items) - 1; i >= 0; i-- {
				attempt := result.Items[i]
				if err := writeAttemptEvent(response, attempt); err != nil {
					return nil
				}
				if !attempt.CreatedAt.Before(since) {
					since = attempt.CreatedAt.Add(time.Microsecond)
				}
			}
			if len(result.Items) > 0 {
				_ = controller.Flush()
			}
		}
	}
}

func writeAttemptEvent(w http.ResponseWriter, attempt *entities.DeliveryAttempt) error {
	encoded, err := jsonMarshal(newAttemptResponse(attempt))
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: attempt\nid: %s\ndata: %s\n\n", attempt.ID, encoded)
	return err
}
