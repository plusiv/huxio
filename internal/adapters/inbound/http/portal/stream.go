package portal

import (
	"fmt"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/application/usecases"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
)

// streamPoll is how often the tail looks for new attempts. It polls because
// attempts are written in batches: a notification per attempt would undo the
// batching the whole delivery path is built on.
const streamPoll = time.Second

// streamKeepAlive stops proxies closing an idle stream.
const streamKeepAlive = 20 * time.Second

// Stream writes this application's new delivery attempts as server-sent
// events. It is the portal's headline feature and the only dynamic surface,
// which is why the portal needs no frontend framework.
func (h *Handler) Stream(c *echo.Context) error {
	session, err := sessionFrom(c)
	if err != nil {
		return err
	}

	response := c.Response()
	header := response.Header()
	header.Set(echo.HeaderContentType, "text/event-stream")
	header.Set(echo.HeaderCacheControl, "no-cache")
	header.Set("Connection", "keep-alive")
	header.Set("X-Accel-Buffering", "no")
	response.WriteHeader(http.StatusOK)

	controller := http.NewResponseController(response)
	_ = controller.Flush()

	ctx := c.Request().Context()
	input := usecases.ListAttemptsInput{
		OrgID:      session.OrgID,
		AppIDOrUID: session.AppID,
		EndpointID: c.QueryParam("endpointId"),
		Limit:      50,
	}

	// Only attempts newer than the connection: the log itself is paginated,
	// and replaying history down a live tail would be surprising.
	since := time.Now().UTC()
	poll := time.NewTicker(streamPoll)
	defer poll.Stop()
	keepAlive := time.NewTicker(streamKeepAlive)
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

		case <-poll.C:
			input.Filters.After = &since
			result, err := h.attemptUseCase.ListAttempts(ctx, input)
			if err != nil {
				logger.FromContext(ctx).Warn().Err(err).Msg("portal tail query failed")
				continue
			}

			// Oldest first, so the tail reads in the order things happened.
			for i := len(result.Items) - 1; i >= 0; i-- {
				attempt := result.Items[i]

				encoded, err := jsonEvent(attempt)
				if err != nil {
					continue
				}
				if _, err := fmt.Fprintf(response, "event: attempt\nid: %s\ndata: %s\n\n", attempt.ID, encoded); err != nil {
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
