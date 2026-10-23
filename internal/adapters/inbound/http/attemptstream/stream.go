// Package attemptstream serves live delivery attempts as server-sent events.
package attemptstream

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/plusiv/huxio/internal/application/usecases"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
)

// Lister reads attempts within the application and filters chosen by the caller.
type Lister interface {
	ListAttempts(context.Context, usecases.ListAttemptsInput) (repositories.CursorResult[*entities.DeliveryAttempt], error)
}

// Serve blocks until cancellation or an event cannot be written. The caller
// must authenticate and scope input before calling it: headers are committed
// immediately. encode supplies the caller's event format.
func Serve(ctx context.Context, response http.ResponseWriter, lister Lister, input usecases.ListAttemptsInput,
	encode func(*entities.DeliveryAttempt) ([]byte, error),
) {
	header := response.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache")
	header.Set("Connection", "keep-alive")
	// Disable proxy buffering so events reach the client before the stream ends.
	header.Set("X-Accel-Buffering", "no")
	response.WriteHeader(http.StatusOK)
	controller := http.NewResponseController(response)
	_ = controller.Flush()

	// History is available through pagination; the live stream starts here.
	since := time.Now().UTC()
	// Polling avoids adding a notification for every batched attempt write.
	poll := time.NewTicker(time.Second)
	defer poll.Stop()
	keepAlive := time.NewTicker(20 * time.Second)
	defer keepAlive.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-keepAlive.C:
			if _, err := fmt.Fprint(response, ": keep-alive\n\n"); err != nil {
				return
			}
			_ = controller.Flush()
		case <-poll.C:
			input.Filters.After = &since
			result, err := lister.ListAttempts(ctx, input)
			if err != nil {
				logger.FromContext(ctx).Warn().Err(err).Msg("attempt stream query failed")
				continue
			}

			// Listings are newest first; stream each batch oldest first.
			for i := len(result.Items) - 1; i >= 0; i-- {
				attempt := result.Items[i]
				encoded, err := encode(attempt)
				if err != nil {
					return
				}
				if _, err := fmt.Fprintf(response, "event: attempt\nid: %s\ndata: %s\n\n", attempt.ID, encoded); err != nil {
					return
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
