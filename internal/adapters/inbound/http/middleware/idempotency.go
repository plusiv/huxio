package middleware

import (
	"bytes"
	"context"
	"encoding/hex"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/authctx"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/plusiv/huxio/internal/utils"
	"golang.org/x/crypto/blake2b"
)

// IdempotencyHeader is the request header that opts a POST into replay
// protection.
const IdempotencyHeader = "Idempotency-Key"

// IdempotencyOptions configures the middleware.
type IdempotencyOptions struct {
	// LockTTL is how long an in-progress row holds the key. It bounds how long
	// a crashed request wedges a retry.
	LockTTL time.Duration
	// ResponseTTL is how long a completed response is replayed for.
	ResponseTTL time.Duration
}

// Idempotency replays the original response for a repeated POST carrying the
// same Idempotency-Key, so a client that retries after a timeout does not
// create a second message. It sits in front of the router rather than inside
// each handler.
func Idempotency(repo repositories.IdempotencyRepository, opts IdempotencyOptions) echo.MiddlewareFunc {
	if opts.LockTTL <= 0 {
		opts.LockTTL = 30 * time.Second
	}
	if opts.ResponseTTL <= 0 {
		opts.ResponseTTL = 12 * time.Hour
	}

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			clientKey := c.Request().Header.Get(IdempotencyHeader)
			if c.Request().Method != http.MethodPost || clientKey == "" {
				return next(c)
			}

			ctx := c.Request().Context()
			key := idempotencyKey(authctx.OrgFrom(ctx), c.Request().Method, c.Request().URL.Path, clientKey)

			existing, won, err := repo.Begin(ctx, key, opts.LockTTL)
			if err != nil {
				return apperrors.NewInternalError(err)
			}
			if !won {
				return replay(c, existing)
			}

			recorder := newResponseRecorder(c.Response())
			c.SetResponse(recorder)

			handlerErr := next(c)

			// Only a completed, non-server-error response is worth replaying. Releasing
			// the lock on failure lets the client retry at once instead of waiting out
			// the lock.
			if handlerErr != nil || recorder.status >= http.StatusInternalServerError {
				if err := repo.Abort(context.WithoutCancel(ctx), key); err != nil {
					logger.FromContext(ctx).Warn().Err(err).Msg("failed to release idempotency lock")
				}
				return handlerErr
			}

			if err := repo.Complete(context.WithoutCancel(ctx), key, int16(recorder.status), recorder.body.Bytes(), opts.ResponseTTL); err != nil {
				// The work already happened and the client already has its answer; a
				// failed cache write must not turn that into a 500.
				logger.FromContext(ctx).Warn().Err(err).Msg("failed to store idempotent response")
			}
			return nil
		}
	}
}

// idempotencyKey hashes the tenant, request and client key together, so a
// client key can never collide across tenants or across endpoints.
func idempotencyKey(orgID, method, path, clientKey string) string {
	sum := blake2b.Sum256([]byte(orgID + "\x00" + method + "\x00" + path + "\x00" + clientKey))
	return hex.EncodeToString(sum[:])
}

func replay(c *echo.Context, record *entities.IdempotencyRecord) error {
	if record == nil || record.Status != entities.IdempotencyComplete {
		// Still in flight: the first request is the one that will answer.
		return apperrors.NewConflictError("a request with this Idempotency-Key is already in progress")
	}
	status := int(utils.Deref(record.StatusCode, http.StatusOK))
	if len(record.Response) == 0 {
		return c.NoContent(status)
	}
	return c.Blob(status, echo.MIMEApplicationJSON, record.Response)
}

// responseRecorder buffers the response so a completed one can be replayed
// later, while still writing it to the client immediately.
type responseRecorder struct {
	http.ResponseWriter

	status int
	body   bytes.Buffer
}

func newResponseRecorder(w http.ResponseWriter) *responseRecorder {
	return &responseRecorder{ResponseWriter: w, status: http.StatusOK}
}

func (r *responseRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	r.body.Write(b)
	return r.ResponseWriter.Write(b)
}

// Unwrap lets echo.UnwrapResponse and http.ResponseController reach the
// underlying writer, which the error handler and flushing depend on.
func (r *responseRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
