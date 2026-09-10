package middleware

import (
	"time"

	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
	"github.com/plusiv/huxio/internal/adapters/inbound/http/httperr"
	"github.com/plusiv/huxio/internal/infrastructure/authctx"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
)

// RequestID reads or generates an X-Request-Id and injects it into the request
// context, both as a typed value and as a persistent zerolog field, so every
// downstream log entry carries it.
func RequestID() echo.MiddlewareFunc {
	return middleware.RequestIDWithConfig(middleware.RequestIDConfig{
		RequestIDHandler: func(c *echo.Context, requestID string) {
			req := c.Request()
			enriched := logger.FromContext(req.Context()).With().Str("request_id", requestID).Logger()
			ctx := logger.Context(req.Context(), enriched)
			c.SetRequest(req.WithContext(WithRequestID(ctx, requestID)))
		},
	})
}

// RequestLogger logs one line per request through the context logger, skipping
// the health endpoints so a liveness probe does not dominate the log.
func RequestLogger() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			start := time.Now()
			err := next(c)

			path := c.Request().URL.Path
			if isHealthPath(path) {
				return err
			}

			status := httperr.StatusForError(c.Response(), err)
			resp, _ := echo.UnwrapResponse(c.Response())
			event := logger.FromContext(c.Request().Context()).Info()
			if status >= 500 {
				event = logger.FromContext(c.Request().Context()).Error()
			}
			if err != nil {
				event = event.Err(err)
			}
			if org := authctx.OrgFrom(c.Request().Context()); org != "" {
				event = event.Str("org_id", org)
			}
			var written int64
			if resp != nil {
				written = resp.Size
			}

			event.
				Str("method", c.Request().Method).
				Str("path", path).
				Str("route", routeOf(c)).
				Int("status", status).
				Int64("bytes", written).
				Dur("latency", time.Since(start)).
				Msg("request")
			return err
		}
	}
}

func isHealthPath(path string) bool {
	return path == "/api/v1/health/" || path == "/api/v1/health" ||
		path == "/api/v1/health/ready/" || path == "/api/v1/health/ready"
}

// routeOf returns the matched route template, which is the low-cardinality
// label metrics and logs should carry rather than the concrete path.
func routeOf(c *echo.Context) string {
	if path := c.RouteInfo().Path; path != "" {
		return path
	}
	return "unmatched"
}
