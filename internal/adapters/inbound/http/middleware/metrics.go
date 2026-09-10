package middleware

import (
	"time"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/adapters/inbound/http/httperr"
	"github.com/plusiv/huxio/internal/infrastructure/telemetry"
)

// Metrics records request duration by method, matched route and status class.
// The route template is used rather than the concrete path: labelling by path
// would put every application id into Prometheus.
func Metrics(metrics *telemetry.Metrics) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			start := time.Now()
			err := next(c)

			status := httperr.StatusForError(c.Response(), err)
			metrics.APIRequestDuration.WithLabelValues(
				c.Request().Method,
				routeOf(c),
				telemetry.StatusClass(status),
			).Observe(time.Since(start).Seconds())

			return err
		}
	}
}
