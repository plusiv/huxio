package middleware

import (
	"errors"
	"net/http"
	"runtime"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/rotisserie/eris"
)

// Recover converts a panic into an internal error, logging the stack. It must
// be registered first so panics in any later middleware are caught.
func Recover() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) (err error) {
			defer func() {
				recovered := recover()
				if recovered == nil {
					return
				}
				if cause, ok := recovered.(error); ok && errors.Is(cause, http.ErrAbortHandler) {
					panic(recovered)
				}

				stack := make([]byte, 8<<10)
				stack = stack[:runtime.Stack(stack, false)]

				cause, ok := recovered.(error)
				if !ok {
					cause = eris.Errorf("panic: %v", recovered)
				}
				logger.FromContext(c.Request().Context()).Error().
					Err(cause).
					Bytes("stack", stack).
					Msg("recovered from panic")

				err = apperrors.NewInternalError(cause)
			}()
			return next(c)
		}
	}
}
