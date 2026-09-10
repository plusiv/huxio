// Package handlers holds the Echo handlers. They are thin: parse and validate
// input, call a use case, map the result or the error. No SQL, no business
// rules.
package handlers

import (
	"net/http"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/adapters/inbound/http/httperr"
	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/rotisserie/eris"
)

// ErrorResponse is the envelope every failed request returns. It never
// carries internal detail: error strings leak table names, hostnames and
// query fragments to whoever is probing.
type ErrorResponse struct {
	Code             string            `json:"code"`
	Detail           string            `json:"detail"`
	ValidationErrors []ValidationError `json:"validationErrors,omitempty"`
}

// ValidationError describes one rejected field.
type ValidationError struct {
	Loc []string `json:"loc"`
	Msg string   `json:"msg"`
	Typ string   `json:"type"`
}

// CustomHTTPErrorHandler maps AppError kinds onto status codes, logs internal
// causes server-side, and returns nothing a client should not see.
func CustomHTTPErrorHandler(c *echo.Context, err error) {
	if resp, _ := echo.UnwrapResponse(c.Response()); resp != nil && resp.Committed {
		return
	}

	log := logger.FromContext(c.Request().Context())

	var appErr *apperrors.AppError
	if eris.As(err, &appErr) {
		if appErr.Kind == apperrors.KindInternal {
			log.Error().Err(appErr.Cause).Stack().Msg("internal error")
			respond(c, http.StatusInternalServerError, ErrorResponse{
				Code: "internal_error", Detail: "internal server error",
			})
			return
		}
		respond(c, httperr.StatusForKind(appErr.Kind), ErrorResponse{Code: appErr.Code, Detail: appErr.Message})
		return
	}

	// Echo's own errors: routing, the binder, body limits.
	var httpErr *echo.HTTPError
	if eris.As(err, &httpErr) {
		detail := httpErr.Message
		if detail == "" {
			detail = http.StatusText(httpErr.Code)
		}
		respond(c, httpErr.Code, ErrorResponse{Code: httperr.CodeForStatus(httpErr.Code), Detail: detail})
		return
	}

	log.Error().Err(err).Stack().Msg("unexpected error")
	respond(c, http.StatusInternalServerError, ErrorResponse{
		Code: "internal_error", Detail: "internal server error",
	})
}

func respond(c *echo.Context, status int, body ErrorResponse) {
	if err := c.JSON(status, body); err != nil {
		logger.FromContext(c.Request().Context()).Error().Err(err).Msg("failed to write error response")
	}
}
