// Package httperr maps application errors onto HTTP status codes and the
// stable error codes the API returns. It is shared by the error handler, the
// request logger and the metrics middleware so all three agree on what status
// a request actually produced.
package httperr

import (
	"net/http"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/rotisserie/eris"
)

// StatusForKind maps an application error kind onto a status code.
func StatusForKind(kind apperrors.ErrorKind) int {
	switch kind {
	case apperrors.KindNotFound:
		return http.StatusNotFound
	case apperrors.KindConflict:
		return http.StatusConflict
	case apperrors.KindValidation, apperrors.KindBusiness:
		return http.StatusUnprocessableEntity
	case apperrors.KindUnauthorized:
		return http.StatusUnauthorized
	case apperrors.KindForbidden:
		return http.StatusForbidden
	case apperrors.KindRateLimit:
		return http.StatusTooManyRequests
	case apperrors.KindPayloadTooLarge:
		return http.StatusRequestEntityTooLarge
	case apperrors.KindGone:
		return http.StatusGone
	default:
		return http.StatusInternalServerError
	}
}

// StatusForError resolves the status a request will produce, given the
// response written so far and the error returned by the handler chain. It
// exists because Echo's own resolver only understands errors that carry a
// status code, and an AppError deliberately does not: mapping a domain error
// to a transport code is the adapter's job, not the domain's.
func StatusForError(response http.ResponseWriter, err error) int {
	var appErr *apperrors.AppError
	if eris.As(err, &appErr) {
		if resp, _ := echo.UnwrapResponse(response); resp != nil && resp.Committed && resp.Status != 0 {
			return resp.Status
		}
		return StatusForKind(appErr.Kind)
	}
	_, status := echo.ResolveResponseStatus(response, err)
	return status
}

// CodeForStatus returns the stable machine-readable code for a status.
func CodeForStatus(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "bad_request"
	case http.StatusUnauthorized:
		return "authentication_failed"
	case http.StatusForbidden:
		return "forbidden"
	case http.StatusNotFound:
		return "not_found"
	case http.StatusMethodNotAllowed:
		return "method_not_allowed"
	case http.StatusRequestEntityTooLarge:
		return "payload_too_large"
	case http.StatusUnprocessableEntity:
		return "validation"
	case http.StatusTooManyRequests:
		return "rate_limited"
	default:
		return "error"
	}
}
