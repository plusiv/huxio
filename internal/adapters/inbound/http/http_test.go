package http_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/adapters/inbound/http/handlers"
	appmiddleware "github.com/plusiv/huxio/internal/adapters/inbound/http/middleware"
	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/rotisserie/eris"
)

// newTestEcho builds an Echo instance with the real error handler, so the
// tests below exercise the mapping production uses.
func newTestEcho() *echo.Echo {
	e := echo.New()
	e.HTTPErrorHandler = handlers.CustomHTTPErrorHandler
	return e
}

func callHandler(e *echo.Echo, handler echo.HandlerFunc, middleware ...echo.MiddlewareFunc) *httptest.ResponseRecorder {
	e.GET("/probe", handler, middleware...)
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/probe", nil))
	return recorder
}

func TestErrorHandlerMapsAppErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{"not found", apperrors.NewNotFoundError("endpoint"), http.StatusNotFound, "not_found"},
		{"conflict", apperrors.NewConflictError("uid taken"), http.StatusConflict, "conflict"},
		{"validation", apperrors.NewValidationError("url is required"), http.StatusUnprocessableEntity, "validation"},
		{"business rule", apperrors.NewBusinessRuleError("endpoint disabled"), http.StatusUnprocessableEntity, "business_rule"},
		{"unauthorized", apperrors.NewUnauthorizedError("bad token"), http.StatusUnauthorized, "authentication_failed"},
		{"forbidden", apperrors.NewForbiddenError("wrong app"), http.StatusForbidden, "forbidden"},
		{"rate limited", apperrors.NewRateLimitError("slow down"), http.StatusTooManyRequests, "rate_limited"},
		{"payload too large", apperrors.NewPayloadTooLargeError("too big"), http.StatusRequestEntityTooLarge, "payload_too_large"},
		{"gone", apperrors.NewGoneError("expired"), http.StatusGone, "gone"},
		{"wrapped", eris.Wrap(apperrors.NewNotFoundError("message"), "load message"), http.StatusNotFound, "not_found"},
		{"echo http error", echo.NewHTTPError(http.StatusBadRequest, "invalid request body"), http.StatusBadRequest, "bad_request"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			recorder := callHandler(newTestEcho(), func(*echo.Context) error { return tc.err })
			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", recorder.Code, tc.wantStatus, recorder.Body)
			}

			var body handlers.ErrorResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode %q: %v", recorder.Body, err)
			}
			if body.Code != tc.wantCode {
				t.Errorf("code = %q, want %q", body.Code, tc.wantCode)
			}
			if body.Detail == "" {
				t.Error("every error response needs a detail")
			}
		})
	}
}

func TestErrorHandlerHidesInternalDetail(t *testing.T) {
	t.Parallel()

	cause := eris.New("dial tcp 10.1.2.3:5432: connection refused")
	recorder := callHandler(newTestEcho(), func(*echo.Context) error {
		return apperrors.NewInternalError(cause)
	})

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", recorder.Code)
	}
	if got := recorder.Body.String(); strings.Contains(got, "10.1.2.3") || strings.Contains(got, "connection refused") {
		t.Errorf("internal detail leaked to the client: %s", got)
	}
}

func TestRecoverTurnsPanicIntoInternalError(t *testing.T) {
	t.Parallel()

	recorder := callHandler(newTestEcho(), func(*echo.Context) error {
		panic("something went badly wrong in a handler")
	}, appmiddleware.Recover())

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", recorder.Code)
	}
	if got := recorder.Body.String(); strings.Contains(got, "badly wrong") {
		t.Errorf("panic message leaked to the client: %s", got)
	}
}

func TestRecoverLeavesSuccessfulHandlersAlone(t *testing.T) {
	t.Parallel()

	recorder := callHandler(newTestEcho(), func(c *echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	}, appmiddleware.Recover())

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
}
