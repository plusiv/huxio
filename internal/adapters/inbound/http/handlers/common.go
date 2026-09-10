package handlers

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/authctx"
)

// CursorResponse is the envelope every list route returns. There is
// deliberately no total: a COUNT(*) alongside a paginated query is exactly the
// query that gets slow first.
type CursorResponse[T any] struct {
	Data         []T     `json:"data"`
	Iterator     *string `json:"iterator"`
	PrevIterator *string `json:"prevIterator"`
	Done         bool    `json:"done"`
}

// newCursorResponse converts a repository result into the wire envelope.
func newCursorResponse[T any](result repositories.CursorResult[T]) CursorResponse[T] {
	data := result.Items
	if data == nil {
		data = []T{}
	}
	return CursorResponse[T]{
		Data:         data,
		Iterator:     result.NextCursor,
		PrevIterator: result.PrevCursor,
		Done:         !result.HasMore,
	}
}

// pagination reads the cursor parameters, accepting both the compatible
// `iterator` name and the plainer `cursor`.
func pagination(c *echo.Context) repositories.CursorPagination {
	cursor := c.QueryParam("iterator")
	if cursor == "" {
		cursor = c.QueryParam("cursor")
	}
	limit, _ := strconv.Atoi(c.QueryParam("limit"))
	return repositories.CursorPagination{Cursor: cursor, Limit: limit}
}

// principal returns the authenticated caller, or 401.
func principal(c *echo.Context) (authctx.Principal, error) {
	p, ok := authctx.FromContext(c.Request().Context())
	if !ok {
		return authctx.Principal{}, apperrors.NewUnauthorizedError("authentication required")
	}
	return p, nil
}

// orgToken returns the caller and rejects portal tokens, for routes that
// manage tenant-wide configuration.
func orgToken(c *echo.Context) (authctx.Principal, error) {
	p, err := principal(c)
	if err != nil {
		return p, err
	}
	if p.Portal {
		return p, apperrors.NewForbiddenError("a portal token cannot access this resource")
	}
	return p, nil
}

// jsonMarshal is the single place responses are encoded outside Echo, for the
// server-sent event stream.
func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }

// parseAttemptStatus accepts either the numeric status or its name, because
// both appear in the wild: the compatible SDKs send numbers and humans send
// words.
func parseAttemptStatus(raw string) (entities.AttemptStatus, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "0", "success", "succeeded":
		return entities.AttemptSucceeded, nil
	case "1", "pending", "sending":
		return entities.AttemptPendingRetry, nil
	case "2", "fail", "failed", "failure":
		return entities.AttemptFailed, nil
	default:
		return 0, apperrors.NewValidationError("status must be one of success, pending or fail")
	}
}

// boolQuery reads a boolean query parameter.
func boolQuery(c *echo.Context, name string) bool {
	return strings.EqualFold(c.QueryParam(name), "true")
}

// intQuery reads an optional positive integer query parameter.
func intQuery(c *echo.Context, name string) *int {
	raw := c.QueryParam(name)
	if raw == "" {
		return nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return nil
	}
	return &value
}
