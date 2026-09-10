// Package repositories defines the outbound port interfaces and the shared
// query abstractions every repository implementation speaks. It is part of the
// domain, so nothing here imports a driver or a web framework.
package repositories

import (
	"encoding/base64"
	"encoding/json"

	"github.com/rotisserie/eris"
)

// Filter holds typed predicate options for a single column. Only the non-nil /
// non-empty fields participate in the WHERE clause.
type Filter[T comparable] struct {
	Is        *T
	IsNot     *T
	In        []T
	NotIn     []T
	IContains *T
	Exists    *bool
	GT        *T
	GTE       *T
	LT        *T
	LTE       *T
}

// IsZero reports whether the filter would contribute no predicate at all.
func (f *Filter[T]) IsZero() bool {
	if f == nil {
		return true
	}
	return f.Is == nil && f.IsNot == nil && len(f.In) == 0 && len(f.NotIn) == 0 &&
		f.IContains == nil && f.Exists == nil &&
		f.GT == nil && f.GTE == nil && f.LT == nil && f.LTE == nil
}

// Eq is shorthand for a Filter matching a single value.
func Eq[T comparable](v T) *Filter[T] { return &Filter[T]{Is: &v} }

// AnyOf is shorthand for a Filter matching any of the supplied values.
func AnyOf[T comparable](vs ...T) *Filter[T] { return &Filter[T]{In: vs} }

// IsNull is shorthand for a Filter asserting a column is (or is not) null.
func IsNull[T comparable](null bool) *Filter[T] {
	exists := !null
	return &Filter[T]{Exists: &exists}
}

// CursorPagination carries the opaque cursor token and page size for
// keyset-based pagination. Cursor is empty on the first page.
type CursorPagination struct {
	// Cursor is the opaque token returned by the previous page. An empty
	// string, and an unparseable one, both mean "start from the beginning".
	Cursor string
	// Limit is the maximum number of rows to return. Defaults to 20 when <= 0
	// and is capped at 100.
	Limit int
}

// DefaultPageSize and MaxPageSize bound every list endpoint.
const (
	DefaultPageSize = 20
	MaxPageSize     = 100
)

// GetLimit returns the effective page size with defaults applied.
func (p CursorPagination) GetLimit() int {
	if p.Limit <= 0 {
		return DefaultPageSize
	}
	if p.Limit > MaxPageSize {
		return MaxPageSize
	}
	return p.Limit
}

// CursorResult is the generic return value for every list operation.
type CursorResult[T any] struct {
	Items      []T
	NextCursor *string
	PrevCursor *string
	HasMore    bool
	Limit      int
}

// cursorPayload is the internal representation encoded inside the token.
type cursorPayload struct {
	V  string `json:"v"`  // sort-column value, stringified
	ID string `json:"id"` // row id for tiebreaking
}

// EncodeCursor produces an opaque, URL-safe cursor token from a sort-column
// value and a row id.
func EncodeCursor(value, id string) string {
	raw, _ := json.Marshal(cursorPayload{V: value, ID: id})
	return base64.RawURLEncoding.EncodeToString(raw)
}

// DecodeCursor parses an opaque cursor token. Callers treat an error as "first
// page" rather than a client error.
func DecodeCursor(encoded string) (value, id string, err error) {
	if encoded == "" {
		return "", "", eris.New("empty cursor")
	}
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return "", "", eris.Wrap(err, "invalid cursor encoding")
	}
	var p cursorPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return "", "", eris.Wrap(err, "invalid cursor payload")
	}
	return p.V, p.ID, nil
}

// OrderBy describes a single sort directive. Column must be a pre-validated,
// allowlist-confirmed identifier: it is inserted verbatim into SQL.
type OrderBy struct {
	Column string
	Desc   bool
}
