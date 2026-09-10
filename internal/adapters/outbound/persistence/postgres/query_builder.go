package postgres

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/plusiv/huxio/internal/domain/repositories"
)

// QueryBuilder accumulates WHERE predicates and their bind arguments, keeping
// placeholder numbering correct as clauses are added. Column names are always
// literals from this package or an allowlist checked at the transport
// boundary; values are always bound.
type QueryBuilder struct {
	conds []string
	args  []any
}

// NewQueryBuilder returns an empty builder.
func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{conds: make([]string, 0, 8), args: make([]any, 0, 8)}
}

// Arg binds a value and returns its placeholder.
func (b *QueryBuilder) Arg(v any) string {
	b.args = append(b.args, v)
	return "$" + strconv.Itoa(len(b.args))
}

// Where appends a predicate fragment. The fragment must already reference its
// values through placeholders returned by Arg.
func (b *QueryBuilder) Where(fragment string) {
	if fragment != "" {
		b.conds = append(b.conds, fragment)
	}
}

// Wheref appends a predicate built with fmt.Sprintf, binding every value and
// substituting its placeholder in argument order.
func (b *QueryBuilder) Wheref(format string, values ...any) {
	placeholders := make([]any, len(values))
	for i, v := range values {
		placeholders[i] = b.Arg(v)
	}
	b.Where(fmt.Sprintf(format, placeholders...))
}

// Clause renders "WHERE a AND b", or an empty string when no predicate was
// added.
func (b *QueryBuilder) Clause() string {
	if len(b.conds) == 0 {
		return ""
	}
	return "WHERE " + strings.Join(b.conds, " AND ")
}

// Conditions renders "a AND b" without the WHERE keyword, for embedding in a
// larger statement.
func (b *QueryBuilder) Conditions() string {
	if len(b.conds) == 0 {
		return "TRUE"
	}
	return strings.Join(b.conds, " AND ")
}

// Args returns the bound arguments in placeholder order.
func (b *QueryBuilder) Args() []any { return b.args }

// ApplyFilter renders the typed predicates of a single column. Only non-nil,
// non-empty fields contribute. The column must be a pre-validated identifier:
// it is inserted verbatim.
func ApplyFilter[T comparable](b *QueryBuilder, filter *repositories.Filter[T], column string) {
	if filter.IsZero() {
		return
	}
	if filter.Exists != nil {
		if *filter.Exists {
			b.Where(column + " IS NOT NULL")
		} else {
			b.Where(column + " IS NULL")
		}
	}
	if filter.Is != nil {
		b.Wheref(column+" = %s", *filter.Is)
	}
	if filter.IsNot != nil {
		b.Wheref(column+" <> %s", *filter.IsNot)
	}
	if len(filter.In) > 0 {
		b.Wheref(column+" = ANY(%s)", filter.In)
	}
	if len(filter.NotIn) > 0 {
		b.Wheref("NOT ("+column+" = ANY(%s))", filter.NotIn)
	}
	if filter.IContains != nil {
		b.Wheref(column+" ILIKE '%%' || %s || '%%'", *filter.IContains)
	}
	if filter.GT != nil {
		b.Wheref(column+" > %s", *filter.GT)
	}
	if filter.GTE != nil {
		b.Wheref(column+" >= %s", *filter.GTE)
	}
	if filter.LT != nil {
		b.Wheref(column+" < %s", *filter.LT)
	}
	if filter.LTE != nil {
		b.Wheref(column+" <= %s", *filter.LTE)
	}
}

// ApplyBoolStateFilter renders a *Filter[bool] against a nullable state
// timestamp column, where "true" means the timestamp is set. It exists because
// the data model records the beginning of a state (disabled_at) rather than a
// boolean (disabled).
func ApplyBoolStateFilter(b *QueryBuilder, filter *repositories.Filter[bool], column string) {
	if filter.IsZero() || filter.Is == nil {
		return
	}
	if *filter.Is {
		b.Where(column + " IS NOT NULL")
	} else {
		b.Where(column + " IS NULL")
	}
}

// ApplyKeysetCursor renders the keyset predicate for a cursor token and
// returns the effective limit. An unparseable cursor is treated as the first
// page rather than an error.
//
// sortValue converts the decoded cursor's stringified sort value into the type
// the column binds as; pass nil when the column is text.
func ApplyKeysetCursor(
	b *QueryBuilder,
	page repositories.CursorPagination,
	sortCol, idCol string,
	desc bool,
	sortValue func(string) (any, error),
) int {
	limit := page.GetLimit()
	if page.Cursor == "" {
		return limit
	}
	rawValue, cursorID, err := repositories.DecodeCursor(page.Cursor)
	if err != nil || cursorID == "" {
		return limit
	}

	var value any = rawValue
	if sortValue != nil {
		converted, convErr := sortValue(rawValue)
		if convErr != nil {
			return limit
		}
		value = converted
	}

	// DESC: (col < v) OR (col = v AND id < cursorID)
	// ASC:  (col > v) OR (col = v AND id > cursorID)
	op := ">"
	if desc {
		op = "<"
	}
	b.Wheref(
		"(("+sortCol+" "+op+" %s) OR ("+sortCol+" = %s AND "+idCol+" "+op+" %s))",
		value, value, cursorID,
	)
	return limit
}

// TimeCursorValue parses an RFC3339Nano cursor value, for timestamp sort
// columns.
func TimeCursorValue(raw string) (any, error) {
	return time.Parse(time.RFC3339Nano, raw)
}

// OrderClause renders an ORDER BY from allowlisted directives, falling back to
// the supplied default when none are given.
func OrderClause(orders []repositories.OrderBy, allowed map[string]string, fallback string) string {
	parts := make([]string, 0, len(orders))
	for _, o := range orders {
		column, ok := allowed[o.Column]
		if !ok {
			continue
		}
		dir := "ASC"
		if o.Desc {
			dir = "DESC"
		}
		parts = append(parts, column+" "+dir)
	}
	if len(parts) == 0 {
		return "ORDER BY " + fallback
	}
	return "ORDER BY " + strings.Join(parts, ", ")
}

// BuildCursorResult trims the over-fetched row to the page size, reports
// whether more rows exist, and encodes the next cursor from the last item.
func BuildCursorResult[T any](
	items []T,
	limit int,
	id func(T) string,
	sortValue func(T) string,
) repositories.CursorResult[T] {
	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}

	result := repositories.CursorResult[T]{Items: items, HasMore: hasMore, Limit: limit}
	if hasMore && len(items) > 0 {
		last := items[len(items)-1]
		next := repositories.EncodeCursor(sortValue(last), id(last))
		result.NextCursor = &next
	}
	return result
}
