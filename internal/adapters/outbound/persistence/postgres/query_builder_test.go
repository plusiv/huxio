package postgres_test

import (
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/adapters/outbound/persistence/postgres"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/utils"
)

func TestQueryBuilderPlaceholderNumbering(t *testing.T) {
	t.Parallel()

	b := postgres.NewQueryBuilder()
	b.Wheref("org_id = %s", "org_1")
	b.Wheref("created_at >= %s", time.Unix(0, 0))
	b.Where("deleted_at IS NULL")

	want := "WHERE org_id = $1 AND created_at >= $2 AND deleted_at IS NULL"
	if got := b.Clause(); got != want {
		t.Errorf("Clause() = %q, want %q", got, want)
	}
	if len(b.Args()) != 2 {
		t.Errorf("Args() = %v, want 2 bound values", b.Args())
	}
}

func TestQueryBuilderEmpty(t *testing.T) {
	t.Parallel()

	b := postgres.NewQueryBuilder()
	if got := b.Clause(); got != "" {
		t.Errorf("Clause() = %q, want empty", got)
	}
	if got := b.Conditions(); got != "TRUE" {
		t.Errorf("Conditions() = %q, want TRUE", got)
	}
}

func TestApplyFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filter   *repositories.Filter[string]
		wantSQL  string
		wantArgs int
	}{
		{"nil filter", nil, "", 0},
		{"empty filter", &repositories.Filter[string]{}, "", 0},
		{"is", repositories.Eq("app_1"), "WHERE app_id = $1", 1},
		{"is not", &repositories.Filter[string]{IsNot: utils.Ptr("app_1")}, "WHERE app_id <> $1", 1},
		{"in", repositories.AnyOf("a", "b"), "WHERE app_id = ANY($1)", 1},
		{"not in", &repositories.Filter[string]{NotIn: []string{"a"}}, "WHERE NOT (app_id = ANY($1))", 1},
		{"icontains", &repositories.Filter[string]{IContains: utils.Ptr("acme")}, "WHERE app_id ILIKE '%' || $1 || '%'", 1},
		{"is null", repositories.IsNull[string](true), "WHERE app_id IS NULL", 0},
		{"is not null", repositories.IsNull[string](false), "WHERE app_id IS NOT NULL", 0},
		{
			"range",
			&repositories.Filter[string]{GT: utils.Ptr("a"), GTE: utils.Ptr("b"), LT: utils.Ptr("c"), LTE: utils.Ptr("d")},
			"WHERE app_id > $1 AND app_id >= $2 AND app_id < $3 AND app_id <= $4",
			4,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := postgres.NewQueryBuilder()
			postgres.ApplyFilter(b, tc.filter, "app_id")
			if got := b.Clause(); got != tc.wantSQL {
				t.Errorf("Clause() = %q, want %q", got, tc.wantSQL)
			}
			if got := len(b.Args()); got != tc.wantArgs {
				t.Errorf("bound %d args, want %d", got, tc.wantArgs)
			}
		})
	}
}

func TestApplyBoolStateFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		filter *repositories.Filter[bool]
		want   string
	}{
		{"nil", nil, ""},
		{"disabled", repositories.Eq(true), "WHERE disabled_at IS NOT NULL"},
		{"enabled", repositories.Eq(false), "WHERE disabled_at IS NULL"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := postgres.NewQueryBuilder()
			postgres.ApplyBoolStateFilter(b, tc.filter, "disabled_at")
			if got := b.Clause(); got != tc.want {
				t.Errorf("Clause() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestApplyKeysetCursor(t *testing.T) {
	t.Parallel()

	created := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	valid := repositories.EncodeCursor(created.Format(time.RFC3339Nano), "msg_2")

	tests := []struct {
		name      string
		page      repositories.CursorPagination
		desc      bool
		wantSQL   string
		wantLimit int
	}{
		{"no cursor", repositories.CursorPagination{Limit: 10}, true, "", 10},
		{
			"descending",
			repositories.CursorPagination{Cursor: valid, Limit: 10}, true,
			"WHERE ((created_at < $1) OR (created_at = $2 AND id < $3))", 10,
		},
		{
			"ascending",
			repositories.CursorPagination{Cursor: valid, Limit: 10}, false,
			"WHERE ((created_at > $1) OR (created_at = $2 AND id > $3))", 10,
		},
		{"garbage cursor is first page", repositories.CursorPagination{Cursor: "!!!", Limit: 0}, true, "", repositories.DefaultPageSize},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := postgres.NewQueryBuilder()
			limit := postgres.ApplyKeysetCursor(b, tc.page, "created_at", "id", tc.desc, postgres.TimeCursorValue)
			if got := b.Clause(); got != tc.wantSQL {
				t.Errorf("Clause() = %q, want %q", got, tc.wantSQL)
			}
			if limit != tc.wantLimit {
				t.Errorf("limit = %d, want %d", limit, tc.wantLimit)
			}
		})
	}
}

func TestApplyKeysetCursorRejectsUnparseableSortValue(t *testing.T) {
	t.Parallel()

	b := postgres.NewQueryBuilder()
	page := repositories.CursorPagination{Cursor: repositories.EncodeCursor("not-a-time", "msg_2"), Limit: 5}
	if limit := postgres.ApplyKeysetCursor(b, page, "created_at", "id", true, postgres.TimeCursorValue); limit != 5 {
		t.Errorf("limit = %d, want 5", limit)
	}
	if got := b.Clause(); got != "" {
		t.Errorf("Clause() = %q, want no predicate for an unparseable sort value", got)
	}
}

func TestOrderClause(t *testing.T) {
	t.Parallel()

	allowed := map[string]string{"createdAt": "m.created_at", "id": "m.id"}

	tests := []struct {
		name   string
		orders []repositories.OrderBy
		want   string
	}{
		{"default", nil, "ORDER BY m.created_at DESC, m.id DESC"},
		{"allowlisted asc", []repositories.OrderBy{{Column: "createdAt"}}, "ORDER BY m.created_at ASC"},
		{"allowlisted desc", []repositories.OrderBy{{Column: "id", Desc: true}}, "ORDER BY m.id DESC"},
		{"unknown column ignored", []repositories.OrderBy{{Column: "; DROP TABLE message"}}, "ORDER BY m.created_at DESC, m.id DESC"},
		{"multiple", []repositories.OrderBy{{Column: "createdAt", Desc: true}, {Column: "id", Desc: true}}, "ORDER BY m.created_at DESC, m.id DESC"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := postgres.OrderClause(tc.orders, allowed, "m.created_at DESC, m.id DESC"); got != tc.want {
				t.Errorf("OrderClause() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBuildCursorResult(t *testing.T) {
	t.Parallel()

	type row struct {
		id   string
		sort string
	}
	id := func(r row) string { return r.id }
	sortVal := func(r row) string { return r.sort }

	t.Run("no more rows", func(t *testing.T) {
		t.Parallel()
		got := postgres.BuildCursorResult([]row{{"a", "1"}, {"b", "2"}}, 5, id, sortVal)
		if got.HasMore || got.NextCursor != nil || len(got.Items) != 2 {
			t.Errorf("result = %+v", got)
		}
	})

	t.Run("trims and encodes next cursor", func(t *testing.T) {
		t.Parallel()
		got := postgres.BuildCursorResult([]row{{"a", "1"}, {"b", "2"}, {"c", "3"}}, 2, id, sortVal)
		if !got.HasMore || len(got.Items) != 2 || got.NextCursor == nil {
			t.Fatalf("result = %+v", got)
		}
		value, cursorID, err := repositories.DecodeCursor(*got.NextCursor)
		if err != nil {
			t.Fatalf("DecodeCursor: %v", err)
		}
		if value != "2" || cursorID != "b" {
			t.Errorf("cursor points at (%q, %q), want the last returned row", value, cursorID)
		}
	})
}
