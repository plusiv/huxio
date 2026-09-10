package repositories_test

import (
	"testing"

	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/utils"
)

func TestCursorRoundTrip(t *testing.T) {
	t.Parallel()

	token := repositories.EncodeCursor("2026-09-08T00:00:00Z", "msg_2Xk")
	value, id, err := repositories.DecodeCursor(token)
	if err != nil {
		t.Fatalf("DecodeCursor: %v", err)
	}
	if value != "2026-09-08T00:00:00Z" || id != "msg_2Xk" {
		t.Errorf("decoded (%q, %q)", value, id)
	}
}

func TestDecodeCursorRejectsGarbage(t *testing.T) {
	t.Parallel()

	for _, token := range []string{"", "!!!!", "YWJj"} {
		if _, _, err := repositories.DecodeCursor(token); err == nil {
			t.Errorf("expected error for %q", token)
		}
	}
}

func TestGetLimitBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   int
		want int
	}{
		{"unset", 0, repositories.DefaultPageSize},
		{"negative", -5, repositories.DefaultPageSize},
		{"in range", 50, 50},
		{"above cap", 5000, repositories.MaxPageSize},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := (repositories.CursorPagination{Limit: tc.in}).GetLimit(); got != tc.want {
				t.Errorf("GetLimit(%d) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestFilterIsZeroAndShorthands(t *testing.T) {
	t.Parallel()

	var nilFilter *repositories.Filter[string]
	if !nilFilter.IsZero() {
		t.Error("nil filter must be zero")
	}
	if !(&repositories.Filter[string]{}).IsZero() {
		t.Error("empty filter must be zero")
	}
	if repositories.Eq("app_1").IsZero() {
		t.Error("Eq must contribute a predicate")
	}
	if repositories.AnyOf("a", "b").IsZero() {
		t.Error("AnyOf must contribute a predicate")
	}
	if got := repositories.IsNull[string](true); utils.Deref(got.Exists, true) {
		t.Error("IsNull(true) must assert the column IS NULL")
	}
	if got := repositories.IsNull[string](false); !utils.Deref(got.Exists, false) {
		t.Error("IsNull(false) must assert the column IS NOT NULL")
	}
}
