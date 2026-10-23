package utils_test

import (
	"testing"

	"github.com/plusiv/huxio/internal/utils"
)

func TestPtrAndDeref(t *testing.T) {
	t.Parallel()

	if got := utils.Deref(utils.Ptr(42), 0); got != 42 {
		t.Errorf("Deref = %d, want 42", got)
	}
	if got := utils.Deref[string](nil, "fallback"); got != "fallback" {
		t.Errorf("Deref(nil) = %q, want fallback", got)
	}
}

func TestStringHelpers(t *testing.T) {
	t.Parallel()

	if !utils.IsBlank("  \t ") {
		t.Error("whitespace must be blank")
	}
	if utils.NilIfBlank("   ") != nil {
		t.Error("blank input must yield nil")
	}
	if got := utils.Deref(utils.NilIfBlank("  ep_1 "), ""); got != "ep_1" {
		t.Errorf("NilIfBlank = %q, want trimmed ep_1", got)
	}
	if got := utils.Fallback("", "default"); got != "default" {
		t.Errorf("Fallback = %q", got)
	}
}

func TestTruncateKeepsUTF8Boundary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		n    int
		want string
	}{
		{"shorter than limit", "abc", 10, "abc"},
		{"exact", "abcd", 4, "abcd"},
		{"ascii cut", "abcdef", 3, "abc"},
		{"multibyte cut", "héllo", 2, "h"},
		{"zero", "abc", 0, "abc"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := utils.Truncate(tc.in, tc.n); got != tc.want {
				t.Errorf("Truncate(%q, %d) = %q, want %q", tc.in, tc.n, got, tc.want)
			}
		})
	}
}

func TestSumAndPercentile(t *testing.T) {
	t.Parallel()

	if got := utils.Sum([]int{1, 2, 3}); got != 6 {
		t.Errorf("Sum = %d", got)
	}
	sorted := []float64{1, 2, 3, 4}
	if got := utils.Percentile(sorted, 0.5); got != 2.5 {
		t.Errorf("Percentile(0.5) = %v, want 2.5", got)
	}
	if got := utils.Percentile(sorted, 1); got != 4 {
		t.Errorf("Percentile(1) = %v, want 4", got)
	}
	if got := utils.Percentile[float64](nil, 0.9); got != 0 {
		t.Errorf("Percentile(empty) = %v, want 0", got)
	}
}

func TestSliceHelpers(t *testing.T) {
	t.Parallel()

	if got := utils.Map([]int{1, 2}, func(v int) int { return v * 2 }); got[0] != 2 || got[1] != 4 {
		t.Errorf("Map = %v", got)
	}
	if got := utils.Unique([]int{1, 1, 2}); len(got) != 2 {
		t.Errorf("Unique = %v", got)
	}
}
