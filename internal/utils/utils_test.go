package utils_test

import (
	"errors"
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
	if got := utils.Normalize("  Invoice.Paid "); got != "invoice.paid" {
		t.Errorf("Normalize = %q", got)
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

func TestClampAndPercentile(t *testing.T) {
	t.Parallel()

	if got := utils.Clamp(120, 1, 64); got != 64 {
		t.Errorf("Clamp = %d, want 64", got)
	}
	if got := utils.Clamp(0, 1, 64); got != 1 {
		t.Errorf("Clamp = %d, want 1", got)
	}
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
	if got := utils.Filter([]int{1, 2, 3}, func(v int) bool { return v%2 == 1 }); len(got) != 2 {
		t.Errorf("Filter = %v", got)
	}
	if !utils.Contains([]string{"a", "b"}, "b") {
		t.Error("Contains must find b")
	}
	if !utils.ContainsAny([]string{"ch_a"}, []string{"ch_b", "ch_a"}) {
		t.Error("ContainsAny must detect the shared channel")
	}
	if utils.ContainsAny([]string{"ch_a"}, nil) {
		t.Error("ContainsAny with empty input must be false")
	}
	if got := utils.Unique([]int{1, 1, 2}); len(got) != 2 {
		t.Errorf("Unique = %v", got)
	}
	if got := utils.Chunk([]int{1, 2, 3}, 2); len(got) != 2 || len(got[1]) != 1 {
		t.Errorf("Chunk = %v", got)
	}
	if utils.Chunk([]int{1}, 0) != nil {
		t.Error("Chunk with non-positive size must be nil")
	}
	if got := len(utils.Keys(map[string]int{"a": 1, "b": 2})); got != 2 {
		t.Errorf("Keys length = %d", got)
	}
	if got := utils.Sum(utils.Values(map[string]int{"a": 1, "b": 2})); got != 3 {
		t.Errorf("Values sum = %d", got)
	}
}

func TestBatchHelpers(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")
	calls := 0
	err := utils.ForEach([]int{1, 2, 3}, func(v int) error {
		calls++
		if v == 2 {
			return boom
		}
		return nil
	})
	if !errors.Is(err, boom) || calls != 2 {
		t.Errorf("ForEach stopped after %d calls, err = %v", calls, err)
	}

	batches := 0
	if err := utils.RunInBatches([]int{1, 2, 3, 4, 5}, 2, func(b []int) error {
		batches++
		return nil
	}); err != nil {
		t.Fatalf("RunInBatches: %v", err)
	}
	if batches != 3 {
		t.Errorf("batches = %d, want 3", batches)
	}

	out, err := utils.Collect([]int{1, 2}, func(v int) (string, error) {
		if v == 0 {
			return "", boom
		}
		return "ok", nil
	})
	if err != nil || len(out) != 2 {
		t.Errorf("Collect = %v, %v", out, err)
	}
}
