package ids_test

import (
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/infrastructure/ids"
)

func TestNewAndSplit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		prefix ids.Prefix
	}{
		{"organization", ids.PrefixOrganization},
		{"application", ids.PrefixApplication},
		{"endpoint", ids.PrefixEndpoint},
		{"message", ids.PrefixMessage},
		{"attempt", ids.PrefixAttempt},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			id := ids.New(tc.prefix)
			if !ids.HasPrefix(id, tc.prefix) {
				t.Fatalf("expected prefix %q on %q", tc.prefix, id)
			}
			gotPrefix, body, err := ids.Split(id)
			if err != nil {
				t.Fatalf("split %q: %v", id, err)
			}
			if gotPrefix != tc.prefix {
				t.Errorf("prefix = %q, want %q", gotPrefix, tc.prefix)
			}
			if body.IsNil() {
				t.Error("expected non-nil ksuid body")
			}
		})
	}
}

func TestSplitRejectsMalformed(t *testing.T) {
	t.Parallel()

	for _, id := range []string{"", "msg", "_abc", "msg_not-a-ksuid", "nounderscore"} {
		if _, _, err := ids.Split(id); err == nil {
			t.Errorf("expected error for %q", id)
		}
	}
}

func TestTimeOfRoundTrip(t *testing.T) {
	t.Parallel()

	want := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	id, err := ids.NewAt(ids.PrefixMessage, want)
	if err != nil {
		t.Fatalf("NewAt: %v", err)
	}
	got, err := ids.TimeOf(id)
	if err != nil {
		t.Fatalf("TimeOf: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("TimeOf = %s, want %s", got, want)
	}
}

func TestIDsSortByTime(t *testing.T) {
	t.Parallel()

	earlier, err := ids.NewAt(ids.PrefixMessage, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("NewAt: %v", err)
	}
	later, err := ids.NewAt(ids.PrefixMessage, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("NewAt: %v", err)
	}
	if earlier >= later {
		t.Errorf("expected %q < %q lexicographically", earlier, later)
	}
}

func TestHasKindPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		id     string
		prefix ids.Prefix
		want   bool
	}{
		{"generated id", ids.New(ids.PrefixOrganization), ids.PrefixOrganization, true},
		{"foreign body still counts", "org_whatever-they-minted", ids.PrefixOrganization, true},
		{"wrong kind", "app_2Xk", ids.PrefixOrganization, false},
		{"prefix only", "org_", ids.PrefixOrganization, false},
		{"no separator", "org", ids.PrefixOrganization, false},
		{"empty", "", ids.PrefixOrganization, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ids.HasKindPrefix(tc.id, tc.prefix); got != tc.want {
				t.Errorf("HasKindPrefix(%q, %q) = %v, want %v", tc.id, tc.prefix, got, tc.want)
			}
		})
	}

	// HasPrefix stays strict: it also validates the KSUID body.
	if ids.HasPrefix("org_whatever-they-minted", ids.PrefixOrganization) {
		t.Error("HasPrefix must reject a malformed body")
	}
}
