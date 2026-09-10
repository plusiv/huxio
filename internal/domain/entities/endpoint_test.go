package entities_test

import (
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/utils"
)

func TestEndpointWantsEventType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		filters []string
		event   string
		want    bool
	}{
		{"nil filter accepts everything", nil, "invoice.paid", true},
		{"empty filter accepts everything", []string{}, "invoice.paid", true},
		{"listed type matches", []string{"invoice.paid", "invoice.void"}, "invoice.paid", true},
		{"unlisted type does not match", []string{"invoice.void"}, "invoice.paid", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ep := &entities.Endpoint{EventTypes: tc.filters}
			if got := ep.WantsEventType(tc.event); got != tc.want {
				t.Errorf("WantsEventType(%q) = %v, want %v", tc.event, got, tc.want)
			}
		})
	}
}

func TestEndpointWantsChannels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		endpoint []string
		message  []string
		want     bool
	}{
		{"no endpoint filter accepts unchannelled", nil, nil, true},
		{"no endpoint filter accepts channelled", nil, []string{"ch_acme"}, true},
		{"filter requires a channel", []string{"ch_acme"}, nil, false},
		{"shared channel matches", []string{"ch_acme", "ch_beta"}, []string{"ch_beta"}, true},
		{"disjoint channels do not match", []string{"ch_acme"}, []string{"ch_beta"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ep := &entities.Endpoint{Channels: tc.endpoint}
			if got := ep.WantsChannels(tc.message); got != tc.want {
				t.Errorf("WantsChannels(%v) = %v, want %v", tc.message, got, tc.want)
			}
		})
	}
}

func TestEndpointDeliverable(t *testing.T) {
	t.Parallel()

	now := time.Now()
	tests := []struct {
		name string
		ep   entities.Endpoint
		want bool
	}{
		{"live", entities.Endpoint{}, true},
		{"disabled", entities.Endpoint{DisabledAt: &now}, false},
		{"deleted", entities.Endpoint{DeletedAt: &now}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.ep.Deliverable(); got != tc.want {
				t.Errorf("Deliverable() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestActiveSecretsDropsExpiredRotations(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	ep := &entities.Endpoint{
		Secret: entities.SealedSecret{Sealed: []byte("current")},
		OldSecrets: []entities.SealedSecret{
			{Sealed: []byte("in-window"), ExpiresAt: utils.Ptr(now.Add(time.Hour))},
			{Sealed: []byte("expired"), ExpiresAt: utils.Ptr(now.Add(-time.Hour))},
		},
	}

	got := ep.ActiveSecrets(now)
	if len(got) != 2 {
		t.Fatalf("ActiveSecrets returned %d secrets, want current + in-window", len(got))
	}
	if string(got[0].Sealed) != "current" || string(got[1].Sealed) != "in-window" {
		t.Errorf("ActiveSecrets = %q, %q", got[0].Sealed, got[1].Sealed)
	}
}

func TestSecretTypeValid(t *testing.T) {
	t.Parallel()

	if !entities.SecretTypeHMAC256.Valid() || !entities.SecretTypeEd25519.Valid() {
		t.Error("supported algorithms must validate")
	}
	if entities.SecretType("rsa").Valid() {
		t.Error("unsupported algorithm must not validate")
	}
}
