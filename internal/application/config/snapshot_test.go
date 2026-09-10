package config_test

import (
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/application/config"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/secrets"
	"github.com/plusiv/huxio/internal/utils"
)

func newSealer(t *testing.T) *secrets.Sealer {
	t.Helper()
	key, err := secrets.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	sealer, err := secrets.NewSealer([]string{key})
	if err != nil {
		t.Fatalf("NewSealer: %v", err)
	}
	return sealer
}

func seal(t *testing.T, sealer *secrets.Sealer, plaintext string) entities.SealedSecret {
	t.Helper()
	sealed, err := sealer.Seal([]byte(plaintext))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return entities.SealedSecret{Sealed: sealed}
}

func TestBuildSnapshotDecryptsSecretsOnce(t *testing.T) {
	t.Parallel()

	sealer := newSealer(t)
	expires := time.Now().UTC().Add(time.Hour)
	data := &repositories.SnapshotData{
		Organizations: []*entities.Organization{{ID: "org_1", Name: "Acme"}},
		Applications:  []*entities.Application{{ID: "app_1", OrgID: "org_1", Name: "Customer"}},
		Endpoints: []*entities.Endpoint{{
			ID:         "ep_1",
			AppID:      "app_1",
			OrgID:      "org_1",
			URL:        "https://example.test/hook",
			Secret:     seal(t, sealer, "whsec_current"),
			SecretType: entities.SecretTypeHMAC256,
			OldSecrets: []entities.SealedSecret{
				{Sealed: seal(t, sealer, "whsec_previous").Sealed, ExpiresAt: utils.Ptr(expires)},
				{Sealed: seal(t, sealer, "whsec_expired").Sealed, ExpiresAt: utils.Ptr(time.Now().UTC().Add(-time.Hour))},
			},
			Headers: []byte(`{"X-Tenant":"acme"}`),
		}},
		EventTypes: []*entities.EventType{{OrgID: "org_1", Name: "invoice.paid"}},
	}

	snapshot, problems := config.BuildSnapshot(data, sealer)
	if len(problems) != 0 {
		t.Fatalf("BuildSnapshot reported problems: %v", problems)
	}

	ep := snapshot.Endpoint("ep_1")
	if ep == nil {
		t.Fatal("endpoint missing from snapshot")
	}
	if len(ep.Secrets) != 2 {
		t.Fatalf("Secrets = %d, want the current key plus the in-window rotation", len(ep.Secrets))
	}
	if string(ep.Secrets[0].Key) != "whsec_current" || string(ep.Secrets[1].Key) != "whsec_previous" {
		t.Errorf("decrypted keys = %q, %q", ep.Secrets[0].Key, ep.Secrets[1].Key)
	}
	if ep.Secrets[0].Type != entities.SecretTypeHMAC256 {
		t.Errorf("secret type = %q", ep.Secrets[0].Type)
	}
	if ep.CustomHeaders["X-Tenant"] != "acme" {
		t.Errorf("CustomHeaders = %v", ep.CustomHeaders)
	}

	if snapshot.Org("org_1") == nil || snapshot.EventType("org_1", "invoice.paid") == nil {
		t.Error("organization and event type must be indexed")
	}
	if snapshot.EventType("org_2", "invoice.paid") != nil {
		t.Error("event types must be scoped per tenant")
	}
	if snapshot.Age() > time.Minute {
		t.Errorf("Age = %s, want roughly zero", snapshot.Age())
	}
}

func TestBuildSnapshotSkipsUnreadableEndpoint(t *testing.T) {
	t.Parallel()

	sealer := newSealer(t)
	data := &repositories.SnapshotData{
		Applications: []*entities.Application{{ID: "app_1", OrgID: "org_1"}},
		Endpoints: []*entities.Endpoint{
			{ID: "ep_ok", AppID: "app_1", OrgID: "org_1", Secret: seal(t, sealer, "good"), SecretType: entities.SecretTypeHMAC256},
			{ID: "ep_bad_secret", AppID: "app_1", OrgID: "org_1", Secret: entities.SealedSecret{Sealed: []byte("not-sealed-by-us")}},
			{ID: "ep_bad_headers", AppID: "app_1", OrgID: "org_1", Secret: seal(t, sealer, "good"), Headers: []byte(`not json`)},
		},
	}

	snapshot, problems := config.BuildSnapshot(data, sealer)
	if len(problems) != 2 {
		t.Fatalf("problems = %d, want one per broken endpoint", len(problems))
	}
	if snapshot.Endpoint("ep_ok") == nil {
		t.Error("a healthy endpoint must survive its neighbour being broken")
	}
	if snapshot.Endpoint("ep_bad_secret") != nil || snapshot.Endpoint("ep_bad_headers") != nil {
		t.Error("broken endpoints must be left out of the snapshot")
	}
}

func TestMatchingEndpoints(t *testing.T) {
	t.Parallel()

	sealer := newSealer(t)
	disabled := time.Now().UTC()
	endpoint := func(id string, mutate func(*entities.Endpoint)) *entities.Endpoint {
		ep := &entities.Endpoint{
			ID: id, AppID: "app_1", OrgID: "org_1", URL: "https://example.test/" + id,
			Secret: seal(t, sealer, "secret"), SecretType: entities.SecretTypeHMAC256,
		}
		if mutate != nil {
			mutate(ep)
		}
		return ep
	}

	data := &repositories.SnapshotData{
		Applications: []*entities.Application{{ID: "app_1", OrgID: "org_1", UID: utils.Ptr("cust_1")}},
		Endpoints: []*entities.Endpoint{
			endpoint("ep_all", nil),
			endpoint("ep_paid", func(e *entities.Endpoint) { e.EventTypes = []string{"invoice.paid"} }),
			endpoint("ep_void", func(e *entities.Endpoint) { e.EventTypes = []string{"invoice.void"} }),
			endpoint("ep_channel", func(e *entities.Endpoint) {
				e.EventTypes = []string{"invoice.paid"}
				e.Channels = []string{"ch_acme"}
			}),
			endpoint("ep_disabled", func(e *entities.Endpoint) { e.DisabledAt = &disabled }),
		},
	}

	snapshot, problems := config.BuildSnapshot(data, sealer)
	if len(problems) != 0 {
		t.Fatalf("BuildSnapshot: %v", problems)
	}

	tests := []struct {
		name      string
		eventType string
		channels  []string
		want      []string
	}{
		{"unchannelled paid", "invoice.paid", nil, []string{"ep_paid", "ep_all"}},
		{"channelled paid", "invoice.paid", []string{"ch_acme"}, []string{"ep_paid", "ep_channel", "ep_all"}},
		{"other channel", "invoice.paid", []string{"ch_other"}, []string{"ep_paid", "ep_all"}},
		{"void", "invoice.void", nil, []string{"ep_void", "ep_all"}},
		{"unknown event type still reaches catch-all", "customer.created", nil, []string{"ep_all"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			matched := snapshot.MatchingEndpoints("app_1", tc.eventType, tc.channels)
			got := make(map[string]bool, len(matched))
			for _, ep := range matched {
				got[ep.ID] = true
			}
			if len(got) != len(tc.want) {
				t.Fatalf("matched %v, want %v", keysOf(got), tc.want)
			}
			for _, id := range tc.want {
				if !got[id] {
					t.Errorf("expected %s in %v", id, keysOf(got))
				}
			}
			if got["ep_disabled"] {
				t.Error("a disabled endpoint must never be matched")
			}
		})
	}

	if snapshot.MatchingEndpoints("app_missing", "invoice.paid", nil) != nil {
		t.Error("an unknown application must match nothing")
	}
}

func TestResolveAppAcceptsIDAndUID(t *testing.T) {
	t.Parallel()

	sealer := newSealer(t)
	data := &repositories.SnapshotData{
		Applications: []*entities.Application{
			{ID: "app_1", OrgID: "org_1", UID: utils.Ptr("cust_1")},
			{ID: "app_2", OrgID: "org_2", UID: utils.Ptr("cust_1")},
		},
	}
	snapshot, _ := config.BuildSnapshot(data, sealer)

	if got := snapshot.ResolveApp("org_1", "app_1"); got == nil || got.ID != "app_1" {
		t.Errorf("ResolveApp by id = %v", got)
	}
	if got := snapshot.ResolveApp("org_1", "cust_1"); got == nil || got.ID != "app_1" {
		t.Errorf("ResolveApp by uid = %v", got)
	}
	// The same uid in another tenant must not be reachable.
	if got := snapshot.ResolveApp("org_1", "app_2"); got != nil {
		t.Errorf("ResolveApp crossed tenants: %v", got.ID)
	}
	if got := snapshot.ResolveApp("org_3", "cust_1"); got != nil {
		t.Errorf("ResolveApp returned %v for an unrelated tenant", got.ID)
	}
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
