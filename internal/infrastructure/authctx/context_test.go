package authctx_test

import (
	"context"
	"testing"

	"github.com/plusiv/huxio/internal/infrastructure/authctx"
)

func TestContextRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := authctx.Context(context.Background(), authctx.Principal{OrgID: "org_1"})
	principal, ok := authctx.FromContext(ctx)
	if !ok || principal.OrgID != "org_1" {
		t.Fatalf("FromContext = %+v, %v", principal, ok)
	}
	if got := authctx.OrgFrom(ctx); got != "org_1" {
		t.Errorf("OrgFrom = %q", got)
	}

	if _, ok := authctx.FromContext(context.Background()); ok {
		t.Error("an unauthenticated context must report no principal")
	}
	if got := authctx.OrgFrom(context.Background()); got != "" {
		t.Errorf("OrgFrom on an unauthenticated context = %q", got)
	}
}

func TestScopedToApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		principal authctx.Principal
		appID     string
		want      bool
	}{
		{"org token reaches any app", authctx.Principal{OrgID: "org_1"}, "app_1", true},
		{"portal token reaches its own app", authctx.Principal{OrgID: "org_1", AppID: "app_1", Portal: true}, "app_1", true},
		{"portal token cannot reach another app", authctx.Principal{OrgID: "org_1", AppID: "app_1", Portal: true}, "app_2", false},
		{"portal token without an app reaches nothing", authctx.Principal{OrgID: "org_1", Portal: true}, "app_1", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.principal.ScopedToApp(tc.appID); got != tc.want {
				t.Errorf("ScopedToApp(%q) = %v, want %v", tc.appID, got, tc.want)
			}
		})
	}
}
