// Package authctx carries the authenticated principal on the request context,
// so no layer below the middleware has to know how authentication happened.
package authctx

import "context"

// Principal is the authenticated caller. Two shapes: an org token, which may
// act on the whole tenant, and a portal token, which is confined to the one
// application in AppID.
type Principal struct {
	OrgID string
	// AppID is set on portal tokens only, and is the single application such
	// a token may touch.
	AppID  string
	Portal bool
}

// ScopedToApp reports whether the principal may act on the supplied
// application. An org token may act on any application it owns; a portal token
// only on its own.
func (p Principal) ScopedToApp(appID string) bool {
	if !p.Portal {
		return true
	}
	return p.AppID != "" && p.AppID == appID
}

type ctxKey struct{}

// Context returns a copy of ctx carrying the principal.
func Context(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, principal)
}

// FromContext returns the principal stored in ctx, and whether one was set.
func FromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(ctxKey{}).(Principal)
	return principal, ok
}

// OrgFrom returns the organization the request is scoped to, or an empty
// string when the request is unauthenticated.
func OrgFrom(ctx context.Context) string {
	principal, _ := FromContext(ctx)
	return principal.OrgID
}
