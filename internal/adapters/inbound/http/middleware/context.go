// Package middleware holds the Echo middleware: request identity, logging,
// metrics, authentication and idempotency. Authorization decisions are made
// here, never in a use case.
package middleware

import "context"

// requestIDKey carries the request id on the context so any layer can log it
// without reaching for a framework type.
type requestIDKey struct{}

// WithRequestID returns a copy of ctx carrying the request id.
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, requestID)
}

// RequestIDFrom returns the request id stored in ctx, if any.
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}
