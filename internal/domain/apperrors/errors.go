// Package apperrors defines the typed application errors that cross layer
// boundaries. Adapters map ErrorKind onto transport status codes; internal
// causes are logged server-side and never returned to clients.
package apperrors

import "github.com/rotisserie/eris"

// ErrorKind is what a use case says went wrong. The kind is the only thing
// the domain decides; picking a status code from it is the HTTP adapter's job
// (see internal/adapters/inbound/http/httperr). The status each kind produces
// is noted below so the two stay legible together.
type ErrorKind string

const (
	KindNotFound        ErrorKind = "not_found"         // 404
	KindConflict        ErrorKind = "conflict"          // 409
	KindValidation      ErrorKind = "validation"        // 422, bad input
	KindBusiness        ErrorKind = "business_rule"     // 422, valid input the rules reject
	KindUnauthorized    ErrorKind = "unauthorized"      // 401, no or bad credentials
	KindForbidden       ErrorKind = "forbidden"         // 403, credentials without the scope
	KindRateLimit       ErrorKind = "rate_limit"        // 429
	KindPayloadTooLarge ErrorKind = "payload_too_large" // 413
	KindGone            ErrorKind = "gone"              // 410, existed once

	// KindInternal is the only kind whose Cause must never reach the client:
	// error strings leak table names, hostnames and query fragments.
	KindInternal ErrorKind = "internal" // 500
)

// AppError is a typed application error with semantics for safe responses.
// Internal errors carry a Cause for server-side logging that must never be
// included in API responses.
type AppError struct {
	Kind    ErrorKind
	Code    string
	Message string
	Cause   error
}

// Error implements the error interface. The Cause (if present) is appended for
// internal logging purposes and must not be surfaced in API responses.
func (e *AppError) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}

// Unwrap enables eris.As / eris.Is traversal through the error chain.
func (e *AppError) Unwrap() error { return e.Cause }

// NewNotFoundError returns an AppError indicating a resource does not exist.
func NewNotFoundError(resource string) *AppError {
	return &AppError{Kind: KindNotFound, Code: "not_found", Message: resource + " not found"}
}

// NewConflictError returns an AppError for a resource uniqueness violation.
func NewConflictError(message string) *AppError {
	return &AppError{Kind: KindConflict, Code: "conflict", Message: message}
}

// NewValidationError returns an AppError for invalid or missing input data.
func NewValidationError(message string) *AppError {
	return &AppError{Kind: KindValidation, Code: "validation", Message: message}
}

// NewBusinessRuleError returns an AppError for a domain business rule violation.
func NewBusinessRuleError(message string) *AppError {
	return &AppError{Kind: KindBusiness, Code: "business_rule", Message: message}
}

// NewUnauthorizedError returns an AppError for missing or invalid credentials.
func NewUnauthorizedError(message string) *AppError {
	return &AppError{Kind: KindUnauthorized, Code: "authentication_failed", Message: message}
}

// NewForbiddenError returns an AppError for insufficient scope.
func NewForbiddenError(message string) *AppError {
	return &AppError{Kind: KindForbidden, Code: "forbidden", Message: message}
}

// NewRateLimitError returns an AppError for request-rate violations.
func NewRateLimitError(message string) *AppError {
	return &AppError{Kind: KindRateLimit, Code: "rate_limited", Message: message}
}

// NewPayloadTooLargeError returns an AppError for oversized request bodies.
func NewPayloadTooLargeError(message string) *AppError {
	return &AppError{Kind: KindPayloadTooLarge, Code: "payload_too_large", Message: message}
}

// NewGoneError returns an AppError for resources that are no longer available.
func NewGoneError(message string) *AppError {
	return &AppError{Kind: KindGone, Code: "gone", Message: message}
}

// NewInternalError wraps an unexpected infrastructure error using eris so that
// a full stack trace is captured for server-side logging. The original cause
// must never be sent to API clients.
func NewInternalError(cause error) *AppError {
	return &AppError{
		Kind:    KindInternal,
		Code:    "internal_error",
		Message: "internal server error",
		Cause:   eris.Wrap(cause, "internal server error"),
	}
}

// WithMessage returns a copy of e carrying the supplied client-safe message.
func (e *AppError) WithMessage(message string) *AppError {
	clone := *e
	clone.Message = message
	return &clone
}
