package apperrors_test

import (
	"testing"

	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/rotisserie/eris"
)

func TestErrorKinds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  *apperrors.AppError
		kind apperrors.ErrorKind
	}{
		{"not found", apperrors.NewNotFoundError("endpoint"), apperrors.KindNotFound},
		{"conflict", apperrors.NewConflictError("uid taken"), apperrors.KindConflict},
		{"validation", apperrors.NewValidationError("payload must be JSON"), apperrors.KindValidation},
		{"business", apperrors.NewBusinessRuleError("endpoint disabled"), apperrors.KindBusiness},
		{"unauthorized", apperrors.NewUnauthorizedError("bad token"), apperrors.KindUnauthorized},
		{"forbidden", apperrors.NewForbiddenError("wrong app"), apperrors.KindForbidden},
		{"rate limit", apperrors.NewRateLimitError("slow down"), apperrors.KindRateLimit},
		{"payload too large", apperrors.NewPayloadTooLargeError("1 MiB max"), apperrors.KindPayloadTooLarge},
		{"gone", apperrors.NewGoneError("expired"), apperrors.KindGone},
		{"internal", apperrors.NewInternalError(eris.New("pg down")), apperrors.KindInternal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.err.Kind != tc.kind {
				t.Errorf("Kind = %q, want %q", tc.err.Kind, tc.kind)
			}
			if tc.err.Code == "" {
				t.Error("every AppError needs a stable client-facing code")
			}
		})
	}
}

func TestInternalErrorHidesCause(t *testing.T) {
	t.Parallel()

	cause := eris.New("connection refused to 10.0.0.1")
	err := apperrors.NewInternalError(cause)

	if err.Message != "internal server error" {
		t.Errorf("Message = %q, want the safe generic message", err.Message)
	}
	if err.Error() == err.Message {
		t.Error("Error() must include the cause for server-side logging")
	}
	if !eris.Is(err, cause) {
		t.Error("eris.Is must reach the original cause through Unwrap")
	}
}

func TestWithMessageDoesNotMutate(t *testing.T) {
	t.Parallel()

	original := apperrors.NewConflictError("original")
	clone := original.WithMessage("replaced")

	if original.Message != "original" {
		t.Errorf("original mutated to %q", original.Message)
	}
	if clone.Message != "replaced" || clone.Kind != original.Kind {
		t.Errorf("clone = %+v", clone)
	}
}

func TestErrorsAs(t *testing.T) {
	t.Parallel()

	wrapped := eris.Wrap(apperrors.NewNotFoundError("message"), "load message")

	var appErr *apperrors.AppError
	if !eris.As(wrapped, &appErr) {
		t.Fatal("eris.As must extract the AppError through the wrap")
	}
	if appErr.Kind != apperrors.KindNotFound {
		t.Errorf("Kind = %q", appErr.Kind)
	}
}
