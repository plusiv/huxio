// Package utils holds generic, reusable helpers that depend on no domain type.
package utils

// Ptr returns a pointer to v. It replaces every one-off xxxPtr helper.
func Ptr[T any](v T) *T { return &v }

// Deref returns *v when v is non-nil, otherwise def.
func Deref[T any](v *T, def T) T {
	if v == nil {
		return def
	}
	return *v
}
