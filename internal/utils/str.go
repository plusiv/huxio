package utils

import "strings"

// IsBlank reports whether s is empty or whitespace-only.
func IsBlank(s string) bool { return strings.TrimSpace(s) == "" }

// NilIfBlank returns nil for blank input and a pointer to the trimmed value
// otherwise. Use it when converting optional query params to *string filters.
func NilIfBlank(s string) *string {
	if IsBlank(s) {
		return nil
	}
	t := strings.TrimSpace(s)
	return &t
}

// Fallback returns s when non-blank, otherwise defaultValue.
func Fallback(s, defaultValue string) string {
	if IsBlank(s) {
		return defaultValue
	}
	return s
}

// Normalize trims and lowercases s.
func Normalize(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

// Truncate returns at most n bytes of s, cutting on a UTF-8 boundary.
func Truncate(s string, n int) string {
	if n <= 0 || len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !isUTF8Start(s[cut]) {
		cut--
	}
	return s[:cut]
}

func isUTF8Start(b byte) bool { return b&0xC0 != 0x80 }
