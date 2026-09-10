package utils

// Map transforms each element of s with f.
func Map[T, U any](s []T, f func(T) U) []U {
	out := make([]U, len(s))
	for i, v := range s {
		out[i] = f(v)
	}
	return out
}

// Filter keeps the elements of s matching p.
func Filter[T any](s []T, p func(T) bool) []T {
	out := make([]T, 0, len(s))
	for _, v := range s {
		if p(v) {
			out = append(out, v)
		}
	}
	return out
}

// Contains reports whether v is present in s.
func Contains[T comparable](s []T, v T) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// ContainsAny reports whether s and other share at least one element.
func ContainsAny[T comparable](s, other []T) bool {
	if len(s) == 0 || len(other) == 0 {
		return false
	}
	set := make(map[T]struct{}, len(s))
	for _, v := range s {
		set[v] = struct{}{}
	}
	for _, v := range other {
		if _, ok := set[v]; ok {
			return true
		}
	}
	return false
}

// Unique removes duplicates from s, preserving first-seen order.
func Unique[T comparable](s []T) []T {
	seen := make(map[T]struct{}, len(s))
	out := make([]T, 0, len(s))
	for _, v := range s {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

// Chunk splits s into consecutive slices of at most size elements.
func Chunk[T any](s []T, size int) [][]T {
	if size <= 0 {
		return nil
	}
	out := make([][]T, 0, (len(s)+size-1)/size)
	for i := 0; i < len(s); i += size {
		end := min(i+size, len(s))
		out = append(out, s[i:end])
	}
	return out
}

// Keys returns the keys of m in unspecified order.
func Keys[K comparable, V any](m map[K]V) []K {
	out := make([]K, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// Values returns the values of m in unspecified order.
func Values[K comparable, V any](m map[K]V) []V {
	out := make([]V, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}
