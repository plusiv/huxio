package utils

// ForEach calls fn for every item, stopping at the first error.
func ForEach[T any](items []T, fn func(T) error) error {
	for _, item := range items {
		if err := fn(item); err != nil {
			return err
		}
	}
	return nil
}

// RunInBatches chunks items and calls fn once per chunk, stopping at the first
// error.
func RunInBatches[T any](items []T, size int, fn func([]T) error) error {
	return ForEach(Chunk(items, size), fn)
}

// Collect maps items to results, stopping at the first error.
func Collect[T, U any](items []T, fn func(T) (U, error)) ([]U, error) {
	out := make([]U, 0, len(items))
	for _, item := range items {
		v, err := fn(item)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}
