package utils

// Number constrains to every built-in numeric type.
type Number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

// Clamp constrains v to the inclusive range [lo, hi].
func Clamp[T Number](v, lo, hi T) T {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// Sum adds every element of s.
func Sum[T Number](s []T) T {
	var total T
	for _, v := range s {
		total += v
	}
	return total
}

// Percentile returns the p-th percentile (p in [0,1]) of an already-sorted
// slice using linear interpolation. It returns the zero value for empty input.
func Percentile[T Number](sorted []T, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return float64(sorted[0])
	}
	if p >= 1 {
		return float64(sorted[len(sorted)-1])
	}
	pos := p * float64(len(sorted)-1)
	lo := int(pos)
	hi := lo + 1
	if hi >= len(sorted) {
		return float64(sorted[lo])
	}
	frac := pos - float64(lo)
	return float64(sorted[lo])*(1-frac) + float64(sorted[hi])*frac
}
