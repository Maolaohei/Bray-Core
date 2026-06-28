package quality

import "time"

// Metric represents a single observed metric value with validity tracking.
// Zero value is NOT valid — use NewMetric to create a valid metric.
// This eliminates the RTT=0 ambiguity (unknown vs perfect).
type Metric[T any] struct {
	Value T
	Valid bool
}

// NewMetric creates a valid Metric with the given value.
func NewMetric[T any](v T) Metric[T] {
	return Metric[T]{Value: v, Valid: true}
}

// Unknown returns an invalid Metric.
func Unknown[T any]() Metric[T] {
	return Metric[T]{}
}

// Or returns the value if valid, otherwise returns the fallback.
func (m Metric[T]) Or(fallback T) T {
	if m.Valid {
		return m.Value
	}
	return fallback
}

// Duration is a convenience alias for Metric[time.Duration].
type Duration = Metric[time.Duration]

// Int64 is a convenience alias for Metric[int64].
type Int64 = Metric[int64]

// Float64 is a convenience alias for Metric[float64].
type Float64 = Metric[float64]

// Uint32 is a convenience alias for Metric[uint32].
type Uint32 = Metric[uint32]
