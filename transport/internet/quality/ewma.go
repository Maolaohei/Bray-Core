package quality

// EWMA is an exponentially weighted moving average for failure rate tracking.
// Replaces dual-counter (shortFails/longFails) approach.
//
// Usage:
//
//   rate := NewEWMA(0.05)   // initial rate 5%
//   rate.OnSuccess()         // rate *= 0.95
//   rate.OnFailure()         // rate = rate*0.95 + 0.05
//   current := rate.Value()  // read current rate
//
// Decay factor 0.95 means ~14 successes to halve the failure rate.
// No cleanup goroutine needed — natural time decay via EWMA.
type EWMA struct {
	rate       float64
	decay      float64
	failWeight float64 // 1 - decay
}

// NewEWMA creates an EWMA with the given initial rate and decay factor.
// decay=0.95 means each sample retains 95% of previous value.
func NewEWMA(initialRate float64) EWMA {
	d := 0.95
	return EWMA{
		rate:       initialRate,
		decay:      d,
		failWeight: 1 - d,
	}
}

// OnSuccess updates the rate after a successful observation.
// rate *= decay (decays toward 0).
func (e *EWMA) OnSuccess() {
	e.rate *= e.decay
}

// OnFailure updates the rate after a failed observation.
// rate = rate*decay + failWeight (decays toward 1).
func (e *EWMA) OnFailure() {
	e.rate = e.rate*e.decay + e.failWeight
}

// Value returns the current failure rate (0.0 to 1.0).
func (e *EWMA) Value() float64 {
	return e.rate
}
