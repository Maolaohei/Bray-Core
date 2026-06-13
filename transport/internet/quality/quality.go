package quality

// Quality is a multi-dimensional quality assessment of a network path.
// Each sub-score ranges 0-100 (100 = best).
// This avoids the single-score ambiguity where different failure modes
// produce the same Overall number.
type Quality struct {
	Overall    uint8
	Latency    uint8
	Loss       uint8
	Stability  uint8
}

// UnknownQuality returns a Quality with all zeros and no valid data.
func UnknownQuality() Quality {
	return Quality{}
}

// QualityWeights defines how sub-scores are combined into an Overall score.
// Different modules use different weights:
//   - XMUX:   Latency=0.3, Loss=0.4, Stability=0.3
//   - HEv3:   Latency=0.7, Loss=0.3
//   - Warmup: Latency=0.5, Loss=0.5
type QualityWeights struct {
	LatencyWeight   float64
	LossWeight      float64
	StabilityWeight float64
}

// DefaultXMUXWeights returns the default quality weights for XMUX scheduling.
func DefaultXMUXWeights() QualityWeights {
	return QualityWeights{
		LatencyWeight:   0.3,
		LossWeight:      0.4,
		StabilityWeight: 0.3,
	}
}

// DefaultHEv3Weights returns the default quality weights for Happy Eyeballs v3.
func DefaultHEv3Weights() QualityWeights {
	return QualityWeights{
		LatencyWeight:   0.7,
		LossWeight:      0.3,
		StabilityWeight: 0.0,
	}
}

// DefaultWarmupWeights returns the default quality weights for Warmup.
func DefaultWarmupWeights() QualityWeights {
	return QualityWeights{
		LatencyWeight:   0.5,
		LossWeight:      0.5,
		StabilityWeight: 0.0,
	}
}

// ComputeOverall calculates the Overall score from sub-scores using the given weights.
// Weights are normalized internally, so they don't need to sum to 1.0.
func (w QualityWeights) ComputeOverall(q Quality) uint8 {
	totalWeight := w.LatencyWeight + w.LossWeight + w.StabilityWeight
	if totalWeight == 0 {
		return q.Overall
	}
	score := float64(q.Latency)*w.LatencyWeight +
		float64(q.Loss)*w.LossWeight +
		float64(q.Stability)*w.StabilityWeight
	score = score / totalWeight
	if score > 100 {
		score = 100
	}
	return uint8(score + 0.5)
}
