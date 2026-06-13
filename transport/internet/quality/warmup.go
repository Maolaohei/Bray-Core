package quality

import "time"

// CalcWarmupDelay computes the adaptive warmup delay based on link quality.
//
// Formula:
//
//	delay = baseRTT * 3 * (1 + lossRate * 5)
//
// Examples:
//
//	RTT 20ms, Loss 0%   → 60ms   (baseline)
//	RTT 20ms, Loss 5%   → 90ms   (3× loss factor)
//	RTT 20ms, Loss 10%  → 120ms
//	RTT 90ms, Loss 0%   → 270ms  (high-latency path)
//	RTT 15ms, Loss 0%   → 45ms   (LotSpeed-like)
//
// Minimum delay: 20ms (prevents degenerate cases)
// Maximum delay: 2000ms (prevents excessive waiting)
func CalcWarmupDelay(rtt time.Duration, lossRate float64) time.Duration {
	if rtt <= 0 {
		rtt = 100 * time.Millisecond // default for unknown RTT
	}

	base := float64(rtt) * 3.0
	lossFactor := 1.0 + lossRate*5.0
	delay := time.Duration(base * lossFactor)

	// Clamp
	if delay < 20*time.Millisecond {
		delay = 20 * time.Millisecond
	}
	if delay > 2*time.Second {
		delay = 2 * time.Second
	}

	return delay
}
