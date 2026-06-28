package quality

// Behavior represents the inferred link behavior type.
// Derived from observed metrics, NOT from CC algorithm names.
//
// Classification is data-driven:
//   - LotSpeed → detected as LOW_LATENCY (RTT low, jitter low, retrans low)
//   - TCP Brutal → detected as AGGRESSIVE (RTT low, throughput high, bursty)
//   - BBR healthy → NORMAL
//   - BBR degraded → LOSSY or SATURATED
//
// This avoids the pitfall of checking "if cc == bbr" which becomes
// less valuable as more CC algorithms emerge.
type Behavior uint8

const (
	// BehaviorUnknown means insufficient data to classify.
	BehaviorUnknown Behavior = iota

	// BehaviorLowLatency: RTT <30ms, jitter <5%, retrans near zero.
	// Typical of LotSpeed, local/LAN, or very good links.
	// XMUX action: allow higher concurrency.
	BehaviorLowLatency

	// BehaviorNormal: typical well-behaved connection.
	// BBR/Cubic in healthy state.
	// XMUX action: default settings.
	BehaviorNormal

	// BehaviorLossy: loss >1% or retrans >10/min.
	// Network congestion or packet corruption.
	// XMUX action: reduce concurrency, increase retry delay.
	BehaviorLossy

	// BehaviorSaturated: RTT >200ms with high unacked.
	// Bufferbloat or long-distance with congestion.
	// XMUX action: reduce concurrency, prefer lower-RTT paths.
	BehaviorSaturated

	// BehaviorAggressive: RTT very low, throughput very high, bursty.
	// Typical of TCP Brutal or similar brute-force senders.
	// XMUX action: reduce pre-connect count, avoid over-stacking.
	BehaviorAggressive
)

func (b Behavior) String() string {
	switch b {
	case BehaviorLowLatency:
		return "low_latency"
	case BehaviorNormal:
		return "normal"
	case BehaviorLossy:
		return "lossy"
	case BehaviorSaturated:
		return "saturated"
	case BehaviorAggressive:
		return "aggressive"
	default:
		return "unknown"
	}
}

// ClassifyBehavior infers link behavior from a snapshot's observed metrics.
// Returns BehaviorUnknown if confidence is too low or data is insufficient.
//
// Thresholds are based on common network profiles:
//   - LotSpeed: RTT ~5-15ms, jitter <2%, zero retrans
//   - TCP Brutal: RTT ~5-10ms, very high throughput, bursty
//   - Healthy BBR: RTT 20-100ms, loss <0.5%, low retrans
//   - Degraded: loss >1%, retrans rising, RTT spiking
func ClassifyBehavior(snap *Snapshot) Behavior {
	if snap == nil || snap.Confidence < 10 {
		return BehaviorUnknown
	}

	hasRTT := snap.RTT.Valid
	hasLoss := snap.Loss.Valid
	hasRetrans := snap.Retrans.Valid
	hasUnacked := snap.Unacked.Valid
	hasJitter := snap.RTTVar.Valid && hasRTT

	if !hasRTT {
		return BehaviorUnknown
	}

	rttMs := float64(snap.RTT.Value.Milliseconds())
	lossRate := float64(0)
	if hasLoss {
		lossRate = snap.Loss.Value
	}
	retrans := uint32(0)
	if hasRetrans {
		retrans = snap.Retrans.Value
	}
	unacked := uint32(0)
	if hasUnacked {
		unacked = snap.Unacked.Value
	}

	// Compute jitter ratio (RTTVar / RTT)
	jitterRatio := float64(0)
	if hasJitter && snap.RTT.Value > 0 {
		jitterRatio = float64(snap.RTTVar.Value) / float64(snap.RTT.Value)
	}

	// Classification cascade: most specific first

	// Aggressive: very low RTT, high throughput implied by low unacked relative to RTT
	if rttMs < 15 && jitterRatio < 0.05 && lossRate < 0.005 && retrans < 3 {
		return BehaviorAggressive
	}

	// Low latency: low RTT, low jitter, minimal loss
	if rttMs < 30 && jitterRatio < 0.10 && lossRate < 0.01 && retrans < 5 {
		return BehaviorLowLatency
	}

	// Lossy: significant loss or retransmissions
	if lossRate > 0.01 || retrans > 10 {
		return BehaviorLossy
	}

	// Saturated: high RTT with significant unacked data (bufferbloat signal)
	if rttMs > 200 && hasUnacked && unacked > 50 {
		return BehaviorSaturated
	}

	return BehaviorNormal
}

// BehaviorConfig returns recommended XMUX parameters for a given behavior.
type BehaviorConfig struct {
	MaxConnections   int
	MaxConcurrency   int
	PreConnectCount  int
	WarmupDelayScale float64 // multiplier for warmup delay (1.0 = normal)
}

// DefaultBehaviorConfig returns the default XMUX configuration.
func DefaultBehaviorConfig() BehaviorConfig {
	return BehaviorConfig{
		MaxConnections:   4,
		MaxConcurrency:   32,
		PreConnectCount:  1,
		WarmupDelayScale: 1.0,
	}
}

// ConfigForBehavior returns recommended XMUX parameters for a link behavior.
func ConfigForBehavior(b Behavior) BehaviorConfig {
	def := DefaultBehaviorConfig()
	switch b {
	case BehaviorLowLatency:
		// LotSpeed-like: more concurrency, faster warmup
		def.MaxConcurrency = 64
		def.WarmupDelayScale = 0.5
	case BehaviorAggressive:
		// Brutal-like: fewer pre-connects to avoid over-stacking
		def.MaxConnections = 2
		def.PreConnectCount = 0
		def.WarmupDelayScale = 0.3
	case BehaviorLossy:
		// Reduce concurrency, slower warmup
		def.MaxConcurrency = 16
		def.WarmupDelayScale = 2.0
	case BehaviorSaturated:
		// Fewer connections, reduce head-of-line blocking
		def.MaxConnections = 2
		def.MaxConcurrency = 16
		def.WarmupDelayScale = 1.5
	}
	return def
}
