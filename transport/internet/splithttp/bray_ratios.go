package splithttp

import "fmt"

// BrayV2Rates is derived success/hit ratios for field A/B (read-only).
// Zero denominators yield 0.0 (no NaN).
type BrayV2Rates struct {
	ModeSuccessRate      float64 // ModeSuccesses / ModeAttempts
	CascadeWinRate       float64 // ModeCascadeWins / ModeSuccesses
	StickyHitRate        float64 // StickyHits / ModeAttempts
	MultiAltWinRate      float64 // MultiEndpointAltWins / MultiEndpointRaces
	EndpointStickyHitRate float64 // EndpointStickyHits / MultiEndpointRaces
}

// ComputeBrayV2Rates derives ratios from a metrics snapshot.
func ComputeBrayV2Rates(m BrayV2MetricsSnapshot) BrayV2Rates {
	r := BrayV2Rates{}
	if m.ModeAttempts > 0 {
		r.ModeSuccessRate = float64(m.ModeSuccesses) / float64(m.ModeAttempts)
		r.StickyHitRate = float64(m.StickyHits) / float64(m.ModeAttempts)
	}
	if m.ModeSuccesses > 0 {
		r.CascadeWinRate = float64(m.ModeCascadeWins) / float64(m.ModeSuccesses)
	}
	if m.MultiEndpointRaces > 0 {
		r.MultiAltWinRate = float64(m.MultiEndpointAltWins) / float64(m.MultiEndpointRaces)
		r.EndpointStickyHitRate = float64(m.EndpointStickyHits) / float64(m.MultiEndpointRaces)
	}
	return r
}

// GetBrayV2Rates returns live ratios from process atomics.
func GetBrayV2Rates() BrayV2Rates {
	return ComputeBrayV2Rates(GetBrayV2Metrics())
}

// BrayV2RatesReport is a one-line A/B summary.
func BrayV2RatesReport() string {
	r := GetBrayV2Rates()
	return fmt.Sprintf(
		"Bray-V2 rates: mode_ok=%.3f cascade_win=%.3f sticky_hit=%.3f multi_alt=%.3f ep_sticky_hit=%.3f",
		r.ModeSuccessRate, r.CascadeWinRate, r.StickyHitRate, r.MultiAltWinRate, r.EndpointStickyHitRate,
	)
}
