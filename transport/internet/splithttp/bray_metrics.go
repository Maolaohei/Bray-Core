package splithttp

import (
	"fmt"
	"sync/atomic"
)

// Bray-V2 Wave-4 process-wide observability for mode cascade / multi-endpoint / sticky.
// Default dial path does not require these counters; they are cheap atomics.

type brayTransportMetrics struct {
	ModeAttempts      atomic.Uint64 // dials that entered cascade loop (one per Dial)
	ModeSuccesses     atomic.Uint64 // successful open (any mode)
	ModeCascadeSteps  atomic.Uint64 // open-fail -> next mode transitions
	ModeCascadeWins   atomic.Uint64 // success after at least one cascade step
	StickyHits        atomic.Uint64 // dials that reordered cascade from sticky
	StickyRemembers   atomic.Uint64 // successful remembers
	MultiEndpointRaces atomic.Uint64
	MultiEndpointAltWins atomic.Uint64 // winner was not primary
	XmuxOpenEvicts    atomic.Uint64
}

var brayV2Metrics brayTransportMetrics

// BrayV2MetricsSnapshot is a point-in-time view.
type BrayV2MetricsSnapshot struct {
	ModeAttempts       uint64
	ModeSuccesses      uint64
	ModeCascadeSteps   uint64
	ModeCascadeWins    uint64
	StickyHits         uint64
	StickyRemembers    uint64
	MultiEndpointRaces uint64
	MultiEndpointAltWins uint64
	XmuxOpenEvicts     uint64
}

// GetBrayV2Metrics returns process-wide Wave-3/4 counters.
func GetBrayV2Metrics() BrayV2MetricsSnapshot {
	return BrayV2MetricsSnapshot{
		ModeAttempts:         brayV2Metrics.ModeAttempts.Load(),
		ModeSuccesses:        brayV2Metrics.ModeSuccesses.Load(),
		ModeCascadeSteps:     brayV2Metrics.ModeCascadeSteps.Load(),
		ModeCascadeWins:      brayV2Metrics.ModeCascadeWins.Load(),
		StickyHits:           brayV2Metrics.StickyHits.Load(),
		StickyRemembers:      brayV2Metrics.StickyRemembers.Load(),
		MultiEndpointRaces:   brayV2Metrics.MultiEndpointRaces.Load(),
		MultiEndpointAltWins: brayV2Metrics.MultiEndpointAltWins.Load(),
		XmuxOpenEvicts:       brayV2Metrics.XmuxOpenEvicts.Load(),
	}
}

// BrayV2MetricsReport is a one-line log summary.
func BrayV2MetricsReport() string {
	m := GetBrayV2Metrics()
	return fmt.Sprintf(
		"Bray-V2 metrics: mode_ok=%d/%d cascade_steps=%d cascade_wins=%d sticky_hit=%d sticky_set=%d multi_race=%d multi_alt=%d xmux_evict=%d",
		m.ModeSuccesses, m.ModeAttempts, m.ModeCascadeSteps, m.ModeCascadeWins,
		m.StickyHits, m.StickyRemembers, m.MultiEndpointRaces, m.MultiEndpointAltWins, m.XmuxOpenEvicts,
	)
}

func recordModeAttempt() { brayV2Metrics.ModeAttempts.Add(1) }

func recordModeSuccess(cascaded bool) {
	brayV2Metrics.ModeSuccesses.Add(1)
	if cascaded {
		brayV2Metrics.ModeCascadeWins.Add(1)
	}
}

func recordModeCascadeStep() { brayV2Metrics.ModeCascadeSteps.Add(1) }

func recordStickyHit()      { brayV2Metrics.StickyHits.Add(1) }
func recordStickyRemember() { brayV2Metrics.StickyRemembers.Add(1) }

func recordMultiEndpointRace(altWin bool) {
	brayV2Metrics.MultiEndpointRaces.Add(1)
	if altWin {
		brayV2Metrics.MultiEndpointAltWins.Add(1)
	}
}

func recordXmuxOpenEvict() { brayV2Metrics.XmuxOpenEvicts.Add(1) }
