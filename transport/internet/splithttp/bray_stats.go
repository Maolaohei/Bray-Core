package splithttp

import (
	"sync"

	"github.com/xtls/xray-core/features/stats"
)

// Bray-V2 Wave-5 optional export of process atomics into stats.Manager.
// No hot-path cost until PublishBrayV2MetricsToStats is called (or a manager
// is bound and Publish is invoked by the operator/poller).

const (
	statsBrayModeAttempts            = "bray-v2>>>mode_attempts"
	statsBrayModeSuccesses           = "bray-v2>>>mode_successes"
	statsBrayModeCascadeSteps        = "bray-v2>>>mode_cascade_steps"
	statsBrayModeCascadeWins         = "bray-v2>>>mode_cascade_wins"
	statsBrayStickyHits              = "bray-v2>>>sticky_hits"
	statsBrayStickyRemembers         = "bray-v2>>>sticky_remembers"
	statsBrayMultiEndpointRaces      = "bray-v2>>>multi_endpoint_races"
	statsBrayMultiEndpointAltWins    = "bray-v2>>>multi_endpoint_alt_wins"
	statsBrayEndpointStickyHits      = "bray-v2>>>endpoint_sticky_hits"
	statsBrayEndpointStickyRemembers = "bray-v2>>>endpoint_sticky_remembers"
	statsBrayXmuxOpenEvicts          = "bray-v2>>>xmux_open_evicts"
)

var (
	brayStatsMu   sync.Mutex
	brayStatsBound stats.Manager
)

// BindBrayV2StatsManager stores a stats.Manager for later Publish calls.
// Pass nil to unbind. Does not register counters until Publish.
func BindBrayV2StatsManager(m stats.Manager) {
	brayStatsMu.Lock()
	brayStatsBound = m
	brayStatsMu.Unlock()
}

// PublishBrayV2MetricsToStats mirrors GetBrayV2Metrics absolute values into
// stats counters (Set). Safe no-op when m is nil. Names use bray-v2>>> prefix.
func PublishBrayV2MetricsToStats(m stats.Manager) {
	if m == nil {
		return
	}
	snap := GetBrayV2Metrics()
	set := func(name string, v uint64) {
		c, err := stats.GetOrRegisterCounter(m, name)
		if err != nil || c == nil {
			return
		}
		// Counter is int64; clamp to avoid overflow panic on absurd values.
		if v > uint64(^uint64(0)>>1) {
			v = uint64(^uint64(0) >> 1)
		}
		c.Set(int64(v))
	}
	set(statsBrayModeAttempts, snap.ModeAttempts)
	set(statsBrayModeSuccesses, snap.ModeSuccesses)
	set(statsBrayModeCascadeSteps, snap.ModeCascadeSteps)
	set(statsBrayModeCascadeWins, snap.ModeCascadeWins)
	set(statsBrayStickyHits, snap.StickyHits)
	set(statsBrayStickyRemembers, snap.StickyRemembers)
	set(statsBrayMultiEndpointRaces, snap.MultiEndpointRaces)
	set(statsBrayMultiEndpointAltWins, snap.MultiEndpointAltWins)
	set(statsBrayEndpointStickyHits, snap.EndpointStickyHits)
	set(statsBrayEndpointStickyRemembers, snap.EndpointStickyRemembers)
	set(statsBrayXmuxOpenEvicts, snap.XmuxOpenEvicts)
}

// PublishBoundBrayV2Metrics publishes using the manager from BindBrayV2StatsManager.
func PublishBoundBrayV2Metrics() {
	brayStatsMu.Lock()
	m := brayStatsBound
	brayStatsMu.Unlock()
	PublishBrayV2MetricsToStats(m)
}

// BrayV2StatsCounterNames returns the stable stats names for operators/docs.
func BrayV2StatsCounterNames() []string {
	return []string{
		statsBrayModeAttempts,
		statsBrayModeSuccesses,
		statsBrayModeCascadeSteps,
		statsBrayModeCascadeWins,
		statsBrayStickyHits,
		statsBrayStickyRemembers,
		statsBrayMultiEndpointRaces,
		statsBrayMultiEndpointAltWins,
		statsBrayEndpointStickyHits,
		statsBrayEndpointStickyRemembers,
		statsBrayXmuxOpenEvicts,
	}
}
