package splithttp

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/features/stats"
)

// Bray-V2 optional export of process atomics into stats.Manager.
// Wave-5: pull Publish API.
// Wave-6: auto-bind on real stats.Manager Start + background mirror (opt-out).
// No hot-path cost on dial; mirror runs on a timer only when bound.

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

// BrayV2StatsMirrorInterval is how often bound metrics are pushed to stats.
// Zero disables the background mirror (Bind still works; call Publish manually).
var BrayV2StatsMirrorInterval = 30 * time.Second

// BrayV2StatsAutoMirror enables Start-hook auto bind+mirror. Default true.
// Set false before stats.Manager.Start to keep pull-only mode.
var BrayV2StatsAutoMirror = true

var (
	brayStatsMu    sync.Mutex
	brayStatsBound stats.Manager

	mirrorStop   chan struct{}
	mirrorWG     sync.WaitGroup
	mirrorActive atomic.Bool
)

// BindBrayV2StatsManager stores a stats.Manager for later Publish calls.
// Pass nil to unbind and stop the mirror goroutine. Does not register counters
// until Publish.
func BindBrayV2StatsManager(m stats.Manager) {
	brayStatsMu.Lock()
	prev := brayStatsBound
	brayStatsBound = m
	brayStatsMu.Unlock()

	if m == nil {
		stopBrayStatsMirror()
		return
	}
	if prev == m && mirrorActive.Load() {
		return
	}
	// Fresh bind: optional background mirror
	if BrayV2StatsAutoMirror && BrayV2StatsMirrorInterval > 0 {
		startBrayStatsMirror()
	}
}

// PublishBrayV2MetricsToStats mirrors GetBrayV2Metrics absolute values into
// stats counters (Set). Safe no-op when m is nil. Names use bray-v2>>> prefix.
func PublishBrayV2MetricsToStats(m stats.Manager) {
	if m == nil {
		return
	}
	// NoopManager RegisterCounter always fails; skip quietly.
	if _, ok := m.(stats.NoopManager); ok {
		return
	}
	snap := GetBrayV2Metrics()
	set := func(name string, v uint64) {
		c, err := m.GetOrRegisterCounter(name)
		if err != nil || c == nil {
			return
		}
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

func startBrayStatsMirror() {
	stopBrayStatsMirror()
	if BrayV2StatsMirrorInterval <= 0 {
		return
	}
	stop := make(chan struct{})
	brayStatsMu.Lock()
	mirrorStop = stop
	brayStatsMu.Unlock()

	mirrorActive.Store(true)
	mirrorWG.Add(1)
	go func(stopCh chan struct{}, interval time.Duration) {
		defer mirrorWG.Done()
		// Immediate first publish so panels have data without waiting interval.
		PublishBoundBrayV2Metrics()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-t.C:
				PublishBoundBrayV2Metrics()
			}
		}
	}(stop, BrayV2StatsMirrorInterval)
}

func stopBrayStatsMirror() {
	brayStatsMu.Lock()
	stop := mirrorStop
	mirrorStop = nil
	brayStatsMu.Unlock()
	if stop != nil {
		close(stop)
		mirrorWG.Wait()
	}
	mirrorActive.Store(false)
}

// brayStatsMirrorActiveForTest exposes mirror state for tests.
func brayStatsMirrorActiveForTest() bool {
	return mirrorActive.Load()
}

func init() {
	// Auto-bind when a real stats.Manager starts (not NoopManager).
	// Dial path never waits on this; only a 30s ticker when stats app is present.
	stats.OnManagerStart(func(m stats.Manager) {
		if !BrayV2StatsAutoMirror || m == nil {
			return
		}
		if _, ok := m.(stats.NoopManager); ok {
			return
		}
		BindBrayV2StatsManager(m)
	})
	stats.OnManagerClose(func(m stats.Manager) {
		brayStatsMu.Lock()
		bound := brayStatsBound
		brayStatsMu.Unlock()
		if bound == m || m == nil {
			BindBrayV2StatsManager(nil)
		}
	})
}
