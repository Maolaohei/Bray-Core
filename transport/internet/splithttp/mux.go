package splithttp

import (
	"context"
	stderrors "errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/quality"
)

type XmuxConn interface {
	IsClosed() bool
}

// Connection lifecycle states.
const (
	StateActive   int32 = 0 // accepting new streams
	StateDraining int32 = 1 // refusing new streams, waiting for active streams to finish
	StateClosed   int32 = 2 // TCP connection closed

	// clientIdleTimeout bounds the time a client can remain unused (no active
	// streams) before being evicted. A fixed long value is a machine
	// signature (real browsers get Keep-Alive timeouts in seconds/minutes),
	// so each connection draws its own skewed timeout in [90s, 240s] at
	// creation; the constant below only bounds the range.
	// Primary failure mechanism is still Fast Eviction on fatal errors.
	clientIdleTimeoutMinSecs = 90
	clientIdleTimeoutMaxSecs = 240

	// Idle-activity (beacon) pacing: a connection that has been quiet for at
	// least idleActivityMinIdleSecs emits a short beacon-style request every
	// skewed [45s, 90s] — simulating browser background traffic instead of a
	// perfectly silent connection. The activity never refreshes LastUsed, so
	// idle eviction above still applies.
	idleActivityMinIdleSecs      = 45
	idleActivityMinIntervalSecs  = 45
	idleActivityMaxIntervalSecs  = 90
	idleActivitySimDelayMs       = 30 // simulated client processing delay
	idleActivitySimDelayMaxMs    = 80
	idleActivityRequestTimeoutMs = 500

	// probeTimeout is the maximum time to wait for a connection probe (HEAD request)
	// to complete. If the probe doesn't finish within this time, the connection is
	// considered broken and removed from the pool.
	probeTimeout = 10 * time.Second

	// overAdmitHardMult / overAdmitHardFloor cap streams on one client when
	// GetXmuxClient over-admits past MaxConcurrency. Prevents a wedged H2
	// session (e.g. long AI SSE) from absorbing unbounded Dial traffic.
	overAdmitHardMult  = 4
	overAdmitHardFloor = 32

	// openTimeoutEvictAfter consecutive header-open timeouts mark the client dead.
	openTimeoutEvictAfter = 2

	// idleBeaconEvictAfter consecutive idle-activity (beacon) failures mark
	// the client dead. Two because a single beacon can fail on transient
	// jitter, but two in a row on an otherwise-idle connection almost
	// always means the path is gone (CDN idle kill / NAT rebinding).
	idleBeaconEvictAfter = 2
)

// XmuxClientPool is a read-write separated connection pool.
// Reads (Len, Snapshot) use RLock for concurrent access.
// Writes (Remove, Append, CloseAll) use exclusive Lock.
type XmuxClientPool struct {
	mu      sync.RWMutex
	clients []*XmuxClient
}

// Len returns the pool size under a read lock.
func (p *XmuxClientPool) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.clients)
}

// Snapshot returns a shallow copy of the client slice for read-only iteration.
// Callers must NOT modify the returned slice.
func (p *XmuxClientPool) Snapshot() []*XmuxClient {
	p.mu.RLock()
	defer p.mu.RUnlock()
	snap := make([]*XmuxClient, len(p.clients))
	copy(snap, p.clients)
	return snap
}

// RemoveAt removes the client at index i. Caller must hold p.mu (write lock).
func (p *XmuxClientPool) RemoveAt(i int) {
	p.clients = append(p.clients[:i], p.clients[i+1:]...)
}

// Append adds a client. Caller must hold p.mu (write lock).
func (p *XmuxClientPool) Append(c *XmuxClient) {
	p.clients = append(p.clients, c)
}

// CloseAll force-closes every client (stop profilers + underlying transports) and nils the slice.
// Caller must hold p.mu (write lock).
func (p *XmuxClientPool) CloseAll() {
	for _, c := range p.clients {
		c.MarkDead()
	}
	p.clients = nil
}

// Remove removes the first client matching target by reference. Caller must hold p.mu (write lock).
// Returns true if removed, false if not found.
func (p *XmuxClientPool) Remove(target *XmuxClient) bool {
	for i, c := range p.clients {
		if c == target {
			p.RemoveAt(i)
			return true
		}
	}
	return false
}

// RemoveAndClose removes the first matching client and force-closes it.
// Caller must hold p.mu (write lock).
func (p *XmuxClientPool) RemoveAndClose(target *XmuxClient) bool {
	if p.Remove(target) {
		target.MarkDead()
		return true
	}
	return false
}

type XmuxClient struct {
	XmuxConn      XmuxConn
	state         atomic.Int32 // StateActive / StateDraining / StateClosed
	activeStreams atomic.Int32 // number of active HTTP/2 streams using this connection
	// downloadPins counts long-lived download legs (the legacy long-GET, or
	// the dseg production leg plus its segment pulls) riding on this
	// connection WITHOUT Borrow accounting: the dialer issues them straight
	// over the shared transport, so pool hygiene cannot see them through
	// activeStreams/remaining. A pin keeps the transport open even after
	// an upload-side rotation releases the connection's last Borrow and a
	// drain fires (budget/lifetime exhaustion): tryClose refuses to close
	// while pins>0, and the unpin path retries the close. Pins never affect
	// selection or admission — a pinned, exhausted connection keeps serving
	// only its existing download leg and receives no new work. Real
	// connection faults bypass this via MarkDead.
	downloadPins atomic.Int32
	// remaining is THE single reuse budget for this connection: how many
	// more stream acquisitions it may serve before a hygiene drain.
	// Config-derived once at creation (min of CMaxReuseTimes /
	// HMaxRequestTimes; -1 = unlimited), decremented exactly once per
	// acquisition — in GetXmuxClient, the single choke point. There is no
	// per-POST or per-leg decrement: a packet-up session burns one unit per
	// Dial regardless of how many POSTs or legs it opens. 0 = exhausted;
	// negative = unlimited. (Runtime state; see deadlineUnix for the
	// lifetime half of the budget and activeStreams for concurrency.)
	remaining atomic.Int32
	// deadlineUnix is the unix-nano lifetime deadline (0 = unlimited):
	// after it passes the connection takes no new work and is drained once
	// idle. Config-derived once at creation (HMaxReusableSecs).
	deadlineUnix atomic.Int64
	createdAt    time.Time
	LastUsed     atomic.Int64 // unix nano: last time this client was borrowed
	// lastIdleActivity is the unix nano timestamp of the last beacon-style
	// idle activity, for per-connection rate limiting.
	lastIdleActivity atomic.Int64
	// idleTimeoutNs is this connection's own skewed idle-eviction timeout
	// (drawn at creation, immutable afterwards — safe to read unlocked).
	idleTimeoutNs int64
	lastRTT       atomic.Int64 // nanoseconds, for RTT-aware scheduling
	// cachedScore is the quality/RTT base only (no inflight term).
	// Selection uses cachedScore + activeStreams*10000 so Borrow/Release
	// never need a full scoreClient recompute under the stream hot path.
	cachedScore atomic.Int64
	// behaviorScale caches behaviorPenaltyScaleFixed (x100) so scoreClient
	// does not take NetworkLearner.Dominant's mutex on every recompute.
	behaviorScale atomic.Int64

	// Ready Promise: blocks concurrent traffic until probe completes
	ready     chan struct{} // closed when probe finishes (success or failure)
	probeErr  error         // set if probe failed
	probeDone atomic.Bool   // true after probe finished or skipped

	// V2.0: link-quality metrics for smarter scheduling
	lastRetrans  atomic.Int32 // cumulative retransmit count from TCP_INFO
	lastLoss     atomic.Int64 // loss rate × 10000 (fixed-point, 0-10000)
	qualityScore atomic.Int32 // 0-100, computed by TransportProfile
	confidence   atomic.Int32 // 0-100, how much we trust the quality data
	consecDrops  atomic.Int32 // consecutive quality drops, for drain

	// openHeaderTimeouts counts consecutive OpenStream header-wait timeouts.
	// After openTimeoutEvictAfter, MarkDead so the next Dial rotates off a wedged H2.
	openHeaderTimeouts atomic.Int32

	// beaconFailures counts consecutive idle-activity (beacon) request
	// failures. A CDN edge that silently dropped the TCP flow (idle kill,
	// no RST) leaves IsClosed() false locally; beacons are the only probe
	// that notices. After idleBeaconEvictAfter consecutive failures the
	// client is MarkDead so the pool rotates instead of lending a dead H2.
	beaconFailures atomic.Int32

	// V2.1: behavior learning for adaptive scheduling
	learner *quality.NetworkLearner // tracks link behavior patterns

	// TransportProfile for this connection. Created when TCP connection is established.
	profileMu sync.Mutex          // protects profile field
	profile   interface{ Stop() } // *tcpinfo.Profile, stored as interface to avoid import cycle

	// closeOnce ensures the underlying XmuxConn is closed at most once.
	closeOnce sync.Once
}

// markProbeReady publishes probe completion for WaitForReady hot path.
// Call after probeErr is final (success or failure) and before/with closing ready.
func (c *XmuxClient) markProbeReady() {
	c.probeDone.Store(true)
}

// WaitForReady blocks until probe completes or context is cancelled.
// Returns probeErr if probe failed.
func (c *XmuxClient) WaitForReady(ctx context.Context) error {
	// Fast path: probe already finished (common on reuse after warm-up).
	// probeDone avoids channel select + ready-queue bookkeeping on every Get.
	if c.probeDone.Load() {
		return c.probeErr
	}
	select {
	case <-c.ready:
		return c.probeErr
	default:
	}
	select {
	case <-c.ready:
		return c.probeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Borrow atomically attempts to reserve a stream slot on this connection.
// Returns true only if state is Active and the increment took effect while still Active.
// This prevents the race: GetXmuxClient returns client -> Draining fires -> stream added to retired connection.
func (c *XmuxClient) Borrow() bool {
	for {
		if c.state.Load() != StateActive {
			return false
		}
		old := c.activeStreams.Load()
		if !c.activeStreams.CompareAndSwap(old, old+1) {
			continue // CAS failed, retry
		}
		// Verify state didn't change between our read and the CAS.
		// If it did, roll back and fail.
		if c.state.Load() != StateActive {
			c.activeStreams.Add(-1)
			return false
		}
		// Inflight is folded into selection score at read time; no recompute.
		return true
	}
}

// Release marks one active stream as finished and tries to close if draining.
func (c *XmuxClient) Release() {
	c.activeStreams.Add(-1)
	// Inflight term is applied at selection; base score unchanged.
	c.tryClose()
}

// NoteOpenHeaderTimeout records an OpenStream header-wait timeout.
// After openTimeoutEvictAfter consecutive timeouts, the client is MarkDead so
// subsequent traffic rotates instead of pinballing onto a blackholed H2 session.
func (c *XmuxClient) NoteOpenHeaderTimeout() {
	if c == nil {
		return
	}
	n := c.openHeaderTimeouts.Add(1)
	if n >= openTimeoutEvictAfter {
		errors.LogInfo(context.Background(), "XMUX: open-header timeout x", n, ", marking client dead")
		c.MarkDead()
	}
}

// NoteOpenSuccess clears the open-header timeout counter after a healthy open.
func (c *XmuxClient) NoteOpenSuccess() {
	if c == nil {
		return
	}
	c.openHeaderTimeouts.Store(0)
}

// NoteBeaconSuccess clears the consecutive beacon-failure counter after a
// successful idle-activity request.
func (c *XmuxClient) NoteBeaconSuccess() {
	if c == nil {
		return
	}
	c.beaconFailures.Store(0)
}

// NoteBeaconFailure records one failed idle-activity request. After
// idleBeaconEvictAfter consecutive failures, MarkDead: the connection is
// almost certainly gone (CDN idle kill without RST, NAT rebinding), and
// lending it to the next Dial would cost a full stream-open timeout.
func (c *XmuxClient) NoteBeaconFailure() {
	if c == nil || c.state.Load() != StateActive {
		return
	}
	n := c.beaconFailures.Add(1)
	if n >= idleBeaconEvictAfter {
		errors.LogInfo(context.Background(), "XMUX: idle-beacon failure x", n, ", marking client dead")
		c.MarkDead()
	}
}

// MarkDead immediately transitions to Closed state.
// Called by Fast Eviction when fatal errors are detected (EOF, broken pipe, GOAWAY, etc.)
// Unlike maybeDrain, this closes the underlying transport even if streams are still marked active
// (those streams are already unusable after a connection-level fault).
func (c *XmuxClient) MarkDead() {
	c.state.Store(StateClosed)
	c.closeConn()
}

// closeConn stops profiling and closes the underlying transport exactly once.
func (c *XmuxClient) closeConn() {
	c.closeOnce.Do(func() {
		c.StopProfiling()
		common.Close(c.XmuxConn)
	})
}

// maybeDrain transitions from Active to Draining.
func (c *XmuxClient) maybeDrain() {
	c.state.CompareAndSwap(StateActive, StateDraining)
	c.tryClose()
}

// tryClose is the single entry point for closing the TCP connection.
// Only succeeds when state is Draining, activeStreams is 0, and CAS to Closed wins.
//
// downloadPins guard: hygiene drains are allowed only once the connection's
// download legs are gone. A pinned connection sits in StateDraining —
// invisible to selection (xmuxClientReusable requires StateActive) and
// exempt from further eviction checks (removed from the pool at drain time)
// — until the last pin releases and the unpin path performs the close.
func (c *XmuxClient) tryClose() bool {
	if c.downloadPins.Load() > 0 {
		return false
	}
	if c.state.Load() != StateDraining {
		return false
	}
	if c.activeStreams.Load() > 0 {
		return false
	}
	if c.state.CompareAndSwap(StateDraining, StateClosed) {
		c.closeConn()
		return true
	}
	return false
}

// PinDownload registers one long-lived download leg on this connection so a
// hygiene drain cannot close the transport under it. Pairs with UnpinDownload.
func (c *XmuxClient) PinDownload() { c.downloadPins.Add(1) }

// UnpinDownload releases one PinDownload reference and retries the drain's
// deferred close when the last pin is gone.
func (c *XmuxClient) UnpinDownload() {
	if c.downloadPins.Add(-1) == 0 {
		c.tryClose()
	}
}

// UpdateRTT updates the smoothed RTT for this connection using EWMA.
func (c *XmuxClient) UpdateRTT(rtt time.Duration) {
	newRTT := int64(rtt)
	for {
		old := c.lastRTT.Load()
		var smoothed int64
		if old == 0 {
			smoothed = newRTT
		} else {
			// EWMA: 80% old + 20% new
			smoothed = (old*8 + newRTT*2) / 10
		}
		if c.lastRTT.CompareAndSwap(old, smoothed) {
			c.recomputeScore()
			return
		}
	}
}

// GetRTT returns the current smoothed RTT.
func (c *XmuxClient) GetRTT() time.Duration {
	return time.Duration(c.lastRTT.Load())
}

// UpdateQuality updates link-quality metrics from a TransportProfile snapshot.
// Called by the profile's background collector or XMUX integration layer.
func (c *XmuxClient) UpdateQuality(qualityScore, confidence, retrans int32, lossRate int64) {
	oldScore := c.qualityScore.Load()
	c.qualityScore.Store(qualityScore)
	c.confidence.Store(confidence)
	c.lastRetrans.Store(retrans)
	c.lastLoss.Store(lossRate)

	// Track consecutive quality drops for drain (CAS loop for safety)
	for {
		old := c.consecDrops.Load()
		var new int32
		if qualityScore < oldScore && oldScore > 0 {
			new = old + 1
		} else {
			new = 0
		}
		if c.consecDrops.CompareAndSwap(old, new) {
			break
		}
	}

	// V2.1: Feed learner for behavior classification
	if c.learner != nil {
		rtt := c.GetRTT()
		snap := &quality.Snapshot{
			Timestamp:  time.Now(),
			Source:     quality.SourceEstimated,
			Confidence: uint8(confidence),
			RTT:        quality.NewMetric(rtt),
			Loss:       quality.NewMetric(float64(lossRate) / 10000.0),
			Retrans:    quality.NewMetric(uint32(retrans)),
			Quality:    quality.Quality{Overall: uint8(qualityScore)},
		}
		c.learner.Record(snap)
		c.behaviorScale.Store(behaviorPenaltyScaleFixed(c.learner.Dominant()))
	}

	c.recomputeScore()
}

// recomputeScore recalculates the quality/RTT base score (no inflight term).
func (c *XmuxClient) recomputeScore() {
	c.cachedScore.Store(scoreClientBase(c))
}

// GetBehavior returns the current dominant behavior learned from observations.
func (c *XmuxClient) GetBehavior() quality.Behavior {
	if c.learner == nil {
		return quality.BehaviorUnknown
	}
	return c.learner.Dominant()
}

// ShouldDrain returns true if this connection should be drained
// due to consecutive quality drops (5+ drops in a row).
func (c *XmuxClient) ShouldDrain() bool {
	return c.consecDrops.Load() >= 5
}

// StartProfiling attaches a TransportProfile to this client.
// The profile periodically collects TCP_INFO and feeds it to UpdateQuality.
// The conn must be the raw TCP socket (before TLS/REALITY wrapping).
func (c *XmuxClient) StartProfiling(conn interface{ Stop() }) {
	if conn == nil {
		return
	}
	c.profileMu.Lock()
	defer c.profileMu.Unlock()
	c.StopProfilingLocked() // stop any existing profile
	c.profile = conn
}

// StopProfiling stops the TransportProfile background goroutine.
func (c *XmuxClient) StopProfiling() {
	c.profileMu.Lock()
	defer c.profileMu.Unlock()
	c.StopProfilingLocked()
}

// StopProfilingLocked stops the profile. Caller must hold profileMu.
func (c *XmuxClient) StopProfilingLocked() {
	if c.profile != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					errors.LogDebug(context.Background(), "XMUX: StopProfiling recovered panic: ", r)
				}
			}()
			c.profile.Stop()
		}()
		c.profile = nil
	}
}

type XmuxManager struct {
	xmuxConfig   *XmuxConfig
	concurrency  int32 // base concurrency (from config)
	connections  int32 // base connections (from config)
	newConnFunc  func() XmuxConn
	probeURL     string // URL for HEAD probe to trigger real TCP/TLS dial
	pool         XmuxClientPool
	stopCh       chan struct{}
	doneCh       chan struct{} // closed when all goroutines exit
	lastActivity atomic.Int64  // nanosecond timestamp of last client obtain; lock-free
	closeOnce    sync.Once     // ensures Close() is idempotent

	// V2.1: Dynamic Connection Scaling
	poolBehavior   quality.Behavior // dominant behavior across all clients
	poolBehaviorMu sync.RWMutex
	behaviorStreak int              // consecutive observations of same behavior (for debounce)
	streakBehavior quality.Behavior // behavior being streaked
	_dynamicConns  atomic.Int32     // current effective connections (AIMD smoothed), lock-free read
	_dynamicConc   atomic.Int32     // current effective concurrency (AIMD smoothed), lock-free read
	scaledOnce     atomic.Bool      // whether dynamic scaling has been applied at least once

	// Dynamic warmup queue (reserved for future use)
	warmupMu     sync.Mutex
	netHash      string // current network hash for change detection
	lastNetCheck time.Time

	// Network change debouncing: require 2 consecutive observations to confirm
	pendingNetChange      string // pending network hash change awaiting confirmation
	pendingNetChangeCount int    // number of consecutive observations of the same change

	// Probe storm control: after repeated connect-refused / dial-dead failures
	// (common when listen is gone mid-bench or migration), suppress MarkDead+log
	// spam while still allowing one real probe after cooldown.
	probeFailMu      sync.Mutex
	probeFailStreak  int
	probeCoolUntil   time.Time
	probeLastFailLog time.Time

	// Lazy CDN keep-warm: probe response headers reveal a CDN edge
	// (cf-ray etc.). When such a path idles out of the pool, one jittered
	// background reconnect pre-pays the cold-start so the next user request
	// finds a warm session. Sticky once detected; never retries on failure.
	isCDN           atomic.Bool
	warmReconnectMu sync.Mutex
	warmTimer       *time.Timer
	warmInFlight    atomic.Bool

	// Metrics for quantifiable validation
	metrics struct {
		// Connection reuse vs new
		reuseHit atomic.Int64 // XMUX pool hit (reuse connection)
		newConn  atomic.Int64 // New connection created

		// TTFB tracking (nanoseconds)
		ttfbSum   atomic.Int64 // Sum of TTFB values
		ttfbCount atomic.Int64 // Number of TTFB samples
		ttfbMax   atomic.Int64 // Max TTFB observed

		// Network recovery
		netRecoveryCount atomic.Int64 // Number of network changes detected
		netRecoveryTime  atomic.Int64 // Last recovery time (nanoseconds)
	}
}

// NewXmuxManager creates a manager and starts the single background
// maintenance loop. Optional probeURL is the HEAD probe target for real dial
// warm-up; pass it at construction time so tick-time newXmuxClient calls
// never race a later field write.
//
// xmuxConfig is held by pointer (protobuf messages carry an internal mutex and
// must never be copied by value). A nil config is treated as all-defaults.
func NewXmuxManager(xmuxConfig *XmuxConfig, newConnFunc func() XmuxConn, probeURL ...string) *XmuxManager {
	if xmuxConfig == nil {
		xmuxConfig = &XmuxConfig{}
	}
	probe := ""
	if len(probeURL) > 0 {
		probe = probeURL[0]
	}
	m := &XmuxManager{
		xmuxConfig:   xmuxConfig,
		concurrency:  xmuxConfig.GetNormalizedMaxConcurrency().rand(),
		connections:  xmuxConfig.GetNormalizedMaxConnections().rand(),
		newConnFunc:  newConnFunc,
		probeURL:     probe,
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
		lastActivity: atomic.Int64{},
	}

	// Start the single background maintenance loop. Initial pool fill and
	// probe-warm happen on the first tick (~instant); a synchronous pre-fill
	// here would only block NewXmuxManager callers on dial latency.
	go func() {
		defer func() {
			if r := recover(); r != nil {
				errors.LogDebug(context.Background(), "XMUX: maintenanceLoop recovered panic: ", r)
			}
		}()
		m.maintenanceLoop()
	}()

	return m
}

// maintenanceLoop is the ONE background goroutine: periodic health checking,
// pool replenishment/migration, network-change detection, and idle-manager
// housekeeping all run on one 5s tick. The former split (preConnectLoop /
// healthCheckLoop / networkWatchLoop goroutines) existed without a reason —
// the network check self-throttles to 30s and pre-connect is a tick job.
func (m *XmuxManager) maintenanceLoop() {
	// doneCh's contract: "closed when all goroutines exit". This is the only
	// long-lived goroutine, so its exit closes doneCh — without this, Close()
	// always burned its full 3s shutdown-timeout fallback.
	defer close(m.doneCh)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.healthCheckTick()
			m.checkNetworkChange()
		}
	}
}

// checkNetworkChange detects network interface changes with debouncing.
// Requires 2 consecutive matching changes (60s apart) to confirm a real network change,
// preventing false positives from DHCP renewals, virtual interface flapping, etc.
func (m *XmuxManager) checkNetworkChange() {
	m.warmupMu.Lock()
	defer m.warmupMu.Unlock()

	// Only check every 30 seconds
	if time.Since(m.lastNetCheck) < 30*time.Second {
		return
	}
	m.lastNetCheck = time.Now()

	// Get current network interfaces
	newHash := getNetworkHash()
	if newHash == "" {
		return
	}

	if m.netHash == "" {
		// First check, just record
		m.netHash = newHash
		return
	}

	if m.netHash == newHash {
		// No change, reset pending change tracking
		m.pendingNetChange = ""
		m.pendingNetChangeCount = 0
		return
	}

	// Network hash changed -> check if this is a new change or continuation
	if m.pendingNetChange == newHash {
		m.pendingNetChangeCount++
	} else {
		// Different change, start tracking
		m.pendingNetChange = newHash
		m.pendingNetChangeCount = 1
	}

	// Require 2 consecutive observations of the same new hash to confirm
	if m.pendingNetChangeCount < 2 {
		return
	}

	// Confirmed network change
	errors.LogInfo(context.Background(), "XMUX: network change confirmed, clearing DNS cache and re-warming up")
	m.netHash = newHash
	m.pendingNetChange = ""
	m.pendingNetChangeCount = 0

	// Track network recovery event
	m.metrics.netRecoveryCount.Add(1)

	// Clear stale DNS cache to prevent serving IPs from the old network
	internet.ClearDNSCache()

	// Re-warmup DNS on the new network (async, non-blocking)
	go internet.TriggerDNSWarmup()

	// Clear stale connections
	m.clearStaleConnections()
}

// clearStaleConnections removes all stale connections after network change
// and immediately creates replacement connections.
func (m *XmuxManager) clearStaleConnections() {
	// Phase 1: Remove and close all stale connections under write lock
	m.pool.mu.Lock()
	for i := 0; i < len(m.pool.clients); {
		c := m.pool.clients[i]
		errors.LogDebug(context.Background(), "XMUX: network-change removing stale xmuxClient, rtt=", c.GetRTT().Milliseconds(), "ms")
		m.pool.RemoveAt(i)
		c.MarkDead()
	}
	effectiveConns := m.effectiveConnections()
	m.pool.mu.Unlock()

	// Phase 2: Immediately create replacement connections (no lock held)
	targetConns := int(effectiveConns)
	if targetConns < 1 {
		targetConns = 1
	}
	for i := 0; i < targetConns; i++ {
		errors.LogDebug(context.Background(), "XMUX: network-change creating replacement xmuxClient, pool+=", i+1, "/", targetConns)
		conn := m.newConnFunc()
		if conn != nil {
			m.addToPool(conn)
		}
	}
}

// getNetworkHash returns a hash of current network interfaces including IPs.
// Uses a two-stage check: interface count first (cheap), then full comparison.
func getNetworkHash() string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return ""
	}

	// Stage 1: count active non-loopback interfaces (cheap O(n) scan)
	activeCount := 0
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp != 0 && iface.Flags&net.FlagLoopback == 0 {
			activeCount++
		}
	}

	// Stage 2: build hash only if count is non-zero
	if activeCount == 0 {
		return ""
	}

	var builder strings.Builder
	builder.Grow(activeCount * 32) // rough estimate: 20 bytes name + 12 bytes addr
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp != 0 && iface.Flags&net.FlagLoopback == 0 {
			builder.WriteString(iface.Name)
			addrs, _ := iface.Addrs()
			for _, addr := range addrs {
				builder.WriteByte(':')
				builder.WriteString(addr.String())
			}
			builder.WriteByte(';')
		}
	}
	return builder.String()
}

const (
	// maxRTTBeforeRemove is the maximum RTT (in ms) before a connection is considered unhealthy.
	maxRTTBeforeRemove = 5000
	// coldStartProtectionMs is the minimum age (in ms) before a connection can be removed by health check.
	coldStartProtectionMs = 10000
	// maxConnectionAge is the maximum lifetime of a connection before it is drained.
	// Prevents stale state accumulation from NAT/LB changes, server rotation, and TLS session staleness.
	// Connections with active streams will finish their work before closing.
	maxConnectionAge = 20 * time.Minute

	// xmuxBurstAbsMax caps soft-expand past steady maxConnections under full
	// concurrency saturation. Absorbs short spikes without thrashing REALITY/TLS.
	// Beyond this, GetXmuxClient over-admits onto the least-loaded active client.
	xmuxBurstAbsMax = 16
)

// burstConnectionLimit returns the max pool size allowed when every active
// client is at effectiveConcurrency. Steady target is effectiveConnections;
// burst is min(16, max(steady*2, steady+2)). Unlimited steady (0) keeps
// unlimited create behavior (return 0).
func burstConnectionLimit(steady int32) int {
	if steady <= 0 {
		return 0
	}
	burst := int(steady * 2)
	if alt := int(steady) + 2; alt > burst {
		burst = alt
	}
	if burst > xmuxBurstAbsMax {
		burst = xmuxBurstAbsMax
	}
	if burst < int(steady) {
		burst = int(steady)
	}
	return burst
}

// rttAdjustedIdleNs stretches or shrinks a connection's idle-eviction
// timeout by the current smoothed RTT: rebuild cost scales with RTT, so
// slow links keep connections longer. Stays within ±25% of the skewed
// base and never leaves the 90-240s band (fingerprint surface unchanged).
func rttAdjustedIdleNs(baseNs int64, rtt time.Duration) int64 {
	base := baseNs / int64(time.Second)
	switch {
	case rtt >= 200*time.Millisecond:
		base = base * 4 / 3
	case rtt <= 20*time.Millisecond:
		base = base * 3 / 4
	}
	if base < clientIdleTimeoutMinSecs {
		base = clientIdleTimeoutMinSecs
	}
	if base > clientIdleTimeoutMaxSecs {
		base = clientIdleTimeoutMaxSecs
	}
	return base * int64(time.Second)
}

// maybeIdleActivity schedules a beacon-style request on an idle connection,
// rate-limited per connection. Called from healthCheckTick for clients with
// zero active streams. Does NOT refresh LastUsed: eviction timing stays tied
// to business traffic, so an idle connection emits a few beacons and then is
// still evicted by clientIdleTimeout — like a browser tab that keeps sending
// background requests until the server's Keep-Alive timeout drops it.
func (m *XmuxManager) maybeIdleActivity(c *XmuxClient, now time.Time) {
	lastUsed := c.LastUsed.Load()
	if lastUsed == 0 || now.UnixNano()-lastUsed < int64(idleActivityMinIdleSecs)*int64(time.Second) {
		return // used recently — not an "idle" connection yet
	}
	interval := int64(biasedRangeRand(idleActivityMinIntervalSecs, idleActivityMaxIntervalSecs)) * int64(time.Second)
	last := c.lastIdleActivity.Load()
	if now.UnixNano()-last < interval {
		return
	}
	if !c.lastIdleActivity.CompareAndSwap(last, now.UnixNano()) {
		return
	}
	// Fire-and-forget; failures are silently ignored (the connection may be
	// evicted or closed in the meantime).
	go m.sendIdleActivity(c)
}

// sendIdleActivity emits one short beacon-style POST (empty body, same
// padding as data streams) over the given connection, after a simulated
// client processing delay. The server treats it as a short unauthenticated
// stream-one long-poll; the 500ms timeout closes it quickly.
func (m *XmuxManager) sendIdleActivity(c *XmuxClient) {
	if c.state.Load() != StateActive {
		return
	}
	time.Sleep(time.Duration(biasedRangeRand(idleActivitySimDelayMs, idleActivitySimDelayMaxMs)) * time.Millisecond)
	if c.state.Load() != StateActive || c.XmuxConn.IsClosed() {
		return
	}
	dc, ok := c.XmuxConn.(DialerClient)
	if !ok {
		return
	}
	bdc, ok := dc.(*DefaultDialerClient)
	if !ok || bdc.IsClosed() {
		return
	}
	u, err := url.Parse(m.probeURL)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), idleActivityRequestTimeoutMs*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), nil)
	if err != nil {
		return
	}
	// Same padding as data streams so observers cannot separate idle
	// activity from real traffic by missing padding.
	if bdc.transportConfig != nil {
		bdc.transportConfig.FillStreamRequest(req, "", "")
	}
	resp, err := bdc.client.Do(req)
	if err != nil {
		// The beacon doubles as the liveness probe for connections a CDN
		// edge silently dropped (no RST reaches us): feed the outcome back
		// so the pool rotates instead of lending a dead H2 session.
		c.NoteBeaconFailure()
		return
	}
	resp.Body.Close()
	c.NoteBeaconSuccess()
}

// retireLocked performs a hygiene retirement of c: transition Active→Draining
// (closing immediately when nothing holds it — downloadPins keep the transport
// open inside tryClose until the last pin releases), remove from the pool, and
// log the reason. Caller must hold m.pool.mu for writing. Fatal faults use
// MarkDead instead — retireLocked never force-closes a live transport.
func (m *XmuxManager) retireLocked(c *XmuxClient, reason string) {
	errors.LogDebug(context.Background(), "XMUX: retiring xmuxClient (", reason, ")")
	c.maybeDrain()
	m.pool.Remove(c)
}

// healthCheckTick performs one tick of health checking.
func (m *XmuxManager) healthCheckTick() {
	// Phase 1: Under write lock -> prune stale connections, compute migration need
	m.pool.mu.Lock()

	now := time.Now()
	for i := 0; i < len(m.pool.clients); {
		c := m.pool.clients[i]
		st := c.state.Load()

		// Draining: try to close, then remove if closed
		if st == StateDraining {
			c.tryClose()
			if c.state.Load() == StateClosed {
				errors.LogDebug(context.Background(), "XMUX: health-check removing drained xmuxClient")
				m.pool.RemoveAt(i)
				continue
			}
			// Still draining with active streams or pinned downloads -> skip
			i++
			continue
		}

		// Already closed (shouldn't be in pool, but handle defensively)
		if st == StateClosed {
			c.closeConn()
			m.pool.RemoveAt(i)
			continue
		}

		// Active state -> check if should be retired
		if c.XmuxConn.IsClosed() {
			errors.LogDebug(context.Background(), "XMUX: health-check removing closed xmuxClient")
			c.MarkDead()
			m.pool.RemoveAt(i)
			continue
		}
		if c.activeStreams.Load() > 0 {
			i++
			continue
		}
		// Idle activity: after a quiet period, emit a short beacon-style
		// request (simulating a browser's background traffic) so an idle
		// connection is not perfectly silent until eviction. Rate-limited
		// per connection; deliberately does NOT refresh LastUsed, so
		// idle eviction still follows business traffic.
		m.maybeIdleActivity(c, now)
		// Exhausted reuse budget / past lifetime: drain when idle.
		if c.remaining.Load() == 0 || c.expiredAt(now.UnixNano()) {
			m.retireLocked(c, "budget/lifetime exhausted")
			continue
		}
		// Idle eviction: only evict if NO active streams AND idle for too long.
		// B7: RTT-aware timeout — a rebuild costs ~1-2 RTT, so high-latency
		// links keep connections longer; low-latency links evict sooner.
		// Recomputed each tick so a link that degrades mid-session gets a
		// longer window without a fresh connection setup.
		if lastUsed := c.LastUsed.Load(); lastUsed > 0 {
			idleNs := c.idleTimeoutNs
			if rtt := c.GetRTT(); rtt > 0 {
				idleNs = rttAdjustedIdleNs(idleNs, rtt)
			}
			if now.UnixNano()-lastUsed > idleNs {
				m.retireLocked(c, "idle")
				// Lazy CDN keep-warm: the last connection just idled out of
				// an empty pool. Pre-pay one reconnect so the next user
				// request does not eat a full CDN cold start.
				if len(m.pool.clients) == 0 {
					m.scheduleWarmReconnect()
				}
				continue
			}
		}
		if c.createdAt.After(now.Add(-coldStartProtectionMs * time.Millisecond)) {
			i++
			continue
		}
		// Max connection age: drain connections that have lived too long.
		// Handles NAT/LB changes, server rotation, TLS session refresh.
		if now.Sub(c.createdAt) > maxConnectionAge {
			m.retireLocked(c, "max connection age")
			continue
		}
		if c.ShouldDrain() && c.confidence.Load() >= 30 {
			m.retireLocked(c, "quality degraded")
			continue
		}
		rttMs := c.GetRTT().Milliseconds()
		if rttMs >= maxRTTBeforeRemove && c.GetRTT() > 0 {
			m.retireLocked(c, "high RTT")
			continue
		}
		i++
	}

	effectiveConns := m.effectiveConnections()
	poolLen := len(m.pool.clients) // safe: caller holds write lock
	needNew := 0
	if effectiveConns > 0 {
		needNew = int(effectiveConns) - poolLen
	} else if poolLen == 0 {
		needNew = 1
	}

	m.pool.mu.Unlock()

	// Phase 2: Without lock -> network I/O (dialing)
	for i := 0; i < needNew; i++ {
		errors.LogDebug(context.Background(), "XMUX: migration creating xmuxClient, pool+=", i+1, "/", needNew)
		conn := m.newConnFunc()
		if conn != nil {
			m.addToPool(conn)
		} else {
			errors.LogWarning(context.Background(), "XMUX: migration dial failed, pool will be under-filled until next health-check")
		}
	}

	// V2.1: Update pool behavior from client observations (gated; see
	// xmuxPoolAutoScale). Disabled by default — skip the per-tick scan.
	if xmuxPoolAutoScale {
		snap := m.pool.Snapshot()
		var dominantBehavior quality.Behavior
		if len(snap) > 0 {
			behaviorCounts := make(map[quality.Behavior]int)
			totalKnown := 0
			badCount := 0

			for _, c := range snap {
				if b := c.GetBehavior(); b != quality.BehaviorUnknown {
					behaviorCounts[b]++
					totalKnown++
					if b == quality.BehaviorLossy || b == quality.BehaviorSaturated {
						badCount++
					}
				}
			}

			if totalKnown > 0 {
				maxCount := 0
				for b, count := range behaviorCounts {
					if count > maxCount {
						maxCount = count
						dominantBehavior = b
					}
				}
				badRatio := float64(badCount) / float64(totalKnown)
				if badRatio > 0.4 && dominantBehavior != quality.BehaviorLossy && dominantBehavior != quality.BehaviorSaturated {
					if behaviorCounts[quality.BehaviorSaturated] > 0 {
						dominantBehavior = quality.BehaviorSaturated
					} else {
						dominantBehavior = quality.BehaviorLossy
					}
				}
			}
		}
		if dominantBehavior != quality.BehaviorUnknown {
			m.UpdatePoolBehavior(dominantBehavior)
		}
	}
}

// Close stops the background goroutines and waits for them to finish.
func (m *XmuxManager) Close() {
	m.closeOnce.Do(func() {
		// Cancel any pending lazy CDN keep-warm reconnect first: the timer
		// callback checks stopCh too, but stopping the timer avoids firing
		// a goroutine at all during teardown.
		m.warmReconnectMu.Lock()
		if m.warmTimer != nil {
			m.warmTimer.Stop()
			m.warmTimer = nil
		}
		m.warmReconnectMu.Unlock()

		close(m.stopCh)

		// Wait for background goroutines with timeout
		select {
		case <-m.doneCh:
		case <-time.After(3 * time.Second):
			errors.LogDebug(context.Background(), "XMUX: Close timeout, forcing shutdown")
		}

		// Drain pool: stop profilers and close connections
		m.pool.mu.Lock()
		m.pool.CloseAll()
		m.pool.mu.Unlock()
	})
}

// V2.1: Dynamic Connection Scaling

// xmuxPoolAutoScale gates the pool-level AIMD/behavior control loop
// (UpdatePoolBehavior + dynamic connections/concurrency). It ships DISABLED
// pending a real-network A/B: on loopback, TCP_INFO quality is constant and
// behavior classification never leaves Unknown, so the loop only costs a
// per-tick voting scan. Flip to true (same-package tests may do so) to
// re-enable; if it stays unused the whole block is scheduled for removal.
var xmuxPoolAutoScale = false

const (
	// debounceThreshold is how many consecutive observations needed before switching behavior.
	debounceThreshold = 3
	// aimdStep is the additive increase step for connections/concurrency.
	aimdStep = 1
)

// UpdatePoolBehavior updates the pool's dominant behavior.
// Improvements require debounceThreshold consecutive observations before
// switching (oscillation prevention). Worsening takes effect IMMEDIATELY:
// the ~15s debounce is acceptable when the path improves, but waiting it
// out while the link is already lossy/saturated keeps the pool mis-sized
// exactly when isolation matters most.
func (m *XmuxManager) UpdatePoolBehavior(b quality.Behavior) {
	if !xmuxPoolAutoScale {
		return
	}
	m.poolBehaviorMu.Lock()
	defer m.poolBehaviorMu.Unlock()

	// Immediate reaction on degradation (including from Unknown).
	if isBehaviorWorsening(b, m.poolBehavior) {
		prevBehavior := m.poolBehavior
		m.streakBehavior = b
		m.behaviorStreak = 0
		m.poolBehavior = b
		m.applyAIMD(b, prevBehavior)
		return
	}

	if b == m.streakBehavior {
		m.behaviorStreak++
	} else {
		m.streakBehavior = b
		m.behaviorStreak = 1
	}

	// Only switch after debounce threshold
	if m.behaviorStreak >= debounceThreshold && b != m.poolBehavior {
		prevBehavior := m.poolBehavior // save old behavior before update
		m.poolBehavior = b
		m.behaviorStreak = 0
		// Apply AIMD adjustment on behavior change
		m.applyAIMD(b, prevBehavior)
	}
}

// applyAIMD adjusts effective connections/concurrency using AIMD.
// Additive Increase: when behavior improves, increase by step
// Multiplicative Decrease: when behavior worsens, multiply by 0.5
// Uses atomic Store for lock-free reads by GetXmuxClient.
func (m *XmuxManager) applyAIMD(b, prevBehavior quality.Behavior) {
	baseConns := m.connections
	baseConc := m.concurrency

	if !m.scaledOnce.Load() {
		// First time: set initial values based on behavior
		m._dynamicConns.Store(m.computeTargetConns(b, baseConns))
		m._dynamicConc.Store(m.computeTargetConc(b, baseConc))
		m.scaledOnce.Store(true)
		return
	}

	if isBehaviorImprovement(b, prevBehavior) {
		// Additive Adjustment toward target: step up when below, step
		// down when above (the old code only increased, so a pool that
		// had grown under Lossy never shrank back after the link
		// recovered).
		targetConns := m.computeTargetConns(b, baseConns)
		if cur := m._dynamicConns.Load(); cur < targetConns {
			m._dynamicConns.Store(cur + aimdStep)
		} else if cur > targetConns {
			m._dynamicConns.Store(cur - aimdStep)
		}
		targetConc := m.computeTargetConc(b, baseConc)
		if cur := m._dynamicConc.Load(); cur < targetConc {
			m._dynamicConc.Store(cur + aimdStep)
		} else if cur > targetConc {
			m._dynamicConc.Store(cur - aimdStep)
		}
	} else if isBehaviorWorsening(b, prevBehavior) {
		// Multiplicative Decrease — but toward the NEW behavior's targets,
		// not a blind halve of both dimensions. Reverse AIMD wants MORE
		// outer connections under loss/saturation (connection-level HoL
		// isolation + total cwnd) while CUTTING per-connection
		// concurrency; the old unconditional halve shrank the pool
		// exactly on the paths that need isolation most.
		m._dynamicConns.Store(m.computeTargetConns(b, baseConns))
		m._dynamicConc.Store(m.computeTargetConc(b, baseConc))
	}

	// Clamp to sane bounds
	if v := m._dynamicConns.Load(); v < 1 {
		m._dynamicConns.Store(1)
	}
	if baseConns > 0 {
		if v := m._dynamicConns.Load(); v > baseConns*2 {
			m._dynamicConns.Store(baseConns * 2)
		}
	}
	if v := m._dynamicConc.Load(); v < 1 {
		m._dynamicConc.Store(1)
	}
	if baseConc > 0 {
		if v := m._dynamicConc.Load(); v > baseConc*2 {
			m._dynamicConc.Store(baseConc * 2)
		}
	}
}

// computeTargetConns returns the ideal connection count for a behavior.
func (m *XmuxManager) computeTargetConns(b quality.Behavior, base int32) int32 {
	if base <= 0 {
		return 0
	}
	switch b {
	case quality.BehaviorLowLatency:
		delta := base / 2
		if delta < 1 {
			delta = 1 // ensure at least +1 for small base values
		}
		return base + delta
	case quality.BehaviorLossy, quality.BehaviorSaturated:
		// Reverse AIMD: on lossy/saturated paths MORE outer connections
		// isolate connection-level head-of-line blocking and add total
		// cwnd, while per-connection concurrency is cut (see
		// computeTargetConc) to shrink the blast radius of any single
		// packet loss. The old base/2 halved the pool exactly on the
		// paths that need isolation, hurting throughput under loss.
		// Steady-state stays bounded by the baseConns*2 clamp, so the
		// fingerprint surface is unchanged at rest.
		return base * 2
	case quality.BehaviorAggressive:
		return base * 2 / 3
	default:
		return base
	}
}

// computeTargetConc returns the ideal concurrency for a behavior.
func (m *XmuxManager) computeTargetConc(b quality.Behavior, base int32) int32 {
	if base <= 0 {
		return 0
	}
	switch b {
	case quality.BehaviorLowLatency:
		delta := base / 2
		if delta < 1 {
			delta = 1
		}
		return base + delta
	case quality.BehaviorLossy, quality.BehaviorSaturated:
		return base / 2
	case quality.BehaviorAggressive:
		return base * 3 / 4
	default:
		return base
	}
}

// isBehaviorImprovement returns true if b is "better" than prev.
func isBehaviorImprovement(b, prev quality.Behavior) bool {
	score := func(b quality.Behavior) int {
		switch b {
		case quality.BehaviorLowLatency:
			return 5
		case quality.BehaviorNormal:
			return 3
		case quality.BehaviorAggressive:
			return 2
		case quality.BehaviorLossy:
			return 1
		case quality.BehaviorSaturated:
			return 0
		default:
			return 3
		}
	}
	return score(b) > score(prev)
}

// isBehaviorWorsening returns true if b is "worse" than prev.
func isBehaviorWorsening(b, prev quality.Behavior) bool {
	return isBehaviorImprovement(prev, b)
}

// GetPoolBehavior returns the current dominant pool behavior.
func (m *XmuxManager) GetPoolBehavior() quality.Behavior {
	m.poolBehaviorMu.RLock()
	defer m.poolBehaviorMu.RUnlock()
	return m.poolBehavior
}

// effectiveConnections returns the pool's connection limit. With
// xmuxPoolAutoScale disabled this is the static config draw; the AIMD
// override only exists behind the gate.
func (m *XmuxManager) effectiveConnections() int32 {
	if !xmuxPoolAutoScale || !m.scaledOnce.Load() {
		return m.connections
	}
	return m._dynamicConns.Load()
}

// effectiveConcurrency returns the pool's per-connection stream limit.
// See effectiveConnections for the gate semantics.
func (m *XmuxManager) effectiveConcurrency() int32 {
	if !xmuxPoolAutoScale || !m.scaledOnce.Load() {
		return m.concurrency
	}
	return m._dynamicConc.Load()
}

// ForceAddClientsForTest appends n fresh clients without burst gating.
// Test/bench only: measures multi-conn selection cost on a filled pool.
func (m *XmuxManager) ForceAddClientsForTest(n int) {
	if m == nil || n <= 0 {
		return
	}
	for i := 0; i < n; i++ {
		m.pool.mu.Lock()
		_ = m.newXmuxClientLocked()
		m.pool.mu.Unlock()
	}
}

// PoolClientsForTest returns a snapshot of live pool clients for tests/benches.
// Do not mutate the slice; UpdateRTT / state on clients is fine.
func (m *XmuxManager) PoolClientsForTest() []*XmuxClient {
	if m == nil {
		return nil
	}
	return m.pool.Snapshot()
}

func (m *XmuxManager) newXmuxClient() *XmuxClient {
	m.pool.mu.Lock()
	defer m.pool.mu.Unlock()
	return m.newXmuxClientLocked()
}

// newXmuxClientLocked creates a new client and appends to the pool.
// Caller must hold m.pool.mu.
func (m *XmuxManager) newXmuxClientLocked() *XmuxClient {
	conn := m.newConnFunc()
	if conn == nil {
		return nil
	}
	xmuxClient := m.initNewClient(conn)
	m.pool.Append(xmuxClient)

	// Probe: send HEAD request to trigger real TCP/TLS connection.
	// If probeURL is empty, close ready immediately (no probe needed).
	if m.probeURL != "" {
		go m.probeConnection(conn, xmuxClient)
	} else {
		xmuxClient.markProbeReady()
		close(xmuxClient.ready) // no probe, mark as ready immediately
	}

	return xmuxClient
}

// probeConnection sends a HEAD request to trigger real dial through the transport.
// Closes ready channel when done (success or failure) to unblock waiting traffic.
func (m *XmuxManager) probeConnection(conn XmuxConn, xmuxClient *XmuxClient) {
	defer func() {
		xmuxClient.markProbeReady()
		close(xmuxClient.ready) // signal ready to waiting traffic
	}()

	dc, ok := conn.(DialerClient)
	if !ok {
		return
	}
	bdc, ok := dc.(*DefaultDialerClient)
	if !ok {
		return
	}
	// Manager already closed: skip HEAD noise (bench/teardown/global map cleanup).
	// Do not MarkDead solely for stop — pool CloseAll owns shutdown.
	select {
	case <-m.stopCh:
		return
	default:
	}
	// Dialer already torn down (MarkDead/Close): HEAD would only produce refused/
	// closed-socket logs and a redundant MarkDead. Health is already gone.
	if bdc.IsClosed() {
		return
	}
	// Cooldown after a streak of dial-dead failures (listen gone / migration).
	// Still close ready so traffic can proceed; skip MarkDead+log storms.
	if m.probeInCooldown() {
		return
	}
	u, err := url.Parse(m.probeURL)
	if err != nil {
		errors.LogDebug(context.Background(), "XMUX: probeConnection url.Parse failed: ", err)
		xmuxClient.probeErr = err
		xmuxClient.MarkDead() // Fast Eviction: probe failed, mark dead immediately
		return
	}
	// Bound by probeTimeout and cancel when manager closes so HEAD does not
	// keep dialing closed localhost ports after Close/bench teardown.
	probeCtx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()
	go func() {
		select {
		case <-m.stopCh:
			cancel()
		case <-probeCtx.Done():
		}
	}()
	req, err := http.NewRequestWithContext(probeCtx, "HEAD", u.String(), nil)
	if err != nil {
		errors.LogDebug(context.Background(), "XMUX: probeConnection NewRequest failed: ", err)
		xmuxClient.probeErr = err
		xmuxClient.MarkDead() // Fast Eviction: probe failed, mark dead immediately
		return
	}
	// Bray-only: health probes must carry the same X-Padding as data streams so
	// passive observers cannot separate probe HEAD from real traffic by missing
	// padding, and so the hub does not treat probes as invalid-padding probes.
	if bdc.transportConfig != nil {
		bdc.transportConfig.FillStreamRequest(req, "", "")
	}
	resp, err := bdc.client.Do(req)
	if err != nil {
		// Shutdown cancel / dialer close is not a live-path health signal.
		select {
		case <-m.stopCh:
			return
		default:
		}
		if bdc.IsClosed() {
			return
		}
		// Context canceled by stopCh race (cancel fired after Do returned).
		if stderrors.Is(err, context.Canceled) || stderrors.Is(err, context.DeadlineExceeded) {
			select {
			case <-m.stopCh:
				return
			default:
			}
		}
		// Repeated connect-refused / unreachable while target is gone: MarkDead
		// this client, rate-limit logs, and cool further HEAD probes.
		// Every probe failure must feed the same failure accounting: it advances
		// the streak (arming the cooldown after coolFailStreak) and rate-limits
		// the log line. Only the dial-dead branch used to do that, so a
		// persistent non-dial-dead error — an x509 certificate failure being the
		// common one — never engaged the cooldown and spun an unthrottled
		// connect → probe → MarkDead → retry loop: every connection was evicted
		// as soon as it was created, the pool never held a usable one, and the
		// outbound failed with "failed to find an available destination"
		// (2026.08.31 断流 report), along with an unbounded log storm.
		//
		// The error class is kept only to annotate the log, so operators can
		// tell "target gone" (refused/unreachable) from "handshake broken"
		// (x509) without re-deriving it from the message text.
		logIt := m.noteProbeFailure()
		if logIt {
			if isProbeDialDead(err) {
				errors.LogDebug(context.Background(), "XMUX: probeConnection Do failed (dial dead): ", err)
			} else {
				errors.LogDebug(context.Background(), "XMUX: probeConnection Do failed: ", err)
			}
		}
		xmuxClient.probeErr = err
		xmuxClient.MarkDead() // Fast Eviction: probe failed, mark dead immediately
		return
	}
	m.noteProbeSuccess()
	resp.Body.Close()
	m.detectCDN(resp.Header)
}

// cdnHeaderMatchers lists conservative response-header signatures of major
// CDN edges. Every pattern requires a value shape that is hard to forge by
// accident (cf-ray = 16-hex + airport code) so a misdirected probe against
// some origin server cannot flip the sticky CDN bit.
var cdnHeaderMatchers = []struct {
	name   string
	header string
	match  func(value string) bool
}{
	{"cloudflare", "Cf-Ray", func(v string) bool {
		// Format: <16 hex chars>-<airport code>, e.g. 8a2b...-SIN.
		i := strings.IndexByte(v, '-')
		if i != 16 || len(v) <= i+1 {
			return false
		}
		for _, c := range v[:16] {
			if !isHexDigit(c) {
				return false
			}
		}
		return true
	}},
	{"cloudfront", "X-Amz-Cf-Pop", func(v string) bool { return v != "" }},
	{"fastly", "X-Served-By", func(v string) bool {
		return strings.HasPrefix(v, "cache-")
	}},
}

func isHexDigit(c rune) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// detectCDN inspects one probe response for CDN edge signatures and latches
// the manager-wide sticky bit on a hit. Called after every successful probe;
// cheap no-op once latched (single atomic load).
func (m *XmuxManager) detectCDN(h http.Header) {
	if m.isCDN.Load() {
		return
	}
	for _, sig := range cdnHeaderMatchers {
		vals := h.Values(sig.header)
		for _, v := range vals {
			if sig.match(v) {
				if m.isCDN.CompareAndSwap(false, true) {
					errors.LogInfo(context.Background(), "XMUX: CDN edge detected via ", sig.name, " probe header, lazy keep-warm enabled")
				}
				return
			}
		}
	}
}

// scheduleWarmReconnect arranges one jittered background reconnect after the
// last pooled connection idled out, so a CDN path does not hand the next user
// request a full cold start. Best effort: never retries on failure (the next
// business request dials anyway), and a fresh eviction reschedules.
func (m *XmuxManager) scheduleWarmReconnect() {
	if !m.isCDN.Load() {
		return
	}
	select {
	case <-m.stopCh:
		return
	default:
	}

	m.warmReconnectMu.Lock()
	defer m.warmReconnectMu.Unlock()
	select {
	case <-m.stopCh:
		return
	default:
	}
	if m.warmInFlight.Load() {
		return // a reconnect is already scheduled or running
	}
	m.warmInFlight.Store(true)
	delay := time.Duration(biasedRangeRand(30, 120)) * time.Second
	warmTimer := time.AfterFunc(delay, func() {
		defer m.warmInFlight.Store(false)
		select {
		case <-m.stopCh:
			return
		default:
		}
		// Pool may have refilled meanwhile (user traffic or pre-connect):
		// only reconnect when it is still empty.
		m.pool.mu.RLock()
		empty := len(m.pool.clients) == 0
		m.pool.mu.RUnlock()
		if !empty {
			return
		}
		errors.LogDebug(context.Background(), "XMUX: CDN keep-warm reconnecting idle-drained pool")
		// newXmuxClient appends to the pool and starts its own HEAD probe
		// when a probeURL is configured — never probe it again here, or
		// probeConnection would close the ready channel twice.
		m.newXmuxClient()
	})
	m.warmTimer = warmTimer
}

// probeInCooldown reports whether further HEAD probes should be skipped.
func (m *XmuxManager) probeInCooldown() bool {
	m.probeFailMu.Lock()
	defer m.probeFailMu.Unlock()
	if m.probeCoolUntil.IsZero() {
		return false
	}
	return time.Now().Before(m.probeCoolUntil)
}

// noteProbeFailure records a probe failure of any class and may start cooldown.
// Returns true when the caller should emit a LogDebug (rate-limited).
//
// It must be called for every failed probe, not just dial-dead ones: the
// cooldown it arms is the only thing that throttles the
// connect → probe → MarkDead → retry cycle. Skipping it for non-dial-dead
// errors (e.g. x509) left that cycle unthrottled and evicted every connection
// (2026.08.31 断流 report).
func (m *XmuxManager) noteProbeFailure() (shouldLog bool) {
	const coolFailStreak = 3
	const coolFor = 2 * time.Second
	const logEvery = 2 * time.Second

	now := time.Now()
	m.probeFailMu.Lock()
	defer m.probeFailMu.Unlock()
	m.probeFailStreak++
	if m.probeFailStreak >= coolFailStreak {
		if m.probeCoolUntil.IsZero() || now.After(m.probeCoolUntil) {
			m.probeCoolUntil = now.Add(coolFor)
		}
	}
	if m.probeLastFailLog.IsZero() || now.Sub(m.probeLastFailLog) >= logEvery {
		m.probeLastFailLog = now
		return true
	}
	return false
}

// noteProbeSuccess resets dial-dead streak after a healthy HEAD.
func (m *XmuxManager) noteProbeSuccess() {
	m.probeFailMu.Lock()
	m.probeFailStreak = 0
	m.probeCoolUntil = time.Time{}
	m.probeFailMu.Unlock()
}

// isProbeDialDead reports connect-refused / unreachable style probe errors
// typical when the listen target disappeared mid-migration or bench teardown.
func isProbeDialDead(err error) bool {
	if err == nil {
		return false
	}
	if stderrors.Is(err, context.Canceled) || stderrors.Is(err, context.DeadlineExceeded) {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "connection refused") ||
		strings.Contains(s, "connectex") ||
		strings.Contains(s, "No connection could be made") ||
		strings.Contains(s, "forcibly closed") ||
		strings.Contains(s, "network is unreachable") ||
		strings.Contains(s, "no route to host") ||
		strings.Contains(s, "server misbehaving") ||
		strings.Contains(s, "i/o timeout")
}

// addToPool creates a new XmuxClient from an already-established conn and appends it to the pool.
func (m *XmuxManager) addToPool(conn XmuxConn) *XmuxClient {
	m.pool.mu.Lock()
	defer m.pool.mu.Unlock()
	return m.addToPoolLocked(conn)
}

// addToPoolLocked creates a new XmuxClient and appends to the pool.
// Caller must hold m.pool.mu. Returns the newly created client.
func (m *XmuxManager) addToPoolLocked(conn XmuxConn) *XmuxClient {
	m.RecordNewConn()
	xmuxClient := m.initNewClient(conn)
	m.pool.Append(xmuxClient)

	// Probe: send HEAD request to trigger real TCP/TLS connection.
	// If probeURL is empty, close ready immediately (no probe needed).
	if m.probeURL != "" {
		go m.probeConnection(conn, xmuxClient)
	} else {
		xmuxClient.markProbeReady()
		close(xmuxClient.ready) // no probe, mark as ready immediately
	}
	return xmuxClient
}

// initNewClient initializes a new XmuxClient with config-derived limits.
func (m *XmuxManager) initNewClient(conn XmuxConn) *XmuxClient {
	c := &XmuxClient{
		XmuxConn:  conn,
		createdAt: time.Now(),
		learner:   quality.NewNetworkLearner(),
		ready:     make(chan struct{}), // probe not yet completed
	}
	c.behaviorScale.Store(100) // BehaviorUnknown -> 1.0x
	// Per-connection skewed idle timeout (90-240s): short enough that idle
	// connections do not live forever (a machine signature), long enough to
	// survive video/viewing gaps.
	c.idleTimeoutNs = int64(biasedRangeRand(clientIdleTimeoutMinSecs, clientIdleTimeoutMaxSecs)) * int64(time.Second)
	// Single reuse budget = the tighter of the two configured request caps.
	// (CMaxReuseTimes historically counted Get-level reuses, HMaxRequestTimes
	// counted per-Dial stream opens; both are logical-wear counters and the
	// pool OR-ed them at exhaustion, so min() of the two reproduces today's
	// effective retirement point with one counter instead of two.)
	c.remaining.Store(-1)
	lim := int32(-1)
	if x := m.xmuxConfig.GetNormalizedCMaxReuseTimes().rand(); x > 0 {
		lim = x
	}
	if x := m.xmuxConfig.GetNormalizedHMaxRequestTimes().rand(); x > 0 && (lim < 0 || x < lim) {
		lim = x
	}
	if lim >= 0 {
		// Store lim-1: creation IS the first use (the caller dials this
		// transport immediately), matching the old Xray semantics where
		// CMaxReuseTimes=2 allowed exactly one GetXmuxClient reuse after
		// the create. Without this, every client serves one extra Get and
		// the pool rotates 1.5x slower (TestCMaxReuseTimes regression).
		c.remaining.Store(lim - 1)
	}
	if x := m.xmuxConfig.GetNormalizedHMaxReusableSecs(); x != nil && x.To > 0 {
		// Skewed lifetime draw: most connections rotate in 10-12min, a few
		// live 20min — a uniform 600-1200s spread is a machine signature.
		deadline := time.Now().Add(time.Duration(biasedRangeRand(x.From, x.To)) * time.Second)
		c.deadlineUnix.Store(deadline.UnixNano())
	}
	c.recomputeScore()
	return c
}

// pickBestXmuxClient scans candidates and returns the lowest selectionScore
// reusable client under concurrency/idle rules. overAdmit prefers lower
// inflight first, then score.
//
// Callers that unlocked the pool must re-validate the winner before use
// (budget charge + state) because the set can change after the snapshot.
func pickBestXmuxClient(candidates []*XmuxClient, n int, nowNs int64, effectiveConc int32, overAdmit bool) *XmuxClient {
	if n <= 0 {
		return nil
	}
	var best *XmuxClient
	bestScore := int64(math.MaxInt64)
	var bestInflight int32 = math.MaxInt32
	for i := 0; i < n; i++ {
		c := candidates[i]
		if c == nil || !xmuxClientReusable(c, nowNs) {
			continue
		}
		inf := c.activeStreams.Load()
		if !overAdmit {
			// Idle eviction: only skip if NO active streams AND idle for too long.
			if inf == 0 {
				if lastUsed := c.LastUsed.Load(); lastUsed > 0 && nowNs-lastUsed > c.idleTimeoutNs {
					continue
				}
			}
			if effectiveConc > 0 && inf >= effectiveConc {
				continue
			}
			if sc := selectionScoreWithInflight(c, inf); sc < bestScore {
				best = c
				bestScore = sc
			}
			continue
		}
		if !underOverAdmitCap(c, effectiveConc) {
			continue
		}
		sc := selectionScoreWithInflight(c, inf)
		if inf < bestInflight || (inf == bestInflight && sc < bestScore) {
			best = c
			bestInflight = inf
			bestScore = sc
		}
	}
	return best
}

func (m *XmuxManager) GetXmuxClient(ctx context.Context) (*XmuxClient, error) {
	// Bypass: always create new connection (for debugging XMUX issues)
	if forceNewConnection {
		conn := m.newConnFunc()
		if conn != nil {
			nowNs := time.Now().UnixNano()
			m.lastActivity.Store(nowNs)
			c := m.initNewClient(conn)
			// Wait for probe to complete before returning
			if err := c.WaitForReady(ctx); err != nil {
				errors.LogDebug(ctx, "XMUX: probe failed for new connection: ", err)
				return nil, fmt.Errorf("XMUX: probe failed: %w", err)
			}
			return c, nil
		}
		return nil, errors.New("XMUX: newConnFunc returned nil")
	}

	// Phase 1: reuse scan. The pool holds a handful of pointers, so a plain
	// copy under RLock is cheaper than any snapshot-pool micro-optimization.
	// Prune of dead/exhausted clients is owned by healthCheckTick (Bray-only).
	nowNs := time.Now().UnixNano()
	effectiveConns := m.effectiveConnections()
	effectiveConc := m.effectiveConcurrency()

	// overAdmit: when saturated past burst, reuse least-loaded Active client
	// instead of creating unbounded REALITY/TLS connections.
	scanPool := func(overAdmit bool) *XmuxClient {
		m.pool.mu.RLock()
		scan := make([]*XmuxClient, len(m.pool.clients))
		copy(scan, m.pool.clients)
		m.pool.mu.RUnlock()
		return pickBestXmuxClient(scan, len(scan), nowNs, effectiveConc, overAdmit)
	}

	for attempt := 0; attempt < 4; attempt++ {
		poolLen := m.pool.Len()
		if poolLen == 0 {
			break
		}
		// Prefer expanding until the base connection target is filled.
		if effectiveConns > 0 && poolLen < int(effectiveConns) {
			break
		}

		best := scanPool(false)
		if best == nil {
			// All Active clients at concurrency limit (or idle-evicted).
			// Soft-expand only up to burst; beyond that over-admit onto H2.
			burst := burstConnectionLimit(effectiveConns)
			if burst == 0 || poolLen < burst {
				break
			}
			best = scanPool(true)
			if best == nil {
				break
			}
		}

		// The pool can race with drain/close between scan and charge;
		// re-validate before spending budget.
		if !xmuxClientReusable(best, nowNs) {
			continue
		}

		// Charge one unit of the reuse budget — the single accounting site
		// for the whole dialer (per acquisition, not per POST/leg).
		if !best.acquireReuse() {
			continue
		}

		m.lastActivity.Store(nowNs)
		best.LastUsed.Store(nowNs)
		m.RecordReuseHit()
		// Warm reuse: probeDone is set; avoid WaitForReady call overhead.
		if best.probeDone.Load() {
			if err := best.probeErr; err != nil {
				// Debug: probe storms (e.g. x509) fire this per eviction;
				// Info here flooded error logs in info mode (~12 lines/request).
				errors.LogDebug(ctx, "XMUX: probe failed for existing connection, removing: ", err)
				m.pool.mu.Lock()
				m.pool.RemoveAndClose(best)
				m.pool.mu.Unlock()
				continue
			}
			return best, nil
		}
		if err := best.WaitForReady(ctx); err != nil {
			errors.LogDebug(ctx, "XMUX: probe failed for existing connection, removing: ", err)
			m.pool.mu.Lock()
			m.pool.RemoveAndClose(best)
			m.pool.mu.Unlock()
			continue
		}
		return best, nil
	}

	// Phase 2: Create new connection only when under burst cap.
	// Re-check pool size under lock so concurrent creators cannot stampede past burst.
	const maxAttempts = 2
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Burst gate before dial (best-effort under race).
		if burst := burstConnectionLimit(m.effectiveConnections()); burst > 0 {
			m.pool.mu.RLock()
			cur := len(m.pool.clients)
			scan := make([]*XmuxClient, cur)
			copy(scan, m.pool.clients)
			m.pool.mu.RUnlock()
			// Use effective concurrency hard-cap so a single wedged H2 cannot
			// absorb every Dial under burst saturation.
			least := pickBestXmuxClient(scan, len(scan), time.Now().UnixNano(), m.effectiveConcurrency(), true)
			if cur >= burst && least != nil {
				now := time.Now().UnixNano()
				m.lastActivity.Store(now)
				least.LastUsed.Store(now)
				m.RecordReuseHit()
				if err := least.WaitForReady(ctx); err != nil {
					errors.LogDebug(ctx, "XMUX: probe failed for burst-cap client, removing: ", err)
					m.pool.mu.Lock()
					m.pool.RemoveAndClose(least)
					m.pool.mu.Unlock()
					lastErr = fmt.Errorf("XMUX: probe failed: %w", err)
					continue
				}
				return least, nil
			}
		}

		conn := m.newConnFunc()
		if conn == nil {
			lastErr = errors.New("XMUX: newConnFunc returned nil")
			continue
		}
		c := m.addToPool(conn)
		m.lastActivity.Store(time.Now().UnixNano())
		if err := c.WaitForReady(ctx); err != nil {
			// Debug: this is the per-attempt detail of the probe storm; the
			// summarized cooldown/eviction state is what matters at Info+.
			errors.LogDebug(ctx, "XMUX: probe failed for new connection (attempt ", attempt+1, "), removing: ", err)
			m.pool.mu.Lock()
			m.pool.RemoveAndClose(c)
			m.pool.mu.Unlock()
			lastErr = fmt.Errorf("XMUX: probe failed: %w", err)
			continue
		}
		return c, nil
	}
	return nil, lastErr
}

// xmuxClientReusable reports whether a client may be scheduled by GetXmuxClient.
// Closed / exhausted / past-lifetime clients are skipped (pruned by health later).
// nowNs is unix nano from the Get hot path (avoids time.Time comparisons under RLock).
func xmuxClientReusable(c *XmuxClient, nowNs int64) bool {
	if c == nil || c.state.Load() != StateActive {
		return false
	}
	if c.XmuxConn != nil && c.XmuxConn.IsClosed() {
		return false
	}
	if c.remaining.Load() == 0 || c.expiredAt(nowNs) {
		return false
	}
	return true
}

// exhausted reports whether the connection's reuse budget is used up.
func (c *XmuxClient) exhausted() bool {
	return c.remaining.Load() == 0
}

// expiredAt reports whether the connection's lifetime deadline has passed.
// 0 = unlimited lifetime. nowNs is unix nano.
func (c *XmuxClient) expiredAt(nowNs int64) bool {
	until := c.deadlineUnix.Load()
	return until > 0 && nowNs > until
}

// acquireReuse atomically charges one unit of the reuse budget for one
// stream acquisition. Returns false when the budget is exhausted, in which
// case the connection is drained for the health check to retire. The
// caller holds m.pool.mu (read or write), serializing scanners; the CAS
// loop still guards the drain-free paths.
func (c *XmuxClient) acquireReuse() bool {
	for {
		old := c.remaining.Load()
		if old == 0 {
			c.maybeDrain()
			return false
		}
		if old < 0 {
			return true // unlimited
		}
		if c.remaining.CompareAndSwap(old, old-1) {
			return true
		}
	}
}

// overAdmitHardCap is the absolute max activeStreams allowed on one client
// when over-admitting past effectiveConcurrency. 0 concurrency means unlimited.
func overAdmitHardCap(effectiveConc int32) int32 {
	if effectiveConc <= 0 {
		return 0
	}
	capN := effectiveConc * overAdmitHardMult
	if capN < overAdmitHardFloor {
		capN = overAdmitHardFloor
	}
	return capN
}

// underOverAdmitCap reports whether client c may take another over-admitted stream.
func underOverAdmitCap(c *XmuxClient, effectiveConc int32) bool {
	if c == nil {
		return false
	}
	capN := overAdmitHardCap(effectiveConc)
	if capN <= 0 {
		return true
	}
	return c.activeStreams.Load() < capN
}

// selectionScoreWithInflight folds a already-loaded inflight into the base score
// so multi-pool scans do not pay a second activeStreams.Load per candidate.
func selectionScoreWithInflight(c *XmuxClient, inflight int32) int64 {
	inf := int64(inflight)
	if inf > 1_000_000 {
		inf = 1_000_000
	}
	return c.cachedScore.Load() + inf*10000
}

// scoreClientBase computes the non-inflight part of the scheduling score.
// Stored in cachedScore; combined with inflight at selection time.
//
// V2.1 formula (behavior-aware, confidence-weighted, fixed-point integer):
//
//	total = inflight * 10000 + base
//	base = rttMs * 10 + qualityPen + retrans/loss penalties
//	retransPenalty = retransCount * 50 * combinedFixed / 10000
//	lossPenalty = lossRate * combinedFixed / (20 * 10000)
//	combinedFixed = confidenceFixed * behaviorFixed / 100
//
// Fixed-point scales: confidence x100, behavior x100, combined x10000.
func scoreClientBase(c *XmuxClient) int64 {
	rttMs := c.GetRTT().Milliseconds()
	if rttMs == 0 {
		rttMs = 100 // default 100ms for unsampled connections
	} else if rttMs > 999 {
		rttMs = 999 // cap at 999ms to prevent score inversion
	}

	score := rttMs * 10

	// Bray quality term: lower qualityScore (0-100) raises scheduling cost.
	// Weight modestly so RTT/inflight remain primary; confidence scales it.
	qs := int64(c.qualityScore.Load())
	if qs < 0 {
		qs = 0
	} else if qs > 100 {
		qs = 100
	}
	// Only apply when confidence is non-trivial (probe has run).
	if conf0 := int64(c.confidence.Load()); conf0 >= 20 {
		qPen := (100 - qs) * 20 // 0..2000
		if conf0 < 80 {
			qPen = qPen * conf0 / 80
		}
		score += qPen
	}

	// V2.0: confidence-weighted penalties (fixed-point x100)
	conf := int64(c.confidence.Load())
	var confidenceFixed int64
	switch {
	case conf >= 80:
		confidenceFixed = 100
	case conf >= 30:
		confidenceFixed = 20 + (conf-30)*2
	default:
		confidenceFixed = 20
	}

	// V2.1: cached behavior scale (x100); avoids learner mutex on hot recompute.
	behaviorFixed := c.behaviorScale.Load()
	if behaviorFixed <= 0 {
		behaviorFixed = 100
	}

	// Combined scale = confidence x behavior (fixed-point x10000)
	combinedFixed := confidenceFixed * behaviorFixed

	// Retrans penalty: each retrans costs 50 points
	retrans := int64(c.lastRetrans.Load())
	if retrans > 100 {
		retrans = 100 // cap
	}
	score += retrans * 50 * combinedFixed / 10000

	// Loss penalty: lossRate is fixed-point x 10000 (0-10000)
	lossRate := c.lastLoss.Load()
	score += lossRate * combinedFixed / (20 * 10000)

	return score
}

// scoreClient returns the full scheduling score (live base + inflight).
// Used by tests/debug; hot Get path uses selectionScore (cached base).
func scoreClient(c *XmuxClient) int64 {
	inflight := int64(c.activeStreams.Load())
	if inflight > 1_000_000 {
		inflight = 1_000_000
	}
	return scoreClientBase(c) + inflight*10000
}

// behaviorPenaltyScaleFixed returns a fixed-point multiplier (×100) for penalties
// based on detected behavior.
func behaviorPenaltyScaleFixed(b quality.Behavior) int64 {
	switch b {
	case quality.BehaviorLowLatency:
		return 50 // 0.5 × 100
	case quality.BehaviorAggressive:
		return 120 // 1.2 × 100
	case quality.BehaviorLossy, quality.BehaviorSaturated:
		return 150 // 1.5 × 100
	default:
		return 100 // 1.0 × 100
	}
}

// Metrics methods for quantifiable validation

// RecordReuseHit records a connection reuse (pool hit).
func (m *XmuxManager) RecordReuseHit() {
	m.metrics.reuseHit.Add(1)
}

// RecordNewConn records a new connection creation.
func (m *XmuxManager) RecordNewConn() {
	m.metrics.newConn.Add(1)
}

// RecordTTFB records a Time-To-First-Byte measurement.
func (m *XmuxManager) RecordTTFB(ttfb time.Duration) {
	ns := int64(ttfb)
	m.metrics.ttfbSum.Add(ns)
	m.metrics.ttfbCount.Add(1)

	// Update max atomically
	for {
		old := m.metrics.ttfbMax.Load()
		if ns <= old {
			break
		}
		if m.metrics.ttfbMax.CompareAndSwap(old, ns) {
			break
		}
	}
}

// GetMetrics returns current metrics snapshot.
func (m *XmuxManager) GetMetrics() XmuxMetrics {
	reuseHit := m.metrics.reuseHit.Load()
	newConn := m.metrics.newConn.Load()

	totalConns := reuseHit + newConn
	reuseRate := float64(0)
	if totalConns > 0 {
		reuseRate = float64(reuseHit) / float64(totalConns) * 100
	}

	ttfbCount := m.metrics.ttfbCount.Load()
	var avgTTFB time.Duration
	if ttfbCount > 0 {
		avgTTFB = time.Duration(m.metrics.ttfbSum.Load() / ttfbCount)
	}

	return XmuxMetrics{
		ReuseHit:    reuseHit,
		NewConn:     newConn,
		ReuseRate:   reuseRate,
		AvgTTFB:     avgTTFB,
		MaxTTFB:     time.Duration(m.metrics.ttfbMax.Load()),
		TTFBSamples: ttfbCount,
		NetRecovery: m.metrics.netRecoveryCount.Load(),
	}
}

// XmuxMetrics holds quantifiable metrics for validation.
type XmuxMetrics struct {
	ReuseHit    int64         // Connection reuse count
	NewConn     int64         // New connection count
	ReuseRate   float64       // Reuse percentage (0-100)
	AvgTTFB     time.Duration // Average TTFB
	MaxTTFB     time.Duration // Max TTFB observed
	TTFBSamples int64         // Number of TTFB samples
	NetRecovery int64         // Number of network change events
}

// String returns a human-readable metrics summary.
func (m XmuxMetrics) String() string {
	return fmt.Sprintf(
		"XMUX Metrics:\n"+
			"  Reuse Rate: %.1f%% (%d/%d)\n"+
			"  Avg TTFB: %v, Max TTFB: %v (samples: %d)\n"+
			"  Network Recoveries: %d",
		m.ReuseRate, m.ReuseHit, m.ReuseHit+m.NewConn,
		m.AvgTTFB, m.MaxTTFB, m.TTFBSamples,
		m.NetRecovery,
	)
}

// IdleFor returns how long since this manager last served a client request.
// Returns 0 if there are active connections in the pool (preventing cleanup of busy managers).
// Used by the global cleanup goroutine to detect idle managers.
func (m *XmuxManager) IdleFor() time.Duration {
	// If there are active connections, the manager is not idle
	if m.pool.Len() > 0 {
		snap := m.pool.Snapshot()
		for _, c := range snap {
			if c.activeStreams.Load() > 0 {
				return 0 // Not idle -> has active streams
			}
		}
	}

	ts := m.lastActivity.Load()
	if ts == 0 {
		return 0
	}
	return time.Since(time.Unix(0, ts))
}
