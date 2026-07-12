package splithttp

import (
	"context"
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

	// clientIdleTimeout is the maximum time a client can remain unused (no active streams)
	// before being evicted. This is a safety net, not the primary failure recovery mechanism.
	// Primary mechanism: Fast Eviction (immediate eviction on fatal errors).
	// 120s balances ISP NAT timeouts (30-300s) and Go's IdleConnTimeout (90s).
	clientIdleTimeout = 120 * time.Second

	// probeTimeout is the maximum time to wait for a connection probe (HEAD request)
	// to complete. If the probe doesn't finish within this time, the connection is
	// considered broken and removed from the pool.
	probeTimeout = 10 * time.Second
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
	leftUsage     atomic.Int32
	LeftRequests  atomic.Int32
	UnreusableAt  time.Time
	createdAt     time.Time
	LastUsed      atomic.Int64 // unix nano: last time this client was borrowed
	lastRTT       atomic.Int64 // nanoseconds, for RTT-aware scheduling
	cachedScore   atomic.Int64 // pre-computed scheduling score, updated on state change

	// Ready Promise: blocks concurrent traffic until probe completes
	ready    chan struct{} // closed when probe finishes (success or failure)
	probeErr error         // set if probe failed

	// V2.0: link-quality metrics for smarter scheduling
	lastRetrans  atomic.Int32 // cumulative retransmit count from TCP_INFO
	lastLoss     atomic.Int64 // loss rate × 10000 (fixed-point, 0-10000)
	qualityScore atomic.Int32 // 0-100, computed by TransportProfile
	confidence   atomic.Int32 // 0-100, how much we trust the quality data
	consecDrops  atomic.Int32 // consecutive quality drops, for drain

	// V2.1: behavior learning for adaptive scheduling
	learner *quality.NetworkLearner // tracks link behavior patterns

	// TransportProfile for this connection. Created when TCP connection is established.
	profileMu sync.Mutex          // protects profile field
	profile   interface{ Stop() } // *tcpinfo.Profile, stored as interface to avoid import cycle

	// closeOnce ensures the underlying XmuxConn is closed at most once.
	closeOnce sync.Once
}

// WaitForReady blocks until probe completes or context is cancelled.
// Returns probeErr if probe failed.
func (c *XmuxClient) WaitForReady(ctx context.Context) error {
	select {
	case <-c.ready:
		return c.probeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Borrow atomically attempts to reserve a stream slot on this connection.
// Returns true only if state is Active and the increment took effect while still Active.
// This prevents the race: GetXmuxClient returns client → Draining fires → stream added to retired connection.
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
		c.recomputeScore()
		return true
	}
}

// Release marks one active stream as finished and tries to close if draining.
func (c *XmuxClient) Release() {
	c.activeStreams.Add(-1)
	c.recomputeScore()
	c.tryClose()
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
func (c *XmuxClient) tryClose() {
	if c.state.Load() != StateDraining {
		return
	}
	if c.activeStreams.Load() > 0 {
		return
	}
	if c.state.CompareAndSwap(StateDraining, StateClosed) {
		c.closeConn()
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
	}

	c.recomputeScore()
}

// recomputeScore recalculates the cached scheduling score from current metrics.
func (c *XmuxClient) recomputeScore() {
	c.cachedScore.Store(scoreClient(c))
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
	xmuxConfig   XmuxConfig
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
	scaledOnce     bool             // whether dynamic scaling has been applied at least once

	// Dynamic warmup queue (reserved for future use)
	warmupMu     sync.Mutex
	netHash      string // current network hash for change detection
	lastNetCheck time.Time

	// Network change debouncing: require 2 consecutive observations to confirm
	pendingNetChange      string // pending network hash change awaiting confirmation
	pendingNetChangeCount int    // number of consecutive observations of the same change

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

func NewXmuxManager(xmuxConfig XmuxConfig, newConnFunc func() XmuxConn) *XmuxManager {
	m := &XmuxManager{
		xmuxConfig:   xmuxConfig,
		concurrency:  xmuxConfig.GetNormalizedMaxConcurrency().rand(),
		connections:  xmuxConfig.GetNormalizedMaxConnections().rand(),
		newConnFunc:  newConnFunc,
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
		lastActivity: atomic.Int64{},
	}

	// Start background goroutines for connection management.
	go func() {
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errors.LogDebug(context.Background(), "XMUX: preConnectLoop recovered panic: ", r)
				}
			}()
			m.preConnectLoop()
		}()
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errors.LogDebug(context.Background(), "XMUX: healthCheckLoop recovered panic: ", r)
				}
			}()
			m.healthCheckLoop()
		}()
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errors.LogDebug(context.Background(), "XMUX: networkWatchLoop recovered panic: ", r)
				}
			}()
			m.networkWatchLoop()
		}()
		wg.Wait()
		close(m.doneCh)
	}()

	return m
}

// preConnectLoop establishes initial pool connections using exponential backoff.
// Fully async — never blocks on probe completion. Health check loop handles cleanup.
func (m *XmuxManager) preConnectLoop() {
	if m.pool.Len() == 0 {
		errors.LogDebug(context.Background(), "XMUX: pre-connect creating xmuxClient (initial)")
		go m.newXmuxClient()
		// Brief pause to let the connection establish before returning.
		// Not a probe wait — just enough time for TCP+TLS to complete locally.
		time.Sleep(100 * time.Millisecond)
	}

	// Optionally fill one more connection to ensure robustness, but do NOT wait.
	if m.pool.Len() < 2 {
		errors.LogDebug(context.Background(), "XMUX: pre-connect filling pool")
		go m.newXmuxClient()
	}
}

// networkWatchLoop monitors network changes and triggers warmup.
func (m *XmuxManager) networkWatchLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
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

	// Network hash changed — check if this is a new change or continuation
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
	maxConnectionAge = 30 * time.Minute
)

// healthCheckLoop periodically checks connection health and removes unhealthy ones.
// Improvements over original:
// - Cold start protection: new connections (<10s) are not removed
// - RTT-based: only remove if RTT > 5000ms (not just IsClosed)
// - Does not remove active connections with low Running
func (m *XmuxManager) healthCheckLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.healthCheckTick()
		}
	}
}

// healthCheckTick performs one tick of health checking.
func (m *XmuxManager) healthCheckTick() {
	// Phase 1: Under write lock — prune stale connections, compute migration need
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
			// Still draining with active streams — skip
			i++
			continue
		}

		// Already closed (shouldn't be in pool, but handle defensively)
		if st == StateClosed {
			c.closeConn()
			m.pool.RemoveAt(i)
			continue
		}

		// Active state — check if should be retired
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
		// Idle eviction: only evict if NO active streams AND idle for too long
		if lastUsed := c.LastUsed.Load(); lastUsed > 0 && now.Sub(time.Unix(0, lastUsed)) > clientIdleTimeout {
			errors.LogDebug(context.Background(), "XMUX: health-check removing idle xmuxClient, lastUsed=", time.Unix(0, lastUsed).Format(time.RFC3339))
			c.maybeDrain()
			m.pool.RemoveAt(i)
			continue
		}
		if c.createdAt.After(now.Add(-coldStartProtectionMs * time.Millisecond)) {
			i++
			continue
		}
		// Max connection age: drain connections that have lived too long.
		// Handles NAT/LB changes, server rotation, TLS session refresh.
		if now.Sub(c.createdAt) > maxConnectionAge {
			errors.LogDebug(context.Background(), "XMUX: health-check draining aged xmuxClient, age=", now.Sub(c.createdAt).Round(time.Second))
			c.maybeDrain()
			m.pool.RemoveAt(i)
			continue
		}
		if c.ShouldDrain() && c.confidence.Load() >= 30 {
			errors.LogDebug(context.Background(), "XMUX: health-check draining quality-degraded xmuxClient, consecutiveDrops=", c.consecDrops.Load())
			c.maybeDrain()
			m.pool.RemoveAt(i)
			continue
		}
		rttMs := c.GetRTT().Milliseconds()
		if rttMs >= maxRTTBeforeRemove && c.GetRTT() > 0 {
			errors.LogDebug(context.Background(), "XMUX: health-check removing high-RTT xmuxClient, rtt=", rttMs, "ms")
			c.maybeDrain()
			m.pool.RemoveAt(i)
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

	// Phase 2: Without lock — network I/O (dialing)
	for i := 0; i < needNew; i++ {
		errors.LogDebug(context.Background(), "XMUX: migration creating xmuxClient, pool+=", i+1, "/", needNew)
		conn := m.newConnFunc()
		if conn != nil {
			m.addToPool(conn)
		} else {
			errors.LogWarning(context.Background(), "XMUX: migration dial failed, pool will be under-filled until next health-check")
		}
	}

	// V2.1: Update pool behavior from client observations
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

// Close stops the background goroutines and waits for them to finish.
func (m *XmuxManager) Close() {
	m.closeOnce.Do(func() {
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

const (
	// debounceThreshold is how many consecutive observations needed before switching behavior.
	debounceThreshold = 3
	// aimdStep is the additive increase step for connections/concurrency.
	aimdStep = 1
)

// UpdatePoolBehavior updates the pool's dominant behavior with debouncing.
// Requires debounceThreshold consecutive observations of the same behavior before switching.
func (m *XmuxManager) UpdatePoolBehavior(b quality.Behavior) {
	m.poolBehaviorMu.Lock()
	defer m.poolBehaviorMu.Unlock()

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

	if !m.scaledOnce {
		// First time: set initial values based on behavior
		m._dynamicConns.Store(m.computeTargetConns(b, baseConns))
		m._dynamicConc.Store(m.computeTargetConc(b, baseConc))
		m.scaledOnce = true
		return
	}

	if isBehaviorImprovement(b, prevBehavior) {
		// Additive Increase: move toward target by 1 step
		targetConns := m.computeTargetConns(b, baseConns)
		if cur := m._dynamicConns.Load(); cur < targetConns {
			m._dynamicConns.Store(cur + aimdStep)
		}
		targetConc := m.computeTargetConc(b, baseConc)
		if cur := m._dynamicConc.Load(); cur < targetConc {
			m._dynamicConc.Store(cur + aimdStep)
		}
	} else if isBehaviorWorsening(b, prevBehavior) {
		// Multiplicative Decrease: halve immediately
		m._dynamicConns.Store(m._dynamicConns.Load() / 2)
		m._dynamicConc.Store(m._dynamicConc.Load() / 2)
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
		return base / 2
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

// effectiveConnections returns the AIMD-smoothed connection limit.
// Lock-free read via atomic.Load — safe for concurrent callers.
func (m *XmuxManager) effectiveConnections() int32 {
	m.poolBehaviorMu.RLock()
	scaled := m.scaledOnce
	m.poolBehaviorMu.RUnlock()
	if !scaled {
		return m.connections
	}
	return m._dynamicConns.Load()
}

// effectiveConcurrency returns the AIMD-smoothed concurrency limit.
// Lock-free read via atomic.Load — safe for concurrent callers.
func (m *XmuxManager) effectiveConcurrency() int32 {
	m.poolBehaviorMu.RLock()
	scaled := m.scaledOnce
	m.poolBehaviorMu.RUnlock()
	if !scaled {
		return m.concurrency
	}
	return m._dynamicConc.Load()
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
		close(xmuxClient.ready) // no probe, mark as ready immediately
	}

	return xmuxClient
}

// probeConnection sends a HEAD request to trigger real dial through the transport.
// Closes ready channel when done (success or failure) to unblock waiting traffic.
func (m *XmuxManager) probeConnection(conn XmuxConn, xmuxClient *XmuxClient) {
	defer close(xmuxClient.ready) // signal ready to waiting traffic

	dc, ok := conn.(DialerClient)
	if !ok {
		return
	}
	bdc, ok := dc.(*DefaultDialerClient)
	if !ok {
		return
	}
	u, err := url.Parse(m.probeURL)
	if err != nil {
		errors.LogDebug(context.Background(), "XMUX: probeConnection url.Parse failed: ", err)
		xmuxClient.probeErr = err
		xmuxClient.MarkDead() // Fast Eviction: probe failed, mark dead immediately
		return
	}
	probeCtx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, "HEAD", u.String(), nil)
	if err != nil {
		errors.LogDebug(context.Background(), "XMUX: probeConnection NewRequest failed: ", err)
		xmuxClient.probeErr = err
		xmuxClient.MarkDead() // Fast Eviction: probe failed, mark dead immediately
		return
	}
	resp, err := bdc.client.Do(req)
	if err != nil {
		errors.LogDebug(context.Background(), "XMUX: probeConnection Do failed: ", err)
		xmuxClient.probeErr = err
		xmuxClient.MarkDead() // Fast Eviction: probe failed, mark dead immediately
		return
	}
	resp.Body.Close()
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
	c.leftUsage.Store(-1)
	if x := m.xmuxConfig.GetNormalizedCMaxReuseTimes().rand(); x > 0 {
		c.leftUsage.Store(x - 1)
	}
	c.LeftRequests.Store(math.MaxInt32)
	if x := m.xmuxConfig.GetNormalizedHMaxRequestTimes().rand(); x > 0 {
		c.LeftRequests.Store(x)
	}
	if x := m.xmuxConfig.GetNormalizedHMaxReusableSecs().rand(); x > 0 {
		c.UnreusableAt = time.Now().Add(time.Duration(x) * time.Second)
	}
	c.recomputeScore()
	return c
}

func (m *XmuxManager) GetXmuxClient(ctx context.Context) (*XmuxClient, error) {
	// Bypass: always create new connection (for debugging XMUX issues)
	if forceNewConnection {
		conn := m.newConnFunc()
		if conn != nil {
			m.lastActivity.Store(time.Now().UnixNano())
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

	// Phase 1: Read lock — prune stale clients and find best candidate
	m.pool.mu.RLock()

	for i := 0; i < len(m.pool.clients); {
		c := m.pool.clients[i]

		// Skip clients not in Active state
		if c.state.Load() != StateActive {
			m.pool.mu.RUnlock()
			m.pool.mu.Lock()
			if i < len(m.pool.clients) && m.pool.clients[i] == c {
				c.closeConn()
				m.pool.RemoveAt(i)
			}
			m.pool.mu.Unlock()
			m.pool.mu.RLock()
			continue
		}

		if c.XmuxConn.IsClosed() ||
			c.leftUsage.Load() == 0 ||
			c.LeftRequests.Load() <= 0 ||
			(c.UnreusableAt != time.Time{} && time.Now().After(c.UnreusableAt)) {
			// Must upgrade to write lock for removal
			m.pool.mu.RUnlock()
			m.pool.mu.Lock()
			// Re-check after acquiring write lock (another goroutine may have removed it)
			if i < len(m.pool.clients) && m.pool.clients[i] == c {
				errors.LogDebug(ctx, "XMUX: removing xmuxClient, IsClosed() = ", c.XmuxConn.IsClosed(),
					", state = ", c.state.Load(),
					", activeStreams = ", c.activeStreams.Load(),
					", leftUsage = ", c.leftUsage.Load(),
					", LeftRequests = ", c.LeftRequests.Load(),
					", UnreusableAt = ", c.UnreusableAt)
				if c.XmuxConn.IsClosed() {
					c.MarkDead()
				} else {
					c.maybeDrain()
				}
				m.pool.RemoveAt(i)
			}
			m.pool.mu.Unlock()
			m.pool.mu.RLock()
			// Don't increment i — re-check this index (new element shifted in)
			continue
		}
		i++
	}

	effectiveConns := m.effectiveConnections()
	poolLen := len(m.pool.clients)
	m.pool.mu.RUnlock()

	needNew := false
	if poolLen == 0 {
		needNew = true
	} else if effectiveConns > 0 && poolLen < int(effectiveConns) {
		needNew = true
	}

	if !needNew {
		effectiveConc := m.effectiveConcurrency()

		// Snapshot-based selection: read-only iteration under RLock
		snap := m.pool.Snapshot()
		var best *XmuxClient
		bestScore := int64(math.MaxInt64)
		now := time.Now().UnixNano()
		for _, c := range snap {
			if c.state.Load() != StateActive {
				continue
			}
			// Idle eviction: only evict if NO active streams AND idle for too long.
			// This prevents killing WebSocket/gRPC connections that have long gaps between requests.
			if c.activeStreams.Load() == 0 {
				if lastUsed := c.LastUsed.Load(); lastUsed > 0 && now-lastUsed > int64(clientIdleTimeout) {
					continue
				}
			}
			if effectiveConc > 0 && c.activeStreams.Load() >= effectiveConc {
				continue
			}
			if s := c.cachedScore.Load(); s < bestScore {
				best = c
				bestScore = s
			}
		}

		if best == nil {
			needNew = true
		} else {
			// CAS loop to atomically decrement leftUsage
			acquired := false
			for {
				old := best.leftUsage.Load()
				if old == 0 {
					best.maybeDrain()
					// Re-snapshot and find next best
					snap = m.pool.Snapshot()
					best = nil
					bestScore = int64(math.MaxInt64)
					for _, c := range snap {
						if c.state.Load() != StateActive {
							continue
						}
						if effectiveConc > 0 && c.activeStreams.Load() >= effectiveConc {
							continue
						}
						if s := c.cachedScore.Load(); s < bestScore {
							best = c
							bestScore = s
						}
					}
					if best == nil {
						needNew = true
						break
					}
					continue
				}
				if old < 0 {
					acquired = true
					break
				}
				if best.leftUsage.CompareAndSwap(old, old-1) {
					acquired = true
					break
				}
			}

			if acquired {
				m.lastActivity.Store(time.Now().UnixNano())
				best.LastUsed.Store(time.Now().UnixNano())
				m.RecordReuseHit()
				// Wait for probe to complete before returning existing connection
				if err := best.WaitForReady(ctx); err != nil {
					errors.LogInfo(ctx, "XMUX: probe failed for existing connection, removing: ", err)
					// Remove broken connection from pool and close transport
					m.pool.mu.Lock()
					m.pool.RemoveAndClose(best)
					m.pool.mu.Unlock()
					// Fall through to Phase 2 to create new connection
				} else {
					return best, nil
				}
			}
		}
	}

	// Phase 2: Create new connection (no lock held)
	// On probe failure, remove broken connection and retry once.
	const maxAttempts = 2
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		errors.LogDebug(ctx, "XMUX: creating xmuxClient (attempt ", attempt+1, ")")

		conn := m.newConnFunc()
		if conn == nil {
			lastErr = errors.New("XMUX: newConnFunc returned nil")
			continue
		}
		c := m.addToPool(conn)
		m.lastActivity.Store(time.Now().UnixNano())

		// Wait for probe to complete before returning new connection
		if err := c.WaitForReady(ctx); err != nil {
			errors.LogInfo(ctx, "XMUX: probe failed for new connection (attempt ", attempt+1, "), removing: ", err)
			// Remove broken connection from pool and close transport
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

// scoreClient computes a scheduling score for a connection.
// Lower score = better candidate.
//
// V2.1 formula (behavior-aware, confidence-weighted, fixed-point integer):
//
//	base = inflight * 10000 + rttMs * 10
//	retransPenalty = retransCount * 50 * combinedFixed / 10000
//	lossPenalty = lossRate * combinedFixed / (20 * 10000)
//	combinedFixed = confidenceFixed * behaviorFixed / 100
//
// Fixed-point scales: confidence ×100, behavior ×100, combined ×10000.
// Max combinedFixed = 100 * 150 = 15000. Max retrans = 100.
// Max intermediate = 100 * 50 * 15000 = 75,000,000 — fits in int64.
func scoreClient(c *XmuxClient) int64 {
	inflight := int64(c.activeStreams.Load())
	rttMs := c.GetRTT().Milliseconds()
	if rttMs == 0 {
		rttMs = 100 // default 100ms for unsampled connections
	} else if rttMs > 999 {
		rttMs = 999 // cap at 999ms to prevent score inversion
	}

	// Saturating arithmetic: cap inflight contribution to prevent overflow.
	// Max intermediate = 100 * 50 * 15000 = 75,000,000 — fits in int64.
	// But inflight*10000 could overflow if inflight > 2^53/10000 ≈ 900 trillion.
	// In practice inflight is small, but cap defensively at 1M.
	if inflight > 1_000_000 {
		inflight = 1_000_000
	}

	score := inflight*10000 + rttMs*10

	// V2.0: confidence-weighted penalties (fixed-point ×100)
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

	// V2.1: behavior-aware penalty scaling (fixed-point ×100)
	behaviorFixed := behaviorPenaltyScaleFixed(c.GetBehavior())

	// Combined scale = confidence × behavior (fixed-point ×10000)
	combinedFixed := confidenceFixed * behaviorFixed

	// Retrans penalty: each retrans costs 50 points
	retrans := int64(c.lastRetrans.Load())
	if retrans > 100 {
		retrans = 100 // cap
	}
	score += retrans * 50 * combinedFixed / 10000

	// Loss penalty: lossRate is fixed-point × 10000 (0-10000)
	lossRate := c.lastLoss.Load()
	score += lossRate * combinedFixed / (20 * 10000)

	return score
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

// LogMetrics logs current metrics at Info level.
func (m *XmuxManager) LogMetrics() {
	metrics := m.GetMetrics()
	errors.LogInfo(context.Background(), metrics.String())
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
				return 0 // Not idle — has active streams
			}
		}
	}

	ts := m.lastActivity.Load()
	if ts == 0 {
		return 0
	}
	return time.Since(time.Unix(0, ts))
}
