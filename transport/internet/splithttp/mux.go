package splithttp

import (
	"context"
	"fmt"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/transport/internet/quality"
)

type XmuxConn interface {
	IsClosed() bool
}

type XmuxClient struct {
	XmuxConn     XmuxConn
	OpenUsage    atomic.Int32
	leftUsage    int32
	LeftRequests atomic.Int32
	UnreusableAt time.Time
	createdAt    time.Time
	lastRTT      atomic.Int64 // nanoseconds, for RTT-aware scheduling

	// V2.0: link-quality metrics for smarter scheduling
	lastRetrans  atomic.Int32 // cumulative retransmit count from TCP_INFO
	lastLoss     atomic.Int64 // loss rate × 10000 (fixed-point, 0-10000)
	qualityScore atomic.Int32 // 0-100, computed by TransportProfile
	confidence   atomic.Int32 // 0-100, how much we trust the quality data
	consecDrops  atomic.Int32 // consecutive quality drops, for drain

	// V2.1: behavior learning for adaptive scheduling
	learner *quality.NetworkLearner // tracks link behavior patterns

	// TransportProfile for this connection. Created when TCP connection is established.
	profile interface{ Stop() } // *tcpinfo.Profile, stored as interface to avoid import cycle
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

	// Track consecutive quality drops for drain
	if qualityScore < oldScore && oldScore > 0 {
		c.consecDrops.Add(1)
	} else {
		c.consecDrops.Store(0)
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
	c.StopProfiling() // stop any existing profile
	c.profile = conn
}

// StopProfiling stops the TransportProfile background goroutine.
func (c *XmuxClient) StopProfiling() {
	if c.profile != nil {
		c.profile.Stop()
		c.profile = nil
	}
}

// WarmupTarget represents a domain that should be pre-connected.
type WarmupTarget struct {
	Domain    string
	Priority  int // lower = higher priority
	CreatedAt time.Time
}

type XmuxManager struct {
	xmuxConfig  XmuxConfig
	concurrency int32 // base concurrency (from config)
	connections int32 // base connections (from config)
	newConnFunc func() XmuxConn
	xmuxClients []*XmuxClient
	clientsMu   sync.Mutex // protects xmuxClients slice
	stopCh      chan struct{}
	doneCh      chan struct{} // closed when all goroutines exit

	// V2.1: Dynamic Connection Scaling
	poolBehavior   quality.Behavior // dominant behavior across all clients
	poolBehaviorMu sync.RWMutex
	behaviorStreak int              // consecutive observations of same behavior (for debounce)
	streakBehavior quality.Behavior // behavior being streaked
	_dynamicConns  int32            // current effective connections (AIMD smoothed)
	_dynamicConc   int32            // current effective concurrency (AIMD smoothed)
	scaledOnce     bool             // whether dynamic scaling has been applied at least once

	// Dynamic warmup queue
	warmupQueue  []WarmupTarget
	warmupMu     sync.Mutex
	warmupSem    chan struct{} // semaphore for concurrent warmups
	netHash      string        // current network hash for change detection
	lastNetCheck time.Time

	// Metrics for quantifiable validation
	metrics struct {
		// Connection reuse vs new
		reuseHit   atomic.Int64 // XMUX pool hit (reuse connection)
		newConn    atomic.Int64 // New connection created
		warmupHit  atomic.Int64 // Connection came from warmup
		warmupMiss atomic.Int64 // Warmup failed or not ready

		// TTFB tracking (nanoseconds)
		ttfbSum   atomic.Int64 // Sum of TTFB values
		ttfbCount atomic.Int64 // Number of TTFB samples
		ttfbMax   atomic.Int64 // Max TTFB observed

		// Network recovery
		netRecoveryCount atomic.Int64 // Number of network changes detected
		netRecoveryTime  atomic.Int64 // Last recovery time (nanoseconds)

		// Warmup stats
		warmupEnqueue atomic.Int64 // Domains enqueued
		warmupSuccess atomic.Int64 // Warmup connections established
		warmupFailed  atomic.Int64 // Warmup connections failed
	}
}

func NewXmuxManager(xmuxConfig XmuxConfig, newConnFunc func() XmuxConn) *XmuxManager {
	m := &XmuxManager{
		xmuxConfig:  xmuxConfig,
		concurrency: xmuxConfig.GetNormalizedMaxConcurrency().rand(),
		connections: xmuxConfig.GetNormalizedMaxConnections().rand(),
		newConnFunc: newConnFunc,
		xmuxClients: make([]*XmuxClient, 0),
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
		warmupQueue: make([]WarmupTarget, 0),
		warmupSem:   make(chan struct{}, 2), // max 2 concurrent warmups
	}

	// Start background goroutines for connection management.
	go func() {
		var wg sync.WaitGroup
		wg.Add(3)
		go func() { defer wg.Done(); m.preConnectLoop() }()
		go func() { defer wg.Done(); m.healthCheckLoop() }()
		go func() { defer wg.Done(); m.networkWatchLoop() }()
		wg.Wait()
		close(m.doneCh)
	}()

	return m
}

// preConnectLoop maintains at least 1 warm connection in the pool.
func (m *XmuxManager) preConnectLoop() {
	// Execute immediately on startup so the pool is warm before the first request
	m.processWarmupQueue()
	m.clientsMu.Lock()
	empty := len(m.xmuxClients) == 0
	m.clientsMu.Unlock()
	if empty {
		errors.LogDebug(context.Background(), "XMUX: pre-connect creating xmuxClient (initial)")
		m.newXmuxClient()
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			// Process warmup queue first
			m.processWarmupQueue()

			// Then ensure pool has connections
			m.clientsMu.Lock()
			empty = len(m.xmuxClients) == 0
			m.clientsMu.Unlock()
			if empty {
				errors.LogDebug(context.Background(), "XMUX: pre-connect creating xmuxClient because pool is empty")
				m.newXmuxClient()
			}
		}
	}
}

// EnqueueWarmup adds a domain to the warmup queue.
func (m *XmuxManager) EnqueueWarmup(domain string, priority int) {
	m.warmupMu.Lock()
	defer m.warmupMu.Unlock()

	// Check if already in queue
	for _, t := range m.warmupQueue {
		if t.Domain == domain {
			return // already queued
		}
	}

	m.warmupQueue = append(m.warmupQueue, WarmupTarget{
		Domain:    domain,
		Priority:  priority,
		CreatedAt: time.Now(),
	})

	m.RecordWarmupEnqueue() // Track enqueue

	// Sort by priority (lower = higher priority)
	for i := len(m.warmupQueue) - 1; i > 0; i-- {
		if m.warmupQueue[i].Priority < m.warmupQueue[i-1].Priority {
			m.warmupQueue[i], m.warmupQueue[i-1] = m.warmupQueue[i-1], m.warmupQueue[i]
		} else {
			break
		}
	}
}

// processWarmupQueue processes pending warmup targets.
func (m *XmuxManager) processWarmupQueue() {
	m.warmupMu.Lock()
	if len(m.warmupQueue) == 0 {
		m.warmupMu.Unlock()
		return
	}

	// Take one target from queue
	target := m.warmupQueue[0]
	m.warmupQueue = m.warmupQueue[1:]
	m.warmupMu.Unlock()

	// Check if we can warm up (semaphore)
	select {
	case m.warmupSem <- struct{}{}:
		go func() {
			defer func() { <-m.warmupSem }()
			m.executeWarmup(target)
		}()
	default:
		// Already at max concurrent warmups, re-queue
		m.warmupMu.Lock()
		m.warmupQueue = append([]WarmupTarget{target}, m.warmupQueue...)
		m.warmupMu.Unlock()
	}
}

// executeWarmup creates a pre-connection for a warmup target.
func (m *XmuxManager) executeWarmup(target WarmupTarget) {
	errors.LogDebug(context.Background(), "XMUX: warmup starting for ", target.Domain)

	// Create connection and add to pool via newXmuxClient
	xmuxClient := m.newXmuxClient()
	if xmuxClient == nil || xmuxClient.XmuxConn == nil {
		errors.LogDebug(context.Background(), "XMUX: warmup failed for ", target.Domain)
		m.RecordWarmupFailed()
		return
	}

	errors.LogDebug(context.Background(), "XMUX: warmup completed for ", target.Domain)
	m.RecordWarmupSuccess()
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

// checkNetworkChange detects network interface changes.
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

	if m.netHash != newHash {
		// Network changed!
		errors.LogInfo(context.Background(), "XMUX: network change detected, triggering warmup")
		m.netHash = newHash

		// Track network recovery event
		m.metrics.netRecoveryCount.Add(1)

		// Clear stale connections
		m.clearStaleConnections()

		// Re-enqueue warmup targets (will be populated by caller)
		// The actual domain list is managed by the warmup manager
	}
}

// clearStaleConnections removes connections that may be invalid after network change.
func (m *XmuxManager) clearStaleConnections() {
	m.clientsMu.Lock()
	defer m.clientsMu.Unlock()
	// Mark all connections as potentially stale
	// They will be removed on next health check if truly broken
	for _, client := range m.xmuxClients {
		client.lastRTT.Store(int64(5 * time.Second)) // Set high RTT to trigger removal
	}
}

// getNetworkHash returns a hash of current network interfaces.
func getNetworkHash() string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return ""
	}

	hash := ""
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp != 0 && iface.Flags&net.FlagLoopback == 0 {
			hash += iface.Name + ":"
		}
	}
	return hash
}

const (
	// maxRTTBeforeRemove is the maximum RTT (in ms) before a connection is considered unhealthy.
	maxRTTBeforeRemove = 5000
	// coldStartProtectionMs is the minimum age (in ms) before a connection can be removed by health check.
	coldStartProtectionMs = 10000
)

// healthCheckLoop periodically checks connection health and removes unhealthy ones.
// Improvements over original:
// - Cold start protection: new connections (<10s) are not removed
// - RTT-based: only remove if RTT > 5000ms (not just IsClosed)
// - Does not remove active connections with low OpenUsage
func (m *XmuxManager) healthCheckLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			now := time.Now()
			m.clientsMu.Lock()
			for i := 0; i < len(m.xmuxClients); {
				xmuxClient := m.xmuxClients[i]

				// Always remove closed connections immediately
				if xmuxClient.XmuxConn.IsClosed() {
					errors.LogDebug(context.Background(), "XMUX: health-check removing closed xmuxClient")
					xmuxClient.StopProfiling()
					m.xmuxClients = append(m.xmuxClients[:i], m.xmuxClients[i+1:]...)
					continue
				}

				// Skip if connection is actively being used
				if xmuxClient.OpenUsage.Load() > 0 {
					i++
					continue
				}

				// Cold start protection: don't remove connections younger than 10s
				if xmuxClient.createdAt.After(now.Add(-coldStartProtectionMs * time.Millisecond)) {
					i++
					continue
				}

				// V2.0: Quality-based drain — consecutive quality drops
				if xmuxClient.ShouldDrain() && xmuxClient.confidence.Load() >= 30 {
					errors.LogDebug(context.Background(), "XMUX: health-check draining quality-degraded xmuxClient, consecutiveDrops=", xmuxClient.consecDrops.Load())
					xmuxClient.StopProfiling()
					m.xmuxClients = append(m.xmuxClients[:i], m.xmuxClients[i+1:]...)
					continue
				}

				// RTT-based removal: only remove if RTT is extremely high
				rttMs := xmuxClient.GetRTT().Milliseconds()
				if rttMs > maxRTTBeforeRemove && xmuxClient.GetRTT() > 0 {
					errors.LogDebug(context.Background(), "XMUX: health-check removing high-RTT xmuxClient, rtt=", rttMs, "ms")
					xmuxClient.StopProfiling()
					m.xmuxClients = append(m.xmuxClients[:i], m.xmuxClients[i+1:]...)
					continue
				}

				i++
			}

			// V2.1: Connection Migration — proactively create replacements
			// When pool drops below effective limit, create new connections immediately
			// instead of waiting for the next preConnectLoop tick.
			effectiveConns := m.effectiveConnections()
			if effectiveConns > 0 {
				for len(m.xmuxClients) < int(effectiveConns) {
					errors.LogDebug(context.Background(), "XMUX: migration creating xmuxClient, pool=", len(m.xmuxClients), ", target=", effectiveConns)
					m.newXmuxClientLocked()
				}
			} else if len(m.xmuxClients) == 0 {
				// Fallback: ensure at least 1 connection
				errors.LogDebug(context.Background(), "XMUX: health-check creating xmuxClient because pool is empty after cleanup")
				m.newXmuxClientLocked()
			}

			// V2.1: Update pool behavior from client observations
			// Use voting mechanism: count behaviors from all active clients
			// Non-linear punishment: bad behavior (Lossy/Saturated) needs >40% to trigger degradation
			var dominantBehavior quality.Behavior
			if len(m.xmuxClients) > 0 {
				behaviorCounts := make(map[quality.Behavior]int)
				totalKnown := 0
				badCount := 0 // Lossy or Saturated

				for _, c := range m.xmuxClients {
					if b := c.GetBehavior(); b != quality.BehaviorUnknown {
						behaviorCounts[b]++
						totalKnown++
						if b == quality.BehaviorLossy || b == quality.BehaviorSaturated {
							badCount++
						}
					}
				}

				if totalKnown > 0 {
					// Find most common behavior (voting)
					maxCount := 0
					for b, count := range behaviorCounts {
						if count > maxCount {
							maxCount = count
							dominantBehavior = b
						}
					}

					// Non-linear punishment: only degrade if bad connections exceed 40%
					// Bad connections harm multiplexing more than good connections help
					badRatio := float64(badCount) / float64(totalKnown)
					if badRatio > 0.4 && dominantBehavior != quality.BehaviorLossy && dominantBehavior != quality.BehaviorSaturated {
						// Override to worst observed bad behavior
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

			m.clientsMu.Unlock()
		}
	}
}

// Close stops the background goroutines and waits for them to finish.
func (m *XmuxManager) Close() {
	close(m.stopCh)
	<-m.doneCh
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
func (m *XmuxManager) applyAIMD(b, prevBehavior quality.Behavior) {
	baseConns := m.connections
	baseConc := m.concurrency

	if !m.scaledOnce {
		// First time: set initial values based on behavior
		m._dynamicConns = m.computeTargetConns(b, baseConns)
		m._dynamicConc = m.computeTargetConc(b, baseConc)
		m.scaledOnce = true
		return
	}

	if isBehaviorImprovement(b, prevBehavior) {
		// Additive Increase: move toward target by 1 step
		targetConns := m.computeTargetConns(b, baseConns)
		if m._dynamicConns < targetConns {
			m._dynamicConns += aimdStep
		}
		targetConc := m.computeTargetConc(b, baseConc)
		if m._dynamicConc < targetConc {
			m._dynamicConc += aimdStep
		}
	} else if isBehaviorWorsening(b, prevBehavior) {
		// Multiplicative Decrease: halve immediately
		m._dynamicConns = m._dynamicConns / 2
		m._dynamicConc = m._dynamicConc / 2
	}

	// Clamp to sane bounds
	if m._dynamicConns < 1 {
		m._dynamicConns = 1
	}
	if baseConns > 0 && m._dynamicConns > baseConns*2 {
		m._dynamicConns = baseConns * 2
	}
	if m._dynamicConc < 1 {
		m._dynamicConc = 1
	}
	if baseConc > 0 && m._dynamicConc > baseConc*2 {
		m._dynamicConc = baseConc * 2
	}
}

// computeTargetConns returns the ideal connection count for a behavior.
func (m *XmuxManager) computeTargetConns(b quality.Behavior, base int32) int32 {
	if base <= 0 {
		return 0
	}
	switch b {
	case quality.BehaviorLowLatency:
		return base + base/2 // +50%
	case quality.BehaviorLossy, quality.BehaviorSaturated:
		return base / 2 // -50%
	case quality.BehaviorAggressive:
		return base * 2 / 3 // -33%
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
		return base * 2 // 2x
	case quality.BehaviorLossy, quality.BehaviorSaturated:
		return base / 2 // 0.5x
	case quality.BehaviorAggressive:
		return base * 3 / 4 // 0.75x
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
func (m *XmuxManager) effectiveConnections() int32 {
	m.poolBehaviorMu.RLock()
	defer m.poolBehaviorMu.RUnlock()
	if !m.scaledOnce {
		return m.connections
	}
	return m._dynamicConns
}

// effectiveConcurrency returns the AIMD-smoothed concurrency limit.
func (m *XmuxManager) effectiveConcurrency() int32 {
	m.poolBehaviorMu.RLock()
	defer m.poolBehaviorMu.RUnlock()
	if !m.scaledOnce {
		return m.concurrency
	}
	return m._dynamicConc
}

func (m *XmuxManager) newXmuxClient() *XmuxClient {
	m.RecordNewConn() // Track new connection creation

	xmuxClient := &XmuxClient{
		XmuxConn:  m.newConnFunc(),
		leftUsage: -1,
		createdAt: time.Now(),
		learner:   quality.NewNetworkLearner(),
	}
	if x := m.xmuxConfig.GetNormalizedCMaxReuseTimes().rand(); x > 0 {
		xmuxClient.leftUsage = x - 1
	}
	xmuxClient.LeftRequests.Store(math.MaxInt32)
	if x := m.xmuxConfig.GetNormalizedHMaxRequestTimes().rand(); x > 0 {
		xmuxClient.LeftRequests.Store(x)
	}
	if x := m.xmuxConfig.GetNormalizedHMaxReusableSecs().rand(); x > 0 {
		xmuxClient.UnreusableAt = time.Now().Add(time.Duration(x) * time.Second)
	}
	m.clientsMu.Lock()
	m.xmuxClients = append(m.xmuxClients, xmuxClient)
	m.clientsMu.Unlock()
	return xmuxClient
}

// newXmuxClientLocked creates a new client and appends to the pool.
// Caller must hold m.clientsMu.
func (m *XmuxManager) newXmuxClientLocked() *XmuxClient {
	m.RecordNewConn()

	xmuxClient := &XmuxClient{
		XmuxConn:  m.newConnFunc(),
		leftUsage: -1,
		createdAt: time.Now(),
		learner:   quality.NewNetworkLearner(),
	}
	if x := m.xmuxConfig.GetNormalizedCMaxReuseTimes().rand(); x > 0 {
		xmuxClient.leftUsage = x - 1
	}
	xmuxClient.LeftRequests.Store(math.MaxInt32)
	if x := m.xmuxConfig.GetNormalizedHMaxRequestTimes().rand(); x > 0 {
		xmuxClient.LeftRequests.Store(x)
	}
	if x := m.xmuxConfig.GetNormalizedHMaxReusableSecs().rand(); x > 0 {
		xmuxClient.UnreusableAt = time.Now().Add(time.Duration(x) * time.Second)
	}
	m.xmuxClients = append(m.xmuxClients, xmuxClient)
	return xmuxClient
}

func (m *XmuxManager) GetXmuxClient(ctx context.Context) *XmuxClient {
	m.clientsMu.Lock()
	defer m.clientsMu.Unlock()

	for i := 0; i < len(m.xmuxClients); {
		xmuxClient := m.xmuxClients[i]
		if xmuxClient.XmuxConn.IsClosed() ||
			xmuxClient.leftUsage == 0 ||
			xmuxClient.LeftRequests.Load() <= 0 ||
			(xmuxClient.UnreusableAt != time.Time{} && time.Now().After(xmuxClient.UnreusableAt)) {
			errors.LogDebug(ctx, "XMUX: removing xmuxClient, IsClosed() = ", xmuxClient.XmuxConn.IsClosed(),
				", OpenUsage = ", xmuxClient.OpenUsage.Load(),
				", leftUsage = ", xmuxClient.leftUsage,
				", LeftRequests = ", xmuxClient.LeftRequests.Load(),
				", UnreusableAt = ", xmuxClient.UnreusableAt)
			m.xmuxClients = append(m.xmuxClients[:i], m.xmuxClients[i+1:]...)
		} else {
			i++
		}
	}

	if len(m.xmuxClients) == 0 {
		errors.LogDebug(ctx, "XMUX: creating xmuxClient because xmuxClients is empty")
		return m.newXmuxClientLocked()
	}

	// V2.1: Dynamic Connection Scaling — use behavior-adjusted limits
	effectiveConns := m.effectiveConnections()
	if effectiveConns > 0 && len(m.xmuxClients) < int(effectiveConns) {
		errors.LogDebug(ctx, "XMUX: creating xmuxClient because maxConnections was not hit, xmuxClients = ", len(m.xmuxClients), ", effective = ", effectiveConns)
		return m.newXmuxClientLocked()
	}

	xmuxClients := make([]*XmuxClient, 0)
	effectiveConc := m.effectiveConcurrency()
	if effectiveConc > 0 {
		for _, xmuxClient := range m.xmuxClients {
			if xmuxClient.OpenUsage.Load() < effectiveConc {
				xmuxClients = append(xmuxClients, xmuxClient)
			}
		}
	} else {
		xmuxClients = m.xmuxClients
	}

	if len(xmuxClients) == 0 {
		errors.LogDebug(ctx, "XMUX: creating xmuxClient because maxConcurrency was hit, xmuxClients = ", len(m.xmuxClients))
		return m.newXmuxClientLocked()
	}

	// RTT-aware min-inflight scheduling:
	// Score = inflight * 1000 + rtt_ms
	// This favors low-inflight connections, but breaks ties using RTT.
	// A connection with lower RTT gets slightly higher priority.
	best := xmuxClients[0]
	bestScore := scoreClient(best)
	for _, c := range xmuxClients[1:] {
		if s := scoreClient(c); s < bestScore {
			best = c
			bestScore = s
		}
	}

	if best.leftUsage > 0 {
		best.leftUsage -= 1
	}

	m.RecordReuseHit() // Track connection reuse
	return best
}

// scoreClient computes a scheduling score for a connection.
// Lower score = better candidate.
//
// V2.1 formula (behavior-aware, confidence-weighted):
//
//	base = inflight * 10000 + rttMs * 10
//	retransPenalty = retransCount * 50 * behaviorScale
//	lossPenalty = lossRate * 10000 * behaviorScale
//	confidence = connection.confidence (0-100)
//
// behaviorScale varies by detected behavior:
//   - LowLatency: 0.5 (penalties reduced — fast link)
//   - Normal: 1.0 (standard penalties)
//   - Lossy/Saturated: 1.5 (penalties increased — problematic link)
//   - Aggressive: 1.2 (slightly higher — avoid over-stacking)
func scoreClient(c *XmuxClient) int64 {
	inflight := int64(c.OpenUsage.Load())
	rttMs := c.GetRTT().Milliseconds()
	if rttMs == 0 {
		rttMs = 100 // default 100ms for unsampled connections
	} else if rttMs > 999 {
		rttMs = 999 // cap at 999ms to prevent score inversion
	}

	score := inflight*10000 + rttMs*10

	// V2.0: confidence-weighted penalties
	conf := int64(c.confidence.Load())
	var confidenceScale float64
	switch {
	case conf >= 80:
		confidenceScale = 1.0
	case conf >= 30:
		confidenceScale = 0.2 + float64(conf-30)*0.02
	default:
		confidenceScale = 0.2
	}

	// V2.1: behavior-aware penalty scaling
	behaviorScale := behaviorPenaltyScale(c.GetBehavior())

	// Combined scale = confidence × behavior
	combinedScale := confidenceScale * behaviorScale

	// Retrans penalty: each retrans costs 50 points
	retrans := int64(c.lastRetrans.Load())
	if retrans > 100 {
		retrans = 100 // cap
	}
	score += int64(float64(retrans*50) * combinedScale)

	// Loss penalty: lossRate is fixed-point × 10000 (0-10000)
	lossRate := c.lastLoss.Load()
	score += int64(float64(lossRate/20) * combinedScale)

	return score
}

// behaviorPenaltyScale returns a multiplier for penalties based on detected behavior.
func behaviorPenaltyScale(b quality.Behavior) float64 {
	switch b {
	case quality.BehaviorLowLatency:
		return 0.5 // fast link, reduce penalties
	case quality.BehaviorAggressive:
		return 1.2 // brute-force sender, slightly higher
	case quality.BehaviorLossy, quality.BehaviorSaturated:
		return 1.5 // problematic link, increase penalties
	default:
		return 1.0 // Normal or Unknown
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

// RecordWarmupHit records a connection that came from warmup.
func (m *XmuxManager) RecordWarmupHit() {
	m.metrics.warmupHit.Add(1)
}

// RecordWarmupMiss records a warmup miss.
func (m *XmuxManager) RecordWarmupMiss() {
	m.metrics.warmupMiss.Add(1)
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

// RecordWarmupEnqueue records a domain enqueued for warmup.
func (m *XmuxManager) RecordWarmupEnqueue() {
	m.metrics.warmupEnqueue.Add(1)
}

// RecordWarmupSuccess records a successful warmup connection.
func (m *XmuxManager) RecordWarmupSuccess() {
	m.metrics.warmupSuccess.Add(1)
}

// RecordWarmupFailed records a failed warmup connection.
func (m *XmuxManager) RecordWarmupFailed() {
	m.metrics.warmupFailed.Add(1)
}

// GetMetrics returns current metrics snapshot.
func (m *XmuxManager) GetMetrics() XmuxMetrics {
	reuseHit := m.metrics.reuseHit.Load()
	newConn := m.metrics.newConn.Load()
	warmupHit := m.metrics.warmupHit.Load()

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
		ReuseHit:      reuseHit,
		NewConn:       newConn,
		WarmupHit:     warmupHit,
		WarmupMiss:    m.metrics.warmupMiss.Load(),
		ReuseRate:     reuseRate,
		AvgTTFB:       avgTTFB,
		MaxTTFB:       time.Duration(m.metrics.ttfbMax.Load()),
		TTFBSamples:   ttfbCount,
		NetRecovery:   m.metrics.netRecoveryCount.Load(),
		WarmupEnqueue: m.metrics.warmupEnqueue.Load(),
		WarmupSuccess: m.metrics.warmupSuccess.Load(),
		WarmupFailed:  m.metrics.warmupFailed.Load(),
	}
}

// XmuxMetrics holds quantifiable metrics for validation.
type XmuxMetrics struct {
	ReuseHit      int64         // Connection reuse count
	NewConn       int64         // New connection count
	WarmupHit     int64         // Connections from warmup
	WarmupMiss    int64         // Warmup failures
	ReuseRate     float64       // Reuse percentage (0-100)
	AvgTTFB       time.Duration // Average TTFB
	MaxTTFB       time.Duration // Max TTFB observed
	TTFBSamples   int64         // Number of TTFB samples
	NetRecovery   int64         // Network change events
	WarmupEnqueue int64         // Domains enqueued
	WarmupSuccess int64         // Successful warmups
	WarmupFailed  int64         // Failed warmups
}

// String returns a human-readable metrics summary.
func (m XmuxMetrics) String() string {
	return fmt.Sprintf(
		"XMUX Metrics:\n"+
			"  Reuse Rate: %.1f%% (%d/%d)\n"+
			"  Warmup Hit: %d, Miss: %d\n"+
			"  Avg TTFB: %v, Max TTFB: %v (samples: %d)\n"+
			"  Network Recoveries: %d\n"+
			"  Warmup: %d enqueued, %d success, %d failed",
		m.ReuseRate, m.ReuseHit, m.ReuseHit+m.NewConn,
		m.WarmupHit, m.WarmupMiss,
		m.AvgTTFB, m.MaxTTFB, m.TTFBSamples,
		m.NetRecovery,
		m.WarmupEnqueue, m.WarmupSuccess, m.WarmupFailed,
	)
}

// LogMetrics logs current metrics at Info level.
func (m *XmuxManager) LogMetrics() {
	metrics := m.GetMetrics()
	errors.LogInfo(context.Background(), metrics.String())
}
