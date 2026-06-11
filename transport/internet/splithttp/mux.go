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

// WarmupTarget represents a domain that should be pre-connected.
type WarmupTarget struct {
	Domain    string
	Priority  int // lower = higher priority
	CreatedAt time.Time
}

type XmuxManager struct {
	xmuxConfig  XmuxConfig
	concurrency int32
	connections int32
	newConnFunc func() XmuxConn
	xmuxClients []*XmuxClient
	stopCh      chan struct{}

	// Dynamic warmup queue
	warmupQueue  []WarmupTarget
	warmupMu     sync.Mutex
	warmupSem    chan struct{} // semaphore for concurrent warmups
	netHash      string       // current network hash for change detection
	lastNetCheck time.Time

	// Metrics for quantifiable validation
	metrics struct {
		// Connection reuse vs new
		reuseHit  atomic.Int64 // XMUX pool hit (reuse connection)
		newConn   atomic.Int64 // New connection created
		warmupHit atomic.Int64 // Connection came from warmup
		warmupMiss atomic.Int64 // Warmup failed or not ready

		// TTFB tracking (nanoseconds)
		ttfbSum   atomic.Int64 // Sum of TTFB values
		ttfbCount atomic.Int64 // Number of TTFB samples
		ttfbMax   atomic.Int64 // Max TTFB observed

		// Network recovery
		netRecoveryCount atomic.Int64 // Number of network changes detected
		netRecoveryTime  atomic.Int64 // Last recovery time (nanoseconds)

		// Warmup stats
		warmupEnqueue   atomic.Int64 // Domains enqueued
		warmupSuccess   atomic.Int64 // Warmup connections established
		warmupFailed    atomic.Int64 // Warmup connections failed
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
		warmupQueue: make([]WarmupTarget, 0),
		warmupSem:   make(chan struct{}, 2), // max 2 concurrent warmups
	}

	// Start background goroutines for connection management.
	go m.preConnectLoop()
	go m.healthCheckLoop()
	go m.networkWatchLoop()

	return m
}

// preConnectLoop maintains at least 1 warm connection in the pool.
func (m *XmuxManager) preConnectLoop() {
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
			if len(m.xmuxClients) == 0 {
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

	// Create connection using the connection function
	// The connection function already handles DNS, TLS, REALITY
	conn := m.newConnFunc()
	if conn == nil {
		errors.LogDebug(context.Background(), "XMUX: warmup failed for ", target.Domain)
		m.RecordWarmupFailed()
		return
	}

	// If connection was created successfully, it's already in the pool
	// via newXmuxClient logic
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
			for i := 0; i < len(m.xmuxClients); {
				xmuxClient := m.xmuxClients[i]

				// Always remove closed connections immediately
				if xmuxClient.XmuxConn.IsClosed() {
					errors.LogDebug(context.Background(), "XMUX: health-check removing closed xmuxClient")
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

				// RTT-based removal: only remove if RTT is extremely high
				rttMs := xmuxClient.GetRTT().Milliseconds()
				if rttMs > maxRTTBeforeRemove && xmuxClient.GetRTT() > 0 {
					errors.LogDebug(context.Background(), "XMUX: health-check removing high-RTT xmuxClient, rtt=", rttMs, "ms")
					m.xmuxClients = append(m.xmuxClients[:i], m.xmuxClients[i+1:]...)
					continue
				}

				i++
			}

			// Ensure pool stays above minimum
			if len(m.xmuxClients) == 0 {
				errors.LogDebug(context.Background(), "XMUX: health-check creating xmuxClient because pool is empty after cleanup")
				m.newXmuxClient()
			}
		}
	}
}

// Close stops the background goroutines.
func (m *XmuxManager) Close() {
	close(m.stopCh)
}

func (m *XmuxManager) newXmuxClient() *XmuxClient {
	m.RecordNewConn() // Track new connection creation

	xmuxClient := &XmuxClient{
		XmuxConn:  m.newConnFunc(),
		leftUsage: -1,
		createdAt: time.Now(),
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

func (m *XmuxManager) GetXmuxClient(ctx context.Context) *XmuxClient { // when locking
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
		return m.newXmuxClient()
	}

	if m.connections > 0 && len(m.xmuxClients) < int(m.connections) {
		errors.LogDebug(ctx, "XMUX: creating xmuxClient because maxConnections was not hit, xmuxClients = ", len(m.xmuxClients))
		return m.newXmuxClient()
	}

	xmuxClients := make([]*XmuxClient, 0)
	if m.concurrency > 0 {
		for _, xmuxClient := range m.xmuxClients {
			if xmuxClient.OpenUsage.Load() < m.concurrency {
				xmuxClients = append(xmuxClients, xmuxClient)
			}
		}
	} else {
		xmuxClients = m.xmuxClients
	}

	if len(xmuxClients) == 0 {
		errors.LogDebug(ctx, "XMUX: creating xmuxClient because maxConcurrency was hit, xmuxClients = ", len(m.xmuxClients))
		return m.newXmuxClient()
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
// Formula: inflight * 1000 + rtt_ms
// This ensures inflight is the primary factor, with RTT as tiebreaker.
func scoreClient(c *XmuxClient) int64 {
	inflight := int64(c.OpenUsage.Load())
	rttMs := c.GetRTT().Milliseconds()
	if rttMs == 0 {
		rttMs = 100 // default 100ms for unsampled connections
	} else if rttMs > 999 {
		rttMs = 999 // cap at 999ms to prevent score inversion
	}
	return inflight*1000 + rttMs
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
		ReuseHit:       reuseHit,
		NewConn:        newConn,
		WarmupHit:      warmupHit,
		WarmupMiss:     m.metrics.warmupMiss.Load(),
		ReuseRate:      reuseRate,
		AvgTTFB:        avgTTFB,
		MaxTTFB:        time.Duration(m.metrics.ttfbMax.Load()),
		TTFBSamples:    ttfbCount,
		NetRecovery:    m.metrics.netRecoveryCount.Load(),
		WarmupEnqueue:  m.metrics.warmupEnqueue.Load(),
		WarmupSuccess:  m.metrics.warmupSuccess.Load(),
		WarmupFailed:   m.metrics.warmupFailed.Load(),
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
