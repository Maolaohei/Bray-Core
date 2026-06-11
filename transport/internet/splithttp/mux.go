package splithttp

import (
	"context"
	"math"
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

type XmuxManager struct {
	xmuxConfig  XmuxConfig
	concurrency int32
	connections int32
	newConnFunc func() XmuxConn
	xmuxClients []*XmuxClient
	stopCh      chan struct{}
}

func NewXmuxManager(xmuxConfig XmuxConfig, newConnFunc func() XmuxConn) *XmuxManager {
	m := &XmuxManager{
		xmuxConfig:  xmuxConfig,
		concurrency: xmuxConfig.GetNormalizedMaxConcurrency().rand(),
		connections: xmuxConfig.GetNormalizedMaxConnections().rand(),
		newConnFunc: newConnFunc,
		xmuxClients: make([]*XmuxClient, 0),
		stopCh:      make(chan struct{}),
	}

	// Start background goroutines for connection management.
	go m.preConnectLoop()
	go m.healthCheckLoop()

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
			if len(m.xmuxClients) == 0 {
				errors.LogDebug(context.Background(), "XMUX: pre-connect creating xmuxClient because pool is empty")
				m.newXmuxClient()
			}
		}
	}
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
	return best
}

// scoreClient computes a scheduling score for a connection.
// Lower score = better candidate.
// Formula: inflight * 1000 + rtt_ms
// This ensures inflight is the primary factor, with RTT as tiebreaker.
func scoreClient(c *XmuxClient) int64 {
	inflight := int64(c.OpenUsage.Load())
	rttMs := c.GetRTT().Milliseconds()
	return inflight*1000 + rttMs
}
