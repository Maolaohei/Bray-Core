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

// healthCheckLoop periodically removes closed connections and ensures
// the pool maintains a minimum number of healthy connections.
// This provides fast fault detection (5s interval) and automatic recovery.
func (m *XmuxManager) healthCheckLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			removed := 0
			for i := 0; i < len(m.xmuxClients); {
				xmuxClient := m.xmuxClients[i]
				if xmuxClient.XmuxConn.IsClosed() {
					errors.LogDebug(context.Background(), "XMUX: health-check removing closed xmuxClient")
					m.xmuxClients = append(m.xmuxClients[:i], m.xmuxClients[i+1:]...)
					removed++
				} else {
					i++
				}
			}

			// If we removed connections, ensure pool stays above minimum
			if removed > 0 && len(m.xmuxClients) == 0 {
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

	// min-inflight: select the connection with the fewest active streams
	// to distribute load evenly and reduce tail latency.
	best := xmuxClients[0]
	bestUsage := best.OpenUsage.Load()
	for _, c := range xmuxClients[1:] {
		if usage := c.OpenUsage.Load(); usage < bestUsage {
			best = c
			bestUsage = usage
		}
	}

	if best.leftUsage > 0 {
		best.leftUsage -= 1
	}
	return best
}
