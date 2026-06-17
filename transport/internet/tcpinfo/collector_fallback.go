//go:build !linux

package tcpinfo

import (
	"math"
	"net"
	"sync"
	"time"

	"github.com/xtls/xray-core/transport/internet/quality"
)

// fallbackCollector provides estimated TCP_INFO on platforms without getsockopt.
// Uses RTT measurements from the HTTP layer (onRTT callback) to estimate quality.
type fallbackCollector struct {
	mu           sync.Mutex
	lastRTT      time.Duration // latest RTT from HTTP layer
	rttCount     int           // number of samples received
	ewmaRTT      float64       // EWMA-smoothed RTT (nanoseconds)
	maxDeviation float64       // peak RTT deviation (decays over time)
}

func newDefaultCollector() Collector {
	return &fallbackCollector{}
}

func (c *fallbackCollector) Source() quality.Source {
	return quality.SourceEstimated
}

// FeedRTT is called by the HTTP layer's onRTT callback.
// This provides real RTT data even on platforms without TCP_INFO.
func (c *fallbackCollector) FeedRTT(rtt time.Duration) {
	if rtt <= 0 {
		return // reject invalid RTT
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.lastRTT = rtt
	c.rttCount++

	newRTT := float64(rtt)
	if c.ewmaRTT == 0 {
		c.ewmaRTT = newRTT
	} else {
		// EWMA: 80% old + 20% new
		c.ewmaRTT = c.ewmaRTT*0.8 + newRTT*0.2
	}

	// Track peak deviation for loss estimation
	deviation := math.Abs(newRTT-c.ewmaRTT) / c.ewmaRTT
	if deviation > c.maxDeviation {
		c.maxDeviation = deviation
	}
	// Decay max deviation slowly so it adapts to changing conditions
	c.maxDeviation *= 0.99
}

func (c *fallbackCollector) Collect(conn net.Conn) (*quality.Snapshot, error) {
	_ = conn

	c.mu.Lock()
	rtt := c.lastRTT
	ewma := c.ewmaRTT
	count := c.rttCount
	maxDev := c.maxDeviation
	c.mu.Unlock()

	snap := &quality.Snapshot{
		Timestamp:  time.Now(),
		Source:     quality.SourceEstimated,
		Confidence: c.estimateConfidence(count),
	}

	if rtt > 0 {
		snap.RTT = quality.NewMetric(time.Duration(ewma))

		// RTT variance estimate
		if ewma > 0 {
			deviation := math.Abs(float64(rtt)-ewma) / ewma
			rttVar := time.Duration(deviation * ewma)
			snap.RTTVar = quality.NewMetric(rttVar)

			// Estimate loss from peak RTT instability:
			// maxDeviation > 30% suggests packet loss or congestion.
			if maxDev > 0.3 {
				lossPct := math.Min(maxDev*30, 100) // 30% deviation → ~9% loss
				snap.Loss = quality.NewMetric(lossPct / 100.0)
			} else {
				snap.Loss = quality.NewMetric(0.0)
			}
		}
	} else {
		snap.RTT = quality.Unknown[time.Duration]()
		snap.RTTVar = quality.Unknown[time.Duration]()
		snap.Loss = quality.Unknown[float64]()
	}

	snap.Retrans = quality.Unknown[uint32]() // not available on Windows
	snap.Unacked = quality.Unknown[uint32]() // not available on Windows

	snap.Quality = computeQuality(snap)

	return snap, nil
}

// estimateConfidence returns confidence based on sample count.
func (c *fallbackCollector) estimateConfidence(count int) uint8 {
	if count == 0 {
		return 10 // no data yet
	}
	if count < 5 {
		return 30 // barely any data
	}
	if count < 20 {
		return 50
	}
	if count < 50 {
		return 65
	}
	return 80 // estimated but well-sampled
}
