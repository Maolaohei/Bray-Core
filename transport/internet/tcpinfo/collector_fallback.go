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
	mu       sync.Mutex
	lastRTT  time.Duration // latest RTT from HTTP layer
	rttCount int           // number of samples received
	ewmaRTT  float64       // EWMA-smoothed RTT (nanoseconds)
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
}

func (c *fallbackCollector) Collect(conn net.Conn) (*quality.Snapshot, error) {
	_ = conn

	c.mu.Lock()
	rtt := c.lastRTT
	ewma := c.ewmaRTT
	count := c.rttCount
	c.mu.Unlock()

	snap := &quality.Snapshot{
		Timestamp:  time.Now(),
		Source:     quality.SourceEstimated,
		Confidence: c.estimateConfidence(count),
	}

	if rtt > 0 {
		snap.RTT = quality.NewMetric(time.Duration(ewma))

		// RTT variance estimate: if current RTT deviates significantly from EWMA,
		// treat it as jitter (proxy for loss/congestion).
		if ewma > 0 {
			deviation := math.Abs(float64(rtt)-ewma) / ewma
			rttVar := time.Duration(deviation * ewma)
			snap.RTTVar = quality.NewMetric(rttVar)

			// Estimate loss from RTT instability:
			// High deviation (>50% of EWMA) suggests packet loss or congestion.
			if deviation > 0.5 {
				lossPct := math.Min(deviation*20, 100) // scale: 50% deviation → 10% loss
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

// computeQuality computes multi-dimensional quality from raw metrics.
func computeQuality(snap *quality.Snapshot) quality.Quality {
	q := quality.Quality{}

	if snap.RTT.Valid {
		rttMs := float64(snap.RTT.Value.Milliseconds())
		if rttMs < 5 {
			q.Latency = 100
		} else if rttMs > 500 {
			q.Latency = 0
		} else {
			q.Latency = uint8(100 - (rttMs/500)*100)
		}
	}

	if snap.Loss.Valid {
		lossPct := snap.Loss.Value * 100
		if lossPct < 0.1 {
			q.Loss = 100
		} else if lossPct > 10 {
			q.Loss = 0
		} else {
			q.Loss = uint8(100 - (lossPct/10)*100)
		}
	}

	if snap.RTTVar.Valid && snap.RTT.Valid && snap.RTT.Value > 0 {
		jitterRatio := float64(snap.RTTVar.Value) / float64(snap.RTT.Value)
		if jitterRatio < 0.05 {
			q.Stability = 100
		} else if jitterRatio > 0.5 {
			q.Stability = 0
		} else {
			q.Stability = uint8(100 - ((jitterRatio-0.05)/0.45)*100)
		}
	} else {
		q.Stability = 50
	}

	q.Overall = quality.DefaultXMUXWeights().ComputeOverall(q)

	return q
}
