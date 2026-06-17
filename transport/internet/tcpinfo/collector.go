package tcpinfo

import (
	"net"
	"time"

	"github.com/xtls/xray-core/transport/internet/quality"
)

// Collector gathers TCP_INFO data from a live TCP connection.
// Implementations are platform-specific (linux, android, windows fallback).
type Collector interface {
	// Collect reads TCP_INFO from the connection and returns a snapshot.
	// Returns (nil, nil) if the connection doesn't support TCP_INFO.
	Collect(conn net.Conn) (*quality.Snapshot, error)

	// Source identifies the data source for this collector.
	Source() quality.Source

	// FeedRTT provides RTT measurements from the HTTP layer.
	// Used by fallback collectors to estimate quality when TCP_INFO is unavailable.
	// No-op on platforms with real TCP_INFO.
	FeedRTT(rtt time.Duration)
}

// DefaultInterval is the default sampling interval.
const DefaultInterval = 2 * time.Second

// DefaultMaxStale is the maximum age before a snapshot is considered stale.
const DefaultMaxStale = 10 * time.Second

// computeQuality computes multi-dimensional quality from raw metrics.
// Shared across all platform implementations.
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
