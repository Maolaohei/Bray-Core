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
