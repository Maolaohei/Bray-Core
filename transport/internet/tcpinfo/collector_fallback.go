//go:build !linux

package tcpinfo

import (
	"net"
	"time"

	"github.com/xtls/xray-core/transport/internet/quality"
)

// fallbackCollector is used on platforms without TCP_INFO support.
// Returns a snapshot with Source=estimated and lower confidence.
type fallbackCollector struct{}

func newDefaultCollector() Collector {
	return &fallbackCollector{}
}

func (c *fallbackCollector) Source() quality.Source {
	return quality.SourceEstimated
}

func (c *fallbackCollector) Collect(conn net.Conn) (*quality.Snapshot, error) {
	// On Windows/macOS, we can't read TCP_INFO.
	// Return an estimated snapshot with lower confidence.
	_ = conn // may be used in future Windows IOCTL implementation

	snap := &quality.Snapshot{
		Timestamp:  time.Now(),
		Source:     quality.SourceEstimated,
		Confidence: 30, // estimated data is less trustworthy
	}

	// TODO: implement Windows IOCTL TCP_CONNECTION_ESTIMATION
	// For now, mark everything as unknown (no valid data)
	snap.RTT = quality.Unknown[time.Duration]()
	snap.RTTVar = quality.Unknown[time.Duration]()
	snap.Loss = quality.Unknown[float64]()
	snap.Retrans = quality.Unknown[uint32]()
	snap.Unacked = quality.Unknown[uint32]()

	return snap, nil
}
