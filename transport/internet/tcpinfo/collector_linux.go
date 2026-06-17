//go:build linux

package tcpinfo

import (
	"net"
	"time"

	"github.com/xtls/xray-core/transport/internet/quality"
	"golang.org/x/sys/unix"
)

// linuxCollector reads TCP_INFO via getsockopt syscall.
// Works on Linux, Android, and other Linux-kernel systems.
type linuxCollector struct{}

func newDefaultCollector() Collector {
	return &linuxCollector{}
}

func (c *linuxCollector) Source() quality.Source {
	return quality.SourceTCPInfo
}

// FeedRTT is a no-op on Linux — real RTT comes from getsockopt(TCP_INFO).
func (c *linuxCollector) FeedRTT(rtt time.Duration) {}

func (c *linuxCollector) Collect(conn net.Conn) (*quality.Snapshot, error) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return nil, nil
	}

	raw, err := tcpConn.SyscallConn()
	if err != nil {
		return nil, err
	}

	var info *unix.TCPInfo
	var sysErr error
	err = raw.Control(func(fd uintptr) {
		info, sysErr = unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
	})
	if err != nil {
		return nil, err
	}
	if sysErr != nil {
		return nil, sysErr
	}

	snap := &quality.Snapshot{
		Timestamp:  time.Now(),
		Source:     quality.SourceTCPInfo,
		Confidence: computeConfidence(info),
	}

	snap.RTT = quality.NewMetric(time.Duration(info.Rtt) * time.Microsecond)
	snap.RTTVar = quality.NewMetric(time.Duration(info.Rttvar) * time.Microsecond)
	snap.Retrans = quality.NewMetric(info.Total_retrans)
	snap.Unacked = quality.NewMetric(info.Unacked)

	// Compute loss from Lost count (absolute, not ratio — Segs_in not available on all archs)
	if info.Lost > 0 {
		// Scale: 1 lost packet → 5% loss, capped at 100%
		lossPct := float64(info.Lost) * 5.0
		if lossPct > 100 {
			lossPct = 100
		}
		snap.Loss = quality.NewMetric(lossPct / 100.0)
	} else {
		snap.Loss = quality.NewMetric(0.0)
	}

	snap.Quality = computeQuality(snap)

	return snap, nil
}

// computeConfidence returns a confidence score based on how much data we have.
func computeConfidence(info *unix.TCPInfo) uint8 {
	// Use Unacked + Total_retrans as proxy for connection maturity
	// (Segs_in not available on all architectures)
	totalSamples := int(info.Unacked) + int(info.Total_retrans)
	if totalSamples == 0 && info.Rtt == 0 {
		return 10 // barely connected, no RTT yet
	}
	if info.Rtt == 0 {
		return 20 // have some data but no RTT measurement
	}
	if totalSamples < 5 {
		return 40
	}
	if totalSamples < 20 {
		return 60
	}
	if totalSamples < 100 {
		return 80
	}
	return 95
}
