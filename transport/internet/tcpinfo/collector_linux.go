//go:build linux

package tcpinfo

import (
	"net"
	"syscall"
	"time"
	"unsafe"

	"github.com/xtls/xray-core/transport/internet/quality"
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

func (c *linuxCollector) Collect(conn net.Conn) (*quality.Snapshot, error) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return nil, nil
	}

	raw, err := tcpConn.SyscallConn()
	if err != nil {
		return nil, err
	}

	var info syscall.TCPInfo
	var sysErr error
	err = raw.Control(func(fd uintptr) {
		sysErr = getsockoptTCPInfo(int(fd), &info)
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
		Confidence: computeConfidence(&info),
	}

	snap.RTT = quality.NewMetric(time.Duration(info.Rtt) * time.Microsecond)
	snap.RTTVar = quality.NewMetric(time.Duration(info.Rttvar) * time.Microsecond)
	snap.Retrans = quality.NewMetric(info.Total_retrans)
	snap.Unacked = quality.NewMetric(info.Unacked)

	// Compute loss rate from lost/received
	if info.Segs_in > 0 {
		lossRate := float64(info.Lost) / float64(info.Segs_in)
		if lossRate > 1.0 {
			lossRate = 1.0
		}
		snap.Loss = quality.NewMetric(lossRate)
	} else {
		snap.Loss = quality.Unknown[float64]()
	}

	snap.Quality = computeQuality(snap)

	return snap, nil
}

// computeConfidence returns a confidence score based on how much data we have.
func computeConfidence(info *syscall.TCPInfo) uint8 {
	// More retransmits and received segments = more data to judge from
	if info.Segs_in == 0 {
		return 10 // barely connected
	}
	if info.Segs_in < 10 {
		return 30
	}
	if info.Segs_in < 50 {
		return 60
	}
	if info.Segs_in < 200 {
		return 80
	}
	return 95
}

// computeQuality computes multi-dimensional quality from raw TCP_INFO metrics.
func computeQuality(snap *quality.Snapshot) quality.Quality {
	q := quality.Quality{}

	// Latency score: 100 at 0ms, 0 at 500ms+
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

	// Loss score: 100 at 0%, 0 at 10%+
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

	// Stability: based on RTT variance
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
		q.Stability = 50 // unknown → middle
	}

	// Overall = weighted average (XMUX weights: Latency 30%, Loss 40%, Stability 30%)
	q.Overall = quality.DefaultXMUXWeights().ComputeOverall(q)

	return q
}

func getsockoptTCPInfo(fd int, info *syscall.TCPInfo) error {
	size := uint32(unsafe.Sizeof(*info))
	_, _, errno := syscall.Syscall6(
		syscall.SYS_GETSOCKOPT,
		uintptr(fd),
		syscall.IPPROTO_TCP,
		syscall.TCP_INFO,
		uintptr(unsafe.Pointer(info)),
		uintptr(unsafe.Pointer(&size)),
		0,
	)
	if errno != 0 {
		return errno
	}
	return nil
}
