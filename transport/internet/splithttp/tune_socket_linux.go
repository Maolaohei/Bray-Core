//go:build linux

package splithttp

import (
	"net"

	"golang.org/x/sys/unix"
)

// tuneOuterSocket tunes a freshly dialed outer TCP connection for the
// proxy path: request BBR congestion control (no loss-based halving on
// lossy paths) and enlarge socket buffers beyond Go's conservative
// defaults for high-BDP paths. Every failure is silent — kernels without
// BBR keep their default congestion control.
func tuneOuterSocket(conn net.Conn) {
	tc, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	raw, err := tc.SyscallConn()
	if err != nil {
		return
	}
	raw.Control(func(fd uintptr) {
		_ = unix.SetsockoptString(int(fd), unix.IPPROTO_TCP, unix.TCP_CONGESTION, "bbr")
		// 2MiB vs the ~64KiB-1MiB default; RTT is unknown at dial time so
		// this is a fixed safe bump rather than a per-link BDP value.
		_ = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF, 2<<20)
		_ = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_RCVBUF, 2<<20)
	})
}
