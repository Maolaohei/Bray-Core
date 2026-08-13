//go:build !linux

package splithttp

import "net"

// tuneOuterSocket is a no-op on non-Linux platforms (no BBR, and Go's
// default socket buffers are already reasonable for short RTT paths).
func tuneOuterSocket(net.Conn) {}
