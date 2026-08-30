package internet_test

import (
	"context"
	gonet "net"
	"runtime"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	. "github.com/xtls/xray-core/transport/internet"
)

// dualStackReportsV6 records platforms where binding an IPv4 wildcard (0.0.0.0)
// yields a dual-stack socket whose LocalAddr reports [::]. On those platforms
// the bound address is NOT a faithful signal of the requested family, so the
// IPv4 direction can only be asserted functionally.
//
// Verified empirically on windows/amd64:
//
//	ListenPacket(ctx, "udp", "0.0.0.0:0") -> LocalAddr = [::]:port
const dualStackReportsV6 = runtime.GOOS == "windows"

// startUDPEcho spins up a minimal UDP echo server bound to the given address
// and returns the dialable destination plus a close func. The test is skipped
// when the stack cannot bind the requested family.
func startUDPEcho(t *testing.T, addr string) (xnet.Destination, func()) {
	t.Helper()

	pc, err := gonet.ListenPacket("udp", addr)
	if err != nil {
		t.Skipf("cannot listen on %s (address family unsupported here): %v", addr, err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 2048)
		for {
			n, from, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			if _, err := pc.WriteTo(buf[:n], from); err != nil {
				return
			}
		}
	}()

	local := pc.LocalAddr().(*gonet.UDPAddr)
	dest := xnet.UDPDestination(xnet.IPAddress(local.IP), xnet.Port(local.Port))
	return dest, func() {
		pc.Close()
		<-done
	}
}

// TestPOC_UDPWildcardBindFollowsDestFamily is the regression guard for upstream
// 540b9070 ("Transport: Bind the UDP outbound socket in the destination
// family").
//
// Before the fix the wildcard source was unconditionally 0.0.0.0. On stacks
// without IPv4-mapped IPv6 dual stack (hardened Linux kernels, some FreeBSD
// jails, netstacks), an IPv4-bound socket cannot reach an IPv6 destination, so
// the dialed PacketConn failed on first Write with "address family not
// supported by protocol" / "network is unreachable".
func TestPOC_UDPWildcardBindFollowsDestFamily(t *testing.T) {
	cases := []struct {
		name     string
		listenAt string
		wantIPv6 bool
	}{
		{name: "ipv4-dest", listenAt: "127.0.0.1:0", wantIPv6: false},
		{name: "ipv6-dest", listenAt: "[::1]:0", wantIPv6: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dest, closeServer := startUDPEcho(t, tc.listenAt)
			defer closeServer()

			dialer := DefaultSystemDialer{}

			// src == nil exercises the wildcard branch introduced by 540b9070.
			conn, err := dialer.Dial(context.Background(), nil, dest, &SocketConfig{})
			if err != nil {
				t.Fatalf("dial %v failed: %v", dest, err)
			}
			defer conn.Close()

			udpAddr, ok := conn.LocalAddr().(*gonet.UDPAddr)
			if !ok {
				t.Fatalf("LocalAddr() = %T (%v), want *net.UDPAddr", conn.LocalAddr(), conn.LocalAddr())
			}
			gotIPv6 := udpAddr.IP.To4() == nil

			switch {
			case tc.wantIPv6 && !gotIPv6:
				t.Fatalf("IPv6 destination must bind an IPv6 wildcard: dest=%v local=%v", dest, udpAddr)
			case !tc.wantIPv6 && gotIPv6 && !dualStackReportsV6:
				t.Fatalf("IPv4 destination must bind an IPv4 wildcard: dest=%v local=%v", dest, udpAddr)
			case !tc.wantIPv6 && gotIPv6 && !udpAddr.IP.IsUnspecified():
				t.Fatalf("IPv4 destination bound a specific IPv6 address: dest=%v local=%v", dest, udpAddr)
			}

			// Functional proof: whichever family was chosen, the socket must
			// actually reach the destination and round-trip a datagram.
			const payload = "bray-poc-540b9070"
			if _, err := conn.Write([]byte(payload)); err != nil {
				t.Fatalf("write to %v from local %v failed: %v", dest, udpAddr, err)
			}
			conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			buf := make([]byte, 256)
			n, err := conn.Read(buf)
			if err != nil {
				t.Fatalf("read echo from %v failed: %v", dest, err)
			}
			if string(buf[:n]) != payload {
				t.Fatalf("echo mismatch: got %q want %q", string(buf[:n]), payload)
			}
		})
	}
}

// TestPOC_UDPSrcStillOverridesWildcard guards the other half of the contract:
// 540b9070 only fills in the wildcard when no source address is requested. An
// explicit source must still win, otherwise per-outbound binding (sendThrough)
// would silently break.
func TestPOC_UDPSrcStillOverridesWildcard(t *testing.T) {
	dest, closeServer := startUDPEcho(t, "127.0.0.1:0")
	defer closeServer()

	dialer := DefaultSystemDialer{}

	src := xnet.IPAddress([]byte{127, 0, 0, 1})
	conn, err := dialer.Dial(context.Background(), src, dest, &SocketConfig{})
	if err != nil {
		t.Fatalf("dial with explicit src failed: %v", err)
	}
	defer conn.Close()

	udpAddr, ok := conn.LocalAddr().(*gonet.UDPAddr)
	if !ok {
		t.Fatalf("LocalAddr() = %T, want *net.UDPAddr", conn.LocalAddr())
	}
	if !udpAddr.IP.Equal(src.IP()) {
		t.Fatalf("explicit src not honored: local=%v want IP=%v", udpAddr, src.IP())
	}
}
