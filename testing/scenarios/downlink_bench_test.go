package scenarios

// Real downlink throughput benchmark for dseg (downlink segmentation) over a
// genuine dual-end: VLESS inbound <- XHTTP packet-up + dseg <- VLESS outbound,
// upstream being a data-push server that writes a fixed payload (rather than
// the XOR echo, which is round-trip-limited).
//
// This is the baseline that was missing: BenchmarkDownlinkSegments in the
// splithttp package measures the synthetic producer path (in-process
// &httpServerConn{sess}) and never exercises the real VLESS-inbound ->
// production leg -> segment cache -> puller path.
//
// Run:
//   go test ./testing/scenarios -run '^$' -bench 'DsegRealDownlink' -benchtime 5s -count 3

import (
	"context"
	"io"
	stdnet "net"
	"strconv"
	"sync"
	"testing"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/testing/servers/tcp"
)

// startPushServer listens on 127.0.0.1, and for every accepted connection
// writes totalBytes of a fixed repeating pattern as fast as possible (models
// a large file / high-bitrate stream downstream). The peer (here: the VLESS
// server's freedom outbound) reads all of it.
func startPushServer(totalBytes int64) (net.Destination, func()) {
	ln, err := stdnet.Listen("tcp", "127.0.0.1:0")
	common.Must(err)
	dest := net.TCPDestination(net.LocalHostIP, net.Port(ln.Addr().(*stdnet.TCPAddr).Port))

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func(conn stdnet.Conn) {
				defer wg.Done()
				defer conn.Close()
				payload := make([]byte, 256<<10)
				for i := range payload {
					payload[i] = 0x5A
				}
				written := int64(0)
				for written < totalBytes {
					select {
					case <-ctx.Done():
						return
					default:
					}
					n := int64(len(payload))
					if written+n > totalBytes {
						n = totalBytes - written
					}
					if _, err := conn.Write(payload[:n]); err != nil {
						return
					}
					written += n
				}
			}(c)
		}
	}()

	cleanup := func() {
		cancel()
		_ = ln.Close()
		wg.Wait()
	}
	return dest, cleanup
}

func BenchmarkDsegRealDownlink(b *testing.B) {
	// 64 MiB per iteration: big enough to amortize connection/session setup
	// and to hit the adaptive-window / segment-pull path under load.
	const totalBytes = int64(32 << 20)

	pushDest, cleanup := startPushServer(totalBytes)
	b.Cleanup(cleanup)

	ep := &dsegEndpoints{serverPort: tcp.PickPort(), destOverride: pushDest}
	startServerOnly(b, ep) // *testing.B is a testing.TB
	startClientOnly(b, ep, "127.0.0.1", ep.serverPort)
	clientPort := ep.clientPort
	addr := "127.0.0.1:" + strconv.Itoa(int(clientPort))

	b.SetBytes(totalBytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := stdnet.Dial("tcp", addr)
		if err != nil {
			b.Fatal(err)
		}
		// Read the full downstream exactly once op = one download.
		n, err := io.Copy(io.Discard, conn)
		_ = conn.Close()
		if err != nil {
			b.Fatal(err)
		}
		if n != totalBytes {
			b.Fatalf("got %d bytes, want %d", n, totalBytes)
		}
	}
}
