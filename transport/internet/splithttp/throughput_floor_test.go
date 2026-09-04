package splithttp_test

// Performance checklist item: a sustained loopback echo through the real
// splithttp stack (H2C packet-up) must not collapse. The floor is RELATIVE —
// measured against a raw TCP echo on the same host, same payload — so slow CI
// boxes don't flake while a genuine collapse (regression, pathological
// pacing, queue livelock) still fails. Hosts that rewrite HTTP skip via the
// sanity guard: their numbers describe the interceptor, not the product.

import (
	"context"
	crand "crypto/rand"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport/internet"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/stat"
)

const throughputPayload = 64 * 1024
const throughputCopies = 128 // 8 MiB per run

// echoThroughput writes copies×64KiB and reads the echo back concurrently,
// returning goodput in Mbit/s. Errors fail the test (collapse = test failure,
// not a skipped measurement).
func echoThroughput(t *testing.T, conn net.Conn) float64 {
	t.Helper()
	defer conn.Close()

	payload := make([]byte, throughputPayload)
	crand.Read(payload)
	total := throughputCopies * len(payload)

	var wg sync.WaitGroup
	var readErr error
	readBuf := make([]byte, total)
	start := time.Now()
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, readErr = io.ReadFull(conn, readBuf)
	}()
	for i := 0; i < throughputCopies; i++ {
		if _, err := conn.Write(payload); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	wg.Wait()
	elapsed := time.Since(start)
	if readErr != nil {
		t.Fatalf("echo read: %v", readErr)
	}
	for i := 0; i < throughputCopies; i++ {
		off := i * len(payload)
		if string(readBuf[off:off+len(payload)]) != string(payload) {
			t.Fatalf("echo corruption at copy %d", i)
		}
	}
	return float64(total) * 8 / elapsed.Seconds() / 1e6
}

func TestThroughputFloor_LoopbackEcho(t *testing.T) {
	if testing.Short() {
		t.Skip("throughput floor skipped under -short")
	}
	// Wire-clean bind IP: loopback when possible, else first clean LAN IPv4.
	bindIP := testBindIP(t)

	// Raw TCP baseline: plain echo server, same payload, same copy count.
	tln, err := net.Listen("tcp", net.JoinHostPort(bindIP, "0"))
	common.Must(err)
	defer tln.Close()
	go func() {
		for {
			c, err := tln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				io.Copy(c, c)
			}(c)
		}
	}()
	tConn, err := net.Dial("tcp", tln.Addr().String())
	common.Must(err)
	tcpMbps := echoThroughput(t, tConn)

	// splithttp H2C packet-up echo.
	p := tcp.PickPort()
	settings := &internet.MemoryStreamConfig{
		ProtocolName: "splithttp",
		ProtocolSettings: &Config{
			Path: "/sh",
			Mode: "packet-up",
			Headers: map[string]string{BraySessionSecretHeader: "throughput-floor"},
		},
	}
	listen, err := ListenXH(context.Background(), xnet.ParseAddress(bindIP), p, settings, func(conn stat.Connection) {
		go func(c stat.Connection) {
			defer c.Close()
			buf.Copy(buf.NewReader(c), buf.NewWriter(c))
		}(conn)
	})
	common.Must(err)
	defer listen.Close()
	dest := xnet.TCPDestination(xnet.ParseAddress(bindIP), p)
	conn, err := Dial(context.Background(), dest, settings)
	common.Must(err)
	shMbps := echoThroughput(t, conn)

	ratio := shMbps / tcpMbps
	t.Logf("loopback echo goodput: tcp=%.0f Mbps splithttp=%.0f Mbps (%.1f%%)", tcpMbps, shMbps, ratio*100)

	// Collapse floor: splithttp must reach at least 8% of raw loopback TCP
	// AND at least 40 Mbps absolute (whichever is looser on a slow box).
	// A healthy stack lands far above both; a livelocked or pathologically
	// paced one lands far below.
	floor := tcpMbps * 0.08
	if floor < 40 {
		floor = 40
	}
	if shMbps < floor {
		t.Fatalf("splithttp goodput %.0f Mbps below collapse floor %.0f Mbps (tcp %.0f Mbps, %.1f%%)",
			shMbps, floor, tcpMbps, ratio*100)
	}
}
