//go:build endurance

package splithttp_test

import (
	"context"
	"crypto/rand"
	"io"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport/internet"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/stat"
)

// Bulk uplink: continuous writes then one bulk read. Exercises adaptive
// launch pacing (skip interval when backlog / full chunks exist).
func TestBenchmark_PacketUpBulkDefaultPacing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long XHTTP packet-up bulk bench under -short")
	}
	// Wire-clean bind IP: loopback when possible, else first clean LAN IPv4.
	bindIP := testBindIP(t)
	p := tcp.PickPort()
	settings := &internet.MemoryStreamConfig{
		ProtocolName: "splithttp",
		ProtocolSettings: &Config{
			Path: "/sh",
			Mode: "packet-up",
			// Session wire modes require a shared MAC secret (fail-closed).
			Headers: map[string]string{BraySessionSecretHeader: "bench-test-secret"},
			// Keep defaults for max post size / interval (30ms) via nil fields.
		},
	}
	listen, err := ListenXH(context.Background(), net.ParseAddress(bindIP), p, settings, func(conn stat.Connection) {
		go func(c stat.Connection) {
			defer c.Close()
			buf.Copy(buf.NewReader(c), buf.NewWriter(c))
		}(conn)
	})
	common.Must(err)
	defer listen.Close()

	dest := net.TCPDestination(net.ParseAddress(bindIP), p)
	conn, err := Dial(context.Background(), dest, settings)
	common.Must(err)
	defer conn.Close()

	payload := make([]byte, 256*1024)
	rand.Read(payload)
	const n = 40
	total := n * len(payload)
	start := time.Now()
	for i := 0; i < n; i++ {
		if _, err := conn.Write(payload); err != nil {
			t.Fatal(err)
		}
	}
	readBuf := make([]byte, total)
	if _, err := io.ReadFull(conn, readBuf); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start).Seconds()
	mbps := float64(total*8) / 1e6 / elapsed
	t.Logf("H2C packet-up bulk default-pacing 256KB x %d = %.1f MB in %.2fs = %.1f Mbps", n, float64(total)/1e6, elapsed, mbps)
}
