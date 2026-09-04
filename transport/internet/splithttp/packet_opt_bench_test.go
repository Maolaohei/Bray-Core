//go:build endurance

package splithttp_test

import (
	"context"
	"crypto/rand"
	"io"
	"sort"
	"sync"
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

// TestBenchmark_PacketUpBulkChunkJitterAB quantifies the cost of the
// anti-fingerprint body jitter. The echo must be CONCURRENT (read while
// writing): writing everything first wedges the echo path — the client's
// segment-pull window stalls while the echo keeps filling the server downseg
// cache, the upload queue backpressures at 64 packets, and the session
// tears down (packet queue full → 404 retry loop). That wedge is a
// test-echo artifact, not a product path: real uplinks stream while the
// far end consumes.
//
// Methodology: interleaved OFF/ON rounds (var flip between rounds), first
// round discarded as warmup (TCP ramp dominates), medians compared. PASS
// requires median(ON) ≥ 85% of median(OFF).
func TestBenchmark_PacketUpBulkChunkJitterAB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping jitter A/B under -short")
	}
	bindIP := testBindIP(t)
	p := tcp.PickPort()
	settings := &internet.MemoryStreamConfig{
		ProtocolName: "splithttp",
		ProtocolSettings: &Config{
			Path:    "/sh",
			Mode:    "packet-up",
			Headers: map[string]string{BraySessionSecretHeader: "bench-test-secret"},
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

	run := func() float64 {
		dest := net.TCPDestination(net.ParseAddress(bindIP), p)
		conn, err := Dial(context.Background(), dest, settings)
		common.Must(err)
		defer conn.Close()

		payload := make([]byte, 256*1024)
		rand.Read(payload)
		const n = 80 // 20MB per round
		total := n * len(payload)
		readBuf := make([]byte, total)

		var wg sync.WaitGroup
		var readErr error
		start := time.Now()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, readErr = io.ReadFull(conn, readBuf)
		}()
		for i := 0; i < n; i++ {
			if _, err := conn.Write(payload); err != nil {
				t.Fatal(err)
			}
		}
		wg.Wait()
		elapsed := time.Since(start).Seconds()
		if readErr != nil {
			t.Fatal(readErr)
		}
		for i := 0; i < n; i++ {
			off := i * len(payload)
			if string(readBuf[off:off+len(payload)]) != string(payload) {
				t.Fatalf("echo corruption at copy %d", i)
			}
		}
		return float64(total*8) / 1e6 / elapsed
	}

	jFn := PacketUploadChunkJitterFn
	defer func() { PacketUploadChunkJitterFn = jFn }()

	var fixedRates, jitterRates []float64
	for round := 0; round < 5; round++ {
		PacketUploadChunkJitterFn = nil // OFF
		f := run()
		PacketUploadChunkJitterFn = jFn // ON
		j := run()
		t.Logf("round %d: OFF=%.0f Mbps ON=%.0f Mbps", round, f, j)
		if round == 0 {
			continue // warmup: discard, TCP ramp + allocator settle
		}
		fixedRates = append(fixedRates, f)
		jitterRates = append(jitterRates, j)
	}
	median := func(xs []float64) float64 {
		s := append([]float64(nil), xs...)
		sort.Float64s(s)
		return s[len(s)/2]
	}
	fixed, jittered := median(fixedRates), median(jitterRates)
	delta := (jittered - fixed) / fixed * 100
	t.Logf("median: OFF=%.0f Mbps ON=%.0f Mbps (delta %+.1f%%)", fixed, jittered, delta)
	if jittered < fixed*0.85 {
		t.Fatalf("median jittered goodput %.0f Mbps < 85%% of fixed %.0f Mbps: body jitter is too expensive",
			jittered, fixed)
	}
}
