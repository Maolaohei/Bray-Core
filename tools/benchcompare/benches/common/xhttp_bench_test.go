// Copyright 2026 Bray-Core. Harness-owned benchmark source.
//
// This file is injected by tools/benchcompare into EVERY target worktree
// (Bray-Core and upstream Xray-core alike) so the exact same scenario code
// runs on both. It only uses public, upstream-compatible splithttp APIs.
//
// The one target difference — Bray's fail-closed session wire mode — is
// handled via the BENCHCMP_XHTTP_SESSION environment variable, which the
// harness sets to "bray-xhttp-session-v1=<secret>" only for the Bray target.
// Upstream ignores the unknown header entirely.
package splithttp_test

import (
	"context"
	"crypto/rand"
	"io"
	stdnet "net"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport/internet"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/internet/tls"
)

// Quiet default log handler for microbenches so probe/teardown noise does not
// interleave with go test bench result lines or burn CPU on console writes.
type benchcmpNopLogHandler struct{}

func (benchcmpNopLogHandler) Handle(msg log.Message) {}

func init() {
	log.RegisterHandler(benchcmpNopLogHandler{})
}

// benchXHTTPHeaders returns the session-MAC header for the current target, or
// nil when none is required (upstream).
func benchXHTTPHeaders() map[string]string {
	v := os.Getenv("BENCHCMP_XHTTP_SESSION")
	if v == "" {
		return nil
	}
	kv := strings.SplitN(v, "=", 2)
	if len(kv) != 2 {
		return nil
	}
	return map[string]string{kv[0]: kv[1]}
}

// =========================================================================
// Throughput Benchmarks
// =========================================================================

func BenchmarkXHTTP_H2C_Throughput(b *testing.B) {
	p := tcp.PickPort()
	settings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Path: "/sh", Mode: "packet-up", Headers: benchXHTTPHeaders()},
	}

	listen, err := ListenXH(context.Background(), net.LocalHostIP, p, settings, func(conn stat.Connection) {
		go func(c stat.Connection) {
			defer c.Close()
			buf.Copy(buf.NewReader(c), buf.NewWriter(c))
		}(conn)
	})
	common.Must(err)
	defer listen.Close()

	conn, err := Dial(context.Background(),
		net.TCPDestination(net.DomainAddress("localhost"), p), settings)
	common.Must(err)
	defer conn.Close()

	payload := make([]byte, 32*1024)
	rand.Read(payload)

	b.SetBytes(int64(len(payload)) * 2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.Write(payload); err != nil {
			b.Fatal(err)
		}
		readBuf := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, readBuf); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkXHTTP_H2_Throughput(b *testing.B) {
	if runtime.GOARCH == "arm64" {
		b.Skip("arm64")
	}

	p := tcp.PickPort()
	ct, ctHash := cert.MustGenerate(nil, cert.CommonName("localhost"))
	settings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Path: "/sh", Mode: "packet-up", Headers: benchXHTTPHeaders()},
		SecurityType:     "tls",
		SecuritySettings: &tls.Config{
			Certificate:          []*tls.Certificate{tls.ParseCertificate(ct)},
			PinnedPeerCertSha256: [][]byte{ctHash[:]},
		},
	}

	listen, err := ListenXH(context.Background(), net.LocalHostIP, p, settings, func(conn stat.Connection) {
		go func(c stat.Connection) {
			defer c.Close()
			buf.Copy(buf.NewReader(c), buf.NewWriter(c))
		}(conn)
	})
	common.Must(err)
	defer listen.Close()

	conn, err := Dial(context.Background(),
		net.TCPDestination(net.DomainAddress("localhost"), p), settings)
	common.Must(err)
	defer conn.Close()

	payload := make([]byte, 32*1024)
	rand.Read(payload)

	b.SetBytes(int64(len(payload)) * 2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.Write(payload); err != nil {
			b.Fatal(err)
		}
		readBuf := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, readBuf); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkXHTTP_StreamUp_Throughput(b *testing.B) {
	if runtime.GOARCH == "arm64" {
		b.Skip("arm64")
	}

	p := tcp.PickPort()
	ct, ctHash := cert.MustGenerate(nil, cert.CommonName("localhost"))
	settings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Path: "/sh", Mode: "stream-up", Headers: benchXHTTPHeaders()},
		SecurityType:     "tls",
		SecuritySettings: &tls.Config{
			Certificate:          []*tls.Certificate{tls.ParseCertificate(ct)},
			PinnedPeerCertSha256: [][]byte{ctHash[:]},
		},
	}

	listen, err := ListenXH(context.Background(), net.LocalHostIP, p, settings, func(conn stat.Connection) {
		go func(c stat.Connection) {
			defer c.Close()
			buf.Copy(buf.NewReader(c), buf.NewWriter(c))
		}(conn)
	})
	common.Must(err)
	defer listen.Close()

	conn, err := Dial(context.Background(),
		net.TCPDestination(net.DomainAddress("localhost"), p), settings)
	common.Must(err)
	defer conn.Close()

	payload := make([]byte, 32*1024)
	rand.Read(payload)

	b.SetBytes(int64(len(payload)) * 2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.Write(payload); err != nil {
			b.Fatal(err)
		}
		readBuf := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, readBuf); err != nil {
			b.Fatal(err)
		}
	}
}

// =========================================================================
// Parallel Throughput Benchmarks
// =========================================================================

func BenchmarkXHTTP_Parallel_H2C(b *testing.B) {
	p := tcp.PickPort()
	settings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Path: "/sh", Mode: "packet-up", Headers: benchXHTTPHeaders()},
	}

	listen, err := ListenXH(context.Background(), net.LocalHostIP, p, settings, func(conn stat.Connection) {
		go func(c stat.Connection) {
			defer c.Close()
			buf.Copy(buf.NewReader(c), buf.NewWriter(c))
		}(conn)
	})
	common.Must(err)
	defer listen.Close()

	payload := make([]byte, 16*1024)
	rand.Read(payload)

	b.SetBytes(int64(len(payload)) * 2)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		conn, err := Dial(context.Background(),
			net.TCPDestination(net.DomainAddress("localhost"), p), settings)
		if err != nil {
			b.Error(err)
			return
		}
		defer conn.Close()

		for pb.Next() {
			if _, err := conn.Write(payload); err != nil {
				b.Error(err)
				return
			}
			readBuf := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, readBuf); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// =========================================================================
// Latency Benchmarks
// =========================================================================

func BenchmarkXHTTP_TTFB(b *testing.B) {
	p := tcp.PickPort()
	settings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Path: "/sh", Mode: "packet-up", Headers: benchXHTTPHeaders()},
	}

	listen, err := ListenXH(context.Background(), net.LocalHostIP, p, settings, func(conn stat.Connection) {
		go func(c stat.Connection) {
			defer c.Close()
			io.Copy(c, c)
		}(conn)
	})
	common.Must(err)
	defer listen.Close()

	payload := make([]byte, 128)
	rand.Read(payload)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := Dial(context.Background(),
			net.TCPDestination(net.DomainAddress("localhost"), p), settings)
		if err != nil {
			b.Fatal(err)
		}
		start := time.Now()
		if _, err := conn.Write(payload); err != nil {
			conn.Close()
			b.Fatal(err)
		}
		readBuf := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, readBuf); err != nil {
			conn.Close()
			b.Fatal(err)
		}
		_ = time.Since(start)
		conn.Close()
	}
}

// =========================================================================
// Burst Benchmarks
// =========================================================================

func BenchmarkXHTTP_Burst_64KB(b *testing.B) {
	benchcmpBurstXHTTP(b, 64*1024, 10)
}

func BenchmarkXHTTP_Burst_1MB(b *testing.B) {
	benchcmpBurstXHTTP(b, 1024*1024, 5)
}

func benchcmpBurstXHTTP(b *testing.B, payloadSize int, bursts int) {
	p := tcp.PickPort()
	settings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Path: "/sh", Mode: "packet-up", Headers: benchXHTTPHeaders()},
	}

	listen, err := ListenXH(context.Background(), net.LocalHostIP, p, settings, func(conn stat.Connection) {
		go func(c stat.Connection) {
			defer c.Close()
			buf.Copy(buf.NewReader(c), buf.NewWriter(c))
		}(conn)
	})
	common.Must(err)
	defer listen.Close()

	conn, err := Dial(context.Background(),
		net.TCPDestination(net.DomainAddress("localhost"), p), settings)
	common.Must(err)
	defer conn.Close()

	payload := make([]byte, payloadSize)
	rand.Read(payload)

	b.SetBytes(int64(payloadSize) * 2 * int64(bursts))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < bursts; j++ {
			if _, err := conn.Write(payload); err != nil {
				b.Fatal(err)
			}
			readBuf := make([]byte, payloadSize)
			if _, err := io.ReadFull(conn, readBuf); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// =========================================================================
// TCP Echo Baseline (network-only round-trip, no XHTTP layer)
// =========================================================================

// BenchmarkTCP_Echo measures a plain 128B write+read round-trip over a
// loopback TCP connection (stdlib net, echo server). It isolates the network
// baseline so XHTTP-layer costs (session, headers, hub) can be separated from
// raw transport latency when comparing targets.
func BenchmarkTCP_Echo(b *testing.B) {
	ln, err := stdnet.Listen("tcp", "127.0.0.1:0")
	common.Must(err)
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c stdnet.Conn) {
				defer c.Close()
				io.Copy(c, c)
			}(c)
		}
	}()

	conn, err := stdnet.Dial("tcp", ln.Addr().String())
	common.Must(err)
	defer conn.Close()

	payload := make([]byte, 128)
	rand.Read(payload)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.Write(payload); err != nil {
			b.Fatal(err)
		}
		readBuf := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, readBuf); err != nil {
			b.Fatal(err)
		}
	}
}

// =========================================================================
// Cold Start Benchmark (includes Dial latency)
// =========================================================================

// BenchmarkXHTTP_ColdStart measures the full cold-start latency of a brand-new
// session: Dial + session establishment + first write/read round-trip, per
// iteration. Unlike BenchmarkXHTTP_TTFB (write+read only, Dial outside the
// timer), this includes connection establishment, so the two scenarios together
// decompose cold-start cost into connect vs first-packet.
func BenchmarkXHTTP_ColdStart(b *testing.B) {
	p := tcp.PickPort()
	settings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Path: "/sh", Mode: "packet-up", Headers: benchXHTTPHeaders()},
	}

	listen, err := ListenXH(context.Background(), net.LocalHostIP, p, settings, func(conn stat.Connection) {
		go func(c stat.Connection) {
			defer c.Close()
			buf.Copy(buf.NewReader(c), buf.NewWriter(c))
		}(conn)
	})
	common.Must(err)
	defer listen.Close()

	dest := net.TCPDestination(net.DomainAddress("localhost"), p)
	payload := make([]byte, 128)
	rand.Read(payload)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		conn, err := Dial(context.Background(), dest, settings)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := conn.Write(payload); err != nil {
			conn.Close()
			b.Fatal(err)
		}
		readBuf := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, readBuf); err != nil {
			conn.Close()
			b.Fatal(err)
		}
		_ = time.Since(start)
		conn.Close()
	}
}

// =========================================================================
// Connection Storm Benchmark
// =========================================================================

func BenchmarkXHTTP_ConnectionStorm(b *testing.B) {
	p := tcp.PickPort()
	settings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Path: "/sh", Mode: "packet-up", Headers: benchXHTTPHeaders()},
	}

	listen, err := ListenXH(context.Background(), net.LocalHostIP, p, settings, func(conn stat.Connection) {
		go func(c stat.Connection) {
			defer c.Close()
			io.Copy(c, c)
		}(conn)
	})
	common.Must(err)
	defer listen.Close()

	payload := make([]byte, 128)
	rand.Read(payload)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := Dial(context.Background(),
			net.TCPDestination(net.DomainAddress("localhost"), p), settings)
		if err != nil {
			b.Fatal(err)
		}
		conn.Write(payload)
		readBuf := make([]byte, 128)
		io.ReadFull(conn, readBuf)
		conn.Close()
	}
}

// =========================================================================
// Multi-mode Comparison
// =========================================================================

func BenchmarkXHTTP_Modes(b *testing.B) {
	for _, mode := range []string{"packet-up", "stream-up", "stream-one"} {
		b.Run(mode, func(b *testing.B) {
			p := tcp.PickPort()
			settings := &internet.MemoryStreamConfig{
				ProtocolName:     "splithttp",
				ProtocolSettings: &Config{Path: "/sh", Mode: mode, Headers: benchXHTTPHeaders()},
			}

			listen, err := ListenXH(context.Background(), net.LocalHostIP, p, settings, func(conn stat.Connection) {
				go func(c stat.Connection) {
					defer c.Close()
					buf.Copy(buf.NewReader(c), buf.NewWriter(c))
				}(conn)
			})
			common.Must(err)
			defer listen.Close()

			conn, err := Dial(context.Background(),
				net.TCPDestination(net.DomainAddress("localhost"), p), settings)
			common.Must(err)
			defer conn.Close()

			payload := make([]byte, 32*1024)
			rand.Read(payload)

			b.SetBytes(int64(len(payload)) * 2)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := conn.Write(payload); err != nil {
					b.Fatal(err)
				}
				readBuf := make([]byte, len(payload))
				if _, err := io.ReadFull(conn, readBuf); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// =========================================================================
// Concurrent Connection Throughput
// =========================================================================

func BenchmarkXHTTP_ConcurrentConnections(b *testing.B) {
	for _, numConns := range []int{1, 4, 8, 16} {
		b.Run("conns_"+benchcmpItoa(numConns), func(b *testing.B) {
			p := tcp.PickPort()
			settings := &internet.MemoryStreamConfig{
				ProtocolName:     "splithttp",
				ProtocolSettings: &Config{Path: "/sh", Mode: "packet-up", Headers: benchXHTTPHeaders()},
			}

			listen, err := ListenXH(context.Background(), net.LocalHostIP, p, settings, func(conn stat.Connection) {
				go func(c stat.Connection) {
					defer c.Close()
					buf.Copy(buf.NewReader(c), buf.NewWriter(c))
				}(conn)
			})
			common.Must(err)
			defer listen.Close()

			payload := make([]byte, 16*1024)
			rand.Read(payload)

			conns := make([]stat.Connection, numConns)
			for i := range conns {
				conn, err := Dial(context.Background(),
					net.TCPDestination(net.DomainAddress("localhost"), p), settings)
				if err != nil {
					b.Fatal(err)
				}
				conns[i] = conn
			}
			defer func() {
				for _, c := range conns {
					c.Close()
				}
			}()

			b.SetBytes(int64(len(payload)) * 2 * int64(numConns))
			b.ResetTimer()
			var wg sync.WaitGroup
			for _, c := range conns {
				wg.Add(1)
				go func(conn stat.Connection) {
					defer wg.Done()
					for i := 0; i < b.N/numConns+1; i++ {
						conn.Write(payload)
						readBuf := make([]byte, len(payload))
						io.ReadFull(conn, readBuf)
					}
				}(c)
			}
			wg.Wait()
		})
	}
}

// =========================================================================
// Memory Benchmarks
// =========================================================================

func BenchmarkXHTTP_MemoryAllocations(b *testing.B) {
	p := tcp.PickPort()
	settings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Path: "/sh", Mode: "packet-up", Headers: benchXHTTPHeaders()},
	}

	listen, err := ListenXH(context.Background(), net.LocalHostIP, p, settings, func(conn stat.Connection) {
		go func(c stat.Connection) {
			defer c.Close()
			buf.Copy(buf.NewReader(c), buf.NewWriter(c))
		}(conn)
	})
	common.Must(err)
	defer listen.Close()

	conn, err := Dial(context.Background(),
		net.TCPDestination(net.DomainAddress("localhost"), p), settings)
	common.Must(err)
	defer conn.Close()

	payload := make([]byte, 4*1024)
	rand.Read(payload)

	b.SetBytes(int64(len(payload)) * 2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.Write(payload); err != nil {
			b.Fatal(err)
		}
		readBuf := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, readBuf); err != nil {
			b.Fatal(err)
		}
	}
}

func benchcmpItoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := [20]byte{}
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
