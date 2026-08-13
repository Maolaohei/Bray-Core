package splithttp_test

import (
	"context"
	"crypto/rand"
	"io"
	"runtime"
	"sync"
	"testing"

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
type benchNopLogHandler struct{}

// testBenchHeaders is the shared session MAC secret for benchmark configs
// (session wire modes are fail-closed without one).
var testBenchHeaders = map[string]string{BraySessionSecretHeader: "bench-test-secret"}

func (benchNopLogHandler) Handle(msg log.Message) {}

func init() {
	log.RegisterHandler(benchNopLogHandler{})
}

// =========================================================================
// Throughput Benchmarks
// =========================================================================

func BenchmarkXHTTP_H2C_Throughput(b *testing.B) {
	p := tcp.PickPort()
	settings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Path: "/sh", Mode: "packet-up", Headers: testBenchHeaders},
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
		ProtocolSettings: &Config{Path: "/sh", Mode: "packet-up", Headers: testBenchHeaders},
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
		ProtocolSettings: &Config{Path: "/sh", Mode: "stream-up", Headers: testBenchHeaders},
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
		ProtocolSettings: &Config{Path: "/sh", Mode: "packet-up", Headers: testBenchHeaders},
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
		ProtocolSettings: &Config{Path: "/sh", Mode: "packet-up", Headers: testBenchHeaders},
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
	readBuf := make([]byte, len(payload))

	// Warm up: establish the full connection path once (outer TCP + H2
	// negotiation + probe + GET/POST stream setup) outside the timer, and
	// keep the connection for the loop — this measures the pure first-byte
	// latency of a pooled connection (the common steady-state case), not
	// connection lifecycle. The per-iteration Dial+Close lifecycle is
	// covered by BenchmarkXHTTP_ConnectionStorm.
	conn, err := Dial(context.Background(),
		net.TCPDestination(net.DomainAddress("localhost"), p), settings)
	common.Must(err)
	defer conn.Close()
	if _, err := conn.Write(payload); err != nil {
		b.Fatal(err)
	}
	if _, err := io.ReadFull(conn, readBuf); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.Write(payload); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(conn, readBuf); err != nil {
			b.Fatal(err)
		}
	}
}

// =========================================================================
// Burst Benchmarks
// =========================================================================

func BenchmarkXHTTP_Burst_64KB(b *testing.B) {
	burstXHTTP(b, 64*1024, 10)
}

func BenchmarkXHTTP_Burst_1MB(b *testing.B) {
	burstXHTTP(b, 1024*1024, 5)
}

func burstXHTTP(b *testing.B, payloadSize int, bursts int) {
	p := tcp.PickPort()
	settings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Path: "/sh", Mode: "packet-up", Headers: testBenchHeaders},
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
// Connection Storm Benchmark
// =========================================================================

func BenchmarkXHTTP_ConnectionStorm(b *testing.B) {
	p := tcp.PickPort()
	settings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Path: "/sh", Mode: "packet-up", Headers: testBenchHeaders},
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
				ProtocolSettings: &Config{Path: "/sh", Mode: mode, Headers: testBenchHeaders},
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
		b.Run("conns_"+itoa(numConns), func(b *testing.B) {
			p := tcp.PickPort()
			settings := &internet.MemoryStreamConfig{
				ProtocolName:     "splithttp",
				ProtocolSettings: &Config{Path: "/sh", Mode: "packet-up", Headers: testBenchHeaders},
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
		ProtocolSettings: &Config{Path: "/sh", Mode: "packet-up", Headers: testBenchHeaders},
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
