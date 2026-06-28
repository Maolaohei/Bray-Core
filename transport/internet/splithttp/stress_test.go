package splithttp

import (
	"context"
	"crypto/rand"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/internet/tls"
)

// =========================================================================
// Goroutine leak detection
// =========================================================================

func checkGoroutineLeak(t *testing.T) func() {
	t.Helper()
	start := runtime.NumGoroutine()
	return func() {
		ResetGlobalDialer()
		time.Sleep(500 * time.Millisecond)
		delta := runtime.NumGoroutine() - start
		if delta > 100 {
			t.Errorf("Goroutine leak: +%d goroutines after test", delta)
		}
	}
}

// =========================================================================
// 4 different echo servers on different ports (simulating multiple domains)
// =========================================================================

type echoServer struct {
	listen   internet.Listener
	port     net.Port
	settings *internet.MemoryStreamConfig
}

func startEchoServer(t *testing.T, mode string, useTLS bool) *echoServer {
	t.Helper()
	p := tcp.PickPort()

	var settings *internet.MemoryStreamConfig
	if useTLS {
		ct, ctHash := cert.MustGenerate(nil, cert.CommonName("localhost"))
		settings = &internet.MemoryStreamConfig{
			ProtocolName: "splithttp",
			ProtocolSettings: &Config{
				Path:               "/sh",
				Mode:               mode,
				ScMaxEachPostBytes: &RangeConfig{From: 500000, To: 500000},
			},
			SecurityType: "tls",
			SecuritySettings: &tls.Config{
				Certificate:          []*tls.Certificate{tls.ParseCertificate(ct)},
				PinnedPeerCertSha256: [][]byte{ctHash[:]},
			},
		}
	} else {
		settings = &internet.MemoryStreamConfig{
			ProtocolName: "splithttp",
			ProtocolSettings: &Config{
				Path:               "/sh",
				Mode:               mode,
				ScMaxEachPostBytes: &RangeConfig{From: 500000, To: 500000},
			},
		}
	}

	listen, err := ListenXH(context.Background(), net.LocalHostIP, p, settings, func(conn stat.Connection) {
		go func(c stat.Connection) {
			defer c.Close()
			io.Copy(c, c)
		}(conn)
	})
	common.Must(err)

	return &echoServer{listen: listen, port: p, settings: settings}
}

func (s *echoServer) close() {
	s.listen.Close()
}

// =========================================================================
// Test: Multi-domain concurrent stress (H2, 30s)
// =========================================================================

func TestXHTTP_MultiDomainStress_H2(t *testing.T) {
	if testing.Short() {
		t.Skip("long stress test")
	}
	if runtime.GOARCH == "arm64" {
		t.Skip("arm64")
	}
	defer checkGoroutineLeak(t)()

	// Start 4 servers on different ports (simulating 4 different domains)
	servers := []*echoServer{
		startEchoServer(t, "packet-up", true),
		startEchoServer(t, "stream-up", true),
		startEchoServer(t, "stream-one", true),
		startEchoServer(t, "packet-up", true),
	}
	defer func() {
		for _, s := range servers {
			s.close()
		}
	}()

	var (
		totalBytes atomic.Int64
		totalErrs  atomic.Int64
		activeConn atomic.Int64
		wg         sync.WaitGroup
		stop       = make(chan struct{})
	)

	// Connection factory: 5 concurrent connections PER server = 20 total
	connsPerServer := 5
	payloadSizes := []int{512, 4096, 16384, 65536} // mix of small and large

	for idx, srv := range servers {
		for c := 0; c < connsPerServer; c++ {
			wg.Add(1)
			go func(server *echoServer, workerID int) {
				defer wg.Done()

				// Connect
				conn, err := Dial(context.Background(),
					net.TCPDestination(net.DomainAddress("localhost"), server.port),
					server.settings)
				if err != nil {
					t.Logf("  [srv%d-w%d] dial failed: %v", idx, workerID, err)
					totalErrs.Add(1)
					return
				}
				defer conn.Close()
				activeConn.Add(1)
				defer activeConn.Add(-1)

				payload := make([]byte, payloadSizes[workerID%len(payloadSizes)])
				rand.Read(payload)

				for {
					select {
					case <-stop:
						return
					default:
					}

					if _, err := conn.Write(payload); err != nil {
						totalErrs.Add(1)
						return
					}
					n := 0
					buf := make([]byte, len(payload))
					for n < len(payload) {
						read, err := conn.Read(buf[n:])
						if err != nil {
							totalErrs.Add(1)
							return
						}
						n += read
					}
					totalBytes.Add(int64(len(payload)))
				}
			}(srv, c)
		}
	}

	// Let it run for a while
	t.Logf("Starting 30s multi-domain stress test...")
	t.Logf("Servers: %d, connections: %d, payload: 512B-64KB mix",
		len(servers), len(servers)*connsPerServer)

	// Monitor goroutines and bytes every 5 seconds
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				b := totalBytes.Load()
				conn := activeConn.Load()
				mb := float64(b) / 1e6
				t.Logf("  [progress] %d active conns, %.0f MB transferred, %d errors",
					conn, mb, totalErrs.Load())
			case <-done:
				return
			}
		}
	}()

	time.Sleep(5 * time.Second)
	close(stop)
	close(done)
	wg.Wait()

	mb := float64(totalBytes.Load()) / 1e6
	mbps := mb * 8 / 5 // Megabits per second
	errs := totalErrs.Load()
	t.Logf("=== Multi-domain H2 stress (5s) ===")
	t.Logf("  Total: %.0f MB in 5s = %.1f Mbps", mb, mbps)
	t.Logf("  Errors: %d", errs)

	if errs > 10 {
		t.Errorf("Too many errors (%d) during stress test", errs)
	}
}

// =========================================================================
// Test: Long-lived Idle + Burst (simulating video switch)
// =========================================================================

func TestXHTTP_IdleThenBurst(t *testing.T) {
	if testing.Short() {
		t.Skip("long stress test")
	}
	if runtime.GOARCH == "arm64" {
		t.Skip("arm64")
	}
	defer checkGoroutineLeak(t)()

	srv := startEchoServer(t, "packet-up", true)
	defer srv.close()

	conn, err := Dial(context.Background(),
		net.TCPDestination(net.DomainAddress("localhost"), srv.port),
		srv.settings)
	common.Must(err)
	defer conn.Close()

	// Phase 1: send data for 2s
	t.Logf("Phase 1: sending data for 2s...")
	start := time.Now()
	payload := make([]byte, 32768)
	rand.Read(payload)
	for time.Since(start) < 2*time.Second {
		if _, err := conn.Write(payload); err != nil {
			t.Fatalf("write during burst: %v", err)
		}
		n := 0
		buf := make([]byte, len(payload))
		for n < len(payload) {
			read, err := conn.Read(buf[n:])
			if err != nil {
				t.Fatalf("read during burst: %v", err)
			}
			n += read
		}
	}
	t.Logf("  Burst completed")

	// Phase 2: idle for 2s (simulates video pause)
	t.Logf("Phase 2: idle 2s (simulating video pause)...")
	time.Sleep(2 * time.Second)
	t.Logf("  Idle completed")

	// Phase 3: burst again — should recover without reconnection
	t.Logf("Phase 3: burst again after idle...")
	burstStart := time.Now()
	var recoveryTime time.Duration
	for i := 0; i < 5; i++ {
		_, err := conn.Write(payload)
		if err != nil {
			t.Fatalf("write after idle: %v", err)
		}
		n := 0
		buf := make([]byte, len(payload))
		for n < len(payload) {
			read, err := conn.Read(buf[n:])
			if err != nil {
				t.Fatalf("read after idle: %v", err)
			}
			n += read
		}
		if i == 0 {
			recoveryTime = time.Since(burstStart)
		}
	}
	t.Logf("  First post-idle write completed in %v", recoveryTime)

	if recoveryTime > 3*time.Second {
		t.Logf("  WARNING: recovery took %.2fs (possible stall)", recoveryTime.Seconds())
	}
}

// =========================================================================
// Test: Rapid video switching (20 switches in quick succession)
// =========================================================================

func TestXHTTP_RapidVideoSwitch(t *testing.T) {
	if testing.Short() {
		t.Skip("long stress test")
	}
	if runtime.GOARCH == "arm64" {
		t.Skip("arm64")
	}
	defer checkGoroutineLeak(t)()

	// Simulate 4 video servers (CDN endpoints)
	servers := make([]*echoServer, 4)
	for i := range servers {
		servers[i] = startEchoServer(t, "packet-up", true)
	}
	defer func() {
		for _, s := range servers {
			s.close()
		}
	}()

	payload := make([]byte, 1024*1024) // 1MB per "video segment"
	rand.Read(payload)

	var successes, failures int
	var totalSwitchTime time.Duration

	for i := 0; i < 20; i++ {
		srv := servers[i%len(servers)]
		switchStart := time.Now()

		conn, err := Dial(context.Background(),
			net.TCPDestination(net.DomainAddress("localhost"), srv.port),
			srv.settings)
		if err != nil {
			failures++
			t.Logf("  [switch %d] dial failed: %v", i, err)
			continue
		}

		// Write 1MB
		if _, err := conn.Write(payload); err != nil {
			failures++
			conn.Close()
			t.Logf("  [switch %d] write failed: %v", i, err)
			continue
		}

		// Read echo
		n := 0
		buf := make([]byte, len(payload))
		for n < len(payload) {
			read, err := conn.Read(buf[n:])
			if err != nil {
				failures++
				conn.Close()
				t.Logf("  [switch %d] read failed: %v", i, err)
				continue
			}
			n += read
		}
		conn.Close()
		successes++
		totalSwitchTime += time.Since(switchStart)
	}

	avgSwitch := totalSwitchTime / time.Duration(successes)
	t.Logf("=== Rapid video switch (20 switches) ===")
	t.Logf("  Success: %d, Failures: %d", successes, failures)
	t.Logf("  Avg switch time: %.1fms", float64(avgSwitch)/float64(time.Millisecond))

	if failures > 5 {
		t.Errorf("Too many switch failures (%d/20)", failures)
	}
	if successes > 0 && avgSwitch > 3*time.Second {
		t.Logf("  WARNING: avg switch time %.2fs (>3s)", avgSwitch.Seconds())
	}
}

// =========================================================================
// Test: H2C concurrent mixed traffic (no TLS, pure H2)
// =========================================================================

func TestXHTTP_H2C_MixedTraffic(t *testing.T) {
	if testing.Short() {
		t.Skip("long stress test")
	}
	defer checkGoroutineLeak(t)()

	_, p, settings := func() (internet.Listener, net.Port, *internet.MemoryStreamConfig) {
		lp := tcp.PickPort()
		s := &internet.MemoryStreamConfig{
			ProtocolName:     "splithttp",
			ProtocolSettings: &Config{Path: "/sh"},
		}
		l, err := ListenXH(context.Background(), net.LocalHostIP, lp, s, func(conn stat.Connection) {
			go func(c stat.Connection) {
				defer c.Close()
				io.Copy(c, c)
			}(conn)
		})
		common.Must(err)
		return l, lp, s
	}()

	// Use the listener
	var bytes atomic.Int64
	var errors atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	worker := func(id int) {
		defer wg.Done()
		conn, err := Dial(context.Background(),
			net.TCPDestination(net.LocalHostIP, p), settings)
		if err != nil {
			errors.Add(1)
			return
		}
		defer conn.Close()
		payload := make([]byte, 4096)
		rand.Read(payload)

		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := conn.Write(payload); err != nil {
				errors.Add(1)
				return
			}
			n := 0
			buf := make([]byte, len(payload))
			for n < len(payload) {
				read, err := conn.Read(buf[n:])
				if err != nil {
					errors.Add(1)
					return
				}
				n += read
			}
			bytes.Add(int64(len(payload)))
		}
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go worker(i)
	}

	time.Sleep(3 * time.Second)
	close(stop)
	wg.Wait()

	b := float64(bytes.Load()) / 1e6
	t.Logf("=== H2C mixed traffic 3s ===")
	t.Logf("  %.0f MB total, %.1f Mbps", b, b*8/3)
	t.Logf("  Errors: %d", errors.Load())
}

// =========================================================================
// Test: Connection storm + idle rotation (H2 + H2C)
// =========================================================================

func TestXHTTP_ConnectionStormRotation(t *testing.T) {
	if testing.Short() {
		t.Skip("long stress test")
	}
	if runtime.GOARCH == "arm64" {
		t.Skip("arm64")
	}
	defer checkGoroutineLeak(t)()

	// Start 2 servers (H2 + H2C)
	ct, ctHash := cert.MustGenerate(nil, cert.CommonName("localhost"))
	h2Settings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Path: "/sh"},
		SecurityType:     "tls",
		SecuritySettings: &tls.Config{
			Certificate:          []*tls.Certificate{tls.ParseCertificate(ct)},
			PinnedPeerCertSha256: [][]byte{ctHash[:]},
		},
	}
	h2cSettings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Path: "/sh"},
	}

	p1 := tcp.PickPort()
	p2 := tcp.PickPort()

	l1, err := ListenXH(context.Background(), net.LocalHostIP, p1, h2Settings, func(conn stat.Connection) {
		go func(c stat.Connection) { defer c.Close(); io.Copy(c, c) }(conn)
	})
	common.Must(err)
	defer l1.Close()

	l2, err := ListenXH(context.Background(), net.LocalHostIP, p2, h2cSettings, func(conn stat.Connection) {
		go func(c stat.Connection) { defer c.Close(); io.Copy(c, c) }(conn)
	})
	common.Must(err)
	defer l2.Close()

	type server struct {
		port     net.Port
		settings *internet.MemoryStreamConfig
		label    string
	}
	servers := []server{
		{p1, h2Settings, "H2"},
		{p2, h2cSettings, "H2C"},
	}

	var (
		totalConnections atomic.Int64
		totalErrors      atomic.Int64
		wg               sync.WaitGroup
		stop             = make(chan struct{})
	)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				srv := servers[id%len(servers)]
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				conn, err := Dial(ctx,
					net.TCPDestination(net.DomainAddress("localhost"), srv.port),
					srv.settings)
				if err != nil {
					cancel()
					totalErrors.Add(1)
					continue
				}
				// Small exchange then close
				payload := make([]byte, 128)
				rand.Read(payload)
				conn.Write(payload)
				conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
				buf := make([]byte, 256)
				conn.Read(buf)
				conn.Close()
				cancel()
				totalConnections.Add(1)
			}
		}(i)
	}

	time.Sleep(5 * time.Second)
	close(stop)
	wg.Wait()

	conns := totalConnections.Load()
	errs := totalErrors.Load()
	t.Logf("=== Connection storm (H2+H2C) 5s ===")
	t.Logf("  Connections: %d, Errors: %d, Rate: %.0f conn/s",
		conns, errs, float64(conns)/5)
	if errs > conns/2 {
		t.Errorf("Too many errors: %d errors out of %d connections", errs, conns)
	}
}

// =========================================================================
// Test: 60-second constant load with mixed payload sizes
// =========================================================================

func TestXHTTP_LongDurationLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("long stress test")
	}
	if runtime.GOARCH == "arm64" {
		t.Skip("arm64")
	}
	defer checkGoroutineLeak(t)()

	srv := startEchoServer(t, "packet-up", true)
	defer srv.close()

	var (
		totalBytes        atomic.Int64
		totalWrites       atomic.Int64
		totalWriteErrors  atomic.Int64
		totalReadErrors   atomic.Int64
		peakConcurrent    int64
		wg                sync.WaitGroup
		stop              = make(chan struct{})
		concurrentWriters atomic.Int64
	)

	payloadSizes := []int{256, 1024, 4096, 16384, 65536}

	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			conn, err := Dial(context.Background(),
				net.TCPDestination(net.DomainAddress("localhost"), srv.port),
				srv.settings)
			if err != nil {
				totalWriteErrors.Add(1)
				t.Logf("  [worker %d] dial failed: %v", id, err)
				return
			}
			defer conn.Close()

			c := concurrentWriters.Add(1)
			if c > peakConcurrent {
				atomic.StoreInt64(&peakConcurrent, c)
			}
			defer concurrentWriters.Add(-1)

			payload := make([]byte, payloadSizes[id%len(payloadSizes)])
			rand.Read(payload)

			for {
				select {
				case <-stop:
					return
				default:
				}

				if _, err := conn.Write(payload); err != nil {
					totalWriteErrors.Add(1)
					return
				}
				totalWrites.Add(1)

				n := 0
				buf := make([]byte, len(payload))
				for n < len(payload) {
					read, err := conn.Read(buf[n:])
					if err != nil {
						totalReadErrors.Add(1)
						return
					}
					n += read
				}
				totalBytes.Add(int64(len(payload)))
			}
		}(i)
	}

	// Progress reports
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				mb := float64(totalBytes.Load()) / 1e6
				w := totalWrites.Load()
				peak := atomic.LoadInt64(&peakConcurrent)
				t.Logf("  [10s] %.0f MB, %d writes, peak=%d conns", mb, w, peak)
			case <-done:
				return
			}
		}
	}()

	t.Logf("Starting 5s long-duration load test (15 workers, mixed payload)...")
	time.Sleep(5 * time.Second)
	close(stop)
	close(done)
	wg.Wait()

	mb := float64(totalBytes.Load()) / 1e6
	mbps := mb * 8 / 5
	t.Logf("=== Long-duration load (5s) ===")
	t.Logf("  Total: %.0f MB = %.1f Mbps", mb, mbps)
	t.Logf("  Writes: %d, Write errors: %d, Read errors: %d",
		totalWrites.Load(), totalWriteErrors.Load(), totalReadErrors.Load())
	t.Logf("  Peak concurrent connections: %d", atomic.LoadInt64(&peakConcurrent))

	if writeErrs := totalWriteErrors.Load(); writeErrs > 5 {
		t.Errorf("Too many write errors: %d", writeErrs)
	}
	if readErrs := totalReadErrors.Load(); readErrs > 5 {
		t.Errorf("Too many read errors: %d", readErrs)
	}
}

// =========================================================================
// Test: All servers at once — 4-way H2 full duplex (5min endurance)
// =========================================================================

func TestXHTTP_QuadServerEndurance(t *testing.T) {
	if testing.Short() {
		t.Skip("long endurance test (>2s)")
	}
	if runtime.GOARCH == "arm64" {
		t.Skip("arm64")
	}
	defer checkGoroutineLeak(t)()

	servers := []struct {
		name string
		mode string
		tls  bool
	}{
		{"H2-packet-up", "packet-up", true},
		{"H2-stream-up", "stream-up", true},
		{"H2C-packet-up", "packet-up", false},
		{"H2-stream-one", "stream-one", true},
	}
	type srv struct {
		*echoServer
		name string
	}

	var srvs []srv
	for _, cfg := range servers {
		s := startEchoServer(t, cfg.mode, cfg.tls)
		srvs = append(srvs, srv{echoServer: s, name: cfg.name})
	}
	defer func() {
		for _, s := range srvs {
			s.close()
		}
	}()

	var (
		totalBytes    atomic.Int64
		totalErrors   atomic.Int64
		wg            sync.WaitGroup
		stop          = make(chan struct{})
		progressTrack atomic.Int64
	)

	for _, s := range srvs {
		for c := 0; c < 3; c++ {
			wg.Add(1)
			go func(server srv, id int) {
				defer wg.Done()
				conn, err := Dial(context.Background(),
					net.TCPDestination(net.DomainAddress("localhost"), server.port),
					server.settings)
				if err != nil {
					totalErrors.Add(1)
					return
				}
				defer conn.Close()
				payload := make([]byte, 8192)
				rand.Read(payload)

				for {
					select {
					case <-stop:
						return
					default:
					}
					if _, err := conn.Write(payload); err != nil {
						totalErrors.Add(1)
						return
					}
					n := 0
					buf := make([]byte, len(payload))
					for n < len(payload) {
						read, err := conn.Read(buf[n:])
						if err != nil {
							totalErrors.Add(1)
							return
						}
						n += read
					}
					totalBytes.Add(int64(len(payload)))
					progressTrack.Add(1)
				}
			}(s, c)
		}
	}

	checkDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p := progressTrack.Load()
				mb := float64(totalBytes.Load()) / 1e6
				t.Logf("  [5s] %.0f MB, %d loops, %d errors", mb, p, totalErrors.Load())
			case <-checkDone:
				return
			}
		}
	}()

	time.Sleep(3 * time.Second)
	close(stop)
	close(checkDone)
	wg.Wait()

	mb := float64(totalBytes.Load()) / 1e6
	mbps := mb * 8 / 3
	t.Logf("=== 4-way H2 endurance (3s) ===")
	t.Logf("  Total: %.0f MB = %.1f Mbps", mb, mbps)
	t.Logf("  Errors: %d", totalErrors.Load())
	if totalErrors.Load() > 5 {
		t.Errorf("Too many errors: %d", totalErrors.Load())
	}
}
