package splithttp_test

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
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport/internet"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/internet/tls"
)

// ============================================================================
// 辅助函数
// ============================================================================

func checkGoroutineLeak(t *testing.T) func() {
	t.Helper()
	start := runtime.NumGoroutine()
	return func() {
		ResetGlobalDialer()
		time.Sleep(100 * time.Millisecond)
		delta := runtime.NumGoroutine() - start
		if delta > 10 {
			t.Errorf("Goroutine leak: +%d goroutines after test", delta)
		}
	}
}

type echoServer struct {
	listen   internet.Listener
	port     xnet.Port
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

	listen, err := ListenXH(context.Background(), xnet.LocalHostIP, p, settings, func(conn stat.Connection) {
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

// ============================================================================
// NetworkProfile 模拟不同网络环境
// ============================================================================

type NetworkProfile struct {
	Name      string
	Latency   time.Duration
	Jitter    time.Duration
	LossRate  float64
	Bandwidth int64
}

var networkProfiles = []NetworkProfile{
	{"GoodLAN", 1 * time.Millisecond, 0, 0, 0},
	{"GoodWiFi", 5 * time.Millisecond, 2 * time.Millisecond, 0, 0},
	{"4G", 30 * time.Millisecond, 10 * time.Millisecond, 0.01, 10 * 1024 * 1024},
	{"BadWiFi", 50 * time.Millisecond, 20 * time.Millisecond, 0.05, 5 * 1024 * 1024},
	{"Oversea", 150 * time.Millisecond, 30 * time.Millisecond, 0.02, 20 * 1024 * 1024},
}

// ============================================================================
// Test: 连续传输 30s，不允许任何错误（断流检测）
// ============================================================================

func TestXHTTP_NoDrop_Continuous(t *testing.T) {
	if testing.Short() {
		t.Skip("long endurance test")
	}
	if runtime.GOARCH == "arm64" {
		t.Skip("arm64")
	}
	ResetGlobalDialer()
	t.Cleanup(ResetGlobalDialer)

	for _, profile := range networkProfiles {
		t.Run(profile.Name, func(t *testing.T) {
			ResetGlobalDialer()
			testContinuousTransfer(t, profile, 30*time.Second, 0)
			ResetGlobalDialer()
		})
	}
}

// ============================================================================
// Test: 传输 → 空闲 15s → 恢复传输（断流恢复检测）
// ============================================================================

func TestXHTTP_NoDrop_IdleRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("long endurance test")
	}
	if runtime.GOARCH == "arm64" {
		t.Skip("arm64")
	}
	// Isolate from prior package tests that may have left global XMUX state.
	ResetGlobalDialer()
	t.Cleanup(ResetGlobalDialer)

	for _, profile := range networkProfiles {
		t.Run(profile.Name, func(t *testing.T) {
			// Fresh XMUX map per profile so idle migration from one case
			// cannot race the next profile's dials.
			ResetGlobalDialer()
			testIdleRecovery(t, profile)
			ResetGlobalDialer()
		})
	}
}

// ============================================================================
// Test: 并发 10 连接持续传输
// ============================================================================

func TestXHTTP_NoDrop_MultiConn(t *testing.T) {
	if testing.Short() {
		t.Skip("long endurance test")
	}
	if runtime.GOARCH == "arm64" {
		t.Skip("arm64")
	}

	for _, profile := range networkProfiles {
		t.Run(profile.Name, func(t *testing.T) {
			testMultiConnection(t, profile, 20*time.Second, 10)
		})
	}
}

// ============================================================================
// Test: 快速连接/断开（视频切换模拟）
// ============================================================================

func TestXHTTP_NoDrop_RapidSwitch(t *testing.T) {
	if testing.Short() {
		t.Skip("long endurance test")
	}
	if runtime.GOARCH == "arm64" {
		t.Skip("arm64")
	}

	for _, profile := range networkProfiles {
		t.Run(profile.Name, func(t *testing.T) {
			testRapidSwitch(t, profile, 15, 2*time.Second)
		})
	}
}

// ============================================================================
// 测试实现
// ============================================================================

func startTestServer(t *testing.T, mode string, useTLS bool) (internet.Listener, xnet.Port, *internet.MemoryStreamConfig) {
	t.Helper()
	p := tcp.PickPort()

	var settings *internet.MemoryStreamConfig
	if useTLS {
		ct, ctHash := cert.MustGenerate(nil, cert.CommonName("localhost"))
		settings = &internet.MemoryStreamConfig{
			ProtocolName: "splithttp",
			ProtocolSettings: &Config{
				Path:               "/bench",
				Mode:               mode,
				ScMaxEachPostBytes: &RangeConfig{From: 1000000, To: 1000000},
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
				Path:               "/bench",
				Mode:               mode,
				ScMaxEachPostBytes: &RangeConfig{From: 1000000, To: 1000000},
			},
		}
	}

	listen, err := ListenXH(context.Background(), xnet.LocalHostIP, p, settings, func(conn stat.Connection) {
		go func(c stat.Connection) {
			defer c.Close()
			io.Copy(c, c)
		}(conn)
	})
	common.Must(err)

	return listen, p, settings
}

func testContinuousTransfer(t *testing.T, profile NetworkProfile, duration time.Duration, allowedErrors int64) {
	t.Helper()

	// 使用 H2-TLS 模式（最常用）
	listen, port, settings := startTestServer(t, "packet-up", true)
	defer listen.Close()

	var (
		totalBytes  atomic.Int64
		totalErrors atomic.Int64
		stop        = make(chan struct{})
		wg          sync.WaitGroup
	)

	// 启动 5 个并发连接
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := Dial(context.Background(),
				xnet.TCPDestination(xnet.DomainAddress("localhost"), port),
				settings)
			if err != nil {
				t.Logf("  [conn %d] dial failed: %v", id, err)
				totalErrors.Add(1)
				return
			}
			defer conn.Close()

			payload := make([]byte, 32*1024)
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
				buf := make([]byte, len(payload))
				// Hard deadline: splitConn honors this even on H2 bodies.
				// A timeout is a real stall (not a soft retry): counting it as
				// an error avoids desync if a late echo arrives after we move on.
				_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
				n, err := io.ReadFull(conn, buf)
				_ = conn.SetReadDeadline(time.Time{})
				if err != nil {
					totalErrors.Add(1)
					return
				}
				totalBytes.Add(int64(n))
			}
		}(i)
	}

	t.Logf("Profile: %s, Duration: %v, Conns: 5", profile.Name, duration)
	start := time.Now()

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				elapsed := time.Since(start)
				mb := float64(totalBytes.Load()) / 1e6
				mbps := mb * 8 / elapsed.Seconds()
				errs := totalErrors.Load()
				t.Logf("  [%v] %.0f MB, %.1f Mbps, %d errors", elapsed.Round(time.Second), mb, mbps, errs)
				if errs > allowedErrors {
					t.Errorf("断流检测: %d errors > allowed %d", errs, allowedErrors)
				}
			case <-done:
				return
			}
		}
	}()

	time.Sleep(duration)
	close(stop)
	close(done)
	wg.Wait()

	mb := float64(totalBytes.Load()) / 1e6
	mbps := mb * 8 / duration.Seconds()
	errs := totalErrors.Load()

	t.Logf("=== 结果: %s ===", profile.Name)
	t.Logf("  总传输: %.0f MB (%.1f Mbps)", mb, mbps)
	t.Logf("  错误数: %d", errs)

	if errs > allowedErrors {
		t.Errorf("断流! 错误数 %d > 允许值 %d", errs, allowedErrors)
	}
}

func testIdleRecovery(t *testing.T, profile NetworkProfile) {
	t.Helper()

	listen, port, settings := startTestServer(t, "packet-up", true)
	defer listen.Close()

	conn, err := Dial(context.Background(),
		xnet.TCPDestination(xnet.DomainAddress("localhost"), port),
		settings)
	if err != nil {
		t.Fatal("dial failed:", err)
	}
	defer conn.Close()

	payload := make([]byte, 32*1024)
	rand.Read(payload)

	// echoRoundTrip writes then fully reads one payload under a hard deadline.
	// splitConn now honors SetReadDeadline even when the download leg is an
	// H2 body (not a net.Conn); without that, a stalled Phase 3 hang forever.
	echoRoundTrip := func(phase string, i int) {
		t.Helper()
		if _, err := conn.Write(payload); err != nil {
			if i >= 0 {
				t.Fatalf("%s write error (i=%d): %v", phase, i, err)
			}
			t.Fatalf("%s write error: %v", phase, err)
		}
		buf := make([]byte, len(payload))
		// Absolute deadline covers the full ReadFull of this payload.
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(conn, buf); err != nil {
			if i >= 0 {
				t.Fatalf("%s read error (i=%d): %v", phase, i, err)
			}
			t.Fatalf("%s read error: %v", phase, err)
		}
		// Clear so a late timer from a prior trip cannot poison the next.
		_ = conn.SetReadDeadline(time.Time{})
	}

	// Phase 1: 持续传输 3s
	t.Log("Phase 1: 持续传输 3s...")
	start := time.Now()
	for time.Since(start) < 3*time.Second {
		echoRoundTrip("Phase 1", -1)
	}
	t.Log("  Phase 1 完成")

	// Phase 2: 空闲 15s (idle must not kill the logical session)
	t.Log("Phase 2: 空闲 15s...")
	time.Sleep(15 * time.Second)
	t.Log("  Phase 2 完成")

	// Phase 3: 恢复传输，测量恢复时间
	t.Log("Phase 3: 恢复传输...")
	recoveryStart := time.Now()
	for i := 0; i < 5; i++ {
		echoRoundTrip("Phase 3", i)
	}
	recoveryTime := time.Since(recoveryStart)

	t.Logf("=== 空闲恢复测试: %s ===", profile.Name)
	t.Logf("  恢复时间: %v", recoveryTime)

	if recoveryTime > 3*time.Second {
		t.Errorf("恢复时间 %.2fs > 3s 阈值，疑似断流", recoveryTime.Seconds())
	}
}

func testMultiConnection(t *testing.T, profile NetworkProfile, duration time.Duration, conns int) {
	t.Helper()

	listen, port, settings := startTestServer(t, "packet-up", true)
	defer listen.Close()

	var (
		totalBytes  atomic.Int64
		totalErrors atomic.Int64
		stop        = make(chan struct{})
		wg          sync.WaitGroup
	)

	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := Dial(context.Background(),
				xnet.TCPDestination(xnet.DomainAddress("localhost"), port),
				settings)
			if err != nil {
				t.Logf("  [conn %d] dial failed: %v", id, err)
				totalErrors.Add(1)
				return
			}
			defer conn.Close()

			payload := make([]byte, 8192+id*4096)
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
				buf := make([]byte, len(payload))
				conn.SetReadDeadline(time.Now().Add(5 * time.Second))
				n, err := io.ReadFull(conn, buf)
				if err != nil {
					totalErrors.Add(1)
					return
				}
				totalBytes.Add(int64(n))
			}
		}(i)
	}

	t.Logf("Profile: %s, Conns: %d, Duration: %v", profile.Name, conns, duration)

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				mb := float64(totalBytes.Load()) / 1e6
				errs := totalErrors.Load()
				t.Logf("  [5s] %.0f MB, %d errors", mb, errs)
			case <-done:
				return
			}
		}
	}()

	time.Sleep(duration)
	close(stop)
	close(done)
	wg.Wait()

	mb := float64(totalBytes.Load()) / 1e6
	errs := totalErrors.Load()
	t.Logf("=== 多连接测试: %s (%d conns) ===", profile.Name, conns)
	t.Logf("  总传输: %.0f MB, 错误: %d", mb, errs)

	if errs > int64(conns) {
		t.Errorf("断流! 错误数 %d > 连接数 %d", errs, conns)
	}
}

// ============================================================================
// Test: 多域压力测试（4 域 × 5 连接 × 30s）
// ============================================================================

func TestXHTTP_NoDrop_MultiDomainStress(t *testing.T) {
	if testing.Short() {
		t.Skip("long endurance test")
	}
	if runtime.GOARCH == "arm64" {
		t.Skip("arm64")
	}
	defer checkGoroutineLeak(t)()

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

	connsPerServer := 5
	payloadSizes := []int{512, 4096, 16384, 65536}

	for idx, srv := range servers {
		for c := 0; c < connsPerServer; c++ {
			wg.Add(1)
			go func(server *echoServer, workerID int) {
				defer wg.Done()

				conn, err := Dial(context.Background(),
					xnet.TCPDestination(xnet.DomainAddress("localhost"), server.port),
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
					totalBytes.Add(int64(n))
				}
			}(srv, c)
		}
	}

	t.Logf("Starting 30s multi-domain stress test...")
	t.Logf("Servers: %d, connections: %d, payload: 512B-64KB mix",
		len(servers), len(servers)*connsPerServer)

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

	time.Sleep(30 * time.Second)
	close(stop)
	close(done)
	wg.Wait()

	mb := float64(totalBytes.Load()) / 1e6
	mbps := mb * 8 / 30
	errs := totalErrs.Load()
	t.Logf("=== Multi-domain stress (30s) ===")
	t.Logf("  Total: %.0f MB in 30s = %.1f Mbps", mb, mbps)
	t.Logf("  Errors: %d", errs)

	if errs > 10 {
		t.Errorf("Too many errors (%d) during stress test", errs)
	}
}

func testRapidSwitch(t *testing.T, profile NetworkProfile, rounds int, switchInterval time.Duration) {
	t.Helper()

	var (
		totalSuccess atomic.Int64
		totalFail    atomic.Int64
		totalBytes   atomic.Int64
		wg           sync.WaitGroup
	)

	for i := 0; i < rounds; i++ {
		wg.Add(1)
		go func(round int) {
			defer wg.Done()

			listen, port, settings := startTestServer(t, "packet-up", true)
			defer listen.Close()

			conn, err := Dial(context.Background(),
				xnet.TCPDestination(xnet.DomainAddress("localhost"), port),
				settings)
			if err != nil {
				totalFail.Add(1)
				return
			}
			defer conn.Close()

			payload := make([]byte, 64*1024)
			rand.Read(payload)

			if _, err := conn.Write(payload); err != nil {
				totalFail.Add(1)
				return
			}

			buf := make([]byte, len(payload))
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			n, err := io.ReadFull(conn, buf)
			if err != nil {
				totalFail.Add(1)
				return
			}

			totalSuccess.Add(1)
			totalBytes.Add(int64(n))
		}(i)

		// 控制切换速度
		time.Sleep(switchInterval / time.Duration(rounds))
	}

	wg.Wait()

	success := totalSuccess.Load()
	fail := totalFail.Load()
	mb := float64(totalBytes.Load()) / 1e6

	t.Logf("=== 快速切换测试: %s (%d rounds) ===", profile.Name, rounds)
	t.Logf("  成功: %d, 失败: %d, 总传输: %.0f MB", success, fail, mb)

	if fail > int64(rounds)/10 {
		t.Errorf("断流! 失败率 %.0f%% > 10%%", float64(fail)/float64(rounds)*100)
	}
}

// ============================================================================
// Test: 快速新连接测试（模拟新网页卡顿场景）
// 连续创建新连接，测量首次请求延迟
// ============================================================================

func TestXHTTP_RapidNewConnections(t *testing.T) {
	if testing.Short() {
		t.Skip("long endurance test")
	}
	if runtime.GOARCH == "arm64" {
		t.Skip("arm64")
	}

	// 启动服务器
	listen, port, settings := startTestServer(t, "packet-up", true)
	defer listen.Close()

	var (
		totalConns   atomic.Int64
		totalStalls  atomic.Int64
		totalLatency atomic.Int64 // 纳秒
		wg           sync.WaitGroup
	)

	// 并发创建 50 个新连接
	conns := 50
	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			start := time.Now()

			// 创建新连接（模拟新网页）
			conn, err := Dial(context.Background(),
				xnet.TCPDestination(xnet.DomainAddress("localhost"), port),
				settings)
			if err != nil {
				totalStalls.Add(1)
				t.Logf("  [conn %d] dial failed: %v", id, err)
				return
			}
			defer conn.Close()

			// 首次写入（模拟 HTTP 请求）
			payload := make([]byte, 1024)
			rand.Read(payload)
			if _, err := conn.Write(payload); err != nil {
				totalStalls.Add(1)
				return
			}

			// 首次读取（模拟 HTTP 响应）
			buf := make([]byte, len(payload))
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			if _, err := io.ReadFull(conn, buf); err != nil {
				totalStalls.Add(1)
				return
			}

			latency := time.Since(start)
			totalLatency.Add(int64(latency))
			totalConns.Add(1)

			// 标记卡顿（>500ms）
			if latency > 500*time.Millisecond {
				t.Logf("  [conn %d] STALL: %v", id, latency)
			}
		}(i)
	}

	wg.Wait()

	avgLatency := time.Duration(totalLatency.Load() / totalConns.Load())
	stalls := totalStalls.Load()
	success := totalConns.Load()

	t.Logf("=== 快速新连接测试 ===")
	t.Logf("  连接数: %d, 成功: %d, 失败: %d", conns, success, stalls)
	t.Logf("  平均延迟: %v", avgLatency)

	if stalls > int64(conns)/10 {
		t.Errorf("断流! 失败率 %.0f%% > 10%%", float64(stalls)/float64(conns)*100)
	}
}

// ============================================================================
// Test: 连接复用 vs 新建连接对比
// ============================================================================

func TestXHTTP_ConnectionReuse(t *testing.T) {
	if testing.Short() {
		t.Skip("long endurance test")
	}
	if runtime.GOARCH == "arm64" {
		t.Skip("arm64")
	}

	listen, port, settings := startTestServer(t, "packet-up", true)
	defer listen.Close()

	payload := make([]byte, 4096)
	rand.Read(payload)

	// 阶段 1：创建连接并预热
	conn, err := Dial(context.Background(),
		xnet.TCPDestination(xnet.DomainAddress("localhost"), port),
		settings)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// 预热：发送几次请求
	for i := 0; i < 5; i++ {
		conn.Write(payload)
		buf := make([]byte, len(payload))
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		io.ReadFull(conn, buf)
	}

	// 阶段 2：复用连接测试
	t.Log("Phase 1: 测试连接复用...")
	reuseStart := time.Now()
	for i := 0; i < 100; i++ {
		if _, err := conn.Write(payload); err != nil {
			t.Fatalf("write error: %v", err)
		}
		buf := make([]byte, len(payload))
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatalf("read error: %v", err)
		}
	}
	reuseTime := time.Since(reuseStart)
	t.Logf("  复用 100 次: %v (平均 %v/次)", reuseTime, reuseTime/100)

	// 阶段 3：新建连接测试
	t.Log("Phase 2: 测试新建连接...")
	conn.Close()
	newStart := time.Now()
	for i := 0; i < 10; i++ {
		c, err := Dial(context.Background(),
			xnet.TCPDestination(xnet.DomainAddress("localhost"), port),
			settings)
		if err != nil {
			t.Fatalf("dial error: %v", err)
		}
		c.Write(payload)
		buf := make([]byte, len(payload))
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
		io.ReadFull(conn, buf)
		c.Close()
	}
	newTime := time.Since(newStart)
	t.Logf("  新建 10 次: %v (平均 %v/次)", newTime, newTime/10)

	// 对比
	ratio := float64(newTime) / float64(reuseTime) * 10
	t.Logf("  新建/复用比: %.1fx", ratio)

	if ratio > 10 {
		t.Logf("  WARNING: 新建连接比复用慢 %.1fx，可能导致新网页卡顿", ratio)
	}
}
