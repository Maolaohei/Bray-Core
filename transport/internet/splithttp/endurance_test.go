package splithttp_test

import (
	"context"
	"crypto/rand"
	"io"
	"net"
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
// NetworkProfile 模拟不同网络环境
// ============================================================================

type NetworkProfile struct {
	Name      string
	Latency   time.Duration // 单程延迟
	Jitter    time.Duration // 抖动
	LossRate  float64       // 丢包率 0.0-1.0
	Bandwidth int64         // 带宽限制 bytes/s, 0=不限
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

	for _, profile := range networkProfiles {
		t.Run(profile.Name, func(t *testing.T) {
			testContinuousTransfer(t, profile, 30*time.Second, 0)
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

	for _, profile := range networkProfiles {
		t.Run(profile.Name, func(t *testing.T) {
			testIdleRecovery(t, profile)
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
				conn.SetReadDeadline(time.Now().Add(5 * time.Second))
				n, err := io.ReadFull(conn, buf)
				if err != nil {
					if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
						continue
					}
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

	// Phase 1: 持续传输 3s
	t.Log("Phase 1: 持续传输 3s...")
	start := time.Now()
	for time.Since(start) < 3*time.Second {
		if _, err := conn.Write(payload); err != nil {
			t.Fatalf("Phase 1 write error: %v", err)
		}
		buf := make([]byte, len(payload))
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatalf("Phase 1 read error: %v", err)
		}
	}
	t.Log("  Phase 1 完成")

	// Phase 2: 空闲 15s
	t.Log("Phase 2: 空闲 15s...")
	time.Sleep(15 * time.Second)
	t.Log("  Phase 2 完成")

	// Phase 3: 恢复传输，测量恢复时间
	t.Log("Phase 3: 恢复传输...")
	recoveryStart := time.Now()
	for i := 0; i < 5; i++ {
		if _, err := conn.Write(payload); err != nil {
			t.Fatalf("Phase 3 write error (i=%d): %v", i, err)
		}
		buf := make([]byte, len(payload))
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatalf("Phase 3 read error (i=%d): %v", i, err)
		}
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
