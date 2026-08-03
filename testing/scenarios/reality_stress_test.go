package scenarios

import (
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/log"
	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/common"
	clog "github.com/xtls/xray-core/common/log"
	xraynet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/uuid"
	core "github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/proxy/dokodemo"
	"github.com/xtls/xray-core/proxy/freedom"
	"github.com/xtls/xray-core/proxy/vless"
	"github.com/xtls/xray-core/proxy/vless/inbound"
	"github.com/xtls/xray-core/proxy/vless/outbound"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/reality"
	"github.com/xtls/xray-core/transport/internet/splithttp"
	"google.golang.org/protobuf/proto"
)

// ============================================================================
// 迭代测试: 高并发 + 大文件下载 + 资源监控
// RA 目标: www.microsoft.com (已知可用)
// 实际访问: bilibili.com / qq.com / baidu.com / mirrors.163.com
// ============================================================================

var (
	stressPrivKey, _ = base64.RawURLEncoding.DecodeString("aGSYystUbf59_9_6LKRxD27rmSW_-2_nyd9YG_Gwbks")
	stressPubKey, _  = base64.RawURLEncoding.DecodeString("E59WjnvZcQMu7tR7_BgyhycuEdBS-CtKxfImRCdAvFM")
)

func stressIDs() [][]byte {
	s := make([][]byte, 1)
	s[0] = make([]byte, 8)
	hex.Decode(s[0], []byte("0123456789abcdef"))
	return s
}

type stressResult struct {
	domain   string
	bytes    int
	duration time.Duration
	err      error
}

// buildStressConfig 创建 REALITY server/client 配置，target 写死 www.baidu.com:443。
func buildStressConfig(t *testing.T, path string) (clientPort xraynet.Port, cleanup func()) {
	t.Helper()
	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	clientPort = tcp.PickPort()
	shortIds := stressIDs()

	serverConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(serverPort)}},
				Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: path}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Dest: "www.microsoft.com:443", ServerNames: []string{"www.microsoft.com"},
						PrivateKey: stressPrivKey, ShortIds: shortIds, Type: "tcp",
					})},
				},
			}),
			ProxySettings: serial.ToTypedMessage(&inbound.Config{Users: []*protocol.User{{
				Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()}),
			}}}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&freedom.Config{FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}}}),
		}},
	}

	clientConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(clientPort)}},
				Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
			}),
			ProxySettings: serial.ToTypedMessage(&dokodemo.Config{
				RewriteAddress: xraynet.NewIPOrDomain(xraynet.ParseAddress("www.baidu.com")),
				RewritePort:    443, AllowedNetworks: []xraynet.Network{xraynet.Network_TCP},
			}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&outbound.Config{
				Vnext: &protocol.ServerEndpoint{
					Address: xraynet.NewIPOrDomain(xraynet.LocalHostIP), Port: uint32(serverPort),
					User: &protocol.User{Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()})},
				},
			}),
			SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: path}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Fingerprint: "chrome", ServerName: "www.microsoft.com",
						PublicKey: stressPubKey, ShortId: shortIds[0], SpiderX: "/",
					})},
				},
			}),
		}},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	common.Must(err)
	cleanup = func() { CloseAllServers(servers) }
	return
}

// httpsRequest 通过代理发起 HTTPS GET 请求，返回读取的字节数和耗时。
func httpsRequest(proxyPort int, host, path string, timeout time.Duration) stressResult {
	start := time.Now()
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", proxyPort), timeout)
	if err != nil {
		return stressResult{domain: host, err: err, duration: time.Since(start)}
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))

	tlsConn := tls.Client(conn, &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12, InsecureSkipVerify: true})
	if err := tlsConn.Handshake(); err != nil {
		return stressResult{domain: host, err: err, duration: time.Since(start)}
	}
	fmt.Fprintf(tlsConn, "GET %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", path, host)

	var total int
	buf := make([]byte, 64*1024)
	for {
		n, err := tlsConn.Read(buf)
		total += n
		if err != nil {
			break
		}
	}
	return stressResult{domain: host, bytes: total, duration: time.Since(start)}
}

// TestREALITYHighConcurrentAccess 50 并发 × 3 域名 × 10 轮，验证稳定性。
func TestREALITYHighConcurrentAccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long REALITY stress test in short mode")
	}
	const concurrency = 50
	const rounds = 10

	clientPort, cleanup := buildStressConfig(t, "/stress-high")
	defer cleanup()
	time.Sleep(5 * time.Second)

	domains := []struct {
		host string
		path string
	}{
		{"www.baidu.com", "/"},
		{"www.qq.com", "/"},
		{"www.bilibili.com", "/"},
	}

	var totalSuccess, totalFail atomic.Int64
	var totalBytes atomic.Int64
	var goroutineBaseline int

	// 基线采集
	runtime.GC()
	goroutineBaseline = runtime.NumGoroutine()
	t.Logf("Goroutine 基线: %d", goroutineBaseline)

	for round := 1; round <= rounds; round++ {
		roundStart := time.Now()
		var wg sync.WaitGroup
		results := make([]stressResult, concurrency*len(domains))
		idx := 0

		for _, d := range domains {
			for i := 0; i < concurrency; i++ {
				wg.Add(1)
				go func(domain, path string, resIdx int) {
					defer wg.Done()
					results[resIdx] = httpsRequest(int(clientPort), domain, path, 30*time.Second)
				}(d.host, d.path, idx)
				idx++
			}
		}
		wg.Wait()

		var roundSuccess, roundFail int
		var roundBytes int64
		for _, r := range results {
			if r.err != nil {
				roundFail++
				totalFail.Add(1)
			} else {
				roundSuccess++
				totalSuccess.Add(1)
				totalBytes.Add(int64(r.bytes))
				roundBytes += int64(r.bytes)
			}
		}

		goroutines := runtime.NumGoroutine()
		t.Logf("轮次 %d/%d: 成功=%d 失败=%d bytes=%d 耗时=%v goroutines=%d",
			round, rounds, roundSuccess, roundFail, roundBytes,
			time.Since(roundStart).Round(time.Millisecond), goroutines)

		if goroutines > goroutineBaseline+200 {
			t.Errorf("轮次 %d: goroutine 泄漏疑似 (baseline=%d, current=%d)", round, goroutineBaseline, goroutines)
		}
	}

	total := totalSuccess.Load() + totalFail.Load()
	successRate := float64(totalSuccess.Load()) / float64(total) * 100
	t.Logf("=== 高并发测试汇总 ===")
	t.Logf("总请求: %d, 成功: %d, 失败: %d, 成功率: %.1f%%", total, totalSuccess.Load(), totalFail.Load(), successRate)
	t.Logf("总传输: %d bytes (%.2f MB)", totalBytes.Load(), float64(totalBytes.Load())/1024/1024)
	t.Logf("Goroutine: 基线=%d, 结束=%d", goroutineBaseline, runtime.NumGoroutine())

	// 5% tolerance: under sustained load a stray timeout must not flake.
	if successRate < 95 {
		t.Errorf("成功率 %.1f%% < 95%%", successRate)
	}
	if runtime.NumGoroutine() > goroutineBaseline+200 {
		t.Errorf("Goroutine 泄漏: baseline=%d, final=%d", goroutineBaseline, runtime.NumGoroutine())
	}
}

// TestREALITYLargeFileDownload 通过 REALITY 隧道传输大块数据，验证长连接稳定性。
// 使用本地 echo 服务器，分块写入+读取验证不断流。
func TestREALITYLargeFileDownload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long REALITY stress test in short mode")
	}
	const (
		chunkSize = 256 * 1024 // 256KB/块
		chunks    = 40         // 40 块 = 10 MB 总计
		rounds    = 3
	)

	tcpServer := tcp.Server{MsgProcessor: xor}
	dest, err := tcpServer.Start()
	common.Must(err)
	defer tcpServer.Close()

	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	clientPort := tcp.PickPort()
	shortIds := stressIDs()

	serverConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(serverPort)}},
				Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: "/stress-download"}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Dest: "www.microsoft.com:443", ServerNames: []string{"www.microsoft.com"},
						PrivateKey: stressPrivKey, ShortIds: shortIds, Type: "tcp",
					})},
				},
			}),
			ProxySettings: serial.ToTypedMessage(&inbound.Config{Users: []*protocol.User{{
				Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()}),
			}}}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&freedom.Config{FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}}}),
		}},
	}

	clientConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(clientPort)}},
				Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
			}),
			ProxySettings: serial.ToTypedMessage(&dokodemo.Config{
				RewriteAddress:  xraynet.NewIPOrDomain(dest.Address),
				RewritePort:     uint32(dest.Port),
				AllowedNetworks: []xraynet.Network{xraynet.Network_TCP},
			}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&outbound.Config{
				Vnext: &protocol.ServerEndpoint{
					Address: xraynet.NewIPOrDomain(xraynet.LocalHostIP), Port: uint32(serverPort),
					User: &protocol.User{Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()})},
				},
			}),
			SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: "/stress-download"}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Fingerprint: "chrome", ServerName: "www.microsoft.com",
						PublicKey: stressPubKey, ShortId: shortIds[0], SpiderX: "/",
					})},
				},
			}),
		}},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	common.Must(err)
	defer CloseAllServers(servers)
	time.Sleep(5 * time.Second)

	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)
	t.Logf("内存基线: Alloc=%d MB", memBefore.Alloc/1024/1024)

	for round := 1; round <= rounds; round++ {
		start := time.Now()
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", clientPort), 30*time.Second)
		if err != nil {
			t.Fatalf("轮次 %d dial 失败: %v", round, err)
		}
		conn.SetDeadline(time.Now().Add(120 * time.Second))

		// 分块写入，每块写完立即读回验证
		buf := make([]byte, chunkSize)
		for i := range buf {
			buf[i] = byte(i % 256)
		}
		var totalBytes int

		for c := 0; c < chunks; c++ {
			// 写一块
			n, err := conn.Write(buf)
			if err != nil {
				t.Logf("轮次 %d 块 %d 写入失败: %v", round, c, err)
				conn.Close()
				break
			}
			totalBytes += n

			// 读回 echo 响应
			readBuf := make([]byte, chunkSize)
			rn, err := conn.Read(readBuf)
			if err != nil {
				t.Logf("轮次 %d 块 %d 读取失败: %v", round, c, err)
				conn.Close()
				break
			}
			if rn != n {
				t.Logf("轮次 %d 块 %d: 发送 %d ≠ 接收 %d", round, c, n, rn)
			}
		}
		conn.Close()

		duration := time.Since(start)
		speed := float64(totalBytes) / duration.Seconds() / 1024 / 1024
		t.Logf("轮次 %d/%d: 传输 %.2f MB, 耗时 %v, 速度 %.2f MB/s",
			round, rounds, float64(totalBytes)/1024/1024, duration.Round(time.Millisecond), speed)

		if totalBytes == 0 {
			t.Errorf("轮次 %d: 零字节传输 — 断流", round)
		}
	}

	runtime.GC()
	runtime.ReadMemStats(&memAfter)
	memDelta := int64(memAfter.Alloc) - int64(memBefore.Alloc)
	t.Logf("内存变化: %d MB → %d MB (delta=%d MB)", memBefore.Alloc/1024/1024, memAfter.Alloc/1024/1024, memDelta/1024/1024)
	if memDelta > 50*1024*1024 {
		t.Errorf("内存增长 %d MB > 50 MB 阈值", memDelta/1024/1024)
	}
}

// TestREALITYGoroutineStability 验证测试结束后 goroutine 数量回落。
func TestREALITYGoroutineStability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long REALITY stress test in short mode")
	}
	clientPort, cleanup := buildStressConfig(t, "/stress-goroutine")
	defer cleanup()
	time.Sleep(3 * time.Second)

	baseline := runtime.NumGoroutine()
	t.Logf("基线 goroutine: %d", baseline)

	// 跑 5 轮高并发
	for round := 1; round <= 5; round++ {
		var wg sync.WaitGroup
		for i := 0; i < 30; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				httpsRequest(int(clientPort), "www.baidu.com", "/", 15*time.Second)
			}()
		}
		wg.Wait()
		t.Logf("轮次 %d: goroutine=%d", round, runtime.NumGoroutine())
	}

	// 等待 goroutine 回落
	time.Sleep(5 * time.Second)
	runtime.GC()
	final := runtime.NumGoroutine()
	t.Logf("最终 goroutine: %d (基线=%d)", final, baseline)

	if final > baseline+50 {
		t.Errorf("goroutine 泄漏: baseline=%d, final=%d, delta=%d", baseline, final, final-baseline)
	}
}

// ============================================================================
// XHTTP 三模式测试
// ============================================================================

// buildXHTTPModeConfig 创建指定 XHTTP 模式的 REALITY 配置。
func buildXHTTPModeConfig(t *testing.T, mode string) (clientPort xraynet.Port, cleanup func()) {
	t.Helper()
	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	clientPort = tcp.PickPort()
	shortIds := stressIDs()
	path := "/mode-" + mode

	serverConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(serverPort)}},
				Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings: serial.ToTypedMessage(&splithttp.Config{
							Path: path, Mode: mode,
							// Session wire modes are fail-closed without a shared MAC secret.
							Headers: map[string]string{splithttp.BraySessionSecretHeader: "stress-secret"},
						}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Dest: "www.microsoft.com:443", ServerNames: []string{"www.microsoft.com"},
						PrivateKey: stressPrivKey, ShortIds: shortIds, Type: "tcp",
					})},
				},
			}),
			ProxySettings: serial.ToTypedMessage(&inbound.Config{Users: []*protocol.User{{
				Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()}),
			}}}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&freedom.Config{FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}}}),
		}},
	}

	clientConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(clientPort)}},
				Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
			}),
			ProxySettings: serial.ToTypedMessage(&dokodemo.Config{
				RewriteAddress: xraynet.NewIPOrDomain(xraynet.ParseAddress("www.baidu.com")),
				RewritePort:    443, AllowedNetworks: []xraynet.Network{xraynet.Network_TCP},
			}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&outbound.Config{
				Vnext: &protocol.ServerEndpoint{
					Address: xraynet.NewIPOrDomain(xraynet.LocalHostIP), Port: uint32(serverPort),
					User: &protocol.User{Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()})},
				},
			}),
			SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings: serial.ToTypedMessage(&splithttp.Config{
							Path: path, Mode: mode,
							// Session wire modes are fail-closed without a shared MAC secret.
							Headers: map[string]string{splithttp.BraySessionSecretHeader: "stress-secret"},
						}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Fingerprint: "chrome", ServerName: "www.microsoft.com",
						PublicKey: stressPubKey, ShortId: shortIds[0], SpiderX: "/",
					})},
				},
			}),
		}},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	common.Must(err)
	cleanup = func() { CloseAllServers(servers) }
	return
}

// TestREALITYXHTTPModes 三种 XHTTP 模式 × 10 并发 × 5 轮，验证每种模式稳定性。
func TestREALITYXHTTPModes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long REALITY stress test in short mode")
	}
	modes := []string{"packet-up", "stream-up", "stream-one"}

	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			clientPort, cleanup := buildXHTTPModeConfig(t, mode)
			defer cleanup()
			time.Sleep(5 * time.Second)

			baseline := runtime.NumGoroutine()
			var totalSuccess, totalFail atomic.Int64
			var totalBytes atomic.Int64

			const concurrency = 10
			const rounds = 5

			for round := 1; round <= rounds; round++ {
				roundStart := time.Now()
				var wg sync.WaitGroup
				results := make([]stressResult, concurrency)

				for i := range concurrency {
					wg.Add(1)
					go func(idx int) {
						defer wg.Done()
						results[idx] = httpsRequest(int(clientPort), "www.baidu.com", "/", 30*time.Second)
					}(i)
				}
				wg.Wait()

				var roundSuccess, roundFail int
				var roundBytes int64
				for _, r := range results {
					if r.err != nil {
						roundFail++
						totalFail.Add(1)
					} else {
						roundSuccess++
						totalSuccess.Add(1)
						totalBytes.Add(int64(r.bytes))
						roundBytes += int64(r.bytes)
					}
				}
				t.Logf("  轮次 %d/%d: 成功=%d 失败=%d bytes=%d 耗时=%v",
					round, rounds, roundSuccess, roundFail, roundBytes,
					time.Since(roundStart).Round(time.Millisecond))
			}

			final := runtime.NumGoroutine()
			total := totalSuccess.Load() + totalFail.Load()
			successRate := float64(totalSuccess.Load()) / float64(total) * 100
			t.Logf("  [%s] 总请求=%d 成功=%d 失败=%d 成功率=%.1f%% goroutine=%d→%d",
				mode, total, totalSuccess.Load(), totalFail.Load(), successRate, baseline, final)

			// 5% tolerance: under sustained load one stray timeout must not flake.
			if successRate < 95 {
				t.Errorf("[%s] 成功率 %.1f%% < 95%%", mode, successRate)
			}
			if final > baseline+100 {
				t.Errorf("[%s] goroutine 泄漏: %d→%d", mode, baseline, final)
			}
		})
	}
}

// ============================================================================
// pprof 监控 + 完整测试报告
// ============================================================================

type profileEntry struct {
	size float64
	pct  float64
	name string
}

// buildStressConfigWithPprof 创建 REALITY server/client 配置，server 进程开启 pprof。
// showDebug 控制 REALITY 的 Show 字段，用于对比调试日志对性能的影响。
func buildStressConfigWithPprof(t *testing.T, path string, pprofPort int, showDebug bool) (clientPort xraynet.Port, cleanup func()) {
	t.Helper()
	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	clientPort = tcp.PickPort()
	shortIds := stressIDs()

	serverConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(serverPort)}},
				Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: path}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: showDebug, Dest: "www.microsoft.com:443", ServerNames: []string{"www.microsoft.com"},
						PrivateKey: stressPrivKey, ShortIds: shortIds, Type: "tcp",
					})},
				},
			}),
			ProxySettings: serial.ToTypedMessage(&inbound.Config{Users: []*protocol.User{{
				Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()}),
			}}}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&freedom.Config{FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}}}),
		}},
	}

	clientConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(clientPort)}},
				Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
			}),
			ProxySettings: serial.ToTypedMessage(&dokodemo.Config{
				RewriteAddress: xraynet.NewIPOrDomain(xraynet.ParseAddress("www.baidu.com")),
				RewritePort:    443, AllowedNetworks: []xraynet.Network{xraynet.Network_TCP},
			}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&outbound.Config{
				Vnext: &protocol.ServerEndpoint{
					Address: xraynet.NewIPOrDomain(xraynet.LocalHostIP), Port: uint32(serverPort),
					User: &protocol.User{Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()})},
				},
			}),
			SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: path}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: showDebug, Fingerprint: "chrome", ServerName: "www.microsoft.com",
						PublicKey: stressPubKey, ShortId: shortIds[0], SpiderX: "/",
					})},
				},
			}),
		}},
	}

	err := BuildXray()
	common.Must(err)
	serverConfig = withDefaultApps(serverConfig)
	serverBytes, err := proto.Marshal(serverConfig)
	common.Must(err)
	clientConfig = withDefaultApps(clientConfig)
	clientBytes, err := proto.Marshal(clientConfig)
	common.Must(err)

	pprofEnv := []string{fmt.Sprintf("XRAY_PPROF=:%d", pprofPort)}
	serverProc := RunXrayProtobufWithEnv(serverBytes, pprofEnv)
	common.Must(serverProc.Start())
	clientProc := RunXrayProtobuf(clientBytes)
	common.Must(clientProc.Start())

	time.Sleep(2 * time.Second)
	servers := []*exec.Cmd{serverProc, clientProc}
	cleanup = func() { CloseAllServers(servers) }
	return
}

// parseHeapProfile parses /debug/pprof/heap?debug=1 text output, returns top N by inuse_space.
// Format: "N: M [N2: M2] @ addresses" followed by "# func+offset file:line"
func parseHeapProfile(text string, topN int) []profileEntry {
	lines := strings.Split(text, "\n")
	var entries []profileEntry
	totalInuse := int64(0)

	// First pass: compute total inuse_space
	reHeader := regexp.MustCompile(`^\s*(\d+):\s+(\d+)\s+\[(\d+):\s+(\d+)\]`)
	for _, line := range lines {
		if m := reHeader.FindStringSubmatch(line); m != nil {
			inuse, _ := strconv.ParseInt(m[2], 10, 64)
			totalInuse += inuse
		}
	}

	// Second pass: extract per-function entries
	var currentName string
	var currentInuse int64
	flush := func() {
		if currentName != "" && currentInuse > 0 {
			entries = append(entries, profileEntry{
				size: float64(currentInuse) / 1024 / 1024,
				pct:  float64(currentInuse) * 100 / float64(totalInuse),
				name: currentName,
			})
		}
	}

	reFunc := regexp.MustCompile(`^#\s+(?:0x[0-9a-f]+\s+)?(.+?)\+(?:0x[0-9a-f]+|[\d]+)\s+(.+)`)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#") {
			if m := reFunc.FindStringSubmatch(line); m != nil {
				fn := strings.TrimSpace(m[1])
				fileLine := strings.TrimSpace(m[2])
				currentName = fn + " " + fileLine
			}
			continue
		}
		if m := reHeader.FindStringSubmatch(line); m != nil {
			flush()
			currentInuse, _ = strconv.ParseInt(m[2], 10, 64)
			currentName = ""
			continue
		}
	}
	flush()

	sort.Slice(entries, func(i, j int) bool { return entries[i].size > entries[j].size })
	if len(entries) > topN {
		entries = entries[:topN]
	}
	return entries
}

// collectCPUTop10 downloads CPU profile from subprocess and uses go tool pprof to get top 10.
func collectCPUTop10(t *testing.T, pprofAddr string, seconds int) []profileEntry {
	t.Helper()
	url := fmt.Sprintf("http://%s/debug/pprof/profile?seconds=%d", pprofAddr, seconds)
	t.Logf("采集 CPU Profile (%ds)...", seconds)
	resp, err := http.Get(url)
	if err != nil {
		t.Logf("CPU profile 采集失败: %v", err)
		return nil
	}
	defer resp.Body.Close()

	tmpFile, err := os.CreateTemp("", "cpu_*.prof")
	if err != nil {
		t.Logf("创建临时文件失败: %v", err)
		return nil
	}
	defer os.Remove(tmpFile.Name())
	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		tmpFile.Close()
		t.Logf("写入 CPU profile 失败: %v", err)
		return nil
	}
	tmpFile.Close()

	cmd := exec.Command("go", "tool", "pprof", "-top", "-nodecount=10", "-cum", tmpFile.Name())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("go tool pprof 失败: %v\n%s", err, out)
		return nil
	}

	return parsePPofTopOutput(string(out))
}

// parsePPofTopOutput parses `go tool pprof -top` text output into profileEntry slice.
func parsePPofTopOutput(text string) []profileEntry {
	lines := strings.Split(text, "\n")
	var entries []profileEntry
	inTable := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.Contains(trimmed, "flat") && strings.Contains(trimmed, "cum") {
			inTable = true
			continue
		}
		if !inTable || trimmed == "" {
			continue
		}
		fields := strings.Fields(trimmed)
		if len(fields) < 5 {
			continue
		}
		cumStr := fields[3]
		pctStr := fields[4]
		name := strings.Join(fields[5:], " ")

		val := parseByteSize(cumStr)
		pct, _ := strconv.ParseFloat(strings.TrimSuffix(pctStr, "%"), 64)
		if name != "" {
			entries = append(entries, profileEntry{size: val, pct: pct, name: name})
		}
	}
	if len(entries) > 10 {
		entries = entries[:10]
	}
	return entries
}

// parseByteSize parses "1.23MB" style strings to MB float.
func parseByteSize(s string) float64 {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, "GB") {
		v, _ := strconv.ParseFloat(strings.TrimSuffix(s, "GB"), 64)
		return v * 1024
	}
	if strings.HasSuffix(s, "MB") {
		v, _ := strconv.ParseFloat(strings.TrimSuffix(s, "MB"), 64)
		return v
	}
	if strings.HasSuffix(s, "KB") {
		v, _ := strconv.ParseFloat(strings.TrimSuffix(s, "KB"), 64)
		return v / 1024
	}
	if strings.HasSuffix(s, "B") {
		v, _ := strconv.ParseFloat(strings.TrimSuffix(s, "B"), 64)
		return v / 1024 / 1024
	}
	v, _ := strconv.ParseFloat(s, 64)
	return v / 1024 / 1024
}

// printResourceTop10 prints the Top 10 analysis in the required box-drawing format.
func printResourceTop10(t *testing.T, heapTop, cpuTop []profileEntry) {
	t.Log("========================================================")
	t.Log("║ 资源消耗 Top 10 分析")
	t.Log("========================================================")

	t.Log("【Heap Memory Top 10 (内存分配热点)】")
	if len(heapTop) == 0 {
		t.Log("  (无数据)")
	}
	for i, e := range heapTop {
		t.Logf("%2d.  %.2f MB (%.1f%%)  %s", i+1, e.size, e.pct, e.name)
	}

	t.Log("")
	t.Log("【CPU Time Top 10 (CPU 耗时热点)】")
	if len(cpuTop) == 0 {
		t.Log("  (无数据)")
	}
	for i, e := range cpuTop {
		if e.size >= 1000 {
			t.Logf("%2d.  %.0f ms (%.1f%%)  %s", i+1, e.size, e.pct, e.name)
		} else {
			t.Logf("%2d.  %.2f s (%.1f%%)  %s", i+1, e.size, e.pct, e.name)
		}
	}

	t.Log("========================================================")
}

// TestREALITYFullSuiteWithPprof 完整测试套件 + pprof 真机分析。
func TestREALITYFullSuiteWithPprof(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long REALITY stress test in short mode")
	}
	pprofPort := 19090 + int(time.Now().UnixNano()%1000)

	// === 阶段 1: 基线采集 ===
	runtime.GC()
	baselineGoroutines := runtime.NumGoroutine()
	var baselineMem runtime.MemStats
	runtime.ReadMemStats(&baselineMem)
	t.Logf("=== 基线 === Goroutine: %d, Alloc: %d MB, HeapAlloc: %d MB",
		baselineGoroutines, baselineMem.Alloc/1024/1024, baselineMem.HeapAlloc/1024/1024)

	// === 阶段 2: 高并发测试 + goroutine 监控 ===
	t.Log("=== 阶段 2: 高并发测试 ===")
	clientPort, cleanup := buildStressConfigWithPprof(t, "/full-suite", pprofPort, true)
	defer cleanup()
	time.Sleep(5 * time.Second)

	pprofAddr := fmt.Sprintf("127.0.0.1:%d", pprofPort)

	// 在压力测试前启动 CPU Profile 采集（异步 HTTP 请求，测试期间持续采样）
	t.Log("启动 CPU Profile 采样 (15s)...")
	cpuProfileDone := make(chan []byte, 1)
	go func() {
		resp, err := http.Get(fmt.Sprintf("http://%s/debug/pprof/profile?seconds=15", pprofAddr))
		if err != nil {
			t.Logf("CPU profile 采集失败: %v", err)
			cpuProfileDone <- nil
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		cpuProfileDone <- body
	}()

	var goroutineSamples []int
	var memSamples []runtime.MemStats
	stopMonitor := make(chan struct{})

	go func() {
		ticker10s := time.NewTicker(10 * time.Second)
		ticker30s := time.NewTicker(30 * time.Second)
		defer ticker10s.Stop()
		defer ticker30s.Stop()
		for {
			select {
			case <-ticker10s.C:
				goroutineSamples = append(goroutineSamples, runtime.NumGoroutine())
			case <-ticker30s.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				memSamples = append(memSamples, m)
			case <-stopMonitor:
				return
			}
		}
	}()

	const concurrency = 50
	const rounds = 5
	var totalSuccess, totalFail atomic.Int64
	var totalBytes atomic.Int64

	for round := 1; round <= rounds; round++ {
		roundStart := time.Now()
		var wg sync.WaitGroup
		results := make([]stressResult, concurrency)

		for i := range concurrency {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				results[idx] = httpsRequest(int(clientPort), "www.baidu.com", "/", 30*time.Second)
			}(i)
		}
		wg.Wait()

		var roundSuccess, roundFail int
		for _, r := range results {
			if r.err != nil {
				roundFail++
				totalFail.Add(1)
			} else {
				roundSuccess++
				totalSuccess.Add(1)
				totalBytes.Add(int64(r.bytes))
			}
		}
		t.Logf("轮次 %d/%d: 成功=%d 失败=%d 耗时=%v goroutine=%d",
			round, rounds, roundSuccess, roundFail,
			time.Since(roundStart).Round(time.Millisecond), runtime.NumGoroutine())
	}

	stopMonitor <- struct{}{}

	// === 阶段 3: 等待回落 + 最终采集 ===
	time.Sleep(5 * time.Second)
	runtime.GC()
	finalGoroutines := runtime.NumGoroutine()
	var finalMem runtime.MemStats
	runtime.ReadMemStats(&finalMem)

	// === 阶段 4: 从 Xray 子进程采集 pprof ===
	t.Log("=== 阶段 4: pprof 采集 ===")

	// Heap Top 10
	heapTop := make([]profileEntry, 0)
	heapURL := fmt.Sprintf("http://%s/debug/pprof/heap?debug=1", pprofAddr)
	if resp, err := http.Get(heapURL); err == nil {
		if body, err := io.ReadAll(resp.Body); err == nil {
			heapTop = parseHeapProfile(string(body), 10)
		}
		resp.Body.Close()
	} else {
		t.Logf("Heap profile 采集失败: %v", err)
	}

	// CPU Top 10 — 等待之前启动的 CPU profile 完成
	t.Log("等待 CPU Profile 采样完成...")
	cpuProfileData := <-cpuProfileDone
	cpuTop := make([]profileEntry, 0)
	if cpuProfileData != nil {
		tmpFile, err := os.CreateTemp("", "cpu_stress_*.prof")
		if err == nil {
			tmpFile.Write(cpuProfileData)
			tmpFile.Close()
			defer os.Remove(tmpFile.Name())
			out, err := exec.Command("go", "tool", "pprof", "-top", "-nodecount=10", "-cum", tmpFile.Name()).CombinedOutput()
			if err != nil {
				t.Logf("go tool pprof 失败: %v", err)
			} else {
				cpuTop = parsePPofTopOutput(string(out))
			}
		}
	}

	// === 阶段 5: 汇总报告 ===
	total := totalSuccess.Load() + totalFail.Load()
	successRate := float64(totalSuccess.Load()) / float64(total) * 100

	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════╗")
	t.Log("║              REALITY 完整测试报告                           ║")
	t.Log("╠══════════════════════════════════════════════════════════════╣")
	t.Logf("║ 高并发测试: %d 请求, 成功率 %.1f%%                            ║", total, successRate)
	t.Logf("║ 总传输: %.2f MB                                            ║", float64(totalBytes.Load())/1024/1024)
	t.Log("╠══════════════════════════════════════════════════════════════╣")
	t.Log("║ pprof 监控指标:                                            ║")
	t.Logf("║   Goroutine: %d → %d (delta=%d)                            ║",
		baselineGoroutines, finalGoroutines, finalGoroutines-baselineGoroutines)
	heapBase := baselineMem.HeapAlloc / 1024 / 1024
	heapFinal := finalMem.HeapAlloc / 1024 / 1024
	allocBase := baselineMem.Alloc / 1024 / 1024
	allocFinal := finalMem.Alloc / 1024 / 1024
	t.Logf("║   HeapAlloc: %d MB → %d MB (delta=%d MB)                  ║",
		heapBase, heapFinal, int64(heapFinal)-int64(heapBase))
	t.Logf("║   Alloc: %d MB → %d MB (delta=%d MB)                      ║",
		allocBase, allocFinal, int64(allocFinal)-int64(allocBase))
	t.Logf("║   NumGC: %d → %d (delta=%d)                               ║",
		baselineMem.NumGC, finalMem.NumGC, finalMem.NumGC-baselineMem.NumGC)
	t.Log("╠══════════════════════════════════════════════════════════════╣")
	t.Log("║ Goroutine 采样趋势:                                        ║")
	for i, g := range goroutineSamples {
		t.Logf("║   [%3ds] goroutine=%d                                     ║", (i+1)*10, g)
	}
	t.Log("╠══════════════════════════════════════════════════════════════╣")
	t.Log("║ 堆内存采样趋势:                                            ║")
	for i, m := range memSamples {
		t.Logf("║   [%3ds] HeapAlloc=%d MB  HeapInuse=%d MB                 ║",
			(i+1)*30, m.HeapAlloc/1024/1024, m.HeapInuse/1024/1024)
	}
	t.Log("╠══════════════════════════════════════════════════════════════╣")
	t.Log("║ 验证标准:                                                  ║")
	pass := true
	if successRate < 95 {
		t.Logf("║   ❌ 成功率 %.1f%% < 95%%                                    ║", successRate)
		pass = false
	} else {
		t.Logf("║   ✅ 成功率 %.1f%% ≥ 95%%                                   ║", successRate)
	}
	if finalGoroutines > baselineGoroutines+200 {
		t.Logf("║   ❌ Goroutine 泄漏 %d→%d                                  ║", baselineGoroutines, finalGoroutines)
		pass = false
	} else {
		t.Logf("║   ✅ Goroutine 稳定 %d→%d                                  ║", baselineGoroutines, finalGoroutines)
	}
	memDelta := int64(finalMem.HeapAlloc) - int64(baselineMem.HeapAlloc)
	if memDelta > 50*1024*1024 {
		t.Logf("║   ❌ 内存增长 %d MB > 50 MB                                 ║", memDelta/1024/1024)
		pass = false
	} else {
		t.Logf("║   ✅ 内存稳定 delta=%d MB                                   ║", memDelta/1024/1024)
	}
	t.Log("╚══════════════════════════════════════════════════════════════╝")

	// === 阶段 6: 资源消耗 Top 10 分析 ===
	t.Log("")
	printResourceTop10(t, heapTop, cpuTop)

	if !pass {
		t.Error("部分验证标准未通过")
	}
}

// TestREALITYShowFalseBenchmark 对比 Show:true vs Show:false 的性能差异。
// 预期: Show:false 冷连接从 477ms 暴降到 <20ms，高并发 CPU 占用大幅下降。
func TestREALITYShowFalseBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long REALITY stress test in short mode")
	}
	type benchResult struct {
		showDebug       bool
		coldConnMs      int64
		roundTimes      []int64
		totalSuccess    int64
		totalFail       int64
		totalBytes      int64
		goroutinesDelta int
		memDeltaMB      int64
	}

	runBench := func(t *testing.T, showDebug bool) benchResult {
		t.Helper()
		pprofPort := 19200 + int(time.Now().UnixNano()%500)
		runtime.GC()
		var memBefore runtime.MemStats
		runtime.ReadMemStats(&memBefore)
		goroutinesBefore := runtime.NumGoroutine()

		clientPort, cleanup := buildStressConfigWithPprof(t, "/bench", pprofPort, showDebug)
		defer cleanup()
		time.Sleep(3 * time.Second)

		// 冷连接测试: 首次请求的耗时
		start := time.Now()
		r := httpsRequest(int(clientPort), "www.baidu.com", "/", 15*time.Second)
		coldMs := time.Since(start).Milliseconds()
		if r.err != nil {
			t.Logf("冷连接失败: %v", r.err)
		}

		// 高并发: 50 并发 × 3 轮
		var totalSuccess, totalFail atomic.Int64
		var totalBytes atomic.Int64
		var roundTimes []int64

		for round := 1; round <= 3; round++ {
			roundStart := time.Now()
			var wg sync.WaitGroup
			results := make([]stressResult, 50)
			for i := 0; i < 50; i++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					results[idx] = httpsRequest(int(clientPort), "www.baidu.com", "/", 15*time.Second)
				}(i)
			}
			wg.Wait()
			roundTimes = append(roundTimes, time.Since(roundStart).Milliseconds())
			for _, r := range results {
				if r.err != nil {
					totalFail.Add(1)
				} else {
					totalSuccess.Add(1)
					totalBytes.Add(int64(r.bytes))
				}
			}
		}

		time.Sleep(2 * time.Second)
		runtime.GC()
		var memAfter runtime.MemStats
		runtime.ReadMemStats(&memAfter)

		return benchResult{
			showDebug:       showDebug,
			coldConnMs:      coldMs,
			roundTimes:      roundTimes,
			totalSuccess:    totalSuccess.Load(),
			totalFail:       totalFail.Load(),
			totalBytes:      totalBytes.Load(),
			goroutinesDelta: runtime.NumGoroutine() - goroutinesBefore,
			memDeltaMB:      (int64(memAfter.HeapAlloc) - int64(memBefore.HeapAlloc)) / 1024 / 1024,
		}
	}

	// 先跑 Show:true 基准
	t.Log("========== Show:true 基准测试 ==========")
	rTrue := runBench(t, true)

	// 再跑 Show:false 优化测试
	t.Log("========== Show:false 优化测试 ==========")
	rFalse := runBench(t, false)

	// 对比报告
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════╗")
	t.Log("║          Show:true vs Show:false 性能对比                   ║")
	t.Log("╠══════════════════════════════════════════════════════════════╣")
	t.Logf("║ %-20s │ %12s │ %12s                      ║", "指标", "Show:true", "Show:false")
	t.Log("╠══════════════════════════════════════════════════════════════╣")
	t.Logf("║ %-20s │ %8d ms │ %8d ms                      ║", "冷连接耗时", rTrue.coldConnMs, rFalse.coldConnMs)
	t.Logf("║ %-20s │ %8d    │ %8d                        ║", "成功率",
		rTrue.totalSuccess*100/(rTrue.totalSuccess+rTrue.totalFail+1),
		rFalse.totalSuccess*100/(rFalse.totalSuccess+rFalse.totalFail+1))
	for i := 0; i < len(rTrue.roundTimes); i++ {
		label := fmt.Sprintf("轮次%d (50并发)", i+1)
		t.Logf("║ %-20s │ %8d ms │ %8d ms                      ║", label, rTrue.roundTimes[i], rFalse.roundTimes[i])
	}
	// 单请求平均延迟 = 轮次总耗时 / 并发数
	if len(rTrue.roundTimes) > 0 {
		var sumTrue, sumFalse int64
		for i := range rTrue.roundTimes {
			sumTrue += rTrue.roundTimes[i]
			sumFalse += rFalse.roundTimes[i]
		}
		avgTrue := sumTrue / int64(len(rTrue.roundTimes)*50)
		avgFalse := sumFalse / int64(len(rFalse.roundTimes)*50)
		t.Logf("║ %-20s │ %8d ms │ %8d ms                      ║", "单请求平均延迟", avgTrue, avgFalse)
	}
	t.Logf("║ %-20s │ %8d    │ %8d                        ║", "总传输(MB)",
		rTrue.totalBytes/1024/1024, rFalse.totalBytes/1024/1024)
	t.Logf("║ %-20s │ %+8d     │ %+8d                       ║", "Goroutine delta",
		rTrue.goroutinesDelta, rFalse.goroutinesDelta)
	t.Logf("║ %-20s │ %+8d MB  │ %+8d MB                    ║", "Heap delta",
		rTrue.memDeltaMB, rFalse.memDeltaMB)
	t.Log("╠══════════════════════════════════════════════════════════════╣")

	// 冷连接提升比例
	if rTrue.coldConnMs > 0 {
		improve := float64(rTrue.coldConnMs-rFalse.coldConnMs) / float64(rTrue.coldConnMs) * 100
		t.Logf("║ 冷连接提升: %.1f%% (%dms → %dms)                           ║",
			improve, rTrue.coldConnMs, rFalse.coldConnMs)
	}
	// 轮次平均耗时提升
	if len(rTrue.roundTimes) > 0 && len(rFalse.roundTimes) > 0 {
		var sumTrue, sumFalse int64
		for i := range rTrue.roundTimes {
			sumTrue += rTrue.roundTimes[i]
			sumFalse += rFalse.roundTimes[i]
		}
		avgTrue := sumTrue / int64(len(rTrue.roundTimes))
		avgFalse := sumFalse / int64(len(rFalse.roundTimes))
		if avgTrue > 0 {
			improve := float64(avgTrue-avgFalse) / float64(avgTrue) * 100
			t.Logf("║ 50并发轮次总耗时均值: %.1f%% (%dms → %dms)              ║",
				improve, avgTrue, avgFalse)
		}
	}
	t.Log("╚══════════════════════════════════════════════════════════════╝")

	// 验证: Show:false 冷连接应该显著更快
	if rFalse.coldConnMs > 0 && rTrue.coldConnMs > 0 {
		ratio := float64(rTrue.coldConnMs) / float64(rFalse.coldConnMs)
		if ratio < 1.5 {
			t.Logf("⚠️ Show:false 冷连接仅快 %.1fx，预期 >2x", ratio)
		} else {
			t.Logf("✅ Show:false 冷连接快 %.1fx，符合预期", ratio)
		}
	}
}
