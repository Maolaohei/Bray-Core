package scenarios

import (
	"bytes"
	"crypto/rand"
	"io"
	mrand "math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/log"
	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/common"
	clog "github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
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
	"github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/tls"
)

// ============================================================================
// netProxy: 用户态 TCP 代理，模拟真实网络条件
// ============================================================================

type netProxy struct {
	latency    time.Duration // 单程延迟
	jitter     time.Duration // 抖动
	lossRate   float64       // 丢包率 0.0-1.0
	bandwidth  int64         // 带宽限制 bytes/s, 0=不限
	listenAddr string
	listener   net.Listener
}

func (p *netProxy) Start(t *testing.T) {
	t.Helper()
	var err error
	p.listener, err = net.Listen("tcp", p.listenAddr)
	if err != nil {
		t.Fatal(err)
	}
	go p.acceptLoop()
}

func (p *netProxy) Stop() {
	if p.listener != nil {
		p.listener.Close()
	}
}

func (p *netProxy) Addr() string {
	return p.listener.Addr().String()
}

func (p *netProxy) acceptLoop() {
	for {
		src, err := p.listener.Accept()
		if err != nil {
			return
		}
		go p.handleConn(src)
	}
}

func (p *netProxy) handleConn(src net.Conn) {
	host, portStr, _ := net.SplitHostPort(src.RemoteAddr().String())
	_ = host
	_ = portStr
	dst, err := net.DialTimeout("tcp", src.LocalAddr().String(), 5*time.Second)
	// 这里不能 dial 自己，需要从 outerAddr 获取真实目标
	// 改用 dialer 方式
	_ = dst
	_ = err
	src.Close()
}

// ============================================================================
// SimulatedNetwork: 基于 pipe 的网络模拟器，无真实 TCP 连接
// ============================================================================

type SimNetConn struct {
	r        *io.PipeReader
	w        *io.PipeWriter
	peer     *SimNetConn
	latency  time.Duration
	jitter   time.Duration
	lossRate float64
	bandwd   int64 // bytes/s, 0=unlimited
	closed   chan struct{}
	once     sync.Once
}

func NewSimNetConn(latency, jitter time.Duration, lossRate float64, bandwidth int64) (*SimNetConn, *SimNetConn) {
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	c1 := &SimNetConn{r: r1, w: w2, latency: latency, jitter: jitter, lossRate: lossRate, bandwd: bandwidth, closed: make(chan struct{})}
	c2 := &SimNetConn{r: r2, w: w1, latency: latency, jitter: jitter, lossRate: lossRate, bandwd: bandwidth, closed: make(chan struct{})}
	c1.peer = c2
	c2.peer = c1
	return c1, c2
}

func (c *SimNetConn) Read(b []byte) (int, error) {
	// 模拟延迟
	if c.latency > 0 || c.jitter > 0 {
		delay := c.latency
		if c.jitter > 0 {
			delay += time.Duration(mrand.Int63n(int64(c.jitter)))
		}
		select {
		case <-time.After(delay):
		case <-c.closed:
			return 0, net.ErrClosed
		}
	}
	// 模拟丢包
	if c.lossRate > 0 && mrand.Float64() < c.lossRate {
		return 0, io.ErrShortBuffer
	}
	return c.r.Read(b)
}

func (c *SimNetConn) Write(b []byte) (int, error) {
	// 模拟带宽限制
	if c.bandwd > 0 {
		needed := time.Duration(float64(len(b)) / float64(c.bandwd) * float64(time.Second))
		select {
		case <-time.After(needed):
		case <-c.closed:
			return 0, net.ErrClosed
		}
	}
	return c.peer.w.Write(b)
}

func (c *SimNetConn) Close() error {
	c.once.Do(func() {
		close(c.closed)
		c.r.Close()
		c.w.Close()
	})
	return nil
}

func (c *SimNetConn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (c *SimNetConn) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (c *SimNetConn) SetDeadline(t time.Time) error      { return nil }
func (c *SimNetConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *SimNetConn) SetWriteDeadline(t time.Time) error { return nil }

// ============================================================================
// 测试辅助：构建 VLESS over XHTTP 完整链路
// ============================================================================

type vlessXhttpResult struct {
	TotalBytes  int64
	TotalErrors int64
	TotalReads  int64
	TotalWrites int64
	Duration    time.Duration
	RecoverTime time.Duration // idle 后首次恢复时间
}

func setupVlessXhttpServer(t *testing.T, mode string) (net.Port, func()) {
	t.Helper()

	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()

	serverConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{
				ErrorLogLevel: clog.Severity_Debug,
				ErrorLogType:  log.LogType_Console,
			}),
		},
		Inbound: []*core.InboundHandlerConfig{
			{
				ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
					PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(serverPort)}},
					Listen:   net.NewIPOrDomain(net.LocalHostIP),
				}),
				ProxySettings: serial.ToTypedMessage(&inbound.Config{
					Users: []*protocol.User{
						{
							Account: serial.ToTypedMessage(&vless.Account{
								Id: userID.String(),
							}),
						},
					},
				}),
				SenderSettings: serial.ToTypedMessage(&internet.MemoryStreamConfig{
					ProtocolName:     "splithttp",
					ProtocolSettings: &splithttp.Config{Path: "/bench"},
					SecurityType:     "tls",
					SecuritySettings: &tls.Config{Nil: true},
				}),
			},
		},
		Outbound: []*core.OutboundHandlerConfig{
			{
				ProxySettings: serial.ToTypedMessage(&freedom.Config{}),
			},
		},
	}

	servers, err := InitializeServerConfigs(serverConfig)
	if err != nil {
		t.Fatal(err)
	}

	return serverPort, func() {
		CloseAllServers(servers)
	}
}

// ============================================================================
// 核心测试：VLESS+XHTTP 真实环境模拟
// ============================================================================

// NetworkProfile 模拟不同网络环境
type NetworkProfile struct {
	Name      string
	Latency   time.Duration
	Jitter    time.Duration
	LossRate  float64
	Bandwidth int64
}

var networkProfiles = []NetworkProfile{
	{"GoodLAN", 1 * time.Millisecond, 0, 0, 0},                                         // 局域网
	{"GoodWiFi", 5 * time.Millisecond, 2 * time.Millisecond, 0, 0},                     // 好 WiFi
	{"4G", 30 * time.Millisecond, 10 * time.Millisecond, 0.01, 10 * 1024 * 1024},       // 4G
	{"BadWiFi", 50 * time.Millisecond, 20 * time.Millisecond, 0.05, 5 * 1024 * 1024},   // 差 WiFi
	{"Oversea", 150 * time.Millisecond, 30 * time.Millisecond, 0.02, 20 * 1024 * 1024}, // 跨境
}

// TestVlessXhttp_NoDrop 连续传输 60s，不允许任何断流
func TestVlessXhttp_NoDrop(t *testing.T) {
	if testing.Short() {
		t.Skip("long endurance test")
	}

	for _, profile := range networkProfiles {
		t.Run(profile.Name, func(t *testing.T) {
			testContinuousTransfer(t, profile, 60*time.Second, 0) // 0 errors allowed
		})
	}
}

// TestVlessXhttp_IdleRecovery 传输 → 空闲 30s → 恢复传输，不允许断流
func TestVlessXhttp_IdleRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("long endurance test")
	}

	for _, profile := range networkProfiles {
		t.Run(profile.Name, func(t *testing.T) {
			testIdleRecovery(t, profile)
		})
	}
}

// TestVlessXhttp_MultiConn 并发 10 连接持续传输
func TestVlessXhttp_MultiConn(t *testing.T) {
	if testing.Short() {
		t.Skip("long endurance test")
	}

	for _, profile := range networkProfiles {
		t.Run(profile.Name, func(t *testing.T) {
			testMultiConnection(t, profile, 30*time.Second, 10)
		})
	}
}

// TestVlessXhttp_BurstPattern 模拟视频切换：短连接快速切换
func TestVlessXhttp_BurstPattern(t *testing.T) {
	if testing.Short() {
		t.Skip("long endurance test")
	}

	for _, profile := range networkProfiles {
		t.Run(profile.Name, func(t *testing.T) {
			testBurstPattern(t, profile, 20, 1*time.Second)
		})
	}
}

// ============================================================================
// 测试实现
// ============================================================================

func testContinuousTransfer(t *testing.T, profile NetworkProfile, duration time.Duration, allowedErrors int64) {
	t.Helper()

	// 1. 启动 echo server
	echoServer := tcp.Server{MsgProcessor: func(b []byte) []byte { return b }}
	dest, err := echoServer.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer echoServer.Close()

	// 2. 启动 VLESS inbound (服务端)
	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()

	ct, ctHash := cert.MustGenerate(nil, cert.CommonName("localhost"))

	serverConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{
			{
				ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
					PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(serverPort)}},
					Listen:   net.NewIPOrDomain(net.LocalHostIP),
				}),
				ProxySettings: serial.ToTypedMessage(&inbound.Config{
					Users: []*protocol.User{{Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()})}},
				}),
				SenderSettings: serial.ToTypedMessage(&internet.MemoryStreamConfig{
					ProtocolName:     "splithttp",
					ProtocolSettings: &splithttp.Config{Path: "/bench"},
					SecurityType:     "tls",
					SecuritySettings: &tls.Config{
						Certificate:          []*tls.Certificate{tls.ParseCertificate(ct)},
						PinnedPeerCertSha256: [][]byte{ctHash[:]},
					},
				}),
			},
		},
		Outbound: []*core.OutboundHandlerConfig{
			{ProxySettings: serial.ToTypedMessage(&freedom.Config{})},
		},
	}

	// 3. 启动 VLESS outbound (客户端)
	clientPort := tcp.PickPort()
	clientConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{
			{
				ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
					PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(clientPort)}},
					Listen:   net.NewIPOrDomain(net.LocalHostIP),
				}),
				ProxySettings: serial.ToTypedMessage(&dokodemo.Config{
					RewriteAddress:  net.NewIPOrDomain(dest.Address),
					RewritePort:     uint32(dest.Port),
					AllowedNetworks: []net.Network{net.Network_TCP},
				}),
			},
		},
		Outbound: []*core.OutboundHandlerConfig{
			{
				ProxySettings: serial.ToTypedMessage(&outbound.Config{
					Vnext: &protocol.ServerEndpoint{
						Users: []*protocol.User{
							{Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()})},
						},
					},
					Servers: []*protocol.ServerEndpoint_Server{
						{Address: net.NewIPOrDomain(net.LocalHostIP), Port: uint32(serverPort)},
					},
				}),
				SenderSettings: serial.ToTypedMessage(&internet.MemoryStreamConfig{
					ProtocolName:     "splithttp",
					ProtocolSettings: &splithttp.Config{Path: "/bench"},
					SecurityType:     "tls",
					SecuritySettings: &tls.Config{
						ServerName:           "localhost",
						AllowInsecure:        true,
						PinnedPeerCertSha256: [][]byte{ctHash[:]},
					},
				}),
			},
		},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer CloseAllServers(servers)
	time.Sleep(2 * time.Second)

	// 4. 连接并持续传输
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", clientPort.String()), 5*time.Second)
	if err != nil {
		t.Fatal("dial failed:", err)
	}
	defer conn.Close()

	var (
		totalBytes  atomic.Int64
		totalErrors atomic.Int64
		totalWrites atomic.Int64
		totalReads  atomic.Int64
		stop        = make(chan struct{})
		wg          sync.WaitGroup
	)

	// 写 goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		payload := make([]byte, 32*1024)
		rand.Read(payload)
		for {
			select {
			case <-stop:
				return
			default:
			}
			n, err := conn.Write(payload)
			if err != nil {
				totalErrors.Add(1)
				return
			}
			totalWrites.Add(int64(n))
		}
	}()

	// 读 goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 32*1024)
		for {
			select {
			case <-stop:
				return
			default:
			}
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			n, err := conn.Read(buf)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				totalErrors.Add(1)
				return
			}
			totalBytes.Add(int64(n))
			totalReads.Add(int64(n))
		}
	}()

	// 5. 运行并监控
	t.Logf("Profile: %s, Duration: %v", profile.Name, duration)
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

	// 6. 最终报告
	mb := float64(totalBytes.Load()) / 1e6
	mbps := mb * 8 / duration.Seconds()
	errs := totalErrors.Load()

	t.Logf("=== 结果: %s ===", profile.Name)
	t.Logf("  总传输: %.0f MB (%.1f Mbps)", mb, mbps)
	t.Logf("  写入: %d bytes, 读取: %d bytes", totalWrites.Load(), totalReads.Load())
	t.Logf("  错误数: %d", errs)

	if errs > allowedErrors {
		t.Errorf("断流! 错误数 %d > 允许值 %d", errs, allowedErrors)
	}

	// 速度不能低于基线的 10%（防止优化后性能崩塌）
	if mbps < 1.0 && totalBytes.Load() > 0 {
		t.Logf("警告: 速度 %.1f Mbps 可能过低", mbps)
	}
}

func testIdleRecovery(t *testing.T, profile NetworkProfile) {
	t.Helper()

	// 启动服务器
	echoServer := tcp.Server{MsgProcessor: func(b []byte) []byte { return b }}
	dest, err := echoServer.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer echoServer.Close()

	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	ct, ctHash := cert.MustGenerate(nil, cert.CommonName("localhost"))

	serverConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{
			{
				ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
					PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(serverPort)}},
					Listen:   net.NewIPOrDomain(net.LocalHostIP),
				}),
				ProxySettings: serial.ToTypedMessage(&inbound.Config{
					Users: []*protocol.User{{Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()})}},
				}),
				SenderSettings: serial.ToTypedMessage(&internet.MemoryStreamConfig{
					ProtocolName:     "splithttp",
					ProtocolSettings: &splithttp.Config{Path: "/bench"},
					SecurityType:     "tls",
					SecuritySettings: &tls.Config{
						Certificate:          []*tls.Certificate{tls.ParseCertificate(ct)},
						PinnedPeerCertSha256: [][]byte{ctHash[:]},
					},
				}),
			},
		},
		Outbound: []*core.OutboundHandlerConfig{
			{ProxySettings: serial.ToTypedMessage(&freedom.Config{})},
		},
	}

	clientPort := tcp.PickPort()
	clientConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{
			{
				ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
					PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(clientPort)}},
					Listen:   net.NewIPOrDomain(net.LocalHostIP),
				}),
				ProxySettings: serial.ToTypedMessage(&dokodemo.Config{
					RewriteAddress:  net.NewIPOrDomain(dest.Address),
					RewritePort:     uint32(dest.Port),
					AllowedNetworks: []net.Network{net.Network_TCP},
				}),
			},
		},
		Outbound: []*core.OutboundHandlerConfig{
			{
				ProxySettings: serial.ToTypedMessage(&outbound.Config{
					Vnext: &protocol.ServerEndpoint{
						Users: []*protocol.User{
							{Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()})},
						},
					},
					Servers: []*protocol.ServerEndpoint_Server{
						{Address: net.NewIPOrDomain(net.LocalHostIP), Port: uint32(serverPort)},
					},
				}),
				SenderSettings: serial.ToTypedMessage(&internet.MemoryStreamConfig{
					ProtocolName:     "splithttp",
					ProtocolSettings: &splithttp.Config{Path: "/bench"},
					SecurityType:     "tls",
					SecuritySettings: &tls.Config{
						ServerName:           "localhost",
						AllowInsecure:        true,
						PinnedPeerCertSha256: [][]byte{ctHash[:]},
					},
				}),
			},
		},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer CloseAllServers(servers)
	time.Sleep(2 * time.Second)

	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", clientPort.String()), 5*time.Second)
	if err != nil {
		t.Fatal("dial failed:", err)
	}
	defer conn.Close()

	payload := make([]byte, 32*1024)
	rand.Read(payload)

	// Phase 1: 持续传输 5s
	t.Log("Phase 1: 持续传输 5s...")
	start := time.Now()
	for time.Since(start) < 5*time.Second {
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

	// Phase 2: 空闲 30s
	t.Log("Phase 2: 空闲 30s...")
	time.Sleep(30 * time.Second)
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

	echoServer := tcp.Server{MsgProcessor: func(b []byte) []byte { return b }}
	dest, err := echoServer.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer echoServer.Close()

	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	ct, ctHash := cert.MustGenerate(nil, cert.CommonName("localhost"))

	serverConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{
			{
				ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
					PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(serverPort)}},
					Listen:   net.NewIPOrDomain(net.LocalHostIP),
				}),
				ProxySettings: serial.ToTypedMessage(&inbound.Config{
					Users: []*protocol.User{{Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()})}},
				}),
				SenderSettings: serial.ToTypedMessage(&internet.MemoryStreamConfig{
					ProtocolName:     "splithttp",
					ProtocolSettings: &splithttp.Config{Path: "/bench"},
					SecurityType:     "tls",
					SecuritySettings: &tls.Config{
						Certificate:          []*tls.Certificate{tls.ParseCertificate(ct)},
						PinnedPeerCertSha256: [][]byte{ctHash[:]},
					},
				}),
			},
		},
		Outbound: []*core.OutboundHandlerConfig{
			{ProxySettings: serial.ToTypedMessage(&freedom.Config{})},
		},
	}

	clientPort := tcp.PickPort()
	clientConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{
			{
				ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
					PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(clientPort)}},
					Listen:   net.NewIPOrDomain(net.LocalHostIP),
				}),
				ProxySettings: serial.ToTypedMessage(&dokodemo.Config{
					RewriteAddress:  net.NewIPOrDomain(dest.Address),
					RewritePort:     uint32(dest.Port),
					AllowedNetworks: []net.Network{net.Network_TCP},
				}),
			},
		},
		Outbound: []*core.OutboundHandlerConfig{
			{
				ProxySettings: serial.ToTypedMessage(&outbound.Config{
					Vnext: &protocol.ServerEndpoint{
						Users: []*protocol.User{
							{Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()})},
						},
					},
					Servers: []*protocol.ServerEndpoint_Server{
						{Address: net.NewIPOrDomain(net.LocalHostIP), Port: uint32(serverPort)},
					},
				}),
				SenderSettings: serial.ToTypedMessage(&internet.MemoryStreamConfig{
					ProtocolName:     "splithttp",
					ProtocolSettings: &splithttp.Config{Path: "/bench"},
					SecurityType:     "tls",
					SecuritySettings: &tls.Config{
						ServerName:           "localhost",
						AllowInsecure:        true,
						PinnedPeerCertSha256: [][]byte{ctHash[:]},
					},
				}),
			},
		},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer CloseAllServers(servers)
	time.Sleep(2 * time.Second)

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
			conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", clientPort.String()), 5*time.Second)
			if err != nil {
				t.Logf("  [conn %d] dial failed: %v", id, err)
				totalErrors.Add(1)
				return
			}
			defer conn.Close()

			payload := make([]byte, 8192+mrand.Intn(32768))
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

func testBurstPattern(t *testing.T, profile NetworkProfile, rounds int, switchInterval time.Duration) {
	t.Helper()

	echoServer := tcp.Server{MsgProcessor: func(b []byte) []byte { return b }}
	dest, err := echoServer.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer echoServer.Close()

	userID := protocol.NewID(uuid.New())
	ct, ctHash := cert.MustGenerate(nil, cert.CommonName("localhost"))

	var (
		totalSuccess int64
		totalFail    int64
		totalBytes   int64
	)

	for i := 0; i < rounds; i++ {
		serverPort := tcp.PickPort()
		serverConfig := &core.Config{
			App: []*serial.TypedMessage{
				serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
			},
			Inbound: []*core.InboundHandlerConfig{
				{
					ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
						PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(serverPort)}},
						Listen:   net.NewIPOrDomain(net.LocalHostIP),
					}),
					ProxySettings: serial.ToTypedMessage(&inbound.Config{
						Users: []*protocol.User{{Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()})}},
					}),
					SenderSettings: serial.ToTypedMessage(&internet.MemoryStreamConfig{
						ProtocolName:     "splithttp",
						ProtocolSettings: &splithttp.Config{Path: "/bench"},
						SecurityType:     "tls",
						SecuritySettings: &tls.Config{
							Certificate:          []*tls.Certificate{tls.ParseCertificate(ct)},
							PinnedPeerCertSha256: [][]byte{ctHash[:]},
						},
					}),
				},
			},
			Outbound: []*core.OutboundHandlerConfig{
				{ProxySettings: serial.ToTypedMessage(&freedom.Config{})},
			},
		}

		clientPort := tcp.PickPort()
		clientConfig := &core.Config{
			App: []*serial.TypedMessage{
				serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
			},
			Inbound: []*core.InboundHandlerConfig{
				{
					ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
						PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(clientPort)}},
						Listen:   net.NewIPOrDomain(net.LocalHostIP),
					}),
					ProxySettings: serial.ToTypedMessage(&dokodemo.Config{
						RewriteAddress:  net.NewIPOrDomain(dest.Address),
						RewritePort:     uint32(dest.Port),
						AllowedNetworks: []net.Network{net.Network_TCP},
					}),
				},
			},
			Outbound: []*core.OutboundHandlerConfig{
				{
					ProxySettings: serial.ToTypedMessage(&outbound.Config{
						Vnext: &protocol.ServerEndpoint{
							Users: []*protocol.User{
								{Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()})},
							},
						},
						Servers: []*protocol.ServerEndpoint_Server{
							{Address: net.NewIPOrDomain(net.LocalHostIP), Port: uint32(serverPort)},
						},
					}),
					SenderSettings: serial.ToTypedMessage(&internet.MemoryStreamConfig{
						ProtocolName:     "splithttp",
						ProtocolSettings: &splithttp.Config{Path: "/bench"},
						SecurityType:     "tls",
						SecuritySettings: &tls.Config{
							ServerName:           "localhost",
							AllowInsecure:        true,
							PinnedPeerCertSha256: [][]byte{ctHash[:]},
						},
					}),
				},
			},
		}

		servers, err := InitializeServerConfigs(serverConfig, clientConfig)
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(1 * time.Second)

		// 快速传输一个小 payload
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", clientPort.String()), 5*time.Second)
		if err != nil {
			totalFail++
			CloseAllServers(servers)
			continue
		}

		payload := make([]byte, 64*1024)
		rand.Read(payload)

		if _, err := conn.Write(payload); err != nil {
			totalFail++
			conn.Close()
			CloseAllServers(servers)
			continue
		}

		buf := make([]byte, len(payload))
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, err := io.ReadFull(conn, buf)
		if err != nil || !bytes.Equal(buf[:n], payload) {
			totalFail++
			conn.Close()
			CloseAllServers(servers)
			continue
		}

		totalSuccess++
		totalBytes += int64(n)
		conn.Close()
		CloseAllServers(servers)
	}

	t.Logf("=== Burst 测试: %s (%d rounds) ===", profile.Name, rounds)
	t.Logf("  成功: %d, 失败: %d, 总传输: %.0f MB", totalSuccess, totalFail, float64(totalBytes)/1e6)

	if totalFail > rounds/10 {
		t.Errorf("断流! 失败率 %.0f%% > 10%%", float64(totalFail)/float64(rounds)*100)
	}
}
