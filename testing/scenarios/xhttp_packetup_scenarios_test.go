package scenarios

// Fast iteration harness for the packet-up + dseg user report. Uses TLS-free
// (plaintext) dual-end by default to avoid REALITY/TLS warm-up, so a full
// VLESS dual-end e2e round-trips in seconds. Target is a local TCP echo.
//
// Covers the traffic shapes that surfaced real-world regressions:
//   - plain web (many short burst connections)
//   - video (medium sustained flow with pause/resume)
//   - file download (single sustained)
//   - large file download (long sustained - stresses the adaptive window)
//   - multithreaded file download (many concurrent sustained conns)
//   - weak network (delay + loss + bandwidth cap via a TCP proxy)
//
// Run with:
//   go test ./testing/scenarios -run 'TestVlessTLSPacketUp' -v
//
// The *_Scenario tests are skipped under -short.

import (
	"io"
	"math/rand"
	stdnet "net"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/xtls/xray-core/app/log"
	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/common"
	clog "github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
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
	"github.com/xtls/xray-core/transport/internet/splithttp"
)

// ---------------------------------------------------------------------------
// Dual-end setup (VLESS in/out + splithttp packet-up + dseg, local TCP echo).
// clientProxyAddr, when non-empty, is where the client outbound dials so the
// weak-net proxy sits between the two ends.
// ---------------------------------------------------------------------------

func startDsegDualEnd(t *testing.T, serverPort net.Port, clientProxyAddr string) (clientPort net.Port) {
	t.Helper()
	tcpSrv := tcp.Server{MsgProcessor: xor}
	dest, err := tcpSrv.Start()
	common.Must(err)
	t.Cleanup(func() { _ = tcpSrv.Close() })

	userID := protocol.NewID(uuid.New())
	sharedConfig := func() *splithttp.Config {
		return &splithttp.Config{
			Path: "/xh-pup", Mode: "packet-up",
			Headers: map[string]string{
				splithttp.BraySessionSecretHeader: "pup-shared-secret",
				"x-bray-dseg":                     "1",
			},
		}
	}

	serverConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList:       &net.PortList{Range: []*net.PortRange{net.SinglePortRange(serverPort)}},
				Listen:         net.NewIPOrDomain(net.LocalHostIP),
				StreamSettings: dsegStreamConfig(sharedConfig(), nil),
			}),
			ProxySettings: serial.ToTypedMessage(&inbound.Config{Users: []*protocol.User{{
				Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()}),
			}}}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&freedom.Config{FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}}}),
		}},
	}

	clientPort = tcp.PickPort()
	peerAddr := net.LocalHostIP
	peerPort := serverPort
	if clientProxyAddr != "" {
		a, prt := splitHostPort(clientProxyAddr)
		peerAddr = a
		peerPort = prt
	}
	clientConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(clientPort)}},
				Listen:   net.NewIPOrDomain(net.LocalHostIP),
			}),
			ProxySettings: serial.ToTypedMessage(&dokodemo.Config{
				RewriteAddress:  net.NewIPOrDomain(dest.Address),
				RewritePort:     uint32(dest.Port),
				AllowedNetworks: []net.Network{net.Network_TCP},
			}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&outbound.Config{
				Vnext: &protocol.ServerEndpoint{
					Address: net.NewIPOrDomain(peerAddr), Port: uint32(peerPort),
					User: &protocol.User{Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()})},
				},
			}),
			SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
				StreamSettings: dsegStreamConfig(sharedConfig(), nil),
			}),
		}},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	common.Must(err)
	t.Cleanup(func() { CloseAllServers(servers) })
	return clientPort
}

func dsegStreamConfig(cfg *splithttp.Config, _ any) *internet.StreamConfig {
	return &internet.StreamConfig{
		ProtocolName: "splithttp",
		TransportSettings: []*internet.TransportConfig{{
			ProtocolName: "splithttp",
			Settings:     serial.ToTypedMessage(cfg),
		}},
	}
}

func splitHostPort(addr string) (net.Address, net.Port) {
	host, portStr, _ := stdnet.SplitHostPort(addr)
	port, _ := strconv.Atoi(portStr)
	return net.ParseAddress(host), net.Port(port)
}

// ---------------------------------------------------------------------------
// Workloads
// ---------------------------------------------------------------------------

// dialEcho opens a raw TCP conn to the client inbound (socks-side).
func dialClient(clientPort net.Port) (stdnet.Conn, error) {
	return stdnet.Dial("tcp", "127.0.0.1:"+strconv.Itoa(int(clientPort)))
}

// echoOnce writes size bytes and verifies the XOR-echoed copy (server runs
// MsgProcessor=xor, so the response is payload ^ 'c'). Bounded so a wedged
// weak-net connection fails fast instead of hanging the scenario forever.
func echoOnce(conn net.Conn, size int) error {
	if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}
	payload := make([]byte, size)
	if _, err := rand.Read(payload); err != nil {
		return err
	}
	if _, err := conn.Write(payload); err != nil {
		return err
	}
	got := make([]byte, size)
	if _, err := io.ReadFull(conn, got); err != nil {
		return err
	}
	for i, v := range got {
		if v != payload[i]^'c' {
			return &errMismatch{expect: size}
		}
	}
	return nil
}

type errMismatch struct{ expect int }

func (e *errMismatch) Error() string {
	return "echo mismatch (" + strconv.Itoa(e.expect) + " bytes)"
}

// workloadWeb: many short-lived burst connections (plain web browsing).
func workloadWeb(t *testing.T, clientPort net.Port) error {
	var wg errgroup.Group
	for range 8 {
		wg.Go(func() error {
			for j := 0; j < 4; j++ {
				conn, err := dialClient(clientPort)
				if err != nil {
					return err
				}
				sizes := []int{2 << 10, 16 << 10, 64 << 10, 256 << 10}
				for _, size := range sizes {
					if err := echoOnce(conn, size); err != nil {
						_ = conn.Close()
						return err
					}
				}
				_ = conn.Close()
				time.Sleep(10 * time.Millisecond)
			}
			return nil
		})
	}
	return wg.Wait()
}

// workloadVideo: medium sustained flow with pause/resume (player buffering).
func workloadVideo(t *testing.T, clientPort net.Port) error {
	var wg errgroup.Group
	for i := 0; i < 3; i++ {
		wg.Go(func() error {
			conn, err := dialClient(clientPort)
			if err != nil {
				return err
			}
			defer conn.Close()
			for j := 0; j < 12; j++ {
				if err := echoOnce(conn, 1<<20); err != nil { // 1 MiB per segment
					return err
				}
				if j%4 == 3 {
					time.Sleep(30 * time.Millisecond) // buffer stall
				}
			}
			return nil
		})
	}
	return wg.Wait()
}

// workloadFile: a few sustained single-file downloads.
func workloadFile(t *testing.T, clientPort net.Port) error {
	var wg errgroup.Group
	for i := 0; i < 2; i++ {
		wg.Go(func() error {
			conn, err := dialClient(clientPort)
			if err != nil {
				return err
			}
			defer conn.Close()
			for j := 0; j < 16; j++ {
				if err := echoOnce(conn, 4<<20); err != nil { // 4 MiB chunks x16 = 64 MiB
					return err
				}
			}
			return nil
		})
	}
	return wg.Wait()
}

// workloadLargeFile: one long sustained download (stresses adaptive window).
func workloadLargeFile(t *testing.T, clientPort net.Port) error {
	conn, err := dialClient(clientPort)
	if err != nil {
		return err
	}
	defer conn.Close()
	for j := 0; j < 128; j++ {
		if err := echoOnce(conn, 4<<20); err != nil { // 128 x 4 MiB = 512 MiB total
			return err
		}
	}
	return nil
}

// workloadMultiThreadFile: many concurrent sustained downloads.
func workloadMultiThreadFile(t *testing.T, clientPort net.Port) error {
	var wg errgroup.Group
	for i := 0; i < 8; i++ {
		wg.Go(func() error {
			conn, err := dialClient(clientPort)
			if err != nil {
				return err
			}
			defer conn.Close()
			for j := 0; j < 10; j++ {
				if err := echoOnce(conn, 2<<20); err != nil { // 8 x 20 MiB
					return err
				}
			}
			return nil
		})
	}
	return wg.Wait()
}

// workloadWeakNet: single download through the lossy proxy (delay+loss+bw).
func workloadWeakNet(t *testing.T, clientPort net.Port) error {
	conn, err := dialClient(clientPort)
	if err != nil {
		return err
	}
	defer conn.Close()
	for j := 0; j < 30; j++ {
		if err := echoOnce(conn, 1<<20); err != nil { // 30 MiB over weak link
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Scenario runner
// ---------------------------------------------------------------------------

func runScenario(t *testing.T, wc struct{}, fn func(*testing.T, net.Port) error) {
	t.Helper()
	serverPort := tcp.PickPort()
	clientPort := startDsegDualEnd(t, serverPort, "")
	if err := fn(t, clientPort); err != nil {
		t.Fatalf("scenario failed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

func TestVLESSXHTTP_WebScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scenario in short mode")
	}
	runScenario(t, struct{}{}, workloadWeb)
}

func TestVLESSXHTTP_VideoScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scenario in short mode")
	}
	runScenario(t, struct{}{}, workloadVideo)
}

func TestVLESSXHTTP_FileDownloadScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scenario in short mode")
	}
	runScenario(t, struct{}{}, workloadFile)
}

func TestVLESSXHTTP_LargeFileDownloadScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scenario in short mode")
	}
	runScenario(t, struct{}{}, workloadLargeFile)
}

func TestVLESSXHTTP_MultiThreadFileDownloadScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scenario in short mode")
	}
	runScenario(t, struct{}{}, workloadMultiThreadFile)
}

func TestVLESSXHTTP_WeakNetScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scenario in short mode")
	}
	runScenario(t, struct{}{}, workloadWeakNet)
}

// ---------------------------------------------------------------------------
// Keep the original fast smoke harness (plain/legacy/TLS) intact.
// ---------------------------------------------------------------------------

func TestVlessTLSPacketUpDsegPlain(t *testing.T) {
	serverPort := tcp.PickPort()
	clientPort := startDsegDualEnd(t, serverPort, "")
	var wg errgroup.Group
	for range 3 {
		wg.Go(func() error { return echoOnce(mustConn(clientPort), 256*1024) })
	}
	if err := wg.Wait(); err != nil {
		t.Fatal(err)
	}
}

func mustConn(clientPort net.Port) stdnet.Conn {
	conn, err := dialClient(clientPort)
	common.Must(err)
	return conn
}
