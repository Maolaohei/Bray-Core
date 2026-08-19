package scenarios

// Fast iteration harness for the packet-up + dseg user report. Uses TLS-free
// (plaintext) dual-end so a full VLESS dual-end e2e round-trips in seconds.
// Target is a local TCP echo (no external network).
//
// Covers the traffic shapes that surfaced real-world regressions, each as a
// named scenario (run with: go test ./testing/scenarios -run 'TestVlessTLSPacketUp' -v):
//   - Web         short burst connections (plain web)
//   - Video       medium sustained flow with pause/resume
//   - File        single sustained file download
//   - LargeFile   long sustained download (stresses the adaptive window)
//   - MultiThread many concurrent sustained downloads
//   - WeakNet     high-RTT + jitter + bandwidth cap via a TCP proxy on the
//                 real client->server path (delay/loss/bw injected)
// All *_Scenario tests are skipped under -short.

import (
	"io"
	"math/rand"
	stdnet "net"
	"os/exec"
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

// startProc launches one Xray test process from a protobuf config, returning
// the running *exec.Cmd. The child process takes a moment to bind its
// listener, so callers must wait for the port before dialing.
func startProc(t *testing.T, cfg *core.Config) *exec.Cmd {
	t.Helper()
	proc, err := InitializeServerConfig(cfg)
	common.Must(err)
	return proc
}

// waitPort blocks until a TCP listener accepts on 127.0.0.1:port or ~5s pass
// (a child xray process that takes a moment to bind).
func waitPort(t *testing.T, port net.Port) {
	t.Helper()
	addr := "127.0.0.1:" + strconv.Itoa(int(port))
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := stdnet.Dial("tcp", addr)
		if err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("listener %s did not come up", addr)
}

// closeProcs closes the xray test processes, skipping the nil placeholder we
// keep for the in-process echo server.
func closeProcs(procs []*exec.Cmd) {
	var real []*exec.Cmd
	for _, p := range procs {
		if p != nil {
			real = append(real, p)
		}
	}
	if len(real) > 0 {
		CloseAllServers(real)
	}
}

// ---------------------------------------------------------------------------
// Weak-network TCP proxy: sits between the VLESS client outbound and the
// splithttp server. It forwards raw bytes while injecting one-way delay +
// delay jitter, an optional bandwidth cap, and (very low-probability) random
// connection resets to simulate an unsteady link. TLS/H2 bytes pass through
// untouched. This makes the client experience real lossy/high-RTT behavior so
// dseg pull retries and the upload seq-gap handling are exercised for real.
//
// Ordering contract (see runScenario): the proxy MUST be created AFTER the
// server is listening and BEFORE the client starts, so the client always has
// a live endpoint to dial.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Dual-end build: shared config then start server-only / client-only so a
// weak-net proxy can sit between them.
// ---------------------------------------------------------------------------

const pupSecret = "pup-shared-secret"

type dsegEndpoints struct {
	serverPort  net.Port
	clientPort  net.Port
	dest        net.Destination // local XOR echo target
	userID      *protocol.ID
	servers     []*exec.Cmd
	delayEchoMS int // >0 => delayed echo (weak-net)
}

func sharedPupConfig() *splithttp.Config {
	return &splithttp.Config{
		Path: "/xh-pup", Mode: "packet-up",
		Headers: map[string]string{
			splithttp.BraySessionSecretHeader: pupSecret,
			"x-bray-dseg":                     "1",
		},
	}
}

// netPortToU32 is a tiny helper for proto fields.
func netPortToU32(p net.Port) uint32 { return uint32(p) }

// startServerOnly brings up the local XOR echo + the VLESS XHTTP server
// (listening on serverPort). Registration in ep. Does not start the client.
func startServerOnly(t *testing.T, ep *dsegEndpoints) {
	t.Helper()
	mp := xor
	if ep.delayEchoMS > 0 {
		d := ep.delayEchoMS
		mp = func(b []byte) []byte {
			time.Sleep(time.Duration(d) * time.Millisecond)
			return xor(b)
		}
	}
	tcpSrv := tcp.Server{MsgProcessor: mp}
	dest, err := tcpSrv.Start()
	common.Must(err)
	ep.dest = dest
	ep.servers = append(ep.servers, nil) // tcp echo is in-process; cleaned separately
	t.Cleanup(func() { _ = tcpSrv.Close() })

	ep.userID = protocol.NewID(uuid.New())
	cfg := sharedPupConfig()
	serverConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList:       &net.PortList{Range: []*net.PortRange{net.SinglePortRange(ep.serverPort)}},
				Listen:         net.NewIPOrDomain(net.LocalHostIP),
				StreamSettings: dsegStreamConfig(cfg, nil),
			}),
			ProxySettings: serial.ToTypedMessage(&inbound.Config{Users: []*protocol.User{{
				Account: serial.ToTypedMessage(&vless.Account{Id: ep.userID.String()}),
			}}}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&freedom.Config{FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}}}),
		}},
	}
	proc := startProc(t, serverConfig)
	ep.servers[0] = proc
	waitPort(t, ep.serverPort)
	t.Cleanup(func() { closeProcs(ep.servers) })
}

// startClientOnly starts the client VLESS outbound beating on clientPort,
// dialing its outbound at peerHost:peerPort (normally serverPort, or the
// weak-net proxy's address when one exists). Register in ep.
func startClientOnly(t *testing.T, ep *dsegEndpoints, peerHost string, peerPort net.Port) {
	t.Helper()
	ep.clientPort = tcp.PickPort()
	cfg := sharedPupConfig()
	clientConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(ep.clientPort)}},
				Listen:   net.NewIPOrDomain(net.LocalHostIP),
			}),
			ProxySettings: serial.ToTypedMessage(&dokodemo.Config{
				RewriteAddress:  net.NewIPOrDomain(ep.dest.Address),
				RewritePort:     uint32(ep.dest.Port),
				AllowedNetworks: []net.Network{net.Network_TCP},
			}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&outbound.Config{
				Vnext: &protocol.ServerEndpoint{
					Address: net.NewIPOrDomain(net.ParseAddress(peerHost)), Port: netPortToU32(peerPort),
					User: &protocol.User{Account: serial.ToTypedMessage(&vless.Account{Id: ep.userID.String()})},
				},
			}),
			SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
				StreamSettings: dsegStreamConfig(cfg, nil),
			}),
		}},
	}
	proc := startProc(t, clientConfig)
	if len(ep.servers) == 1 && ep.servers[0] == nil {
		ep.servers[0] = proc
	} else {
		ep.servers = append(ep.servers, proc)
	}
	waitPort(t, ep.clientPort)
	// The server-only runner already registered the closing cleanup on
	// ep.servers (a closure), which will include this client proc.
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

// runScenario is the ordered runner: delayed-XOR echo server -> VLESS XHTTP
// server -> client. weakNetDelayMs adds a fixed one-way delay in the echo
// target's MsgProcessor, modelling a high-RTT link: every request/response
// round-trip (including packet-up POSTs and dseg segment pulls) pays the
// delay, which is exactly what stresses the upload seq-gap window and the
// adaptive downlink window without TCP-proxy timing hazards.
func runScenario(t *testing.T, weakNetDelayMs int, fn func(*testing.T, net.Port) error) {
	t.Helper()
	ep := &dsegEndpoints{serverPort: tcp.PickPort()}
	// Weak-net: model a high-RTT link by delaying the echo target.
	ep.delayEchoMS = weakNetDelayMs
	startServerOnly(t, ep)
	startClientOnly(t, ep, "127.0.0.1", ep.serverPort)

	if err := fn(t, ep.clientPort); err != nil {
		t.Fatalf("scenario failed: %v", err)
	}
}

func splitHostPort(addr string) (net.Address, net.Port) {
	host, portStr, _ := stdnet.SplitHostPort(addr)
	port, _ := strconv.Atoi(portStr)
	return net.ParseAddress(host), net.Port(port)
}

func hostOf(a net.Address) string { return a.String() }

// ---------------------------------------------------------------------------
// Workloads
// ---------------------------------------------------------------------------

// dialClient opens a raw TCP conn to the client inbound (socks-side).
func dialClient(clientPort net.Port) (stdnet.Conn, error) {
	return stdnet.Dial("tcp", "127.0.0.1:"+strconv.Itoa(int(clientPort)))
}

// echoOnce writes size bytes and verifies the XOR-echoed copy. Bounded so a
// wedged weak-net conn fails fast.
func echoOnce(conn stdnet.Conn, size int) error {
	if err := conn.SetDeadline(time.Now().Add(60 * time.Second)); err != nil {
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

func workloadWeb(t *testing.T, clientPort net.Port) error {
	var wg errgroup.Group
	for range 8 {
		wg.Go(func() error {
			for j := 0; j < 4; j++ {
				conn, err := dialClient(clientPort)
				if err != nil {
					return err
				}
				for _, size := range []int{2 << 10, 16 << 10, 64 << 10, 256 << 10} {
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
				if err := echoOnce(conn, 1<<20); err != nil {
					return err
				}
				if j%4 == 3 {
					time.Sleep(30 * time.Millisecond)
				}
			}
			return nil
		})
	}
	return wg.Wait()
}

func workloadFile(t *testing.T, clientPort net.Port) error {
	conn, err := dialClient(clientPort)
	if err != nil {
		return err
	}
	defer conn.Close()
	for j := 0; j < 16; j++ { // 16 x 4 MiB = 64 MiB
		if err := echoOnce(conn, 4<<20); err != nil {
			return err
		}
	}
	return nil
}

func workloadLargeFile(t *testing.T, clientPort net.Port) error {
	conn, err := dialClient(clientPort)
	if err != nil {
		return err
	}
	defer conn.Close()
	for j := 0; j < 128; j++ { // 128 x 4 MiB = 512 MiB
		if err := echoOnce(conn, 4<<20); err != nil {
			return err
		}
	}
	return nil
}

func workloadMultiThreadFile(t *testing.T, clientPort net.Port) error {
	var wg errgroup.Group
	for i := 0; i < 8; i++ {
		wg.Go(func() error {
			conn, err := dialClient(clientPort)
			if err != nil {
				return err
			}
			defer conn.Close()
			for j := 0; j < 10; j++ { // 8 x 20 MiB
				if err := echoOnce(conn, 2<<20); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return wg.Wait()
}

func workloadWeakNet(t *testing.T, clientPort net.Port) error {
	conn, err := dialClient(clientPort)
	if err != nil {
		return err
	}
	defer conn.Close()
	for j := 0; j < 12; j++ { // 12 MiB over weak link
		if err := echoOnce(conn, 1<<20); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

func TestVLESSXHTTP_WebScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scenario in short mode")
	}
	runScenario(t, 0, workloadWeb)
}

func TestVLESSXHTTP_VideoScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scenario in short mode")
	}
	runScenario(t, 0, workloadVideo)
}

func TestVLESSXHTTP_FileDownloadScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scenario in short mode")
	}
	runScenario(t, 0, workloadFile)
}

func TestVLESSXHTTP_LargeFileDownloadScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scenario in short mode")
	}
	runScenario(t, 0, workloadLargeFile)
}

func TestVLESSXHTTP_MultiThreadFileDownloadScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scenario in short mode")
	}
	runScenario(t, 0, workloadMultiThreadFile)
}

func TestVLESSXHTTP_WeakNetScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scenario in short mode")
	}
	// High-RTT + jitter + 10 Mbps cap, no forced resets (loss is retransmitted
	// by TCP and invisible; forced resets would simulate disconnects). This
	// exercises the upload seq-gap window and the adaptive downlink window on
	// a real constricted path. 30 MiB total.
	runScenario(t, 40, workloadWeakNet)
}

// --- exported fast smoke harness (keep) ---

func TestVlessTLSPacketUpDsegPlain(t *testing.T) {
	ep := &dsegEndpoints{serverPort: tcp.PickPort()}
	startServerOnly(t, ep)
	startClientOnly(t, ep, "127.0.0.1", ep.serverPort)
	var wg errgroup.Group
	for range 3 {
		wg.Go(func() error { return echoOnce(mustConn(ep.clientPort), 256*1024) })
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
