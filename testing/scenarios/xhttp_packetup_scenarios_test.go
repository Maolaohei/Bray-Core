package scenarios

// Fast iteration harness for the packet-up + dseg user report. Uses a
// self-signed TLS/H2 dual-end by default so the H2/H3 dseg gate is genuinely
// exercised (not silently routed through the plaintext/H1 legacy fallback).
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
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

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
	itls "github.com/xtls/xray-core/transport/internet/tls"
)

// startProc launches one Xray test process from a protobuf config, returning
// the running *exec.Cmd. The child process takes a moment to bind its
// listener, so callers must wait for the port before dialing.
func startProc(tb testing.TB, cfg *core.Config) *exec.Cmd {
	tb.Helper()
	proc, err := InitializeServerConfig(cfg)
	common.Must(err)
	return proc
}

// waitPort blocks until a TCP listener accepts on 127.0.0.1:port or ~5s pass
// (a child xray process that takes a moment to bind).
func waitPort(tb testing.TB, port net.Port) {
	tb.Helper()
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
	tb.Fatalf("listener %s did not come up", addr)
}

// closeProcs closes the xray test processes.
func closeProcs(procs []*exec.Cmd) {
	if len(procs) > 0 {
		CloseAllServers(procs)
	}
}

// ---------------------------------------------------------------------------
// Deterministic weak-link TCP proxy: sits between the VLESS client outbound
// and the XHTTP server, forwarding TLS/H2 bytes untouched while applying a
// one-way startup delay and paced bandwidth in BOTH directions. Unlike the
// old delayed-XOR-target test, this actually constrains the client↔server
// packet-up transport (POSTs, GET segment pulls, H2 flow control, retries).
//
// It intentionally does NOT "drop bytes": loss inside a TCP stream is byte
// corruption, not IP packet loss/retransmission. Physical loss validation is
// a separate privileged Linux netem lane; this deterministic proxy is the
// portable PR/release gate for latency + bandwidth pressure.
// ---------------------------------------------------------------------------

type weakLinkProfile struct {
	// One-way delay applied before the first bytes in each direction. Two
	// directions give a reproducible request/response RTT component.
	oneWayDelay time.Duration
	// bytesPerSecond limits each direction independently. Zero = unlimited.
	bytesPerSecond int64
}

// startWeakLinkProxy starts a raw TCP forwarder to upstreamPort. It returns
// its listening port and registers cleanup on tb. The server MUST already be
// listening before this is called; the client starts only after this proxy is
// ready, so no bind/order race exists.
func startWeakLinkProxy(tb testing.TB, upstreamPort net.Port, profile weakLinkProfile) net.Port {
	tb.Helper()
	ln, err := stdnet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	var wg sync.WaitGroup
	var conns sync.Map // stdnet.Conn -> struct{}, active client+upstream sockets
	acceptDone := make(chan struct{})
	upstream := "127.0.0.1:" + strconv.Itoa(int(upstreamPort))
	go func() {
		defer close(acceptDone)
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			conns.Store(client, struct{}{})
			wg.Add(1)
			go func() {
				defer wg.Done()
				serveWeakLink(client, upstream, profile, &conns)
			}()
		}
	}()
	tb.Cleanup(func() {
		_ = ln.Close()
		// No accept goroutine may add a socket after acceptDone. Wait for it
		// before ranging, otherwise a connection accepted concurrently with
		// listener close can miss the snapshot and leave wg.Wait hung.
		<-acceptDone
		// Closing only the listener is insufficient: a live TLS/H2 stream
		// can sit idle forever in a proxy pump. Close every tracked socket
		// first, which wakes both pump Read calls, then wait for cleanup.
		conns.Range(func(key, _ any) bool {
			_ = key.(stdnet.Conn).Close()
			return true
		})
		wg.Wait()
	})
	return net.Port(ln.Addr().(*stdnet.TCPAddr).Port)
}

func serveWeakLink(client stdnet.Conn, upstreamAddr string, profile weakLinkProfile, conns *sync.Map) {
	defer conns.Delete(client)
	upstream, err := stdnet.Dial("tcp", upstreamAddr)
	if err != nil {
		_ = client.Close()
		return
	}
	conns.Store(upstream, struct{}{})
	defer conns.Delete(upstream)
	defer client.Close()
	defer upstream.Close()

	done := make(chan struct{}, 2)
	go func() { weakLinkPump(upstream, client, profile); done <- struct{}{} }()
	go func() { weakLinkPump(client, upstream, profile); done <- struct{}{} }()
	// First direction ending means the logical link ends; closing both wakes
	// the counterpart out of Read without leaking a proxy goroutine.
	<-done
	_ = client.Close()
	_ = upstream.Close()
	<-done
}

func weakLinkPump(dst, src stdnet.Conn, profile weakLinkProfile) {
	buf := make([]byte, 32<<10)
	first := true
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if first && profile.oneWayDelay > 0 {
				time.Sleep(profile.oneWayDelay)
			}
			first = false
			if profile.bytesPerSecond > 0 {
				time.Sleep(time.Duration(int64(n) * int64(time.Second) / profile.bytesPerSecond))
			}
			for off := 0; off < n; {
				w, werr := dst.Write(buf[off:n])
				off += w
				if werr != nil {
					return
				}
			}
		}
		if err != nil {
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Dual-end build: shared config then start server-only / client-only so a
// weak-net proxy can sit between them.
// ---------------------------------------------------------------------------

const pupSecret = "pup-shared-secret"

type dsegEndpoints struct {
	serverPort net.Port
	clientPort net.Port
	dest       net.Destination // local upstream target (XOR echo by default)
	userID     *protocol.ID
	servers    []*exec.Cmd // real xray test processes only
	// delayEchoMS > 0 makes the default echo target add that one-way delay
	// (weak-net simulation).
	delayEchoMS int
	// destOverride, when Address is non-nil, replaces the default XOR echo
	// upstream with a caller-provided downstream (e.g. a data-push server for
	// downlink-throughput benchmarking). startServerOnly skips creating the
	// echo server in that case.
	destOverride net.Destination
	// echoCleanup closes the in-process XOR echo server (if started).
	echoCleanup func()
	// dsegDisable turns off downlink segmentation on BOTH ends (header
	// x-bray-dseg:0) so the legacy long-GET download leg is exercised — the
	// clean A/B baseline for "new packet-up (dseg) vs legacy packet-up".
	dsegDisable bool
	// mode overrides the XHTTP wire mode on BOTH ends (default "packet-up"
	// when empty). Stream-one / stream-up / packet-up are exercised via the
	// same real dual-end push link for cross-mode downlink comparison.
	mode string
	// maxBufferedPosts overrides the packet-up reorder capacity on BOTH test
	// ends. Zero preserves the production default (64); a tiny value forces
	// the full-queue path while keeping the client in-flight window capped to
	// half the advertised reorder capacity (the protocol safety invariant).
	maxBufferedPosts int64
	// forcePlain leaves the dual-end on H1 plaintext for the specifically
	// named plain smoke case. All other scenarios default to TLS/H2 so the
	// H2/H3 gate actually exercises dseg (plaintext otherwise falls back to
	// legacy long-GET by design).
	forcePlain bool
	serverTLS  []*serial.TypedMessage
	clientTLS  []*serial.TypedMessage
}

func sharedPupConfig() *splithttp.Config {
	return sharedPupConfigMode("1", "packet-up")
}

// sharedPupConfigFor picks the packet-up config per the endpoint's dseg
// setting: dsegDisable selects the legacy long-GET downlink for A/B.
func sharedPupConfigFor(ep *dsegEndpoints) *splithttp.Config {
	dseg := "1"
	if ep != nil && ep.dsegDisable {
		dseg = "0"
	}
	mode := "packet-up"
	if ep != nil && ep.mode != "" {
		mode = ep.mode
	}
	cfg := sharedPupConfigMode(dseg, mode)
	if ep != nil && ep.maxBufferedPosts > 0 {
		cfg.ScMaxBufferedPosts = ep.maxBufferedPosts
	}
	return cfg
}

// sharedPupConfigMode returns the XHTTP config for the given dseg value
// ("1" = downlink segmentation ON, "0"/"false" = legacy long-GET downlink
// under packet-up) and wire mode (packet-up / stream-up / stream-one).
// The harness defaults to dseg-on packet-up; dsegDisable/mode on
// dsegEndpoints switch for A/B and cross-mode benchmarking.
func sharedPupConfigMode(dseg, mode string) *splithttp.Config {
	return &splithttp.Config{
		Path: "/xh-pup", Mode: mode,
		Headers: map[string]string{
			splithttp.BraySessionSecretHeader: pupSecret,
			"x-bray-dseg":                     dseg,
		},
	}
}

// ensureScenarioTLS creates a paired self-signed TLS configuration once per
// endpoint. TLS makes decideHTTPVersion select H2, which is essential after
// the dseg H2/H3 gate: a plaintext harness silently tests the legacy long-GET
// fallback rather than the new segmented packet-up path.
func ensureScenarioTLS(tb testing.TB, ep *dsegEndpoints) {
	tb.Helper()
	if ep.forcePlain || ep.serverTLS != nil {
		return
	}
	ct, ctHash := cert.MustGenerate(nil, cert.CommonName("localhost"))
	ep.serverTLS = []*serial.TypedMessage{serial.ToTypedMessage(&itls.Config{
		Certificate: []*itls.Certificate{itls.ParseCertificate(ct)},
	})}
	ep.clientTLS = []*serial.TypedMessage{serial.ToTypedMessage(&itls.Config{
		PinnedPeerCertSha256: [][]byte{ctHash[:]},
	})}
}

func dsegStreamConfig(cfg *splithttp.Config, security []*serial.TypedMessage) *internet.StreamConfig {
	sc := &internet.StreamConfig{
		ProtocolName: "splithttp",
		TransportSettings: []*internet.TransportConfig{{
			ProtocolName: "splithttp",
			Settings:     serial.ToTypedMessage(cfg),
		}},
	}
	if len(security) > 0 {
		sc.SecurityType = serial.GetMessageType(&itls.Config{})
		sc.SecuritySettings = security
	}
	return sc
}

// netPortToU32 is a tiny helper for proto fields.
func netPortToU32(p net.Port) uint32 { return uint32(p) }

// startServerOnly brings up the upstream target + the VLESS XHTTP server
// (listening on serverPort). The upstream is the caller-provided
// destOverride when set (e.g. a data-push server for downlink benchmarking),
// else the in-process XOR echo. Registration in ep. Does not start client.
func startServerOnly(tb testing.TB, ep *dsegEndpoints) {
	tb.Helper()
	if ep.destOverride.Address != nil {
		// Caller supplied a custom downstream (push server): use it directly.
		ep.dest = ep.destOverride
	} else {
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
		ep.echoCleanup = func() { _ = tcpSrv.Close() }
		tb.Cleanup(ep.echoCleanup)
	}

	ep.userID = protocol.NewID(uuid.New())
	ensureScenarioTLS(tb, ep)
	cfg := sharedPupConfigFor(ep)
	serverConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList:       &net.PortList{Range: []*net.PortRange{net.SinglePortRange(ep.serverPort)}},
				Listen:         net.NewIPOrDomain(net.LocalHostIP),
				StreamSettings: dsegStreamConfig(cfg, ep.serverTLS),
			}),
			ProxySettings: serial.ToTypedMessage(&inbound.Config{Users: []*protocol.User{{
				Account: serial.ToTypedMessage(&vless.Account{Id: ep.userID.String()}),
			}}}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&freedom.Config{FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}}}),
		}},
	}
	proc := startProc(tb, serverConfig)
	ep.servers = append(ep.servers, proc)
	waitPort(tb, ep.serverPort)
	tb.Cleanup(func() { closeProcs(ep.servers) })
}

// startClientOnly starts the client VLESS outbound beating on clientPort,
// dialing its outbound at peerHost:peerPort (normally serverPort, or the
// weak-net proxy's address when one exists). Register in ep.
func startClientOnly(tb testing.TB, ep *dsegEndpoints, peerHost string, peerPort net.Port) {
	tb.Helper()
	ep.clientPort = tcp.PickPort()
	cfg := sharedPupConfigFor(ep)
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
				StreamSettings: dsegStreamConfig(cfg, ep.clientTLS),
			}),
		}},
	}
	proc := startProc(tb, clientConfig)
	ep.servers = append(ep.servers, proc)
	waitPort(tb, ep.clientPort)
	// The server-only runner already registered the closing cleanup on
	// ep.servers (a closure), which will include this client proc.
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
	// Target delay is useful for deterministic application turnaround tests;
	// it does not model the XHTTP transport link (runWeakLinkScenario does).
	ep.delayEchoMS = weakNetDelayMs
	startServerOnly(t, ep)
	startClientOnly(t, ep, "127.0.0.1", ep.serverPort)

	if err := fn(t, ep.clientPort); err != nil {
		t.Fatalf("scenario failed: %v", err)
	}
}

// runWeakLinkScenario applies delay/bandwidth on the actual client↔server
// XHTTP link, after the server is listening and before the client starts.
// This is the portable deterministic weak-network gate; it uses TLS/H2 by
// default, therefore it really runs packet-up+dseg after the H2/H3 gate.
func runWeakLinkScenario(t *testing.T, profile weakLinkProfile, fn func(*testing.T, net.Port) error) {
	t.Helper()
	ep := &dsegEndpoints{serverPort: tcp.PickPort()}
	startServerOnly(t, ep)
	proxyPort := startWeakLinkProxy(t, ep.serverPort, profile)
	startClientOnly(t, ep, "127.0.0.1", proxyPort)
	if err := fn(t, ep.clientPort); err != nil {
		t.Fatalf("weak-link scenario failed: %v", err)
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

// workloadUploadBackpressure writes a sustained burst without waiting for
// each echo response while a concurrent reader drains the downstream. With a
// deliberately tiny server queue and a delayed target, this deterministically
// drives packet-up into the full-queue path: correct behavior is bounded
// server backpressure, then intact delivery — NOT HTTP 404/retry/thrash.
func workloadUploadBackpressure(t *testing.T, clientPort net.Port) error {
	conn, err := dialClient(clientPort)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(60 * time.Second)); err != nil {
		return err
	}
	defer conn.SetDeadline(time.Time{})

	const total = 8 << 20
	payload := make([]byte, total)
	if _, err := rand.Read(payload); err != nil {
		return err
	}
	got := make([]byte, total)

	var g errgroup.Group
	g.Go(func() error {
		for off := 0; off < len(payload); {
			n, err := conn.Write(payload[off:])
			off += n
			if err != nil {
				return err
			}
		}
		return nil
	})
	g.Go(func() error {
		if _, err := io.ReadFull(conn, got); err != nil {
			return err
		}
		for i, v := range got {
			if v != payload[i]^'c' {
				return &errMismatch{expect: total}
			}
		}
		return nil
	})
	return g.Wait()
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
	// 80ms cold-link latency (40ms first bytes per direction) plus 10Mbps
	// per-direction pacing on the real TLS/H2 client↔server path. This
	// deterministically exercises packet-up POST concurrency, dseg pulls and
	// bandwidth pressure on the actual constricted link. It is deliberately
	// not claimed to be physical per-packet loss/RTT emulation; that requires
	// the separate privileged Linux netem validation lane.
	runWeakLinkScenario(t, weakLinkProfile{
		oneWayDelay:    40 * time.Millisecond,
		bytesPerSecond: 10_000_000 / 8,
	}, workloadWeakNet)
}

// TestVLESSXHTTP_UploadBackpressureScenario forces the server queue down to
// two buffered packet-up posts while the target delays consumption. Before
// upload backpressure this produced queue-full -> HTTP 404 -> client retry
// storms; now the same burst must complete byte-perfectly by flowing back to
// the sender for at most uploadQueueBackpressureWait per transient stall.
func TestVLESSXHTTP_UploadBackpressureScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scenario in short mode")
	}
	ep := &dsegEndpoints{
		serverPort:       tcp.PickPort(),
		delayEchoMS:      2,
		maxBufferedPosts: 2,
	}
	startServerOnly(t, ep)
	startClientOnly(t, ep, "127.0.0.1", ep.serverPort)
	if err := workloadUploadBackpressure(t, ep.clientPort); err != nil {
		t.Fatalf("upload backpressure scenario failed: %v", err)
	}
}

// --- exported fast smoke harness (keep) ---

// TestVlessTLSPacketUpDsegPlain preserves the H1/plain fallback smoke path.
// After the H2/H3 dseg gate, forcePlain intentionally exercises legacy
// long-GET (not dseg); the TLS/H2 scenarios above are the dseg coverage.
func TestVlessTLSPacketUpDsegPlain(t *testing.T) {
	ep := &dsegEndpoints{serverPort: tcp.PickPort(), forcePlain: true}
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
