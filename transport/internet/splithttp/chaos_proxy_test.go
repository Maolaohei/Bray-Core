package splithttp_test

// End-to-end fault injection (checklist: 干扰对抗 + 稳定性):
// a hostile TCP proxy sits between client and server and injects
//   - connection RSTs (SO_LINGER 0 close mid-flight, both directions),
//   - added latency before the first byte of a connection,
// while the client runs a packet-up session with continuous traffic. The
// session must SURVIVE the fault budget: the transfer completes, the echo is
// byte-intact, and recovery happens through the product's own retry/rescue
// paths (postPacketReliable same-seq retry, fresh dial, dup-seq idempotency).
//
// Uses skipIfHostLoopbackHTTPRewrite: a host stack that rewrites HTTP makes
// fault attribution meaningless.

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport/internet"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/stat"
)

type chaosProxy struct {
	fwdCount atomic.Int32
	rstCount atomic.Int32
	// injectRST=false turns the proxy into a transparent forwarder (used to
	// attribute failures: corruption without faults means the host stack, not
	// the chaos injection).
	injectRST bool
	// firstByteDelay models path RTT: each direction sleeps this long before
	// its first forwarded byte (not per chunk — that would model bandwidth
	// delay and pile up over hundreds of chunks).
	firstByteDelay time.Duration
}

// delayConn sleeps once before the first successful Read.
type delayConn struct {
	net.Conn
	delay     time.Duration
	delayOnce sync.Once
}

func (c *delayConn) Read(p []byte) (int, error) {
	c.delayOnce.Do(func() { time.Sleep(c.delay) })
	return c.Conn.Read(p)
}

// handle forwards one client conn to the server, injecting an RST on every
// other connection after 50-350ms of forwarding.
func (p *chaosProxy) handle(client net.Conn, serverAddr string) {
	defer client.Close()
	upstream, err := net.Dial("tcp", serverAddr)
	if err != nil {
		return
	}
	defer upstream.Close()
	if p.firstByteDelay > 0 {
		client = &delayConn{Conn: client, delay: p.firstByteDelay}
		upstream = &delayConn{Conn: upstream, delay: p.firstByteDelay}
	}
	n := p.fwdCount.Add(1)

	var rst <-chan time.Time
	if p.injectRST && n%2 == 0 {
		p.rstCount.Add(1)
		if tc, ok := client.(*net.TCPConn); ok {
			tc.SetLinger(0) // close => RST, not FIN
		}
		if tc, ok := upstream.(*net.TCPConn); ok {
			tc.SetLinger(0)
		}
		rst = time.After(time.Duration(50+rand.Int63n(300)) * time.Millisecond)
	}

	done := make(chan struct{}, 2)
	cp := func(dst, src net.Conn) {
		io.Copy(dst, src)
		done <- struct{}{}
	}
	go cp(upstream, client)
	go cp(client, upstream)

	select {
	case <-done:
	case <-rst:
		client.Close()
		upstream.Close()
		<-done
		<-done
	}
}

func TestFaultInjectE2E_ChaosProxySessionSurvives(t *testing.T) {
	runChaosProxyTest(t, false)
}

// TestFaultInjectE2E_TransparentProxy is the attribution control: identical
// path, zero faults. If THIS corrupts on a given host, the host stack is
// rewriting HTTP (see host_http_sanity_test.go) and chaos results there are
// meaningless.
func TestFaultInjectE2E_TransparentProxy(t *testing.T) {
	runChaosProxyTest(t, true)
}

func runChaosProxyTest(t *testing.T, transparent bool) {
	runChaosProxyTestFull(t, transparent, 0)
}

// TestScenario_HighRTTPlusReset collapses the 性能/稳定性 scenario: ~100ms
// RTT (50ms per direction) plus the usual every-other-connection reset
// budget. The session must survive with intact echo — the product's retry,
// rescue and dup-seq idempotency paths absorb both.
func TestScenario_HighRTTPlusReset(t *testing.T) {
	if testing.Short() {
		t.Skip("high-RTT scenario skipped under -short")
	}
	runChaosProxyTestFull(t, false, 50*time.Millisecond)
}

func runChaosProxyTestFull(t *testing.T, transparent bool, rttHalf time.Duration) {
	if testing.Short() {
		t.Skip("fault injection e2e skipped under -short")
	}
	// Bind to a wire-clean IP: loopback when possible, else the first clean
	// LAN IPv4 (Huorong's callout covers loopback only). Traffic stays
	// on-host either way.
	bindIP := testBindIP(t)

	serverPort := tcp.PickPort()
	settings := &internet.MemoryStreamConfig{
		ProtocolName: "splithttp",
		ProtocolSettings: &Config{
			Path:    "/sh",
			Mode:    "packet-up",
			Headers: map[string]string{BraySessionSecretHeader: "chaos-secret"},
		},
	}
	listen, err := ListenXH(context.Background(), xnet.ParseAddress(bindIP), serverPort, settings, func(conn stat.Connection) {
		go func(c stat.Connection) {
			defer c.Close()
			buf.Copy(buf.NewReader(c), buf.NewWriter(c))
		}(conn)
	})
	common.Must(err)
	defer listen.Close()
	serverAddr := fmt.Sprintf("%s:%d", bindIP, int(serverPort))

	proxyPort := tcp.PickPort()
	proxy := &chaosProxy{injectRST: !transparent, firstByteDelay: rttHalf}
	ln, err := net.Listen("tcp", net.JoinHostPort(bindIP, fmt.Sprint(int(proxyPort))))
	common.Must(err)
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go proxy.handle(c, serverAddr)
		}
	}()

	dest := xnet.TCPDestination(xnet.ParseAddress(bindIP), proxyPort)
	conn, err := Dial(context.Background(), dest, settings)
	common.Must(err)
	defer conn.Close()

	payload := make([]byte, 64*1024)
	crand.Read(payload)
	const n = 12
	total := n * len(payload)

	errCh := make(chan error, 1)
	go func() {
		for i := 0; i < n; i++ {
			if _, werr := conn.Write(payload); werr != nil {
				errCh <- fmt.Errorf("write %d: %w", i, werr)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	readBuf := make([]byte, total)
	_, rerr := io.ReadFull(conn, readBuf)
	select {
	case werr := <-errCh:
		t.Fatalf("client write died under chaos (rst=%d fwd=%d): %v",
			proxy.rstCount.Load(), proxy.fwdCount.Load(), werr)
	default:
	}
	if rerr != nil {
		t.Fatalf("echo read died under chaos (rst=%d fwd=%d): %v",
			proxy.rstCount.Load(), proxy.fwdCount.Load(), rerr)
	}
	for i := 0; i < n; i++ {
		if string(readBuf[i*len(payload):(i+1)*len(payload)]) != string(payload) {
			if transparent {
				t.Fatalf("echo corruption at copy %d with a ZERO-fault proxy: "+
					"the host stack is rewriting HTTP (see host_http_sanity_test.go); "+
					"chaos results on this host cannot be attributed to injected faults", i)
			}
			t.Fatalf("echo corruption at copy %d under chaos (rst=%d fwd=%d)",
				i, proxy.rstCount.Load(), proxy.fwdCount.Load())
		}
	}
	t.Logf("chaos proxy: fwd=%d rst=%d — session survived with intact echo",
		proxy.fwdCount.Load(), proxy.rstCount.Load())
}
