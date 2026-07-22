package xmux_e2e_test

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/app/log"
	"github.com/xtls/xray-core/app/proxyman"
	_ "github.com/xtls/xray-core/app/proxyman/inbound"
	_ "github.com/xtls/xray-core/app/proxyman/outbound"
	clog "github.com/xtls/xray-core/common/log"
	xraynet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	core "github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/proxy/freedom"
	httpproxy "github.com/xtls/xray-core/proxy/http"
	"github.com/xtls/xray-core/proxy/vless"
	"github.com/xtls/xray-core/proxy/vless/inbound"
	"github.com/xtls/xray-core/proxy/vless/outbound"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/reality"
	"github.com/xtls/xray-core/transport/internet/splithttp"
)

var (
	testPrivKey, _ = base64.RawURLEncoding.DecodeString("aGSYystUbf59_9_6LKRxD27rmSW_-2_nyd9YG_Gwbks")
	testPubKey, _  = base64.RawURLEncoding.DecodeString("E59WjnvZcQMu7tR7_BgyhycuEdBS-CtKxfImRCdAvFM")
)

// TestXMUXCrossDomainViaProxy tests XMUX with a full proxy chain:
//   HTTP client → Xray client (XMUX) → Xray server → HTTP test servers
//
// Two test HTTP servers return different bodies.
// If XMUX incorrectly shares connections across domains,
// responses will be mixed up.
func TestXMUXCrossDomainViaProxy(t *testing.T) {
	// Step 1: Start two HTTP test servers
	siteA := startHTTPTestServer(t, "site-a", "A", 16081)
	siteB := startHTTPTestServer(t, "site-b", "B", 16082)
	defer siteA.Close()
	defer siteB.Close()

	// Step 2: Build proxy chain with XMUX
	serverPort := xraynet.Port(16090)
	clientPort := xraynet.Port(16091)
	userID := "12345678-1234-1234-1234-123456789abc"

	shortIds := make([][]byte, 1)
	shortIds[0] = make([]byte, 8)
	hex.Decode(shortIds[0], []byte("0123456789abcdef"))

	// Server config: splithttp inbound (XMUX) + freedom outbound
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
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: "/xmux-test"}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show:        true,
						Dest:        "www.microsoft.com:443",
						ServerNames: []string{"www.microsoft.com"},
						PrivateKey:  testPrivKey,
						ShortIds:    shortIds,
						Type:        "tcp",
					})},
				},
			}),
			ProxySettings: serial.ToTypedMessage(&inbound.Config{
				Users: []*protocol.User{{
					Account: serial.ToTypedMessage(&vless.Account{Id: userID}),
				}},
			}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&freedom.Config{
				FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}},
			}),
		}},
	}

	// Client config: HTTP proxy inbound + splithttp outbound with XMUX
	clientConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(clientPort)}},
				Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
			}),
			ProxySettings: serial.ToTypedMessage(&httpproxy.ServerConfig{}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&outbound.Config{
				Vnext: &protocol.ServerEndpoint{
					Address: xraynet.NewIPOrDomain(xraynet.LocalHostIP),
					Port:    uint32(serverPort),
					User: &protocol.User{
						Account: serial.ToTypedMessage(&vless.Account{Id: userID}),
					},
				},
			}),
			SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings: serial.ToTypedMessage(&splithttp.Config{
							Path: "/xmux-test",
							Xmux: &splithttp.XmuxConfig{
								MaxConcurrency: &splithttp.RangeConfig{From: 16, To: 32},
								MaxConnections: &splithttp.RangeConfig{From: 4, To: 8},
							},
						}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show:        true,
						Fingerprint: "chrome",
						ServerName:  "www.microsoft.com",
						PublicKey:    testPubKey,
						ShortId:     shortIds[0],
						SpiderX:     "/",
					})},
				},
			}),
		}},
	}

	servers, err := initializeServers(serverConfig, clientConfig)
	if err != nil {
		t.Fatalf("failed to initialize servers: %v", err)
	}
	defer closeServers(servers)

	// Step 3: Send requests through the proxy to both sites
	var wrongBody atomic.Int32
	var success atomic.Int32
	var connectErrors atomic.Int32

	for round := 0; round < 50; round++ {
		var wg sync.WaitGroup

		// Request to site A via proxy
		wg.Add(1)
		go func() {
			defer wg.Done()
			body, err := httpGetViaProxy(t, clientPort, "127.0.0.1:16081", 5*time.Second)
			if err != nil {
				connectErrors.Add(1)
				t.Logf("round %d site-a error: %v", round, err)
				return
			}
			if body != "A" {
				wrongBody.Add(1)
				t.Errorf("WRONG BODY round %d site-a: got %q expected %q", round, body, "A")
			} else {
				success.Add(1)
			}
		}()

		// Request to site B via proxy
		wg.Add(1)
		go func() {
			defer wg.Done()
			body, err := httpGetViaProxy(t, clientPort, "127.0.0.1:16082", 5*time.Second)
			if err != nil {
				connectErrors.Add(1)
				t.Logf("round %d site-b error: %v", round, err)
				return
			}
			if body != "B" {
				wrongBody.Add(1)
				t.Errorf("WRONG BODY round %d site-b: got %q expected %q", round, body, "B")
			} else {
				success.Add(1)
			}
		}()

		wg.Wait()
	}

	t.Logf("XMUX proxy test: success=%d wrong_body=%d connect_errors=%d",
		success.Load(), wrongBody.Load(), connectErrors.Load())

	if wrongBody.Load() > 0 {
		t.Fatalf("CROSS-DOMAIN REUSE DETECTED: %d requests got wrong body", wrongBody.Load())
	}
}

// startHTTPTestServer starts a simple HTTP server that returns a fixed body.
func startHTTPTestServer(t *testing.T, name, body string, port int) *http.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Site", name)
		w.Header().Set("X-Host", r.Host)
		fmt.Fprint(w, body)
	})
	server := &http.Server{
		Addr:    fmt.Sprintf("127.0.0.1:%d", port),
		Handler: mux,
	}
	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		t.Fatalf("listen %s: %v", server.Addr, err)
	}
	go func() {
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			t.Logf("server %s: %v", name, err)
		}
	}()
	return server
}

// httpGetViaProxy makes an HTTP GET request through the Xray HTTP proxy.
func httpGetViaProxy(t *testing.T, proxyPort xraynet.Port, targetAddr string, timeout time.Duration) (string, error) {
	t.Helper()

	// Connect to the Xray HTTP proxy
	proxyConn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", proxyPort), timeout)
	if err != nil {
		return "", fmt.Errorf("dial proxy: %w", err)
	}
	defer proxyConn.Close()
	proxyConn.SetDeadline(time.Now().Add(timeout))

	// Send CONNECT request
	connectReq := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", targetAddr, targetAddr)
	_, err = proxyConn.Write([]byte(connectReq))
	if err != nil {
		return "", fmt.Errorf("write CONNECT: %w", err)
	}

	// Read CONNECT response
	buf := make([]byte, 4096)
	n, err := proxyConn.Read(buf)
	if err != nil {
		return "", fmt.Errorf("read CONNECT response: %w", err)
	}
	connectResp := string(buf[:n])
	if !strings.Contains(connectResp, "200") {
		return "", fmt.Errorf("CONNECT failed: %s", connectResp)
	}

	// Send HTTP GET request through the tunnel
	getReq := fmt.Sprintf("GET / HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", targetAddr)
	_, err = proxyConn.Write([]byte(getReq))
	if err != nil {
		return "", fmt.Errorf("write GET: %w", err)
	}

	// Read response
	var response strings.Builder
	for {
		n, err := proxyConn.Read(buf)
		if n > 0 {
			response.Write(buf[:n])
		}
		if err != nil {
			break
		}
	}

	respStr := response.String()
	parts := strings.SplitN(respStr, "\r\n\r\n", 2)
	if len(parts) < 2 {
		return "", fmt.Errorf("no response body: %s", respStr)
	}
	return strings.TrimSpace(parts[1]), nil
}

// initializeServers starts Xray server processes from configs.
func initializeServers(configs ...*core.Config) ([]*core.Instance, error) {
	var servers []*core.Instance
	for _, config := range configs {
		// Add required default apps
		config.App = append(config.App, serial.ToTypedMessage(&dispatcher.Config{}))
		config.App = append(config.App, serial.ToTypedMessage(&proxyman.InboundConfig{}))
		config.App = append(config.App, serial.ToTypedMessage(&proxyman.OutboundConfig{}))

		server, err := core.New(config)
		if err != nil {
			for _, s := range servers {
				s.Close()
			}
			return nil, err
		}
		if err := server.Start(); err != nil {
			for _, s := range servers {
				s.Close()
			}
			return nil, err
		}
		servers = append(servers, server)
	}
	// Wait for servers to start
	time.Sleep(2 * time.Second)
	return servers, nil
}

// closeServers stops all Xray server processes.
func closeServers(servers []*core.Instance) {
	for _, server := range servers {
		server.Close()
	}
}
