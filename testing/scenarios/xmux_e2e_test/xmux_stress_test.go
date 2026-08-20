package xmux_e2e_test

import (
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// startHTTPStressSites starts N plain HTTP test servers on OS-assigned ports.
// Fixed ports collided with unrelated local applications (e.g. steam.exe on
// 27060), making full test results depend on the developer desktop instead of
// XMUX correctness.
func startHTTPStressSites(t *testing.T, count int) []*stressSite {
	t.Helper()
	sites := make([]*stressSite, count)
	for i := 0; i < count; i++ {
		site := &stressSite{
			Name: fmt.Sprintf("site-%d", i),
			Body: string(rune('A' + i)),
		}
		mux := http.NewServeMux()
		body := site.Body
		name := site.Name
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Site", name)
			fmt.Fprint(w, body)
		})
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen stress site: %v", err)
		}
		server := &http.Server{Handler: mux}
		site.addr = ln.Addr().String()
		go func() {
			if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
				t.Logf("server %s: %v", name, err)
			}
		}()
		site.server = server
		sites[i] = site
	}
	return sites
}

type stressSite struct {
	Name   string
	Body   string
	addr   string
	server *http.Server
}

// buildStressProxy starts Xray server+client and returns cleanup. Each stress
// case receives a fresh XMUX global pool; otherwise an old client can retain a
// closed fixed-port peer across test cases.
func buildStressProxy(t *testing.T, serverPort, clientPort xraynet.Port) func() {
	t.Helper()
	splithttp.ResetGlobalDialer()
	userID := "12345678-1234-1234-1234-123456789abc"
	shortIds := make([][]byte, 1)
	shortIds[0] = make([]byte, 8)
	hex.Decode(shortIds[0], []byte("0123456789abcdef"))

	serverConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Error, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(serverPort)}},
				Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: "/stress"}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Dest: "www.microsoft.com:443",
						ServerNames: []string{"www.microsoft.com"},
						PrivateKey:  testPrivKey, ShortIds: shortIds, Type: "tcp",
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

	clientConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Error, ErrorLogType: log.LogType_Console}),
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
							Path: "/stress",
							Xmux: &splithttp.XmuxConfig{
								MaxConcurrency: &splithttp.RangeConfig{From: 32, To: 64},
								MaxConnections: &splithttp.RangeConfig{From: 8, To: 16},
							},
						}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Fingerprint: "chrome", ServerName: "www.microsoft.com",
						PublicKey: testPubKey, ShortId: shortIds[0], SpiderX: "/",
					})},
				},
			}),
		}},
	}

	servers, err := initializeServers(serverConfig, clientConfig)
	if err != nil {
		t.Fatalf("failed to initialize: %v", err)
	}
	return func() { closeServers(servers) }
}

// TestXMUXStressHighConcurrency: 100 goroutines × 20 requests = 2000 total.
func TestXMUXStressHighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("long stress suite: skipped in -short mode (CI)")
	}
	sites := startHTTPStressSites(t, 5)
	for _, s := range sites {
		defer s.server.Close()
	}
	cleanup := buildStressProxy(t, 27070, 27071)
	defer cleanup()
	time.Sleep(3 * time.Second)

	var wrongBody atomic.Int32
	var success atomic.Int32
	var connErrors atomic.Int32
	var wg sync.WaitGroup

	for g := 0; g < 100; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				idx := (gid*20 + i) % len(sites)
				site := sites[idx]
				body, err := httpGetViaProxy(t, 27071, site.addr, 5*time.Second)
				if err != nil {
					connErrors.Add(1)
					return
				}
				if body != site.Body {
					wrongBody.Add(1)
					t.Errorf("WRONG g=%d r=%d site=%s: got %q expected %q", gid, i, site.Name, body, site.Body)
				}
				success.Add(1)
			}
		}(g)
	}

	wg.Wait()
	t.Logf("HighConcurrency: success=%d wrong=%d errors=%d", success.Load(), wrongBody.Load(), connErrors.Load())
	if wrongBody.Load() > 0 {
		t.Fatalf("CROSS-DOMAIN REUSE: %d wrong body", wrongBody.Load())
	}
	if connErrors.Load() > 0 {
		t.Fatalf("XMUX high-concurrency connect errors: %d", connErrors.Load())
	}
}

// TestXMUXStressRandomShuffle: 100 goroutines × 50 requests, random site selection.
func TestXMUXStressRandomShuffle(t *testing.T) {
	if testing.Short() {
		t.Skip("long stress suite: skipped in -short mode (CI)")
	}
	sites := startHTTPStressSites(t, 5)
	for _, s := range sites {
		defer s.server.Close()
	}
	cleanup := buildStressProxy(t, 27090, 27091)
	defer cleanup()
	time.Sleep(3 * time.Second)

	var wrongBody atomic.Int32
	var success atomic.Int32
	var connErrors atomic.Int32
	var wg sync.WaitGroup

	for g := 0; g < 100; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				idx := (gid*50 + i + gid*7) % len(sites)
				site := sites[idx]
				body, err := httpGetViaProxy(t, 27091, site.addr, 5*time.Second)
				if err != nil {
					connErrors.Add(1)
					return
				}
				if body != site.Body {
					wrongBody.Add(1)
					t.Errorf("WRONG g=%d r=%d site=%s: got %q expected %q", gid, i, site.Name, body, site.Body)
				}
				success.Add(1)
			}
		}(g)
	}

	wg.Wait()
	t.Logf("RandomShuffle: success=%d wrong=%d errors=%d", success.Load(), wrongBody.Load(), connErrors.Load())
	if wrongBody.Load() > 0 {
		t.Fatalf("CROSS-DOMAIN REUSE: %d wrong body", wrongBody.Load())
	}
	if connErrors.Load() > 0 {
		t.Fatalf("XMUX random-shuffle connect errors: %d", connErrors.Load())
	}
}

// TestXMUXStressContinuousSwitch: 50 goroutines × 50 requests, rapid A→B switching.
func TestXMUXStressContinuousSwitch(t *testing.T) {
	if testing.Short() {
		t.Skip("long stress suite: skipped in -short mode (CI)")
	}
	sites := startHTTPStressSites(t, 2)
	for _, s := range sites {
		defer s.server.Close()
	}
	cleanup := buildStressProxy(t, 27110, 27111)
	defer cleanup()
	time.Sleep(3 * time.Second)

	var wrongBody atomic.Int32
	var success atomic.Int32
	var connErrors atomic.Int32
	var wg sync.WaitGroup

	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				idx := i % len(sites)
				site := sites[idx]
				body, err := httpGetViaProxy(t, 27111, site.addr, 5*time.Second)
				if err != nil {
					connErrors.Add(1)
					return
				}
				if body != site.Body {
					wrongBody.Add(1)
					t.Errorf("WRONG g=%d r=%d: got %q expected %q (site=%s)", gid, i, body, site.Body, site.Name)
				}
				success.Add(1)
			}
		}(g)
	}

	wg.Wait()
	t.Logf("ContinuousSwitch: success=%d wrong=%d errors=%d", success.Load(), wrongBody.Load(), connErrors.Load())
	if wrongBody.Load() > 0 {
		t.Fatalf("CROSS-DOMAIN REUSE: %d wrong body", wrongBody.Load())
	}
	if connErrors.Load() > 0 {
		t.Fatalf("XMUX continuous-switch connect errors: %d", connErrors.Load())
	}
}
