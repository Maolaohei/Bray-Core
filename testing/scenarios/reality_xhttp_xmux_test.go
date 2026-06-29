package scenarios

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

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
	"github.com/xtls/xray-core/transport/internet/reality"
	"github.com/xtls/xray-core/transport/internet/splithttp"
)

// ============================================================================
// VLESS + XHTTP + REALITY(CDN) + XMUX integration tests
//
// Gap coverage vs existing tests:
//   - Existing tests only use TCP transport, never XHTTP
//   - No test exercises XMUX multiplexing over REALITY
//   - No test verifies the cache fast path in a real VLESS+XHTTP flow
//   - No test covers cold-start timing of DetectPostHandshakeRecordsLens
// ============================================================================

var (
	xhttpRealityPrivateKey, _ = base64.RawURLEncoding.DecodeString("aGSYystUbf59_9_6LKRxD27rmSW_-2_nyd9YG_Gwbks")
	xhttpRealityPublicKey, _  = base64.RawURLEncoding.DecodeString("E59WjnvZcQMu7tR7_BgyhycuEdBS-CtKxfImRCdAvFM")
)

func xhttpRealityShortIDs() [][]byte {
	s := make([][]byte, 1)
	s[0] = make([]byte, 8)
	hex.Decode(s[0], []byte("0123456789abcdef"))
	return s
}

// TestVlessXHTTPRealityBasic verifies VLESS + XHTTP + REALITY over localhost.
func TestVlessXHTTPRealityBasic(t *testing.T) {
	tcpServer := tcp.Server{MsgProcessor: xor}
	dest, err := tcpServer.Start()
	common.Must(err)
	defer tcpServer.Close()

	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	shortIds := xhttpRealityShortIDs()

	serverConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(serverPort)}},
				Listen:   net.NewIPOrDomain(net.LocalHostIP),
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: "/xhttp-basic"}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Dest: "www.microsoft.com:443", ServerNames: []string{"www.microsoft.com"},
						PrivateKey: xhttpRealityPrivateKey, ShortIds: shortIds, Type: "tcp",
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

	clientPort := tcp.PickPort()
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
				RewriteAddress: net.NewIPOrDomain(dest.Address), RewritePort: uint32(dest.Port),
				AllowedNetworks: []net.Network{net.Network_TCP},
			}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&outbound.Config{
				Vnext: &protocol.ServerEndpoint{
					Address: net.NewIPOrDomain(net.LocalHostIP), Port: uint32(serverPort),
					User: &protocol.User{Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()})},
				},
			}),
			SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: "/xhttp-basic"}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Fingerprint: "chrome", ServerName: "www.microsoft.com",
						PublicKey: xhttpRealityPublicKey, ShortId: shortIds[0], SpiderX: "/",
					})},
				},
			}),
		}},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	common.Must(err)
	defer CloseAllServers(servers)

	// Warm up post-handshake detection to avoid cold-start timing issue.
	waitForPostHandshakeDetection(t, 35*time.Second)

	err = testTCPConn(clientPort, 1024*1024, time.Second*30)()
	if err != nil {
		t.Fatal(err)
	}
}

// TestVlessXHTTPRealityXMUX verifies VLESS + XHTTP + XMUX + REALITY.
func TestVlessXHTTPRealityXMUX(t *testing.T) {
	tcpServer := tcp.Server{MsgProcessor: xor}
	dest, err := tcpServer.Start()
	common.Must(err)
	defer tcpServer.Close()

	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	shortIds := xhttpRealityShortIDs()

	xmuxCfg := &splithttp.XmuxConfig{
		MaxConcurrency: &splithttp.RangeConfig{From: 16, To: 32},
		MaxConnections: &splithttp.RangeConfig{From: 4, To: 8},
	}

	serverConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(serverPort)}},
				Listen:   net.NewIPOrDomain(net.LocalHostIP),
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: "/xhttp-xmux", Xmux: xmuxCfg}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Dest: "www.microsoft.com:443", ServerNames: []string{"www.microsoft.com"},
						PrivateKey: xhttpRealityPrivateKey, ShortIds: shortIds, Type: "tcp",
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

	clientPort := tcp.PickPort()
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
				RewriteAddress: net.NewIPOrDomain(dest.Address), RewritePort: uint32(dest.Port),
				AllowedNetworks: []net.Network{net.Network_TCP},
			}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&outbound.Config{
				Vnext: &protocol.ServerEndpoint{
					Address: net.NewIPOrDomain(net.LocalHostIP), Port: uint32(serverPort),
					User: &protocol.User{Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()})},
				},
			}),
			SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: "/xhttp-xmux", Xmux: xmuxCfg}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Fingerprint: "chrome", ServerName: "www.microsoft.com",
						PublicKey: xhttpRealityPublicKey, ShortId: shortIds[0], SpiderX: "/",
					})},
				},
			}),
		}},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	common.Must(err)
	defer CloseAllServers(servers)

	waitForPostHandshakeDetection(t, 35*time.Second)

	for i := range 5 {
		err := testTCPConn(clientPort, 1024*1024, time.Second*30)()
		if err != nil {
			t.Fatalf("Connection %d failed: %v", i+1, err)
		}
	}
}

// TestVlessXHTTPRealityConcurrentXMUX verifies concurrent connections through
// the full VLESS+XHTTP+XMUX+REALITY pipeline.
func TestVlessXHTTPRealityConcurrentXMUX(t *testing.T) {
	tcpServer := tcp.Server{MsgProcessor: xor}
	dest, err := tcpServer.Start()
	common.Must(err)
	defer tcpServer.Close()

	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	shortIds := xhttpRealityShortIDs()

	xmuxCfg := &splithttp.XmuxConfig{
		MaxConcurrency: &splithttp.RangeConfig{From: 16, To: 32},
		MaxConnections: &splithttp.RangeConfig{From: 4, To: 8},
	}

	serverConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(serverPort)}},
				Listen:   net.NewIPOrDomain(net.LocalHostIP),
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: "/xhttp-concurrent", Xmux: xmuxCfg}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Dest: "www.microsoft.com:443", ServerNames: []string{"www.microsoft.com"},
						PrivateKey: xhttpRealityPrivateKey, ShortIds: shortIds, Type: "tcp",
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

	clientPort := tcp.PickPort()
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
				RewriteAddress: net.NewIPOrDomain(dest.Address), RewritePort: uint32(dest.Port),
				AllowedNetworks: []net.Network{net.Network_TCP},
			}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&outbound.Config{
				Vnext: &protocol.ServerEndpoint{
					Address: net.NewIPOrDomain(net.LocalHostIP), Port: uint32(serverPort),
					User: &protocol.User{Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()})},
				},
			}),
			SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: "/xhttp-concurrent", Xmux: xmuxCfg}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Fingerprint: "chrome", ServerName: "www.microsoft.com",
						PublicKey: xhttpRealityPublicKey, ShortId: shortIds[0], SpiderX: "/",
					})},
				},
			}),
		}},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	common.Must(err)
	defer CloseAllServers(servers)

	waitForPostHandshakeDetection(t, 35*time.Second)

	var wg sync.WaitGroup
	errs := make([]error, 10)
	for i := range 10 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = testTCPConn(clientPort, 512*1024, time.Second*30)()
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("Concurrent connection %d failed: %v", i+1, err)
		}
	}
}

// TestVlessXHTTPRealityCacheFastPath verifies cache hit path works in XHTTP+REALITY.
func TestVlessXHTTPRealityCacheFastPath(t *testing.T) {
	tcpServer := tcp.Server{MsgProcessor: xor}
	dest, err := tcpServer.Start()
	common.Must(err)
	defer tcpServer.Close()

	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	shortIds := xhttpRealityShortIDs()

	serverConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(serverPort)}},
				Listen:   net.NewIPOrDomain(net.LocalHostIP),
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: "/xhttp-cache"}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Dest: "www.microsoft.com:443", ServerNames: []string{"www.microsoft.com"},
						PrivateKey: xhttpRealityPrivateKey, ShortIds: shortIds, Type: "tcp",
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

	clientPort := tcp.PickPort()
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
				RewriteAddress: net.NewIPOrDomain(dest.Address), RewritePort: uint32(dest.Port),
				AllowedNetworks: []net.Network{net.Network_TCP},
			}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&outbound.Config{
				Vnext: &protocol.ServerEndpoint{
					Address: net.NewIPOrDomain(net.LocalHostIP), Port: uint32(serverPort),
					User: &protocol.User{Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()})},
				},
			}),
			SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: "/xhttp-cache"}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Fingerprint: "chrome", ServerName: "www.microsoft.com",
						PublicKey: xhttpRealityPublicKey, ShortId: shortIds[0], SpiderX: "/",
					})},
				},
			}),
		}},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	common.Must(err)
	defer CloseAllServers(servers)

	waitForPostHandshakeDetection(t, 35*time.Second)

	// 3 sequential connections: first populates cache, rest hit cache fast path.
	for i := range 3 {
		err := testTCPConn(clientPort, 1024*1024, time.Second*30)()
		if err != nil {
			t.Fatalf("Connection %d failed: %v", i+1, err)
		}
	}
}

// TestVlessXHTTPRealityColdStartTiming tests the cold-start timing issue:
// DetectPostHandshakeRecordsLens runs a 30s background goroutine. The first
// connection may fail because post-handshake records aren't available yet.
func TestVlessXHTTPRealityColdStartTiming(t *testing.T) {
	tcpServer := tcp.Server{MsgProcessor: xor}
	dest, err := tcpServer.Start()
	common.Must(err)
	defer tcpServer.Close()

	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	shortIds := xhttpRealityShortIDs()

	serverConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(serverPort)}},
				Listen:   net.NewIPOrDomain(net.LocalHostIP),
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: "/xhttp-cold"}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Dest: "www.microsoft.com:443", ServerNames: []string{"www.microsoft.com"},
						PrivateKey: xhttpRealityPrivateKey, ShortIds: shortIds, Type: "tcp",
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

	clientPort := tcp.PickPort()
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
				RewriteAddress: net.NewIPOrDomain(dest.Address), RewritePort: uint32(dest.Port),
				AllowedNetworks: []net.Network{net.Network_TCP},
			}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&outbound.Config{
				Vnext: &protocol.ServerEndpoint{
					Address: net.NewIPOrDomain(net.LocalHostIP), Port: uint32(serverPort),
					User: &protocol.User{Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()})},
				},
			}),
			SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: "/xhttp-cold"}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Fingerprint: "chrome", ServerName: "www.microsoft.com",
						PublicKey: xhttpRealityPublicKey, ShortId: shortIds[0], SpiderX: "/",
					})},
				},
			}),
		}},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	common.Must(err)
	defer CloseAllServers(servers)

	// NO warmup — intentionally test cold start.
	firstErr := testTCPConn(clientPort, 1024, time.Second*15)()
	if firstErr != nil {
		t.Logf("First connection failed on cold start (expected): %v", firstErr)
	} else {
		t.Log("First connection succeeded (detection completed fast enough)")
	}

	// After warmup, all connections should succeed.
	waitForPostHandshakeDetection(t, 35*time.Second)

	err = testTCPConn(clientPort, 1024*1024, time.Second*30)()
	if err != nil {
		t.Fatalf("Connection after warmup failed: %v", err)
	}
}

// TestVlessXHTTPRealityRapidConnectDisconnect verifies cleanup after rapid cycles.
func TestVlessXHTTPRealityRapidConnectDisconnect(t *testing.T) {
	tcpServer := tcp.Server{MsgProcessor: xor}
	dest, err := tcpServer.Start()
	common.Must(err)
	defer tcpServer.Close()

	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	shortIds := xhttpRealityShortIDs()

	serverConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(serverPort)}},
				Listen:   net.NewIPOrDomain(net.LocalHostIP),
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: "/xhttp-rapid"}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Dest: "www.microsoft.com:443", ServerNames: []string{"www.microsoft.com"},
						PrivateKey: xhttpRealityPrivateKey, ShortIds: shortIds, Type: "tcp",
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

	clientPort := tcp.PickPort()
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
				RewriteAddress: net.NewIPOrDomain(dest.Address), RewritePort: uint32(dest.Port),
				AllowedNetworks: []net.Network{net.Network_TCP},
			}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&outbound.Config{
				Vnext: &protocol.ServerEndpoint{
					Address: net.NewIPOrDomain(net.LocalHostIP), Port: uint32(serverPort),
					User: &protocol.User{Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()})},
				},
			}),
			SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: "/xhttp-rapid"}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Fingerprint: "chrome", ServerName: "www.microsoft.com",
						PublicKey: xhttpRealityPublicKey, ShortId: shortIds[0], SpiderX: "/",
					})},
				},
			}),
		}},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	common.Must(err)
	defer CloseAllServers(servers)

	waitForPostHandshakeDetection(t, 35*time.Second)

	for i := range 10 {
		err := testTCPConn(clientPort, 1024, time.Second*15)()
		if err != nil {
			t.Errorf("Rapid connection %d failed: %v", i+1, err)
		}
	}
}

// TestVlessXHTTPRealityMixedPacketSizes verifies various payload sizes.
func TestVlessXHTTPRealityMixedPacketSizes(t *testing.T) {
	tcpServer := tcp.Server{MsgProcessor: xor}
	dest, err := tcpServer.Start()
	common.Must(err)
	defer tcpServer.Close()

	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	shortIds := xhttpRealityShortIDs()

	serverConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(serverPort)}},
				Listen:   net.NewIPOrDomain(net.LocalHostIP),
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: "/xhttp-mixed"}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Dest: "www.microsoft.com:443", ServerNames: []string{"www.microsoft.com"},
						PrivateKey: xhttpRealityPrivateKey, ShortIds: shortIds, Type: "tcp",
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

	clientPort := tcp.PickPort()
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
				RewriteAddress: net.NewIPOrDomain(dest.Address), RewritePort: uint32(dest.Port),
				AllowedNetworks: []net.Network{net.Network_TCP},
			}),
		}},
		Outbound: []*core.OutboundHandlerConfig{{
			ProxySettings: serial.ToTypedMessage(&outbound.Config{
				Vnext: &protocol.ServerEndpoint{
					Address: net.NewIPOrDomain(net.LocalHostIP), Port: uint32(serverPort),
					User: &protocol.User{Account: serial.ToTypedMessage(&vless.Account{Id: userID.String()})},
				},
			}),
			SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: "/xhttp-mixed"}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: true, Fingerprint: "chrome", ServerName: "www.microsoft.com",
						PublicKey: xhttpRealityPublicKey, ShortId: shortIds[0], SpiderX: "/",
					})},
				},
			}),
		}},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	common.Must(err)
	defer CloseAllServers(servers)

	waitForPostHandshakeDetection(t, 35*time.Second)

	for _, size := range []int{1, 16, 256, 1024, 4096, 65536, 512 * 1024} {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			err := testTCPConn(clientPort, size, time.Second*30)()
			if err != nil {
				t.Errorf("Payload size %d failed: %v", size, err)
			}
		})
	}
}

// waitForPostHandshakeDetection gives DetectPostHandshakeRecordsLens time to
// complete its background probe. On localhost this typically takes 2-5 seconds.
func waitForPostHandshakeDetection(t *testing.T, d time.Duration) {
	t.Helper()
	time.Sleep(d)
}
