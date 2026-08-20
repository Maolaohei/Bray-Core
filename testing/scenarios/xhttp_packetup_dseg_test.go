package scenarios

// Regression: V2rayN-style packet-up against an auto-mode dseg-enabled server
// returned 404 on both the production-leg GET and the packet-up upload POST.
// This test reproduces the user scenario with a REAL dual-end (VLESS inbound
// + VLESS outbound + REALITY + splithttp packet-up) on localhost and a local
// TCP echo target. The server deliberately stays mode:auto so this covers the
// production policy that accepts all three XHTTP wire shapes.

import (
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

func TestVlessXHTTPRealityPacketUpDseg(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long REALITY scenario under -short")
	}
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
						Settings: serial.ToTypedMessage(&splithttp.Config{
							Path: "/xhttp-pup", Mode: "auto",
							Headers: map[string]string{splithttp.BraySessionSecretHeader: "pup-shared-secret"},
						}),
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
						Settings: serial.ToTypedMessage(&splithttp.Config{
							Path: "/xhttp-pup", Mode: "packet-up",
							Headers: map[string]string{splithttp.BraySessionSecretHeader: "pup-shared-secret"},
						}),
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

	// Warm up REALITY handshake detection (mirrors prior tests).
	waitForPostHandshakeDetection(t, 35*time.Second)

	err = testTCPConn(clientPort, 1024*1024, time.Second*30)()
	if err != nil {
		t.Fatal(err)
	}
}
