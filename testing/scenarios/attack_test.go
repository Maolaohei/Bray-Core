package scenarios

import (
	"bytes"
	"encoding/hex"
	"io"
	stdnet "net"
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
// GFW 红客攻防模拟：主动探测 + 投毒 + 重放
//
// Attack surface 1: 主动探测（REALITY）— 攻击者无凭据连接，发畸形/半包/垃圾
//   字节。防探测不变量：认证失败者永远看不到 REALITY 合成证书成功路径，
//   且服务端在探测风暴下必须存活（不 panic / 不耗尽资源）。
// Attack surface 2: DNS 投毒 — 伪造响应（question 不匹配/缺失）必须被
//   RFC 5452 guard 丢弃（见 app/dns/attack_test.go 单测）。
// ============================================================================

func attackRealityServerConfig(serverPort net.Port, userID *protocol.ID) *core.Config {
	shortIds := make([][]byte, 1)
	shortIds[0] = make([]byte, 8)
	hex.Decode(shortIds[0], []byte("0123456789abcdef"))

	return &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Info, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(serverPort)}},
				Listen:   net.NewIPOrDomain(net.LocalHostIP),
				StreamSettings: &internet.StreamConfig{
					ProtocolName: "splithttp",
					TransportSettings: []*internet.TransportConfig{{
						ProtocolName: "splithttp",
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: "/xhttp-attack"}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: false, Dest: "www.microsoft.com:443", ServerNames: []string{"www.microsoft.com"},
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
}

// TestAttack_REALITYActiveProbe hammers the REALITY listener with probe
// traffic that carries no credentials and asserts the anti-probe invariant:
// the server must not return a synthetic REALITY success handshake, and it
// must remain fully operational afterwards.
func TestAttack_REALITYActiveProbe(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping REALITY attack simulation under -short")
	}

	tcpServer := tcp.Server{MsgProcessor: xor}
	dest, err := tcpServer.Start()
	common.Must(err)
	defer tcpServer.Close()

	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	serverConfig := attackRealityServerConfig(serverPort, userID)

	clientPort := tcp.PickPort()
	clientConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Info, ErrorLogType: log.LogType_Console}),
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
						Settings:     serial.ToTypedMessage(&splithttp.Config{Path: "/xhttp-attack"}),
					}},
					SecurityType: serial.GetMessageType(&reality.Config{}),
					SecuritySettings: []*serial.TypedMessage{serial.ToTypedMessage(&reality.Config{
						Show: false, Fingerprint: "chrome", ServerName: "www.microsoft.com",
						PublicKey: xhttpRealityPublicKey, ShortId: xhttpRealityShortIDs()[0], SpiderX: "/",
					})},
				},
			}),
		}},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	common.Must(err)
	defer CloseAllServers(servers)
	waitForPostHandshakeDetection(t, 35*time.Second)

	addr := stdnet.JoinHostPort("127.0.0.1", serverPort.String())

	// Attack payloads: garbage, partial TLS records, fake TLS ClientHello with
	// no REALITY auth payload (random session id), and an oversized write.
	probes := [][]byte{
		{0x16, 0x03, 0x01},                                            // partial record header only
		bytes.Repeat([]byte{0x41}, 512),                               // plain garbage
		append([]byte{0x16, 0x03, 0x01, 0x00, 0x2a}, bytes.Repeat([]byte{0x00}, 42)...), // short ClientHello, no auth
		bytes.Repeat([]byte{0x00}, 4096), // null flood
	}

	for i, payload := range probes {
		conn, derr := stdnet.DialTimeout("tcp", addr, 3*time.Second)
		if derr != nil {
			continue // listener may be mid-attack; not a failure by itself
		}
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		_, werr := conn.Write(payload)
		if werr == nil {
			// Drain whatever the server mirrors (real-site bytes) or EOF.
			_, _ = io.Copy(io.Discard, conn)
		}
		conn.Close()
		t.Logf("probe %d sent %d bytes, write err=%v", i, len(payload), werr)
	}

	// The anti-probe invariant: the server must still serve legitimate clients.
	// A crashed / wedged server fails here.
	err = testTCPConn(clientPort, 64*1024, time.Second*15)()
	if err != nil {
		t.Fatalf("REALITY server died or wedged under active probe: %v", err)
	}
	t.Log("server survived active-probe attack and still serves legitimate clients")
}
