package scenarios

// Fast iteration harness for the packet-up + dseg user report. Uses TLS
// (H2, same as REALITY decides) but no REALITY handshake warm-up, so a full
// VLESS dual-end e2e round-trips in ~5s instead of ~70s. Target is a local
// TCP echo (no external network).
//
// Run with:
//   go test ./testing/scenarios -run 'TestVlessTLSPacketUpDseg($|Legacy|Plain)' -v
//
// TestVlessTLSPacketUpDseg        -> dseg ENABLED, TLS (reproduces the user bug)
// TestVlessTLSPacketUpDsegLegacy  -> dseg DISABLED, TLS control
// TestVlessTLSPacketUpDsegPlain   -> dseg ENABLED, plaintext control (no TLS)

import (
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
	"github.com/xtls/xray-core/transport/internet/tls"
)

func runPacketUpDseg(t *testing.T, dseg string, useTLS bool) {
	tcpServer := tcp.Server{MsgProcessor: xor}
	dest, err := tcpServer.Start()
	common.Must(err)
	defer tcpServer.Close()

	var ctHash []byte
	if useTLS {
		_, hash := cert.MustGenerate(nil, cert.CommonName("localhost"))
		ctHash = hash[:]
	}

	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()

	sharedConfig := func() *splithttp.Config {
		return &splithttp.Config{
			Path: "/xh-pup", Mode: "packet-up",
			Headers: map[string]string{
				splithttp.BraySessionSecretHeader: "pup-shared-secret",
				"x-bray-dseg":                     dseg,
			},
		}
	}
	serverSecurity := func(stream *internet.StreamConfig) {}
	_ = serverSecurity

	serverStream := &internet.StreamConfig{
		ProtocolName: "splithttp",
		TransportSettings: []*internet.TransportConfig{{
			ProtocolName: "splithttp",
			Settings:     serial.ToTypedMessage(sharedConfig()),
		}},
	}
	if useTLS {
		ct, _ := cert.MustGenerate(nil, cert.CommonName("localhost"))
		serverStream.SecurityType = serial.GetMessageType(&tls.Config{})
		serverStream.SecuritySettings = []*serial.TypedMessage{serial.ToTypedMessage(&tls.Config{
			Certificate: []*tls.Certificate{tls.ParseCertificate(ct)},
		})}
	}

	serverConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{ErrorLogLevel: clog.Severity_Debug, ErrorLogType: log.LogType_Console}),
		},
		Inbound: []*core.InboundHandlerConfig{{
			ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
				PortList:       &net.PortList{Range: []*net.PortRange{net.SinglePortRange(serverPort)}},
				Listen:         net.NewIPOrDomain(net.LocalHostIP),
				StreamSettings: serverStream,
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
	clientStream := &internet.StreamConfig{
		ProtocolName: "splithttp",
		TransportSettings: []*internet.TransportConfig{{
			ProtocolName: "splithttp",
			Settings:     serial.ToTypedMessage(sharedConfig()),
		}},
	}
	if useTLS {
		clientStream.SecurityType = serial.GetMessageType(&tls.Config{})
		csec := &tls.Config{ServerName: "localhost"}
		if ctHash != nil {
			csec.PinnedPeerCertSha256 = [][]byte{ctHash[:]}
		}
		clientStream.SecuritySettings = []*serial.TypedMessage{serial.ToTypedMessage(csec)}
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
				StreamSettings: clientStream,
			}),
		}},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	common.Must(err)
	defer CloseAllServers(servers)

	// 3 concurrent e2e round-trips, 256 KiB each (span bursts + a short tail).
	var wg errgroup.Group
	for range 3 {
		wg.Go(testTCPConn(clientPort, 256*1024, time.Second*20))
	}
	if err := wg.Wait(); err != nil {
		t.Fatal(err)
	}
}

// --- exported test entry points ---

func TestVlessTLSPacketUpDseg(t *testing.T) {
	runPacketUpDseg(t, "1", true)
}

func TestVlessTLSPacketUpDsegLegacy(t *testing.T) {
	runPacketUpDseg(t, "0", true)
}

func TestVlessTLSPacketUpDsegPlain(t *testing.T) {
	runPacketUpDseg(t, "1", false)
}
