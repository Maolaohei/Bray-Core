package scenarios

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
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/proxy/dokodemo"
	"github.com/xtls/xray-core/proxy/freedom"
	"github.com/xtls/xray-core/proxy/vmess"
	"github.com/xtls/xray-core/proxy/vmess/inbound"
	"github.com/xtls/xray-core/proxy/vmess/outbound"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/testing/servers/udp"
	"golang.org/x/sync/errgroup"
)

func TestDokodemoTCP(t *testing.T) {
	tcpServer := tcp.Server{
		MsgProcessor: xor,
	}
	dest, err := tcpServer.Start()
	common.Must(err)
	defer tcpServer.Close()

	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	serverConfig := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&log.Config{
				ErrorLogLevel: clog.Severity_Debug,
				ErrorLogType:  log.LogType_Console,
			}),
		},
		Inbound: []*core.InboundHandlerConfig{
			{
				ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
					PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(serverPort)}},
					Listen:   net.NewIPOrDomain(net.LocalHostIP),
				}),
				ProxySettings: serial.ToTypedMessage(&inbound.Config{
					User: []*protocol.User{
						{
							Account: serial.ToTypedMessage(&vmess.Account{
								Id: userID.String(),
							}),
						},
					},
				}),
			},
		},
		Outbound: []*core.OutboundHandlerConfig{
			{
				ProxySettings: serial.ToTypedMessage(&freedom.Config{
					FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}},
				}),
			},
		},
	}
	server, err := InitializeServerConfig(serverConfig)
	common.Must(err)
	defer CloseServer(server)

	// Port range is inclusive (From..To). Pick a contiguous free block rather
	// than assuming PickPort()+N are free; always close failed client procs.
	const clientPortCount = 6 // range value 5 => 6 ports inclusive
	clientPortRange := uint32(clientPortCount - 1)
	var clientPort uint32
	for retry := 1; ; retry++ {
		base, err := pickFreeTCPPortRange(clientPortCount)
		if err != nil {
			t.Fatal(err)
		}
		clientPort = uint32(base)
		clientConfig := &core.Config{
			App: []*serial.TypedMessage{
				serial.ToTypedMessage(&log.Config{
					ErrorLogLevel: clog.Severity_Debug,
					ErrorLogType:  log.LogType_Console,
				}),
			},
			Inbound: []*core.InboundHandlerConfig{
				{
					ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
						PortList: &net.PortList{Range: []*net.PortRange{{From: clientPort, To: clientPort + clientPortRange}}},
						Listen:   net.NewIPOrDomain(net.LocalHostIP),
					}),
					ProxySettings: serial.ToTypedMessage(&dokodemo.Config{
						RewriteAddress:  net.NewIPOrDomain(dest.Address),
						RewritePort:     uint32(dest.Port),
						AllowedNetworks: []net.Network{net.Network_TCP},
					}),
				},
			},
			Outbound: []*core.OutboundHandlerConfig{
				{
					ProxySettings: serial.ToTypedMessage(&outbound.Config{
						Receiver: &protocol.ServerEndpoint{
							Address: net.NewIPOrDomain(net.LocalHostIP),
							Port:    uint32(serverPort),
							User: &protocol.User{
								Account: serial.ToTypedMessage(&vmess.Account{
									Id: userID.String(),
								}),
							},
						},
					}),
				},
			},
		}

		client, _ := InitializeServerConfig(clientConfig)
		if client != nil && WaitConnAvailableWithTest(t, testTCPConn(net.Port(clientPort), 1024, time.Second*2)) {
			// Verify the whole port range, not just the base port: under
			// parallel CI load a sibling package can grab a port inside the
			// range after pickFreeTCPPortRange, making that listener fail to
			// bind. Listeners come up asynchronously, so probe each port with
			// short retries before deciding the range is broken.
			allPortsOK := true
			for p := clientPort; p <= clientPort+clientPortRange; p++ {
				portOK := false
				for try := 0; try < 5; try++ {
					if err := testTCPConn(net.Port(p), 1024, 300*time.Millisecond)(); err == nil {
						portOK = true
						break
					}
				}
				if !portOK {
					allPortsOK = false
					break
				}
			}
			if allPortsOK {
				defer CloseServer(client)
				break
			}
		}
		if client != nil {
			CloseServer(client)
		}
		if retry >= 8 {
			t.Fatal("All attempts failed to start client")
		}
	}

	for port := clientPort; port <= clientPort+clientPortRange; port++ {
		if err := testTCPConn(net.Port(port), 1024, time.Second*2)(); err != nil {
			t.Error(err)
		}
	}
}

func TestDokodemoUDP(t *testing.T) {
	udpServer := udp.Server{
		MsgProcessor: xor,
	}
	dest, err := udpServer.Start()
	common.Must(err)
	defer udpServer.Close()

	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	serverConfig := &core.Config{
		Inbound: []*core.InboundHandlerConfig{
			{
				ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
					PortList: &net.PortList{Range: []*net.PortRange{net.SinglePortRange(serverPort)}},
					Listen:   net.NewIPOrDomain(net.LocalHostIP),
				}),
				ProxySettings: serial.ToTypedMessage(&inbound.Config{
					User: []*protocol.User{
						{
							Account: serial.ToTypedMessage(&vmess.Account{
								Id: userID.String(),
							}),
						},
					},
				}),
			},
		},
		Outbound: []*core.OutboundHandlerConfig{
			{
				ProxySettings: serial.ToTypedMessage(&freedom.Config{
					FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}},
				}),
			},
		},
	}
	server, err := InitializeServerConfig(serverConfig)
	common.Must(err)
	defer CloseServer(server)

	const clientPortCount = 4 // range value 3 => 4 ports inclusive
	clientPortRange := uint32(clientPortCount - 1)
	var clientPort uint32
	for retry := 1; ; retry++ {
		base, err := pickFreeUDPPortRange(clientPortCount)
		if err != nil {
			t.Fatal(err)
		}
		clientPort = uint32(base)
		clientConfig := &core.Config{
			Inbound: []*core.InboundHandlerConfig{
				{
					ReceiverSettings: serial.ToTypedMessage(&proxyman.ReceiverConfig{
						PortList: &net.PortList{Range: []*net.PortRange{{From: clientPort, To: clientPort + clientPortRange}}},
						Listen:   net.NewIPOrDomain(net.LocalHostIP),
					}),
					ProxySettings: serial.ToTypedMessage(&dokodemo.Config{
						RewriteAddress:  net.NewIPOrDomain(dest.Address),
						RewritePort:     uint32(dest.Port),
						AllowedNetworks: []net.Network{net.Network_UDP},
					}),
				},
			},
			Outbound: []*core.OutboundHandlerConfig{
				{
					ProxySettings: serial.ToTypedMessage(&outbound.Config{
						Receiver: &protocol.ServerEndpoint{
							Address: net.NewIPOrDomain(net.LocalHostIP),
							Port:    uint32(serverPort),
							User: &protocol.User{
								Account: serial.ToTypedMessage(&vmess.Account{
									Id: userID.String(),
								}),
							},
						},
					}),
				},
			},
		}

		client, _ := InitializeServerConfig(clientConfig)
		if client != nil && WaitConnAvailableWithTest(t, testUDPConn(net.Port(clientPort), 1024, time.Second*2)) {
			defer CloseServer(client)
			break
		}
		if client != nil {
			CloseServer(client)
		}
		if retry >= 5 {
			t.Fatal("All attempts failed to start client")
		}
	}

	var errg errgroup.Group
	for port := clientPort; port <= clientPort+clientPortRange; port++ {
		errg.Go(testUDPConn(net.Port(port), 1024, time.Second*5))
	}
	if err := errg.Wait(); err != nil {
		t.Error(err)
	}
}
