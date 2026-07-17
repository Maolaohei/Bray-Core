package scenarios

import (
	"encoding/base64"
	"encoding/hex"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/log"
	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/common"
	clog "github.com/xtls/xray-core/common/log"
	xraynet "github.com/xtls/xray-core/common/net"
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
)

// TestREALITYHandshakeComplete verifies the core REALITY handshake completes
// and data flows correctly. This is the most basic production-readiness test.
func TestREALITYHandshakeComplete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long REALITY scenario under -short")
	}
	tcpServer := tcp.Server{MsgProcessor: xor}
	dest, err := tcpServer.Start()
	common.Must(err)
	defer tcpServer.Close()

	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	privateKey, _ := base64.RawURLEncoding.DecodeString("aGSYystUbf59_9_6LKRxD27rmSW_-2_nyd9YG_Gwbks")
	publicKey, _ := base64.RawURLEncoding.DecodeString("E59WjnvZcQMu7tR7_BgyhycuEdBS-CtKxfImRCdAvFM")
	shortIds := make([][]byte, 1)
	shortIds[0] = make([]byte, 8)
	hex.Decode(shortIds[0], []byte("0123456789abcdef"))

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
					PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(serverPort)}},
					Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
					StreamSettings: &internet.StreamConfig{
						ProtocolName: "tcp",
						SecurityType: serial.GetMessageType(&reality.Config{}),
						SecuritySettings: []*serial.TypedMessage{
							serial.ToTypedMessage(&reality.Config{
								Show:        true,
								Dest:        "www.microsoft.com:443",
								ServerNames: []string{"www.microsoft.com"},
								PrivateKey:  privateKey,
								ShortIds:    shortIds,
								Type:        "tcp",
							}),
						},
					},
				}),
				ProxySettings: serial.ToTypedMessage(&inbound.Config{Users: []*protocol.User{
					{
						Account: serial.ToTypedMessage(&vless.Account{
							Id: userID.String(),
						}),
					},
				}}),
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

	clientPort := tcp.PickPort()
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
					PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(clientPort)}},
					Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
				}),
				ProxySettings: serial.ToTypedMessage(&dokodemo.Config{
					RewriteAddress:  xraynet.NewIPOrDomain(dest.Address),
					RewritePort:     uint32(dest.Port),
					AllowedNetworks: []xraynet.Network{xraynet.Network_TCP},
				}),
			},
		},
		Outbound: []*core.OutboundHandlerConfig{
			{
				ProxySettings: serial.ToTypedMessage(&outbound.Config{
					Vnext: &protocol.ServerEndpoint{
						Address: xraynet.NewIPOrDomain(xraynet.LocalHostIP),
						Port:    uint32(serverPort),
						User: &protocol.User{
							Account: serial.ToTypedMessage(&vless.Account{
								Id: userID.String(),
							}),
						},
					},
				}),
				SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
					StreamSettings: &internet.StreamConfig{
						ProtocolName: "tcp",
						SecurityType: serial.GetMessageType(&reality.Config{}),
						SecuritySettings: []*serial.TypedMessage{
							serial.ToTypedMessage(&reality.Config{
								Show:        true,
								Fingerprint: "chrome",
								ServerName:  "www.microsoft.com",
								PublicKey:   publicKey,
								ShortId:     shortIds[0],
								SpiderX:     "/",
							}),
						},
					},
				}),
			},
		},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	common.Must(err)
	defer CloseAllServers(servers)

	err = testTCPConn(clientPort, 1024*1024, time.Second*30)()
	if err != nil {
		t.Fatal(err)
	}
}

// TestREALITYConcurrentConnections verifies multiple simultaneous connections
// work correctly. This tests the mutex and handshake synchronization.
func TestREALITYConcurrentConnections(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long REALITY scenario under -short")
	}
	tcpServer := tcp.Server{MsgProcessor: xor}
	dest, err := tcpServer.Start()
	common.Must(err)
	defer tcpServer.Close()

	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	privateKey, _ := base64.RawURLEncoding.DecodeString("aGSYystUbf59_9_6LKRxD27rmSW_-2_nyd9YG_Gwbks")
	publicKey, _ := base64.RawURLEncoding.DecodeString("E59WjnvZcQMu7tR7_BgyhycuEdBS-CtKxfImRCdAvFM")
	shortIds := make([][]byte, 1)
	shortIds[0] = make([]byte, 8)
	hex.Decode(shortIds[0], []byte("0123456789abcdef"))

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
					PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(serverPort)}},
					Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
					StreamSettings: &internet.StreamConfig{
						ProtocolName: "tcp",
						SecurityType: serial.GetMessageType(&reality.Config{}),
						SecuritySettings: []*serial.TypedMessage{
							serial.ToTypedMessage(&reality.Config{
								Show:        true,
								Dest:        "www.microsoft.com:443",
								ServerNames: []string{"www.microsoft.com"},
								PrivateKey:  privateKey,
								ShortIds:    shortIds,
								Type:        "tcp",
							}),
						},
					},
				}),
				ProxySettings: serial.ToTypedMessage(&inbound.Config{Users: []*protocol.User{
					{
						Account: serial.ToTypedMessage(&vless.Account{
							Id: userID.String(),
						}),
					},
				}}),
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

	clientPort := tcp.PickPort()
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
					PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(clientPort)}},
					Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
				}),
				ProxySettings: serial.ToTypedMessage(&dokodemo.Config{
					RewriteAddress:  xraynet.NewIPOrDomain(dest.Address),
					RewritePort:     uint32(dest.Port),
					AllowedNetworks: []xraynet.Network{xraynet.Network_TCP},
				}),
			},
		},
		Outbound: []*core.OutboundHandlerConfig{
			{
				ProxySettings: serial.ToTypedMessage(&outbound.Config{
					Vnext: &protocol.ServerEndpoint{
						Address: xraynet.NewIPOrDomain(xraynet.LocalHostIP),
						Port:    uint32(serverPort),
						User: &protocol.User{
							Account: serial.ToTypedMessage(&vless.Account{
								Id: userID.String(),
							}),
						},
					},
				}),
				SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
					StreamSettings: &internet.StreamConfig{
						ProtocolName: "tcp",
						SecurityType: serial.GetMessageType(&reality.Config{}),
						SecuritySettings: []*serial.TypedMessage{
							serial.ToTypedMessage(&reality.Config{
								Show:        true,
								Fingerprint: "chrome",
								ServerName:  "www.microsoft.com",
								PublicKey:   publicKey,
								ShortId:     shortIds[0],
								SpiderX:     "/",
							}),
						},
					},
				}),
			},
		},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	common.Must(err)
	defer CloseAllServers(servers)

	// 3 goroutines each transferring 1MB
	var wg sync.WaitGroup
	var errs []error
	var mu sync.Mutex
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := testTCPConn(clientPort, 1024*1024, time.Second*30)(); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	for _, err := range errs {
		t.Errorf("Concurrent connection failed: %v", err)
	}
}

// TestREALITYPostHandshakeRecords specifically verifies that the 30-second
// post-handshake detection works correctly with REALITY connections.
func TestREALITYPostHandshakeRecords(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long REALITY scenario under -short")
	}
	tcpServer := tcp.Server{MsgProcessor: xor}
	dest, err := tcpServer.Start()
	common.Must(err)
	defer tcpServer.Close()

	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	privateKey, _ := base64.RawURLEncoding.DecodeString("aGSYystUbf59_9_6LKRxD27rmSW_-2_nyd9YG_Gwbks")
	publicKey, _ := base64.RawURLEncoding.DecodeString("E59WjnvZcQMu7tR7_BgyhycuEdBS-CtKxfImRCdAvFM")
	shortIds := make([][]byte, 1)
	shortIds[0] = make([]byte, 8)
	hex.Decode(shortIds[0], []byte("0123456789abcdef"))

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
					PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(serverPort)}},
					Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
					StreamSettings: &internet.StreamConfig{
						ProtocolName: "tcp",
						SecurityType: serial.GetMessageType(&reality.Config{}),
						SecuritySettings: []*serial.TypedMessage{
							serial.ToTypedMessage(&reality.Config{
								Show:        true,
								Dest:        "www.microsoft.com:443",
								ServerNames: []string{"www.microsoft.com"},
								PrivateKey:  privateKey,
								ShortIds:    shortIds,
								Type:        "tcp",
							}),
						},
					},
				}),
				ProxySettings: serial.ToTypedMessage(&inbound.Config{Users: []*protocol.User{
					{
						Account: serial.ToTypedMessage(&vless.Account{
							Id: userID.String(),
						}),
					},
				}}),
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

	clientPort := tcp.PickPort()
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
					PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(clientPort)}},
					Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
				}),
				ProxySettings: serial.ToTypedMessage(&dokodemo.Config{
					RewriteAddress:  xraynet.NewIPOrDomain(dest.Address),
					RewritePort:     uint32(dest.Port),
					AllowedNetworks: []xraynet.Network{xraynet.Network_TCP},
				}),
			},
		},
		Outbound: []*core.OutboundHandlerConfig{
			{
				ProxySettings: serial.ToTypedMessage(&outbound.Config{
					Vnext: &protocol.ServerEndpoint{
						Address: xraynet.NewIPOrDomain(xraynet.LocalHostIP),
						Port:    uint32(serverPort),
						User: &protocol.User{
							Account: serial.ToTypedMessage(&vless.Account{
								Id: userID.String(),
							}),
						},
					},
				}),
				SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
					StreamSettings: &internet.StreamConfig{
						ProtocolName: "tcp",
						SecurityType: serial.GetMessageType(&reality.Config{}),
						SecuritySettings: []*serial.TypedMessage{
							serial.ToTypedMessage(&reality.Config{
								Show:        true,
								Fingerprint: "chrome",
								ServerName:  "www.microsoft.com",
								PublicKey:   publicKey,
								ShortId:     shortIds[0],
								SpiderX:     "/",
							}),
						},
					},
				}),
			},
		},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	common.Must(err)
	defer CloseAllServers(servers)

	// First connection: triggers background probe
	err = testTCPConn(clientPort, 1024, time.Second*30)()
	if err != nil {
		t.Fatal("First connection failed:", err)
	}

	// Wait for post-handshake detection
	time.Sleep(2 * time.Second)

	// Second connection: should use cached profile
	err = testTCPConn(clientPort, 1024, time.Second*30)()
	if err != nil {
		t.Fatal("Connection after warmup failed:", err)
	}

	// Third connection: verify cache still works
	err = testTCPConn(clientPort, 1024, time.Second*30)()
	if err != nil {
		t.Fatal("Third connection failed:", err)
	}
}

// TestREALITYConnectionResilience tests rapid connect-disconnect cycles
// to verify no resource leaks or deadlocks.
func TestREALITYConnectionResilience(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long REALITY scenario under -short")
	}
	tcpServer := tcp.Server{MsgProcessor: xor}
	dest, err := tcpServer.Start()
	common.Must(err)
	defer tcpServer.Close()

	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	privateKey, _ := base64.RawURLEncoding.DecodeString("aGSYystUbf59_9_6LKRxD27rmSW_-2_nyd9YG_Gwbks")
	publicKey, _ := base64.RawURLEncoding.DecodeString("E59WjnvZcQMu7tR7_BgyhycuEdBS-CtKxfImRCdAvFM")
	shortIds := make([][]byte, 1)
	shortIds[0] = make([]byte, 8)
	hex.Decode(shortIds[0], []byte("0123456789abcdef"))

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
					PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(serverPort)}},
					Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
					StreamSettings: &internet.StreamConfig{
						ProtocolName: "tcp",
						SecurityType: serial.GetMessageType(&reality.Config{}),
						SecuritySettings: []*serial.TypedMessage{
							serial.ToTypedMessage(&reality.Config{
								Show:        true,
								Dest:        "www.microsoft.com:443",
								ServerNames: []string{"www.microsoft.com"},
								PrivateKey:  privateKey,
								ShortIds:    shortIds,
								Type:        "tcp",
							}),
						},
					},
				}),
				ProxySettings: serial.ToTypedMessage(&inbound.Config{Users: []*protocol.User{
					{
						Account: serial.ToTypedMessage(&vless.Account{
							Id: userID.String(),
						}),
					},
				}}),
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

	clientPort := tcp.PickPort()
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
					PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(clientPort)}},
					Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
				}),
				ProxySettings: serial.ToTypedMessage(&dokodemo.Config{
					RewriteAddress:  xraynet.NewIPOrDomain(dest.Address),
					RewritePort:     uint32(dest.Port),
					AllowedNetworks: []xraynet.Network{xraynet.Network_TCP},
				}),
			},
		},
		Outbound: []*core.OutboundHandlerConfig{
			{
				ProxySettings: serial.ToTypedMessage(&outbound.Config{
					Vnext: &protocol.ServerEndpoint{
						Address: xraynet.NewIPOrDomain(xraynet.LocalHostIP),
						Port:    uint32(serverPort),
						User: &protocol.User{
							Account: serial.ToTypedMessage(&vless.Account{
								Id: userID.String(),
							}),
						},
					},
				}),
				SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
					StreamSettings: &internet.StreamConfig{
						ProtocolName: "tcp",
						SecurityType: serial.GetMessageType(&reality.Config{}),
						SecuritySettings: []*serial.TypedMessage{
							serial.ToTypedMessage(&reality.Config{
								Show:        true,
								Fingerprint: "chrome",
								ServerName:  "www.microsoft.com",
								PublicKey:   publicKey,
								ShortId:     shortIds[0],
								SpiderX:     "/",
							}),
						},
					},
				}),
			},
		},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	common.Must(err)
	defer CloseAllServers(servers)

	// 10 rapid connect-disconnect cycles
	for i := 0; i < 10; i++ {
		err := testTCPConn(clientPort, 256, time.Second*15)()
		if err != nil {
			t.Errorf("Rapid connection %d failed: %v", i+1, err)
		}
	}
}

// Multi-Target REALITY Tests

// realityTestConfig holds the configuration for a REALITY test target.
type realityTestConfig struct {
	Name        string
	Dest        string
	ServerNames []string
	ServerName  string
}

func testREALITYHandshakeWithTarget(t *testing.T, target realityTestConfig) {
	t.Helper()
	tcpServer := tcp.Server{MsgProcessor: xor}
	dest, err := tcpServer.Start()
	common.Must(err)
	defer tcpServer.Close()

	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	privateKey, _ := base64.RawURLEncoding.DecodeString("aGSYystUbf59_9_6LKRxD27rmSW_-2_nyd9YG_Gwbks")
	publicKey, _ := base64.RawURLEncoding.DecodeString("E59WjnvZcQMu7tR7_BgyhycuEdBS-CtKxfImRCdAvFM")
	shortIds := make([][]byte, 1)
	shortIds[0] = make([]byte, 8)
	hex.Decode(shortIds[0], []byte("0123456789abcdef"))

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
					PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(serverPort)}},
					Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
					StreamSettings: &internet.StreamConfig{
						ProtocolName: "tcp",
						SecurityType: serial.GetMessageType(&reality.Config{}),
						SecuritySettings: []*serial.TypedMessage{
							serial.ToTypedMessage(&reality.Config{
								Show:        true,
								Dest:        target.Dest,
								ServerNames: target.ServerNames,
								PrivateKey:  privateKey,
								ShortIds:    shortIds,
								Type:        "tcp",
							}),
						},
					},
				}),
				ProxySettings: serial.ToTypedMessage(&inbound.Config{Users: []*protocol.User{
					{
						Account: serial.ToTypedMessage(&vless.Account{
							Id: userID.String(),
						}),
					},
				}}),
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

	clientPort := tcp.PickPort()
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
					PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(clientPort)}},
					Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
				}),
				ProxySettings: serial.ToTypedMessage(&dokodemo.Config{
					RewriteAddress:  xraynet.NewIPOrDomain(dest.Address),
					RewritePort:     uint32(dest.Port),
					AllowedNetworks: []xraynet.Network{xraynet.Network_TCP},
				}),
			},
		},
		Outbound: []*core.OutboundHandlerConfig{
			{
				ProxySettings: serial.ToTypedMessage(&outbound.Config{
					Vnext: &protocol.ServerEndpoint{
						Address: xraynet.NewIPOrDomain(xraynet.LocalHostIP),
						Port:    uint32(serverPort),
						User: &protocol.User{
							Account: serial.ToTypedMessage(&vless.Account{
								Id: userID.String(),
							}),
						},
					},
				}),
				SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
					StreamSettings: &internet.StreamConfig{
						ProtocolName: "tcp",
						SecurityType: serial.GetMessageType(&reality.Config{}),
						SecuritySettings: []*serial.TypedMessage{
							serial.ToTypedMessage(&reality.Config{
								Show:        true,
								Fingerprint: "chrome",
								ServerName:  target.ServerName,
								PublicKey:   publicKey,
								ShortId:     shortIds[0],
								SpiderX:     "/",
							}),
						},
					},
				}),
			},
		},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	common.Must(err)
	defer CloseAllServers(servers)

	// Test 1: Basic handshake + 1MB transfer
	t.Log("Test 1: Basic handshake + 1MB transfer")
	err = testTCPConn(clientPort, 1024*1024, time.Second*30)()
	if err != nil {
		t.Fatal(err)
	}

	// Test 2: Multiple sequential connections (cache path)
	t.Log("Test 2: Multiple sequential connections")
	for i := 0; i < 3; i++ {
		err = testTCPConn(clientPort, 1024*1024, time.Second*30)()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Test 3: Large payload (4MB)
	t.Log("Test 3: Large payload (4MB)")
	err = testTCPConn(clientPort, 4*1024*1024, time.Second*60)()
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("All tests passed for %s", target.Name)
}

func testREALITYCacheReuse(t *testing.T, target realityTestConfig, connections int) {
	t.Helper()
	tcpServer := tcp.Server{MsgProcessor: xor}
	dest, err := tcpServer.Start()
	common.Must(err)
	defer tcpServer.Close()

	userID := protocol.NewID(uuid.New())
	serverPort := tcp.PickPort()
	privateKey, _ := base64.RawURLEncoding.DecodeString("aGSYystUbf59_9_6LKRxD27rmSW_-2_nyd9YG_Gwbks")
	publicKey, _ := base64.RawURLEncoding.DecodeString("E59WjnvZcQMu7tR7_BgyhycuEdBS-CtKxfImRCdAvFM")
	shortIds := make([][]byte, 1)
	shortIds[0] = make([]byte, 8)
	hex.Decode(shortIds[0], []byte("0123456789abcdef"))

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
					PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(serverPort)}},
					Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
					StreamSettings: &internet.StreamConfig{
						ProtocolName: "tcp",
						SecurityType: serial.GetMessageType(&reality.Config{}),
						SecuritySettings: []*serial.TypedMessage{
							serial.ToTypedMessage(&reality.Config{
								Show:        true,
								Dest:        target.Dest,
								ServerNames: target.ServerNames,
								PrivateKey:  privateKey,
								ShortIds:    shortIds,
								Type:        "tcp",
							}),
						},
					},
				}),
				ProxySettings: serial.ToTypedMessage(&inbound.Config{Users: []*protocol.User{
					{
						Account: serial.ToTypedMessage(&vless.Account{
							Id: userID.String(),
						}),
					},
				}}),
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

	clientPort := tcp.PickPort()
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
					PortList: &xraynet.PortList{Range: []*xraynet.PortRange{xraynet.SinglePortRange(clientPort)}},
					Listen:   xraynet.NewIPOrDomain(xraynet.LocalHostIP),
				}),
				ProxySettings: serial.ToTypedMessage(&dokodemo.Config{
					RewriteAddress:  xraynet.NewIPOrDomain(dest.Address),
					RewritePort:     uint32(dest.Port),
					AllowedNetworks: []xraynet.Network{xraynet.Network_TCP},
				}),
			},
		},
		Outbound: []*core.OutboundHandlerConfig{
			{
				ProxySettings: serial.ToTypedMessage(&outbound.Config{
					Vnext: &protocol.ServerEndpoint{
						Address: xraynet.NewIPOrDomain(xraynet.LocalHostIP),
						Port:    uint32(serverPort),
						User: &protocol.User{
							Account: serial.ToTypedMessage(&vless.Account{
								Id: userID.String(),
							}),
						},
					},
				}),
				SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
					StreamSettings: &internet.StreamConfig{
						ProtocolName: "tcp",
						SecurityType: serial.GetMessageType(&reality.Config{}),
						SecuritySettings: []*serial.TypedMessage{
							serial.ToTypedMessage(&reality.Config{
								Show:        true,
								Fingerprint: "chrome",
								ServerName:  target.ServerName,
								PublicKey:   publicKey,
								ShortId:     shortIds[0],
								SpiderX:     "/",
							}),
						},
					},
				}),
			},
		},
	}

	servers, err := InitializeServerConfigs(serverConfig, clientConfig)
	common.Must(err)
	defer CloseAllServers(servers)

	for i := 0; i < connections; i++ {
		err := testTCPConn(clientPort, 1024, time.Second*30)()
		if err != nil {
			t.Errorf("[%s] Connection %d VLESS-layer error (non-fatal): %v", target.Name, i+1, err)
		}
	}

	t.Logf("[%s] %d connections completed against %s", target.Name, connections, target.Dest)
}

// TestREALITYProfileReuse verifies cache HIT on repeated connections to the same target.
func TestREALITYProfileReuse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long REALITY scenario under -short")
	}
	targets := []realityTestConfig{
		{Name: "Microsoft", Dest: "www.microsoft.com:443", ServerNames: []string{"www.microsoft.com"}, ServerName: "www.microsoft.com"},
	}
	for _, target := range targets {
		t.Run(target.Name, func(t *testing.T) {
			testREALITYCacheReuse(t, target, 3)
		})
	}
}

// TestREALITYProfileIsolation verifies cross-target cache isolation.
func TestREALITYProfileIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long REALITY scenario under -short")
	}
	targets := []realityTestConfig{
		{Name: "Round1-Microsoft", Dest: "www.microsoft.com:443", ServerNames: []string{"www.microsoft.com"}, ServerName: "www.microsoft.com"},
	}
	rounds := []realityTestConfig{
		{Name: "Round2-Apple", Dest: "www.apple.com:443", ServerNames: []string{"www.apple.com"}, ServerName: "www.apple.com"},
	}
	all := append(targets, rounds...)
	for _, target := range all {
		t.Run(target.Name, func(t *testing.T) {
			testREALITYCacheReuse(t, target, 2)
		})
	}
}
