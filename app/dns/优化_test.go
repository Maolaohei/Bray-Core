package dns_test

import (
	stdnet "net"
	"strconv"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/xtls/xray-core/app/dispatcher"
	appdns "github.com/xtls/xray-core/app/dns"
	"github.com/xtls/xray-core/app/policy"
	"github.com/xtls/xray-core/app/proxyman"
	_ "github.com/xtls/xray-core/app/proxyman/outbound"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/geodata"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	featuredns "github.com/xtls/xray-core/features/dns"
	"github.com/xtls/xray-core/proxy/freedom"
	"github.com/xtls/xray-core/testing/servers/udp"
)

type returnsOnly114Handler struct{}

func (*returnsOnly114Handler) ServeDNS(w dns.ResponseWriter, r *dns.Msg) {
	ans := new(dns.Msg)
	ans.Id = r.Id
	ans.Question = r.Question // RFC 1035: echo the question; RFC 5452 guard relies on it
	ans.RecursionAvailable = true
	for _, q := range r.Question {
		if q.Qtype == dns.TypeA {
			rr, _ := dns.NewRR(q.Name + " IN A 1.1.1.1")
			ans.Answer = append(ans.Answer, rr)
		}
	}
	w.WriteMsg(ans)
}

func TestErrEmptyResponseFallsBackToSecondDNS(t *testing.T) {
	port := udp.PickPort()

	dnsServer := dns.Server{
		Addr:    "127.0.0.1:" + port.String(),
		Net:     "udp",
		Handler: &returnsOnly114Handler{},
	}
	go dnsServer.ListenAndServe()
	time.Sleep(time.Second)

	config := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&appdns.Config{
				NameServer: []*appdns.NameServer{
					{
						Address: &net.Endpoint{
							Network: net.Network_UDP,
							Address: &net.IPOrDomain{
								Address: &net.IPOrDomain_Ip{Ip: []byte{127, 0, 0, 1}},
							},
							Port: uint32(port),
						},
						ExpectedIp: []*geodata.IPRule{
							{Value: &geodata.IPRule_Custom{Custom: &geodata.CIDRRule{Cidr: &geodata.CIDR{Ip: []byte{2, 2, 2, 2}, Prefix: 32}}}},
						},
					},
					{
						Address: &net.Endpoint{
							Network: net.Network_UDP,
							Address: &net.IPOrDomain{
								Address: &net.IPOrDomain_Ip{Ip: []byte{127, 0, 0, 1}},
							},
							Port: uint32(port),
						},
					},
				},
			}),
			serial.ToTypedMessage(&dispatcher.Config{}),
			serial.ToTypedMessage(&proxyman.OutboundConfig{}),
			serial.ToTypedMessage(&policy.Config{}),
		},
		Outbound: []*core.OutboundHandlerConfig{
			{
				ProxySettings: serial.ToTypedMessage(&freedom.Config{
					FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}},
				}),
			},
		},
	}

	v, err := core.New(config)
	common.Must(err)
	defer v.Close()

	client := v.GetFeature(featuredns.ClientType()).(featuredns.Client)

	ips, _, err := client.LookupIP("example.com", featuredns.IPOption{
		IPv4Enable: true, IPv6Enable: true, FakeEnable: false,
	})
	if err != nil {
		t.Fatal("expected success after fallback, got:", err)
	}
	if len(ips) == 0 {
		t.Fatal("expected IPs from second server")
	}
}

func TestWarmupCompletesBeforeLookup(t *testing.T) {
	port := udp.PickPort()

	dnsServer := dns.Server{
		Addr:    "127.0.0.1:" + port.String(),
		Net:     "udp",
		Handler: &staticHandler{},
	}
	go dnsServer.ListenAndServe()
	time.Sleep(time.Second)

	config := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&appdns.Config{
				NameServer: []*appdns.NameServer{
					{
						Address: &net.Endpoint{
							Network: net.Network_UDP,
							Address: &net.IPOrDomain{
								Address: &net.IPOrDomain_Ip{Ip: []byte{127, 0, 0, 1}},
							},
							Port: uint32(port),
						},
					},
				},
			}),
			serial.ToTypedMessage(&dispatcher.Config{}),
			serial.ToTypedMessage(&proxyman.OutboundConfig{}),
			serial.ToTypedMessage(&policy.Config{}),
		},
		Outbound: []*core.OutboundHandlerConfig{
			{
				ProxySettings: serial.ToTypedMessage(&freedom.Config{
					FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}},
				}),
			},
		},
	}

	v, err := core.New(config)
	common.Must(err)
	defer v.Close()

	client := v.GetFeature(featuredns.ClientType()).(featuredns.Client)

	type warmupAPI interface {
		SetWarmupDomains([]string)
		WarmupNow()
	}
	ws, ok := client.(warmupAPI)
	if !ok {
		t.Skip("client does not support warmup")
	}
	ws.SetWarmupDomains([]string{"google.com"})
	ws.WarmupNow()
	time.Sleep(2 * time.Second)

	start := time.Now()
	ips, _, err := client.LookupIP("google.com", featuredns.IPOption{
		IPv4Enable: true, IPv6Enable: false, FakeEnable: false,
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatal(err)
	}
	if len(ips) == 0 {
		t.Fatal("expected IPs from warmup cache")
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("cached lookup took %v, expected < 100ms", elapsed)
	}
}

func TestRecordPoolReuse(t *testing.T) {
	port := udp.PickPort()

	dnsServer := dns.Server{
		Addr:    "127.0.0.1:" + port.String(),
		Net:     "udp",
		Handler: &staticHandler{},
	}
	go dnsServer.ListenAndServe()
	time.Sleep(time.Second)

	config := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&appdns.Config{
				NameServer: []*appdns.NameServer{
					{
						Address: &net.Endpoint{
							Network: net.Network_UDP,
							Address: &net.IPOrDomain{
								Address: &net.IPOrDomain_Ip{Ip: []byte{127, 0, 0, 1}},
							},
							Port: uint32(port),
						},
					},
				},
			}),
			serial.ToTypedMessage(&dispatcher.Config{}),
			serial.ToTypedMessage(&proxyman.OutboundConfig{}),
			serial.ToTypedMessage(&policy.Config{}),
		},
		Outbound: []*core.OutboundHandlerConfig{
			{
				ProxySettings: serial.ToTypedMessage(&freedom.Config{
					FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}},
				}),
			},
		},
	}

	v, err := core.New(config)
	common.Must(err)
	defer v.Close()

	client := v.GetFeature(featuredns.ClientType()).(featuredns.Client)

	ips1, _, err := client.LookupIP("google.com", featuredns.IPOption{
		IPv4Enable: true, IPv6Enable: false, FakeEnable: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ips1) == 0 {
		t.Fatal("first query returned no IPs")
	}

	ips2, _, err := client.LookupIP("google.com", featuredns.IPOption{
		IPv4Enable: true, IPv6Enable: false, FakeEnable: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ips2) == 0 {
		t.Fatal("second query returned no IPs (cache might be corrupt)")
	}
}

func TestTCPLocalNameServerWithConnectionPool(t *testing.T) {
	// Hold the TCP listener for the whole test so the port can never be stolen
	// between a probe (listen+close) and Serve. The old udp.PickPort() +
	// ListenAndServe + fixed 1s sleep had a race window that flaked on
	// parallel CI runners: "dial tcp ... connectex: actively refused".
	ln, err := stdnet.Listen("tcp", "127.0.0.1:0")
	common.Must(err)
	port := strconv.Itoa(ln.Addr().(*stdnet.TCPAddr).Port)

	dnsServer := dns.Server{
		Listener: ln,
		Handler:  &staticHandler{},
	}
	go func() { _ = dnsServer.ActivateAndServe() }()

	config := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&appdns.Config{
				NameServer: []*appdns.NameServer{
					{
						Address: &net.Endpoint{
							Network: net.Network_TCP,
							Address: &net.IPOrDomain{
								Address: &net.IPOrDomain_Domain{Domain: "tcp+local://127.0.0.1:" + port},
							},
						},
					},
				},
			}),
			serial.ToTypedMessage(&dispatcher.Config{}),
			serial.ToTypedMessage(&proxyman.OutboundConfig{}),
			serial.ToTypedMessage(&policy.Config{}),
		},
		Outbound: []*core.OutboundHandlerConfig{
			{
				ProxySettings: serial.ToTypedMessage(&freedom.Config{
					FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}},
				}),
			},
		},
	}

	v, err := core.New(config)
	common.Must(err)
	defer v.Close()

	client := v.GetFeature(featuredns.ClientType()).(featuredns.Client)

	for _, domain := range []string{"google.com", "api.google.com", "ipv6.google.com"} {
		ips, _, err := client.LookupIP(domain, featuredns.IPOption{
			IPv4Enable: true, IPv6Enable: false, FakeEnable: false,
		})
		if err != nil {
			t.Errorf("query %s: %v", domain, err)
			continue
		}
		if len(ips) == 0 {
			t.Errorf("query %s: no IPs returned", domain)
		}
	}
}

func TestGracefulShutdownDrainsQueries(t *testing.T) {
	port := udp.PickPort()

	dnsServer := dns.Server{
		Addr:    "127.0.0.1:" + port.String(),
		Net:     "udp",
		Handler: &staticHandler{},
	}
	go dnsServer.ListenAndServe()
	time.Sleep(time.Second)

	config := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&appdns.Config{
				NameServer: []*appdns.NameServer{
					{
						Address: &net.Endpoint{
							Network: net.Network_UDP,
							Address: &net.IPOrDomain{
								Address: &net.IPOrDomain_Ip{Ip: []byte{127, 0, 0, 1}},
							},
							Port: uint32(port),
						},
					},
				},
			}),
			serial.ToTypedMessage(&dispatcher.Config{}),
			serial.ToTypedMessage(&proxyman.OutboundConfig{}),
			serial.ToTypedMessage(&policy.Config{}),
		},
		Outbound: []*core.OutboundHandlerConfig{
			{
				ProxySettings: serial.ToTypedMessage(&freedom.Config{
					FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}},
				}),
			},
		},
	}

	v, err := core.New(config)
	common.Must(err)

	// Fire a lookup to populate cache and ensure system is operational
	client := v.GetFeature(featuredns.ClientType()).(featuredns.Client)
	ips, _, err := client.LookupIP("google.com", featuredns.IPOption{
		IPv4Enable: true, IPv6Enable: false, FakeEnable: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ips) == 0 {
		t.Fatal("no IPs")
	}

	// Close should complete within a few seconds (not hang)
	closeStart := time.Now()
	err = v.Close()
	closeElapsed := time.Since(closeStart)

	if err != nil {
		t.Fatal("Close returned error:", err)
	}
	if closeElapsed > 5*time.Second {
		t.Errorf("Close took %v, expected < 5s", closeElapsed)
	}
}
