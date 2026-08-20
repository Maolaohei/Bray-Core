package dns_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	. "github.com/xtls/xray-core/app/dns"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	dns_feature "github.com/xtls/xray-core/features/dns"
)

// testDOHEndpoint is AliDNS and is reachable from both domestic and
// international networks; Cloudflare is not a reliable test dependency in China.
const testDOHEndpoint = "https://223.6.6.6/dns-query"

// testDOHQueryName is a domestic domain suitable for the normal DoH cases.
const testDOHQueryName = "baidu.com"

// AliDNS's domestic answer for baidu.com has no AAAA record, so the IPv6
// override regression needs an explicitly dual-stack name.
const testDOHIPv6QueryName = "dns.alidns.com"

func TestDOHNameServer(t *testing.T) {
	if testing.Short() {
		t.Skip("network-dependent: skipped in -short mode (CI)")
	}
	url, err := url.Parse(testDOHEndpoint)
	common.Must(err)

	s := NewDoHNameServer(url, nil, false, false, false, 0, net.IP(nil))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	ips, _, err := s.QueryIP(ctx, testDOHQueryName, dns_feature.IPOption{
		IPv4Enable: true,
		IPv6Enable: true,
	})
	cancel()
	common.Must(err)
	if len(ips) == 0 {
		t.Error("expect some ips, but got 0")
	}
}

func TestDOHNameServerWithCache(t *testing.T) {
	if testing.Short() {
		t.Skip("network-dependent: skipped in -short mode (CI)")
	}
	url, err := url.Parse(testDOHEndpoint)
	common.Must(err)

	s := NewDoHNameServer(url, nil, false, false, false, 0, net.IP(nil))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	ips, _, err := s.QueryIP(ctx, testDOHQueryName, dns_feature.IPOption{
		IPv4Enable: true,
		IPv6Enable: true,
	})
	cancel()
	common.Must(err)
	if len(ips) == 0 {
		t.Error("expect some ips, but got 0")
	}

	ctx2, cancel := context.WithTimeout(context.Background(), time.Second*5)
	ips2, _, err := s.QueryIP(ctx2, testDOHQueryName, dns_feature.IPOption{
		IPv4Enable: true,
		IPv6Enable: true,
	})
	cancel()
	common.Must(err)
	if r := cmp.Diff(ips2, ips); r != "" {
		t.Fatal(r)
	}
}

func TestDOHNameServerWithIPv4Override(t *testing.T) {
	if testing.Short() {
		t.Skip("network-dependent: skipped in -short mode (CI)")
	}
	url, err := url.Parse(testDOHEndpoint)
	common.Must(err)

	s := NewDoHNameServer(url, nil, false, false, false, 0, net.IP(nil))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	ips, _, err := s.QueryIP(ctx, testDOHQueryName, dns_feature.IPOption{
		IPv4Enable: true,
		IPv6Enable: false,
	})
	cancel()
	common.Must(err)
	if len(ips) == 0 {
		t.Error("expect some ips, but got 0")
	}

	for _, ip := range ips {
		if len(ip) != net.IPv4len {
			t.Error("expect only IPv4 response from DNS query")
		}
	}
}

func TestDOHNameServerWithIPv6Override(t *testing.T) {
	if testing.Short() {
		t.Skip("network-dependent: skipped in -short mode (CI)")
	}
	url, err := url.Parse(testDOHEndpoint)
	common.Must(err)

	s := NewDoHNameServer(url, nil, false, false, false, 0, net.IP(nil))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	ips, _, err := s.QueryIP(ctx, testDOHIPv6QueryName, dns_feature.IPOption{
		IPv4Enable: false,
		IPv6Enable: true,
	})
	cancel()
	common.Must(err)
	if len(ips) == 0 {
		t.Error("expect some ips, but got 0")
	}

	for _, ip := range ips {
		if len(ip) != net.IPv6len {
			t.Error("expect only IPv6 response from DNS query")
		}
	}
}
