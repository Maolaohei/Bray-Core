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
	"github.com/xtls/xray-core/features/dns"
)

// testQUICEndpoints are AliDNS's hostname and direct-IP DoQ endpoints. Both
// are reachable from domestic and international networks; AdGuard is not a
// reliable test dependency in China.
var testQUICEndpoints = []string{
	"quic://dns.alidns.com",
	"quic://223.6.6.6",
}

// testQUICQueryName is a domestic domain suitable for the normal DoQ cases.
const testQUICQueryName = "baidu.com"

// AliDNS's domestic answer for baidu.com has no AAAA record, so the IPv6
// override regression needs an explicitly dual-stack name.
const testQUICIPv6QueryName = "dns.alidns.com"

func forEachTestQUICServer(t *testing.T, fn func(*testing.T, *QUICNameServer)) {
	t.Helper()
	for _, endpoint := range testQUICEndpoints {
		endpoint := endpoint
		t.Run(endpoint, func(t *testing.T) {
			url, err := url.Parse(endpoint)
			common.Must(err)
			s, err := NewQUICNameServer(url, false, false, 0, net.IP(nil))
			common.Must(err)
			fn(t, s)
		})
	}
}

func TestQUICNameServer(t *testing.T) {
	if testing.Short() {
		t.Skip("network-dependent: skipped in -short mode (CI)")
	}
	forEachTestQUICServer(t, func(t *testing.T, s *QUICNameServer) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		ips, _, err := s.QueryIP(ctx, testQUICQueryName, dns.IPOption{
			IPv4Enable: true,
			IPv6Enable: true,
		})
		cancel()
		common.Must(err)
		if len(ips) == 0 {
			t.Error("expect some ips, but got 0")
		}
		ctx2, cancel := context.WithTimeout(context.Background(), time.Second*5)
		ips2, _, err := s.QueryIP(ctx2, testQUICQueryName, dns.IPOption{
			IPv4Enable: true,
			IPv6Enable: true,
		})
		cancel()
		common.Must(err)
		if r := cmp.Diff(ips2, ips); r != "" {
			t.Fatal(r)
		}
	})
}

func TestQUICNameServerWithIPv4Override(t *testing.T) {
	if testing.Short() {
		t.Skip("network-dependent: skipped in -short mode (CI)")
	}
	forEachTestQUICServer(t, func(t *testing.T, s *QUICNameServer) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		ips, _, err := s.QueryIP(ctx, testQUICQueryName, dns.IPOption{
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
	})
}

func TestQUICNameServerWithIPv6Override(t *testing.T) {
	if testing.Short() {
		t.Skip("network-dependent: skipped in -short mode (CI)")
	}
	forEachTestQUICServer(t, func(t *testing.T, s *QUICNameServer) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		ips, _, err := s.QueryIP(ctx, testQUICIPv6QueryName, dns.IPOption{
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
	})
}
