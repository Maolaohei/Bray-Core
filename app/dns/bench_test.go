package dns

import (
	"context"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	dns_feature "github.com/xtls/xray-core/features/dns"
	"golang.org/x/net/dns/dnsmessage"
)

// fakeCachedNS is a cache-only CachedNameserver for cache-hit benchmarks.
type fakeCachedNS struct{ cc *CacheController }

func (f *fakeCachedNS) getCacheController() *CacheController {
	if f.cc == nil {
		f.cc = NewCacheController("bench", false, false, 0)
	}
	return f.cc
}

func (f *fakeCachedNS) sendQuery(ctx context.Context, noResponseErrCh chan<- error, fqdn string, option dns_feature.IPOption) {
}

func BenchmarkParseResponse(b *testing.B) {
	ans := new(dns.Msg)
	ans.Id = 1
	ans.Answer = append(ans.Answer,
		common.Must2(dns.NewRR("google.com. IN A 8.8.8.8")),
		common.Must2(dns.NewRR("google.com. IN A 8.8.4.4")),
		common.Must2(dns.NewRR("google.com. IN AAAA 2001:4860:4860::8888")),
	)
	payload := common.Must2(ans.Pack())

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = parseResponse(payload)
	}
}

func BenchmarkBuildReqMsgs(b *testing.B) {
	stubID := func() uint16 { return 1 }
	opt := dns_feature.IPOption{IPv4Enable: true, IPv6Enable: true}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		reqs, _ := buildReqMsgs("google.com", opt, stubID, nil)
		if len(reqs) != 1 {
			b.Fatal("expected 1 request")
		}
	}
}

func BenchmarkBuildReqMsgsIPv4Only(b *testing.B) {
	stubID := func() uint16 { return 1 }
	opt := dns_feature.IPOption{IPv4Enable: true, IPv6Enable: false}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		reqs, _ := buildReqMsgs("google.com", opt, stubID, nil)
		if len(reqs) != 1 {
			b.Fatal("expected 1 request")
		}
	}
}

func BenchmarkGenEDNS0Options(b *testing.B) {
	clientIP := net.ParseIP("1.2.3.4")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = genEDNS0Options(clientIP, 200)
	}
}

func BenchmarkGenEDNS0OptionsNoIP(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = genEDNS0Options(nil, 200)
	}
}

func BenchmarkFqdn(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Fqdn("google.com")
	}
}

func BenchmarkRecordAlloc(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r := &record{
			A: &IPRecord{
				IP:     []net.IP{net.ParseIP("8.8.8.8")},
				Expire: time.Now().Add(time.Hour),
			},
			AAAA: &IPRecord{
				IP:     []net.IP{net.ParseIP("2001:4860:4860::8888")},
				Expire: time.Now().Add(time.Hour),
			},
		}
		_ = r
	}
}

// BenchmarkQueryIPCacheHit measures the DNS cache-hit hot path: findRecords
// lookup + merge + getIPs, no network I/O. Round-3 optimization target.
func BenchmarkQueryIPCacheHit(b *testing.B) {
	ns := &fakeCachedNS{}
	cache := ns.getCacheController()
	fqdn := "www.example.com."
	cache.updateRecord(&dnsRequest{reqType: dnsmessage.TypeA, domain: fqdn, start: time.Now()}, &IPRecord{
		IP:     []net.IP{net.ParseIP("8.8.8.8")},
		Expire: time.Now().Add(time.Hour),
	})
	cache.updateRecord(&dnsRequest{reqType: dnsmessage.TypeAAAA, domain: fqdn, start: time.Now()}, &IPRecord{
		IP:     []net.IP{net.ParseIP("2001:4860:4860::8888")},
		Expire: time.Now().Add(time.Hour),
	})
	opt := dns_feature.IPOption{IPv4Enable: true, IPv6Enable: true}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := queryIP(context.Background(), ns, "www.example.com", opt); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFindRecordsHit isolates the cache lookup itself (RLock + map hit).
func BenchmarkFindRecordsHit(b *testing.B) {
	cache := NewCacheController("bench", false, false, 0)
	cache.updateRecord(&dnsRequest{reqType: dnsmessage.TypeA, domain: "www.example.com.", start: time.Now()}, &IPRecord{
		IP:     []net.IP{net.ParseIP("8.8.8.8")},
		Expire: time.Now().Add(time.Hour),
	})
	cache.updateRecord(&dnsRequest{reqType: dnsmessage.TypeAAAA, domain: "www.example.com.", start: time.Now()}, &IPRecord{
		IP:     []net.IP{net.ParseIP("2001:4860:4860::8888")},
		Expire: time.Now().Add(time.Hour),
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if cache.findRecords("www.example.com.") == nil {
			b.Fatal("miss")
		}
	}
}
