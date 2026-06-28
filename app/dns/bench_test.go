package dns

import (
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	dns_feature "github.com/xtls/xray-core/features/dns"
)

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
		for _, r := range reqs {
			releaseDnsRequest(r)
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
		for _, r := range reqs {
			releaseDnsRequest(r)
		}
	}
}

func BenchmarkGenEDNS0Options(b *testing.B) {
	clientIP := net.ParseIP("1.2.3.4")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		opt := genEDNS0Options(clientIP, 200)
		releaseOptResource(opt)
	}
}

func BenchmarkGenEDNS0OptionsNoIP(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		opt := genEDNS0Options(nil, 200)
		releaseOptResource(opt)
	}
}

func BenchmarkFqdn(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Fqdn("google.com")
	}
}

func BenchmarkRecordPool(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r := recordObjPool.get()
		r.A = &IPRecord{
			IP:     []net.IP{net.ParseIP("8.8.8.8")},
			Expire: time.Now().Add(time.Hour),
		}
		r.AAAA = &IPRecord{
			IP:     []net.IP{net.ParseIP("2001:4860:4860::8888")},
			Expire: time.Now().Add(time.Hour),
		}
		recordObjPool.put(r)
	}
}
