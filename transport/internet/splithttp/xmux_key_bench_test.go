package splithttp

import (
	"testing"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
)

func BenchmarkMuxDestIdentity(b *testing.B) {
	dests := []net.Destination{
		net.TCPDestination(net.IPAddress(net.ParseIP("1.2.3.4")), 443),
		net.TCPDestination(net.IPAddress(net.ParseIP("2001:db8::1")), 8443),
		net.TCPDestination(net.DomainAddress("www.example.com"), 443),
		net.UDPDestination(net.IPAddress(net.ParseIP("10.0.0.1")), 53),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, d := range dests {
			_ = muxDestIdentity(d)
		}
	}
}

func BenchmarkMuxDestIdentityWithOriginalDomain(b *testing.B) {
	d := net.TCPDestination(net.IPAddress(net.ParseIP("1.2.3.4")), 443)
	d.OriginalDomain = "github.com"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = muxDestIdentity(d)
	}
}

func BenchmarkNewMuxKey(b *testing.B) {
	dest := net.TCPDestination(net.DomainAddress("example.com"), 443)
	settings := &internet.MemoryStreamConfig{
		ProtocolName: "xhttp",
		ProtocolSettings: &Config{
			Host: "example.com",
			Path: "/s",
			Mode: "auto",
		},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = newMuxKey(dest, settings)
	}
}
