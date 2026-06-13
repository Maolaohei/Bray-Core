package internet

import (
	"context"
	"net"
	"sync"
	"testing"

	xnet "github.com/xtls/xray-core/common/net"
)

type benchDNSResolver struct {
	ips []net.IP
	err error
}

func (r *benchDNSResolver) LookupIP(domain string) ([]net.IP, error) {
	return r.ips, r.err
}

func benchDialFunc(ctx context.Context, dest xnet.Destination, sockopt *SocketConfig) (xnet.Conn, error) {
	return nil, nil
}

func BenchmarkWarmupPipelineCreation(b *testing.B) {
	resolver := &benchDNSResolver{
		ips: []net.IP{net.ParseIP("1.1.1.1"), net.ParseIP("8.8.8.8")},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewWarmupPipeline(resolver, benchDialFunc, nil, "example.com", xnet.Port(443))
	}
}

func BenchmarkWarmupPipelineDNSResolution(b *testing.B) {
	resolver := &benchDNSResolver{
		ips: []net.IP{net.ParseIP("1.1.1.1"), net.ParseIP("8.8.8.8"), net.ParseIP("2001:db8::1")},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = resolver.LookupIP("example.com")
	}
}

func BenchmarkWarmupManagerConcurrent(b *testing.B) {
	resolver := &benchDNSResolver{
		ips: []net.IP{net.ParseIP("1.1.1.1")},
	}
	sockopt := &SocketConfig{
		HappyEyeballs: &HappyEyeballsConfig{
			PrioritizeIpv6:   false,
			TryDelayMs:       250,
			MaxConcurrentTry: 4,
			V3Enabled:        true,
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m := NewWarmupManager(resolver, benchDialFunc, sockopt)
		var wg sync.WaitGroup
		for j := 0; j < 10; j++ {
			wg.Add(1)
			go func(domain string) {
				defer wg.Done()
				m.WarmupDomain(context.Background(), domain, xnet.Port(443))
			}("example" + string(rune('a'+j%26)) + ".com")
		}
		wg.Wait()
	}
}

func BenchmarkSortIPsV2Warmup(b *testing.B) {
	ips := []net.IP{
		net.ParseIP("192.168.1.1"),
		net.ParseIP("192.168.1.2"),
		net.ParseIP("2001:db8::1"),
		net.ParseIP("2001:db8::2"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sortIPs(ips, false, 1)
	}
}

func BenchmarkIsIP(b *testing.B) {
	strs := []string{"1.1.1.1", "example.com", "::1", "2001:db8::1", "192.168.1.1", ""}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		isIP(strs[i%len(strs)])
	}
}

func BenchmarkHappyEyeballsScoreIPsWarmup(b *testing.B) {
	ips := make([]net.IP, 10)
	for i := range ips {
		if i%2 == 0 {
			ips[i] = net.IPv4(10, 0, 0, byte(i+1))
		} else {
			ips[i] = net.ParseIP("2001:db8::" + string(rune('0'+i%10)))
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scoreIPs(ips, false, nil)
	}
}

func BenchmarkWarmupPipelineParallel(b *testing.B) {
	resolver := &benchDNSResolver{
		ips: []net.IP{net.ParseIP("1.1.1.1"), net.ParseIP("8.8.8.8")},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = NewWarmupPipeline(resolver, benchDialFunc, nil, "example.com", xnet.Port(443))
		}
	})
}
