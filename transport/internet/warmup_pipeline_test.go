package internet

import (
	"context"
	"errors"
	stdnet "net"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
)

type mockDNSResolver struct {
	ips map[string][]stdnet.IP
	err error
}

func (m *mockDNSResolver) LookupIP(domain string) ([]stdnet.IP, error) {
	if m.err != nil {
		return nil, m.err
	}
	ips, ok := m.ips[domain]
	if !ok {
		return nil, errors.New("domain not found")
	}
	return ips, nil
}

type mockConn struct {
	addr stdnet.Addr
}

func (c *mockConn) Read(b []byte) (int, error)         { return 0, nil }
func (c *mockConn) Write(b []byte) (int, error)        { return len(b), nil }
func (c *mockConn) Close() error                       { return nil }
func (c *mockConn) LocalAddr() stdnet.Addr              { return c.addr }
func (c *mockConn) RemoteAddr() stdnet.Addr             { return c.addr }
func (c *mockConn) SetDeadline(t time.Time) error      { return nil }
func (c *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *mockConn) SetWriteDeadline(t time.Time) error { return nil }

func TestWarmupPipeline_Success(t *testing.T) {
	dns := &mockDNSResolver{
		ips: map[string][]stdnet.IP{
			"example.com": {stdnet.ParseIP("1.2.3.4"), stdnet.ParseIP("5.6.7.8")},
		},
	}

	dialFunc := func(ctx context.Context, dest net.Destination, sockopt *SocketConfig) (stdnet.Conn, error) {
		return &mockConn{addr: &stdnet.TCPAddr{IP: dest.Address.IP(), Port: int(dest.Port)}}, nil
	}

	p := NewWarmupPipeline(dns, dialFunc, nil, "example.com", net.Port(443))
	p.Run(context.Background())

	<-p.Done()
	result := p.Result()

	if result.Err != nil {
		t.Fatalf("expected no error, got %v", result.Err)
	}
	if len(result.IPs) != 2 {
		t.Fatalf("expected 2 IPs, got %d", len(result.IPs))
	}
	if result.Conn == nil {
		t.Fatal("expected connection, got nil")
	}
}

func TestWarmupPipeline_DNSFailure(t *testing.T) {
	dns := &mockDNSResolver{
		err: errors.New("DNS resolution failed"),
	}

	dialFunc := func(ctx context.Context, dest net.Destination, sockopt *SocketConfig) (stdnet.Conn, error) {
		t.Fatal("dial should not be called on DNS failure")
		return nil, nil
	}

	p := NewWarmupPipeline(dns, dialFunc, nil, "example.com", net.Port(443))
	p.Run(context.Background())

	<-p.Done()
	result := p.Result()

	if result.Err == nil {
		t.Fatal("expected error, got nil")
	}
	if result.Conn != nil {
		t.Fatal("expected nil connection on DNS failure")
	}
}

func TestWarmupPipeline_NoIPs(t *testing.T) {
	dns := &mockDNSResolver{
		ips: map[string][]stdnet.IP{
			"example.com": {},
		},
	}

	dialFunc := func(ctx context.Context, dest net.Destination, sockopt *SocketConfig) (net.Conn, error) {
		t.Fatal("dial should not be called with no IPs")
		return nil, nil
	}

	p := NewWarmupPipeline(dns, dialFunc, nil, "example.com", net.Port(443))
	p.Run(context.Background())

	<-p.Done()
	result := p.Result()

	if result.Err == nil {
		t.Fatal("expected error for no IPs, got nil")
	}
}

func TestWarmupPipeline_DialFailure(t *testing.T) {
	dns := &mockDNSResolver{
		ips: map[string][]stdnet.IP{
			"example.com": {stdnet.ParseIP("1.2.3.4")},
		},
	}

	dialFunc := func(ctx context.Context, dest net.Destination, sockopt *SocketConfig) (net.Conn, error) {
		return nil, errors.New("connection refused")
	}

	p := NewWarmupPipeline(dns, dialFunc, nil, "example.com", net.Port(443))
	p.Run(context.Background())

	<-p.Done()
	result := p.Result()

	if result.Err == nil {
		t.Fatal("expected error when all dials fail, got nil")
	}
}

func TestWarmupPipeline_PartialDialFailure(t *testing.T) {
	dns := &mockDNSResolver{
		ips: map[string][]stdnet.IP{
			"example.com": {stdnet.ParseIP("1.2.3.4"), stdnet.ParseIP("5.6.7.8")},
		},
	}

	callCount := 0
	dialFunc := func(ctx context.Context, dest net.Destination, sockopt *SocketConfig) (net.Conn, error) {
		callCount++
		if callCount == 1 {
			return nil, errors.New("first dial fails")
		}
		return &mockConn{addr: &stdnet.TCPAddr{IP: dest.Address.IP(), Port: int(dest.Port)}}, nil
	}

	p := NewWarmupPipeline(dns, dialFunc, nil, "example.com", net.Port(443))
	p.Run(context.Background())

	<-p.Done()
	result := p.Result()

	if result.Err != nil {
		t.Fatalf("expected no error, got %v", result.Err)
	}
	if result.Conn == nil {
		t.Fatal("expected connection after partial failure, got nil")
	}
}

func TestWarmupManager(t *testing.T) {
	dns := &mockDNSResolver{
		ips: map[string][]stdnet.IP{
			"example.com": {stdnet.ParseIP("1.2.3.4")},
		},
	}

	dialFunc := func(ctx context.Context, dest net.Destination, sockopt *SocketConfig) (net.Conn, error) {
		return &mockConn{addr: &stdnet.TCPAddr{IP: dest.Address.IP(), Port: int(dest.Port)}}, nil
	}

	m := NewWarmupManager(dns, dialFunc, nil)

	// GetWarmConn returns nil for unknown domain
	if conn := m.GetWarmConn("unknown.com"); conn != nil {
		t.Fatal("expected nil for unknown domain")
	}

	// WarmupDomain starts warmup
	m.WarmupDomain(context.Background(), "example.com", net.Port(443))

	// Wait a bit for completion
	time.Sleep(100 * time.Millisecond)

	// Now should return a connection
	conn := m.GetWarmConn("example.com")
	if conn == nil {
		t.Fatal("expected connection after warmup, got nil")
	}
}

func TestWarmupManager_Dedup(t *testing.T) {
	dns := &mockDNSResolver{
		ips: map[string][]stdnet.IP{
			"example.com": {stdnet.ParseIP("1.2.3.4")},
		},
	}

	var dialCount int32
	dialFunc := func(ctx context.Context, dest net.Destination, sockopt *SocketConfig) (net.Conn, error) {
		dialCount++
		return &mockConn{addr: &stdnet.TCPAddr{IP: dest.Address.IP(), Port: int(dest.Port)}}, nil
	}

	m := NewWarmupManager(dns, dialFunc, nil)

	// Warmup same domain twice
	m.WarmupDomain(context.Background(), "example.com", net.Port(443))
	m.WarmupDomain(context.Background(), "example.com", net.Port(443))

	time.Sleep(100 * time.Millisecond)

	// Should only dial once
	if dialCount != 1 {
		t.Fatalf("expected 1 dial call, got %d", dialCount)
	}
}

func TestWarmupManager_Concurrent(t *testing.T) {
	dns := &mockDNSResolver{
		ips: map[string][]stdnet.IP{
			"a.com": {stdnet.ParseIP("1.1.1.1")},
			"b.com": {stdnet.ParseIP("2.2.2.2")},
			"c.com": {stdnet.ParseIP("3.3.3.3")},
		},
	}

	dialFunc := func(ctx context.Context, dest net.Destination, sockopt *SocketConfig) (net.Conn, error) {
		return &mockConn{addr: &stdnet.TCPAddr{IP: dest.Address.IP(), Port: int(dest.Port)}}, nil
	}

	m := NewWarmupManager(dns, dialFunc, nil)

	var wg sync.WaitGroup
	domains := []string{"a.com", "b.com", "c.com"}
	for _, d := range domains {
		wg.Add(1)
		go func(domain string) {
			defer wg.Done()
			m.WarmupDomain(context.Background(), domain, net.Port(443))
		}(d)
	}
	wg.Wait()

	time.Sleep(100 * time.Millisecond)

	for _, d := range domains {
		if conn := m.GetWarmConn(d); conn == nil {
			t.Fatalf("expected connection for %s, got nil", d)
		}
	}
}

func TestSortIPs(t *testing.T) {
	ips := []stdnet.IP{
		stdnet.ParseIP("2001:db8::1"),
		stdnet.ParseIP("1.2.3.4"),
		stdnet.ParseIP("5.6.7.8"),
	}

	sorted := sortIPs(ips, false, 1)

	// With interleave=1, IPv4 and IPv6 should be interleaved
	// First IP should be IPv4 when not prioritizing IPv6
	if len(sorted) != 3 {
		t.Fatalf("expected 3 IPs, got %d", len(sorted))
	}

	// Check that IPv4 comes first
	if sorted[0].To4() == nil {
		t.Fatalf("expected IPv4 first, got %s", sorted[0])
	}
}

func TestSortIPs_AllIPv4(t *testing.T) {
	ips := []stdnet.IP{
		stdnet.ParseIP("1.1.1.1"),
		stdnet.ParseIP("2.2.2.2"),
	}

	sorted := sortIPs(ips, false, 1)

	// With only IPv4, order should be preserved
	if len(sorted) != 2 {
		t.Fatalf("expected 2 IPs, got %d", len(sorted))
	}
}

func TestSortIPs_AllIPv6(t *testing.T) {
	ips := []stdnet.IP{
		stdnet.ParseIP("2001:db8::1"),
		stdnet.ParseIP("2001:db8::2"),
	}

	sorted := sortIPs(ips, false, 1)

	// With only IPv6, order should be preserved
	if len(sorted) != 2 {
		t.Fatalf("expected 2 IPs, got %d", len(sorted))
	}
}
