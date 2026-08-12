package splithttp

import (
	"strings"
	"testing"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/reality"
	"github.com/xtls/xray-core/transport/internet/tls"
)

// TestMuxKeyDifferentDomainsSameIP verifies that different domains with the same IP
// produce different MuxKey values, preventing cross-domain connection pool reuse.
// This is the fix for the GitHub *.githubassets.com certificate issue.
func TestMuxKeyDifferentDomainsSameIP(t *testing.T) {
	// Simulate Reality config (same for both domains)
	realityConfig := &reality.Config{
		ServerName:  "www.microsoft.com",
		PublicKey:   []byte("JMKXLLz0sK2Qx9P4G9cvRDGwZlwhSwjsNEMyp7Kgdzc"),
		ShortId:     []byte("fbeb32509a876ac6"),
		Fingerprint: "chrome",
	}

	streamSettings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Mode: "stream-one", Path: "/"},
		SecuritySettings: realityConfig,
	}

	// github.com (IP: 20.205.243.166)
	dest1 := net.Destination{
		Address:        net.IPAddress([]byte{20, 205, 243, 166}),
		Port:           443,
		Network:        net.Network_TCP,
		OriginalDomain: "github.com",
	}

	// githubassets.com (same IP)
	dest2 := net.Destination{
		Address:        net.IPAddress([]byte{20, 205, 243, 166}),
		Port:           443,
		Network:        net.Network_TCP,
		OriginalDomain: "githubassets.com",
	}

	key1 := newMuxKey(dest1, streamSettings)
	key2 := newMuxKey(dest2, streamSettings)

	if key1 == key2 {
		t.Error("MuxKey should be different for different domains, even with same IP")
		t.Logf("key1.destIdentity: %s", key1.destIdentity)
		t.Logf("key2.destIdentity: %s", key2.destIdentity)
	}

	// Verify Reality ServerName is preserved
	if key1.realityServerName != "www.microsoft.com" {
		t.Errorf("key1.realityServerName should be 'www.microsoft.com', got '%s'", key1.realityServerName)
	}
	if key2.realityServerName != "www.microsoft.com" {
		t.Errorf("key2.realityServerName should be 'www.microsoft.com', got '%s'", key2.realityServerName)
	}

	// Verify destIdentity includes OriginalDomain and differs by domain
	if key1.destIdentity != muxDestIdentity(dest1) {
		t.Errorf("key1.destIdentity mismatch: %s", key1.destIdentity)
	}
	if key2.destIdentity != muxDestIdentity(dest2) {
		t.Errorf("key2.destIdentity mismatch: %s", key2.destIdentity)
	}
	if key1.destIdentity == key2.destIdentity {
		t.Error("destIdentity should differ for github.com vs githubassets.com")
	}
}

// TestMuxKeySameDomainSameIP verifies that the same domain with the same IP
// produces the same MuxKey, allowing correct connection pool reuse.
func TestMuxKeySameDomainSameIP(t *testing.T) {
	realityConfig := &reality.Config{
		ServerName:  "www.microsoft.com",
		PublicKey:   []byte("JMKXLLz0sK2Qx9P4G9cvRDGwZlwhSwjsNEMyp7Kgdzc"),
		ShortId:     []byte("fbeb32509a876ac6"),
		Fingerprint: "chrome",
	}

	streamSettings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Mode: "stream-one", Path: "/"},
		SecuritySettings: realityConfig,
	}

	dest1 := net.Destination{
		Address:        net.IPAddress([]byte{20, 205, 243, 166}),
		Port:           443,
		Network:        net.Network_TCP,
		OriginalDomain: "github.com",
	}

	dest2 := net.Destination{
		Address:        net.IPAddress([]byte{20, 205, 243, 166}),
		Port:           443,
		Network:        net.Network_TCP,
		OriginalDomain: "github.com",
	}

	key1 := newMuxKey(dest1, streamSettings)
	key2 := newMuxKey(dest2, streamSettings)

	if key1 != key2 {
		t.Error("MuxKey should be same for same domain with same IP")
	}
}

// TestMuxKeyDifferentRealityConfig verifies that different Reality configs
// produce different MuxKey values.
func TestMuxKeyDifferentRealityConfig(t *testing.T) {
	streamSettings1 := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Mode: "stream-one", Path: "/"},
		SecuritySettings: &reality.Config{
			ServerName:  "www.microsoft.com",
			PublicKey:   []byte("key1"),
			ShortId:     []byte("short1"),
			Fingerprint: "chrome",
		},
	}

	streamSettings2 := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Mode: "stream-one", Path: "/"},
		SecuritySettings: &reality.Config{
			ServerName:  "www.microsoft.com",
			PublicKey:   []byte("key2"), // Different key
			ShortId:     []byte("short1"),
			Fingerprint: "chrome",
		},
	}

	dest := net.Destination{
		Address:        net.IPAddress([]byte{20, 205, 243, 166}),
		Port:           443,
		Network:        net.Network_TCP,
		OriginalDomain: "github.com",
	}

	key1 := newMuxKey(dest, streamSettings1)
	key2 := newMuxKey(dest, streamSettings2)

	if key1 == key2 {
		t.Error("MuxKey should be different for different Reality configs")
	}
}

// TestMuxKeyTLSvsReality verifies that TLS and Reality configs produce different MuxKeys.
func TestMuxKeyTLSvsReality(t *testing.T) {
	streamSettingsTLS := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Mode: "stream-one", Path: "/"},
		SecuritySettings: &tls.Config{
			ServerName:  "github.com",
			Fingerprint: "chrome",
		},
	}

	streamSettingsReality := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Mode: "stream-one", Path: "/"},
		SecuritySettings: &reality.Config{
			ServerName:  "www.microsoft.com",
			PublicKey:   []byte("JMKXLLz0sK2Qx9P4G9cvRDGwZlwhSwjsNEMyp7Kgdzc"),
			ShortId:     []byte("fbeb32509a876ac6"),
			Fingerprint: "chrome",
		},
	}

	dest := net.Destination{
		Address:        net.IPAddress([]byte{20, 205, 243, 166}),
		Port:           443,
		Network:        net.Network_TCP,
		OriginalDomain: "github.com",
	}

	key1 := newMuxKey(dest, streamSettingsTLS)
	key2 := newMuxKey(dest, streamSettingsReality)

	if key1 == key2 {
		t.Error("MuxKey should be different for TLS vs Reality")
	}

	// TLS should use ServerName as tlsServerName
	if key1.tlsServerName != "github.com" {
		t.Errorf("key1.tlsServerName should be 'github.com', got '%s'", key1.tlsServerName)
	}
	if key1.realityServerName != "" {
		t.Errorf("key1.realityServerName should be empty for TLS, got '%s'", key1.realityServerName)
	}

	// Reality should use ServerName as realityServerName
	if key2.tlsServerName != "" {
		t.Errorf("key2.tlsServerName should be empty for Reality, got '%s'", key2.tlsServerName)
	}
	if key2.realityServerName != "www.microsoft.com" {
		t.Errorf("key2.realityServerName should be 'www.microsoft.com', got '%s'", key2.realityServerName)
	}
}

// muxDestIdentityBuilder is the pre-optimization reference implementation.
// Kept in tests only to prove the stack-built version is byte-for-byte
// identical (MuxKey is a process-lifetime map key).
func muxDestIdentityBuilder(dest net.Destination) string {
	var b strings.Builder
	b.Grow(64)
	switch dest.Network {
	case net.Network_TCP:
		b.WriteString("tcp|")
	case net.Network_UDP:
		b.WriteString("udp|")
	case net.Network_UNIX:
		b.WriteString("unix|")
	default:
		b.WriteString("unknown|")
	}
	if dest.Address != nil {
		b.WriteString(dest.Address.String())
	}
	b.WriteByte('|')
	b.WriteString(dest.Port.String())
	b.WriteByte('|')
	b.WriteString(dest.OriginalDomain)
	return b.String()
}

func TestMuxDestIdentityMatchesBuilder(t *testing.T) {
	dests := []net.Destination{
		net.TCPDestination(net.IPAddress(net.ParseIP("1.2.3.4")), 443),
		net.TCPDestination(net.IPAddress(net.ParseIP("8.8.8.8")), 53),
		net.TCPDestination(net.IPAddress(net.ParseIP("2001:db8::1")), 8443),
		net.TCPDestination(net.IPAddress(net.ParseIP("::1")), 443),
		net.TCPDestination(net.IPAddress(net.ParseIP("2001:4860:4860::8888")), 443),
		net.TCPDestination(net.DomainAddress("www.example.com"), 443),
		net.TCPDestination(net.DomainAddress("localhost"), 0),
		net.UDPDestination(net.IPAddress(net.ParseIP("10.0.0.1")), 53),
		net.UDPDestination(net.DomainAddress("dns.example.com"), 5353),
	}
	for _, d := range dests {
		want := muxDestIdentityBuilder(d)
		got := muxDestIdentity(d)
		if got != want {
			t.Errorf("muxDestIdentity(%v) = %q, want %q", d, got, want)
		}
	}

	// OriginalDomain participates in the key.
	d := net.TCPDestination(net.IPAddress(net.ParseIP("1.2.3.4")), 443)
	d.OriginalDomain = "github.com"
	want := muxDestIdentityBuilder(d)
	got := muxDestIdentity(d)
	if got != want {
		t.Errorf("with OriginalDomain: got %q, want %q", got, want)
	}
}

func TestMuxDestIdentityStableAcrossCalls(t *testing.T) {
	d := net.TCPDestination(net.IPAddress(net.ParseIP("2001:db8::1")), 8443)
	first := muxDestIdentity(d)
	for i := 0; i < 100; i++ {
		if got := muxDestIdentity(d); got != first {
			t.Fatalf("unstable output: %q vs %q", got, first)
		}
	}
}

func TestAppendPort(t *testing.T) {
	cases := []struct {
		p    uint16
		want string
	}{
		{0, "0"},
		{1, "1"},
		{53, "53"},
		{443, "443"},
		{65535, "65535"},
	}
	for _, c := range cases {
		got := string(appendPort(nil, c.p))
		if got != c.want {
			t.Errorf("appendPort(%d) = %q, want %q", c.p, got, c.want)
		}
	}
}
