package splithttp

import (
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
		t.Logf("key1.dest.OriginalDomain: %s", key1.dest.OriginalDomain)
		t.Logf("key2.dest.OriginalDomain: %s", key2.dest.OriginalDomain)
	}

	// Verify Reality ServerName is preserved
	if key1.realityServerName != "www.microsoft.com" {
		t.Errorf("key1.realityServerName should be 'www.microsoft.com', got '%s'", key1.realityServerName)
	}
	if key2.realityServerName != "www.microsoft.com" {
		t.Errorf("key2.realityServerName should be 'www.microsoft.com', got '%s'", key2.realityServerName)
	}

	// Verify OriginalDomain is preserved in dest
	if key1.dest.OriginalDomain != "github.com" {
		t.Errorf("key1.dest.OriginalDomain should be 'github.com', got '%s'", key1.dest.OriginalDomain)
	}
	if key2.dest.OriginalDomain != "githubassets.com" {
		t.Errorf("key2.dest.OriginalDomain should be 'githubassets.com', got '%s'", key2.dest.OriginalDomain)
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
