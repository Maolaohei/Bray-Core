package http

import (
	"testing"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/reality"
	"github.com/xtls/xray-core/transport/internet/tls"
)

func TestTLSIdentityHash_DifferentServerName(t *testing.T) {
	a := tlsIdentityHash(&internet.MemoryStreamConfig{
		SecuritySettings: &tls.Config{
			ServerName: "chat.openai.com",
		},
	})
	b := tlsIdentityHash(&internet.MemoryStreamConfig{
		SecuritySettings: &tls.Config{
			ServerName: "google.com",
		},
	})
	if a == b {
		t.Fatal("different ServerName should produce different hash")
	}
}

func TestTLSIdentityHash_SameServerName(t *testing.T) {
	a := tlsIdentityHash(&internet.MemoryStreamConfig{
		SecuritySettings: &tls.Config{
			ServerName: "chat.openai.com",
		},
	})
	b := tlsIdentityHash(&internet.MemoryStreamConfig{
		SecuritySettings: &tls.Config{
			ServerName: "chat.openai.com",
		},
	})
	if a != b {
		t.Fatal("same ServerName should produce same hash")
	}
}

func TestTLSIdentityHash_DifferentFingerprint(t *testing.T) {
	a := tlsIdentityHash(&internet.MemoryStreamConfig{
		SecuritySettings: &tls.Config{
			ServerName:  "example.com",
			Fingerprint: "chrome",
		},
	})
	b := tlsIdentityHash(&internet.MemoryStreamConfig{
		SecuritySettings: &tls.Config{
			ServerName:  "example.com",
			Fingerprint: "firefox",
		},
	})
	if a == b {
		t.Fatal("different Fingerprint should produce different hash")
	}
}

func TestTLSIdentityHash_DifferentALPN(t *testing.T) {
	a := tlsIdentityHash(&internet.MemoryStreamConfig{
		SecuritySettings: &tls.Config{
			ServerName:   "example.com",
			NextProtocol: []string{"h2", "http/1.1"},
		},
	})
	b := tlsIdentityHash(&internet.MemoryStreamConfig{
		SecuritySettings: &tls.Config{
			ServerName:   "example.com",
			NextProtocol: []string{"http/1.1"},
		},
	})
	if a == b {
		t.Fatal("different ALPN should produce different hash")
	}
}

func TestTLSIdentityHash_DifferentRealityServerName(t *testing.T) {
	a := tlsIdentityHash(&internet.MemoryStreamConfig{
		SecuritySettings: &reality.Config{
			ServerName: "www.microsoft.com",
		},
	})
	b := tlsIdentityHash(&internet.MemoryStreamConfig{
		SecuritySettings: &reality.Config{
			ServerName: "www.cloudflare.com",
		},
	})
	if a == b {
		t.Fatal("different REALITY ServerName should produce different hash")
	}
}

func TestTLSIdentityHash_ReplacesOldCacheKey(t *testing.T) {
	// Simulates the old bug: same IP:Port, different TLS identity.
	// Old code: cachedH2Conns[dest] → collision
	// New code: cachedH2Conns[{dest, identityHash}] → isolation

	dest := net.TCPDestination(net.ParseAddress("142.250.191.14"), 443)

	chatKey := h2CacheKey{
		dest: dest,
		identityHash: tlsIdentityHash(&internet.MemoryStreamConfig{
			SecuritySettings: &tls.Config{ServerName: "chat.openai.com"},
		}),
	}
	googleKey := h2CacheKey{
		dest: dest,
		identityHash: tlsIdentityHash(&internet.MemoryStreamConfig{
			SecuritySettings: &tls.Config{ServerName: "google.com"},
		}),
	}

	if chatKey == googleKey {
		t.Fatal("same dest + different TLS identity must produce different cache keys")
	}
}

func TestTLSIdentityHash_NilSecuritySettings(t *testing.T) {
	// No TLS config (plaintext) should still produce a valid hash.
	h := tlsIdentityHash(&internet.MemoryStreamConfig{})
	var zero [32]byte
	if h == zero {
		// Zero hash is acceptable for no-TLS configs; just ensure no panic.
		return
	}
}
