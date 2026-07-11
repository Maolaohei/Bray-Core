package tls

import (
	gotls "crypto/tls"
	"testing"
)

func TestCopyConfig_PropagatesCipherSuites(t *testing.T) {
	in := &gotls.Config{
		CipherSuites: []uint16{
			gotls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			gotls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		},
		ServerName: "example.com",
	}
	out := copyConfig(in)
	if len(out.CipherSuites) != 2 {
		t.Fatalf("CipherSuites len=%d want 2", len(out.CipherSuites))
	}
	if out.CipherSuites[0] != in.CipherSuites[0] || out.CipherSuites[1] != in.CipherSuites[1] {
		t.Fatalf("CipherSuites=%v want %v", out.CipherSuites, in.CipherSuites)
	}
	// mutation isolation
	out.CipherSuites[0] = 0
	if in.CipherSuites[0] == 0 {
		t.Fatal("copyConfig must not share CipherSuites backing array")
	}
}
