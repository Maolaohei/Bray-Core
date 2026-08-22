package tls

import (
	"crypto/tls"
	"net"
	"testing"
	"time"
)

// TestUTLSResumptionWAN dials a TLS server over a real WAN link twice via the
// production uTLS path (GeneraticUClient, Chrome fingerprint + PSK placeholder
// injection); the second handshake must resume (NST arrives post-handshake
// over the network, as in production).
func TestUTLSResumptionWAN(t *testing.T) {
	cfg := &tls.Config{
		ServerName:         "localhost",
		InsecureSkipVerify: true,
		ClientSessionCache: globalSessionCache,
		MinVersion:         tls.VersionTLS12,
	}
	dial := func() bool {
		c, err := net.DialTimeout("tcp", "103.136.185.220:18443", 8*time.Second)
		if err != nil {
			t.Skipf("unreachable: %v", err)
		}
		u := GeneraticUClient(c, cfg)
		defer u.Close()
		if err := u.Handshake(); err != nil {
			t.Skipf("handshake: %v", err)
		}
		resumed := u.ConnectionState().DidResume
		// Process post-handshake NST records.
		_ = u.SetReadDeadline(time.Now().Add(1500 * time.Millisecond))
		buf := make([]byte, 4096)
		n, rerr := u.Read(buf)
		t.Logf("read n=%d err=%v", n, rerr)
		t.Logf("resumed=%v ver=%x", resumed, u.ConnectionState().Version)
		return resumed
	}
	dial()
	if !dial() {
		t.Fatal("second WAN dial should resume")
	}
}
