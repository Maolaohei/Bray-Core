package tls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"
)

// TestUTLSSessionResumption verifies that two sequential uTLS client
// handshakes against the same server reuse the TLS session via
// uGlobalSessionCache (second dial reports SessionReused).
func TestUTLSSessionResumption(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	serverTLS := &tls.Config{Certificates: []tls.Certificate{cert}}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			sc := tls.Server(c, serverTLS)
			go func() {
				_ = sc.Handshake()
				buf := make([]byte, 1)
				_ = sc.SetReadDeadline(time.Now().Add(2 * time.Second))
				_, _ = sc.Read(buf)
				sc.Close()
			}()
		}
	}()

	root := x509.NewCertPool()
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	root.AddCert(leaf)
	cfg := &tls.Config{
		ServerName:         "localhost",
		RootCAs:            root,
		ClientSessionCache: globalSessionCache,
		MinVersion:         tls.VersionTLS12,
	}

	dial := func() bool {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		uconn := GeneraticUClient(c, cfg)
		defer uconn.Close()
		if err := uconn.Handshake(); err != nil {
			t.Fatal(err)
		}
		// In TLS 1.3 session tickets arrive after the handshake; drain a byte
		// from the server (it closes after its own read) so the ticket is
		// processed and stored into the session cache before we close.
		_ = uconn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		buf := make([]byte, 16)
		_, _ = uconn.Read(buf)
		return uconn.ConnectionState().DidResume
	}

	if dial() {
		t.Fatal("first handshake must not be resumed")
	}
	if !dial() {
		t.Fatal("second handshake should have resumed the session")
	}
}
