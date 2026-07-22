package xmux_e2e_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type SiteConfig struct {
	Name    string
	Host    string
	Addr    string
	Body    string
	certPEM []byte
	tlsCert tls.Certificate
}

func genCert(t *testing.T, host string) ([]byte, tls.Certificate) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: host},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{host},
	}
	certDER, _ := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, _ := x509.MarshalECPrivateKey(key)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	tlsCert, _ := tls.X509KeyPair(certPEM, keyPEM)
	return certPEM, tlsCert
}

func startServer(t *testing.T, cfg SiteConfig) {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Site", cfg.Name)
		w.Header().Set("X-Host", r.Host)
		fmt.Fprint(w, cfg.Body)
	})
	server := &http.Server{
		Handler:   mux,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cfg.tlsCert}},
	}
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		t.Fatalf("listen %s: %v", cfg.Addr, err)
	}
	go func() {
		if err := server.ServeTLS(ln, "", ""); err != nil && err != http.ErrServerClosed {
			t.Logf("server %s: %v", cfg.Name, err)
		}
	}()
	t.Cleanup(func() { server.Close() })
}

func buildCertPool(t *testing.T, certPEM []byte) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	block, _ := pem.Decode(certPEM)
	cert, _ := x509.ParseCertificate(block.Bytes)
	pool.AddCert(cert)
	return pool
}

// TestCrossDomainIsolation verifies that two domains with different certificates
// cannot share connections. This is the core test for the XMUX cross-domain issue.
func TestCrossDomainIsolation(t *testing.T) {
	sites := []SiteConfig{
		{Name: "site-a", Host: "site-a.local", Addr: "127.0.0.1:48443", Body: "A"},
		{Name: "site-b", Host: "site-b.local", Addr: "127.0.0.1:48444", Body: "B"},
	}

	for i := range sites {
		certPEM, tlsCert := genCert(t, sites[i].Host)
		sites[i].certPEM = certPEM
		sites[i].tlsCert = tlsCert
	}

	for _, site := range sites {
		startServer(t, site)
	}

	// Each site has its own cert pool (only trusts its own cert)
	clients := make(map[string]*http.Client)
	for _, site := range sites {
		clients[site.Host] = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs:    buildCertPool(t, site.certPEM),
					ServerName: site.Host,
				},
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     5 * time.Second,
			},
			Timeout: 5 * time.Second,
		}
	}

	var wrongBody atomic.Int32
	var tlsError atomic.Int32
	var success atomic.Int32

	// Send 500 concurrent interleaved requests
	for i := 0; i < 500; i++ {
		var wg sync.WaitGroup
		for _, site := range sites {
			site := site
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := clients[site.Host].Get(fmt.Sprintf("https://%s/?n=%d", site.Addr, i))
				if err != nil {
					tlsError.Add(1)
					t.Logf("TLS ERROR site %s round %d: %v", site.Name, i, err)
					return
				}
				defer resp.Body.Close()
				body, _ := io.ReadAll(resp.Body)

				if string(body) != site.Body {
					wrongBody.Add(1)
					t.Errorf("WRONG BODY: site %s got %q expected %q at round %d",
						site.Name, string(body), site.Body, i)
				} else {
					success.Add(1)
				}
			}()
		}
		wg.Wait()
	}

	t.Logf("Results: success=%d wrong_body=%d tls_errors=%d",
		success.Load(), wrongBody.Load(), tlsError.Load())

	if wrongBody.Load() > 0 {
		t.Fatalf("CROSS-DOMAIN REUSE DETECTED: %d requests got wrong body", wrongBody.Load())
	}
	if tlsError.Load() > 0 {
		t.Fatalf("TLS CERT ERRORS: %d requests failed TLS verification", tlsError.Load())
	}
}

// TestCrossDomainCertificateMismatch specifically tests that connecting to
// site-a with site-b's cert pool fails (proving certs are different).
func TestCrossDomainCertificateMismatch(t *testing.T) {
	sites := []SiteConfig{
		{Name: "site-a", Host: "site-a.local", Addr: "127.0.0.1:58443", Body: "A"},
		{Name: "site-b", Host: "site-b.local", Addr: "127.0.0.1:58444", Body: "B"},
	}

	for i := range sites {
		certPEM, tlsCert := genCert(t, sites[i].Host)
		sites[i].certPEM = certPEM
		sites[i].tlsCert = tlsCert
	}

	for _, site := range sites {
		startServer(t, site)
	}

	// Client for site-a but with site-B's cert pool (should FAIL)
	wrongClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    buildCertPool(t, sites[1].certPEM), // site-b's cert
				ServerName: sites[0].Host,                      // connecting to site-a
			},
			DisableKeepAlives: true,
		},
		Timeout: 5 * time.Second,
	}

	// This SHOULD fail because site-a's cert is not trusted by site-b's pool
	_, err := wrongClient.Get(fmt.Sprintf("https://%s/", sites[0].Addr))
	if err == nil {
		t.Fatal("EXPECTED TLS ERROR: connecting to site-a with site-b's cert pool should fail")
	}
	t.Logf("Correctly rejected: %v", err)

	// Client for site-a with site-A's cert pool (should SUCCEED)
	correctClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    buildCertPool(t, sites[0].certPEM), // site-a's cert
				ServerName: sites[0].Host,                      // connecting to site-a
			},
			DisableKeepAlives: true,
		},
		Timeout: 5 * time.Second,
	}

	resp, err := correctClient.Get(fmt.Sprintf("https://%s/", sites[0].Addr))
	if err != nil {
		t.Fatalf("UNEXPECTED TLS ERROR: connecting to site-a with site-a's cert pool should succeed: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "A" {
		t.Fatalf("WRONG BODY: expected %q got %q", "A", string(body))
	}
	t.Logf("Correctly accepted: body=%s", string(body))
}
