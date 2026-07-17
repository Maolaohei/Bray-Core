package scenarios

import (
	"fmt"
	"net"
	"testing"
	"time"
)

// TestSOCKS5_RapidNewPages is a manual integration probe against a local SOCKS5 proxy.
// It reproduces the "new page stalls briefly" symptom when v2rayN is listening.
//
// Usage:
// 1. Start v2rayN and ensure SOCKS5 is available at 127.0.0.1:9996
// 2. Run: go test -v -run TestSOCKS5_RapidNewPages -count=1 ./testing/scenarios/
func TestSOCKS5_RapidNewPages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping manual SOCKS5 stall probe under -short")
	}

	proxyAddr := "127.0.0.1:9996"
	// Manual integration probe: requires a local v2rayN/SOCKS5 listener.
	if conn, err := net.DialTimeout("tcp", proxyAddr, 500*time.Millisecond); err != nil {
		t.Skipf("skipping: local SOCKS5 proxy not available at %s: %v", proxyAddr, err)
	} else {
		_ = conn.Close()
	}

	targets := []string{
		"www.google.com:443",
		"www.youtube.com:443",
		"github.com:443",
		"www.baidu.com:443",
		"www.cloudflare.com:443",
	}

	t.Log("Phase 1: rapid new connections...")
	var (
		totalConns   int
		totalStalls  int
		totalLatency time.Duration
	)

	for i := 0; i < 20; i++ {
		target := targets[i%len(targets)]
		start := time.Now()

		conn, err := net.DialTimeout("tcp", proxyAddr, 5*time.Second)
		if err != nil {
			t.Logf("  [conn %d] dial proxy failed: %v", i, err)
			totalStalls++
			continue
		}

		// SOCKS5 handshake
		_, _ = conn.Write([]byte{0x05, 0x01, 0x00})
		buf := make([]byte, 2)
		_, _ = conn.Read(buf)

		// SOCKS5 connect request
		req := buildSocks5Connect(target)
		_, _ = conn.Write(req)
		resp := make([]byte, 10)
		_, _ = conn.Read(resp)

		latency := time.Since(start)
		totalLatency += latency
		totalConns++

		if latency > 500*time.Millisecond {
			t.Logf("  [conn %d] STALL: %v to %s", i, latency, target)
			totalStalls++
		}

		_ = conn.Close()
	}

	t.Logf("=== rapid new connection probe ===")
	t.Logf("  connections: %d, stalls/failures: %d", totalConns, totalStalls)
	if totalConns == 0 {
		t.Fatal("no successful SOCKS5 connections; cannot evaluate stall ratio")
	}
	avgLatency := totalLatency / time.Duration(totalConns)
	t.Logf("  average latency: %v", avgLatency)

	t.Log("Phase 2: connection reuse...")
	conn, err := net.DialTimeout("tcp", proxyAddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// SOCKS5 handshake
	_, _ = conn.Write([]byte{0x05, 0x01, 0x00})
	buf := make([]byte, 2)
	_, _ = conn.Read(buf)

	req := buildSocks5Connect("www.google.com:443")
	_, _ = conn.Write(req)
	resp := make([]byte, 10)
	_, _ = conn.Read(resp)

	reuseStart := time.Now()
	for i := 0; i < 10; i++ {
		req := buildSocks5Connect(targets[i%len(targets)])
		_, _ = conn.Write(req)
		resp := make([]byte, 10)
		_, _ = conn.Read(resp)
	}
	reuseTime := time.Since(reuseStart)
	t.Logf("  reuse 10 times: %v (mean %v/req)", reuseTime, reuseTime/10)

	ratio := float64(totalLatency/time.Duration(totalConns)) / float64(reuseTime/10)
	t.Logf("  new/reuse ratio: %.1fx", ratio)
}

func buildSocks5Connect(target string) []byte {
	host, portStr, _ := net.SplitHostPort(target)
	port := 0
	fmt.Sscanf(portStr, "%d", &port)

	// SOCKS5 CONNECT request
	// VER CMD RSV ATYP DST.ADDR DST.PORT
	req := []byte{0x05, 0x01, 0x00, 0x03}
	req = append(req, byte(len(host)))
	req = append(req, []byte(host)...)
	req = append(req, byte(port>>8), byte(port))
	return req
}
