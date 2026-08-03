package splithttp_test

import (
	"context"
	"crypto/rand"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport/internet"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/internet/tls"
)

// compile-time check so both sides can share the same test
var _ = runtime.GOARCH
var _ = time.Second

func TestBenchmark_UpstreamCompare(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long XHTTP throughput compare under -short")
	}
	// Results sent to t.Log - just run and compare
	t.Run("H2C", func(t *testing.T) { benchThroughput(t, false, "packet-up", 128*1024) })
	if runtime.GOARCH != "arm64" {
		t.Run("H2-TLS", func(t *testing.T) { benchThroughput(t, true, "packet-up", 128*1024) })
		t.Run("H2-StreamUp", func(t *testing.T) { benchThroughput(t, true, "stream-up", 128*1024) })
	}
}

func benchThroughput(t *testing.T, useTLS bool, mode string, payloadSize int) {
	p := tcp.PickPort()

	var settings *internet.MemoryStreamConfig
	if useTLS {
		if runtime.GOARCH == "arm64" {
			t.Skip("arm64")
		}
		ct, ctHash := cert.MustGenerate(nil, cert.CommonName("localhost"))
		settings = &internet.MemoryStreamConfig{
			ProtocolName: "splithttp",
			ProtocolSettings: &Config{
				Path: "/sh",
				Mode: mode,
				// Session wire modes require a shared MAC secret (fail-closed).
				Headers: map[string]string{BraySessionSecretHeader: "bench-test-secret"},
			},
			SecurityType: "tls",
			SecuritySettings: &tls.Config{
				Certificate:          []*tls.Certificate{tls.ParseCertificate(ct)},
				PinnedPeerCertSha256: [][]byte{ctHash[:]},
			},
		}
	} else {
		settings = &internet.MemoryStreamConfig{
			ProtocolName: "splithttp",
			ProtocolSettings: &Config{
				Path: "/sh",
				Mode: mode,
				// Session wire modes require a shared MAC secret (fail-closed).
				Headers: map[string]string{BraySessionSecretHeader: "bench-test-secret"},
			},
		}
	}

	listen, err := ListenXH(context.Background(), net.LocalHostIP, p, settings, func(conn stat.Connection) {
		go func(c stat.Connection) {
			defer c.Close()
			buf.Copy(buf.NewReader(c), buf.NewWriter(c))
		}(conn)
	})
	common.Must(err)
	defer listen.Close()

	dest := net.TCPDestination(net.DomainAddress("localhost"), p)
	conn, err := Dial(context.Background(), dest, settings)
	common.Must(err)
	defer conn.Close()

	payload := make([]byte, payloadSize)
	rand.Read(payload)

	start := time.Now()
	n := 10
	for i := 0; i < n; i++ {
		if _, err := conn.Write(payload); err != nil {
			t.Fatal(err)
		}
		b := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, b); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	totalMB := float64(len(payload)*n) / 1e6
	mbps := totalMB * 8 / elapsed.Seconds()
	label := "H2C"
	if useTLS {
		label = "H2"
	}
	t.Logf("%-10s %-12s %5.0f KB x %2d = %.0f MB in %.2fs = %.1f Mbps",
		label, mode, float64(payloadSize)/1024, n, totalMB, elapsed.Seconds(), mbps)
}
