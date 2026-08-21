package splithttp_test

import (
	"context"
	"crypto/rand"
	"io"
	"runtime"
	"testing"

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

// BenchmarkXHTTP_StreamModesMatrix compares the two stream modes on identical
// echo payloads. H1 and TLS/H2 are deliberately separate subtests: they have
// different framing and pooling behavior and must not be merged into one claim.
func BenchmarkXHTTP_StreamModesMatrix(b *testing.B) {
	for _, tc := range []struct {
		name   string
		mode   string
		useTLS bool
	}{
		{name: "h1_stream-one", mode: "stream-one"},
		{name: "h1_stream-up", mode: "stream-up"},
		{name: "h2_stream-one", mode: "stream-one", useTLS: true},
		{name: "h2_stream-up", mode: "stream-up", useTLS: true},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			if tc.useTLS && runtime.GOARCH == "arm64" {
				b.Skip("arm64")
			}
			p := tcp.PickPort()
			settings := &internet.MemoryStreamConfig{
				ProtocolName:     "splithttp",
				ProtocolSettings: &Config{Path: "/stream-matrix", Mode: tc.mode, Headers: map[string]string{BraySessionSecretHeader: "stream-matrix-secret"}},
			}
			if tc.useTLS {
				ct, ctHash := cert.MustGenerate(nil, cert.CommonName("localhost"))
				settings.SecurityType = "tls"
				settings.SecuritySettings = &tls.Config{Certificate: []*tls.Certificate{tls.ParseCertificate(ct)}, PinnedPeerCertSha256: [][]byte{ctHash[:]}}
			}
			listen, err := ListenXH(context.Background(), net.LocalHostIP, p, settings, func(conn stat.Connection) {
				go func(c stat.Connection) {
					defer c.Close()
					buf.Copy(buf.NewReader(c), buf.NewWriter(c))
				}(conn)
			})
			common.Must(err)
			defer listen.Close()
			conn, err := Dial(context.Background(), net.TCPDestination(net.DomainAddress("localhost"), p), settings)
			common.Must(err)
			defer conn.Close()
			payload := make([]byte, 32<<10)
			rand.Read(payload)
			out := make([]byte, len(payload))
			b.SetBytes(int64(len(payload)) * 2)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := conn.Write(payload); err != nil {
					b.Fatal(err)
				}
				if _, err := io.ReadFull(conn, out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
