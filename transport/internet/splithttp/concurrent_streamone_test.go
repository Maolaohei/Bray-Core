package splithttp_test

import (
	"context"
	"crypto/rand"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport/internet"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/internet/tls"
)

// Regression guard: concurrent stream-one bulk transfers must not drop each
// other. A soft per-stream EOF once tripped isFatalConnError => MarkDead =>
// forceCloseLiveConns killed sibling streams on the shared H2 socket (断流).
func TestXHTTP_StreamOne_ConcurrentBulk(t *testing.T) {
	ResetGlobalDialer()
	t.Cleanup(ResetGlobalDialer)

	p := tcp.PickPort()
	ct, ctHash := cert.MustGenerate(nil, cert.CommonName("localhost"))
	settings := &internet.MemoryStreamConfig{
		ProtocolName: "splithttp",
		ProtocolSettings: &Config{
			Path: "/conc",
			Mode: "stream-one",
		},
		SecurityType: "tls",
		SecuritySettings: &tls.Config{
			Certificate:          []*tls.Certificate{tls.ParseCertificate(ct)},
			PinnedPeerCertSha256: [][]byte{ctHash[:]},
		},
	}
	listen, err := ListenXH(context.Background(), net.LocalHostIP, p, settings, func(conn stat.Connection) {
		go func(c stat.Connection) {
			defer c.Close()
			io.Copy(c, c)
		}(conn)
	})
	common.Must(err)
	defer listen.Close()

	const (
		conns   = 8
		payload = 2 * 1024 * 1024
		rounds  = 2
	)
	var fail atomic.Int32
	var slow atomic.Int32

	for r := 0; r < rounds; r++ {
		var wg sync.WaitGroup
		for i := 0; i < conns; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				conn, err := Dial(context.Background(), net.TCPDestination(net.DomainAddress("localhost"), p), settings)
				if err != nil {
					t.Logf("dial %d: %v", id, err)
					fail.Add(1)
					return
				}
				defer conn.Close()
				buf := make([]byte, payload)
				rand.Read(buf)
				t0 := time.Now()
				if _, err := conn.Write(buf); err != nil {
					t.Logf("write %d: %v", id, err)
					fail.Add(1)
					return
				}
				out := make([]byte, payload)
				_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
				if _, err := io.ReadFull(conn, out); err != nil {
					t.Logf("read %d after %v: %v", id, time.Since(t0), err)
					fail.Add(1)
					return
				}
				dt := time.Since(t0)
				if dt > 5*time.Second {
					slow.Add(1)
					t.Logf("slow %d: %v", id, dt)
				}
			}(i)
		}
		wg.Wait()
	}
	if fail.Load() > 0 {
		t.Fatalf("failures=%d slow=%d", fail.Load(), slow.Load())
	}
	t.Logf("ok conns=%d rounds=%d slow=%d", conns, rounds, slow.Load())
}
