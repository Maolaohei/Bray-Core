package splithttp

// B1 吞吐/正确性基准（onlyBray）：dseg 下拉（段拉取）的吞吐与正确性，
// 与 legacy 长 GET（BenchmarkXHTTP_H2C_Throughput）对照。当前 DownSegPuller
// 是顺序拉取（窗口=1）——基准揭示它的吞吐上限，证明并发窗口是主要优化空间。

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/signal/done"
)

// assembleDownseg pulls the whole stream for session via segment GETs and
// returns the bytes (used by both the correctness test and the benchmark).
func assembleDownseg(tb testing.TB, h *requestHandler, client *DefaultDialerClient, base *url.URL, sid string, payload []byte) {
	// Production: a goroutine waits for the puller's segment GET to upsert
	// the shared (same source IP) session, then writes into its cache and
	// finalizes (as the server's downlink producer would).
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		deadline := time.Now().Add(5 * time.Second)
		for {
			v, ok := h.sessions.Load(sid)
			if ok {
				sess := v.(*httpSession)
				if !sess.enterDownsegMode() {
					return
				}
				prod := &httpServerConn{Instance: done.New(), sess: sess}
				chunk := 256 << 10
				for off := 0; off < len(payload); off += chunk {
					end := off + chunk
					if end > len(payload) {
						end = len(payload)
					}
					if _, err := prod.Write(payload[off:end]); err != nil {
						return
					}
				}
				_ = prod.Close() // finalize -> EOF
				return
			}
			if time.Now().After(deadline) {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	puller := NewDownSegPuller(context.Background(), client, base, sid, nil)
	defer puller.Close()
	var got bytes.Buffer
	buf := make([]byte, 64<<10)
	for {
		n, err := puller.Read(buf)
		got.Write(buf[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			tb.Fatalf("read: %v", err)
		}
	}
	<-doneCh
	if !bytes.Equal(got.Bytes(), payload) {
		tb.Fatalf("mismatch: got %d want %d", got.Len(), len(payload))
	}
}

// TestDownsegDialIntegration is the door-osmosed correctness check: a
// real httptest stack (server handler = requestHandler, client = native
// dialer with a dseg session) reassembles a 3-segment payload exactly.
func TestDownsegDialIntegration(t *testing.T) {
	h, base, client := newEndToEndServer(t)
	sid := h.config.GenerateSessionID()
	payload := bytes.Repeat([]byte{0x5A}, downsegSize*2+700)
	assembleDownseg(t, h, client, base, sid, payload)
}

// BenchmarkDownlinkSegments measures the sequential segment-pull throughput.
func BenchmarkDownlinkSegments(b *testing.B) {
	b.SetBytes(downsegSize)
	h := refCountTestHandler(b)
	client := &DefaultDialerClient{transportConfig: h.config, client: &http.Client{Timeout: 30 * time.Second}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.ServeHTTP(w, r) }))
	b.Cleanup(srv.Close)
	base := &url.URL{Scheme: "http", Host: srv.Listener.Addr().String(), Path: h.path}

	sidBase := h.config.GenerateSessionID()
	payload := bytes.Repeat([]byte{0x40}, downsegSize*8) // ~8 segments
	_ = sidBase

	// Per-op: reassemble an 8-segment downlink (produce + sequential pull).
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		assembleDownseg(b, h, client, base, h.config.GenerateSessionID(), payload)
	}
}
