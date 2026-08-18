package splithttp

// 客户端段拉取端到端：真实 httptest server 跑服务端 handler；客户端 puller
// 先（由段 GET）在服务端建立 session（真实 IP），随后生产 goroutine 向同一
// session 写段，puller 拉取重组字节流。

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/signal/done"
)

// codeRecorder captures the final status code written by the handler.
type codeRecorder struct {
	http.ResponseWriter
	code int
}

func (c *codeRecorder) WriteHeader(code int) {
	c.code = code
	c.ResponseWriter.WriteHeader(code)
}

func newEndToEndServer(t *testing.T) (*requestHandler, *url.URL, *DefaultDialerClient) {
	t.Helper()
	h := refCountTestHandler(t)
	var lastCode atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &codeRecorder{ResponseWriter: w, code: 200}
		h.ServeHTTP(rec, r)
		lastCode.Store(int64(rec.code))
	}))
	t.Cleanup(func() {
		if c := lastCode.Load(); c != 200 {
			t.Logf("server last code=%d", c)
		}
		srv.Close()
	})
	base := &url.URL{Scheme: "http", Host: srv.Listener.Addr().String(), Path: h.path}
	client := &DefaultDialerClient{transportConfig: h.config, client: &http.Client{Timeout: 30 * time.Second}}
	return h, base, client
}

// TestDownSegPullerEndToEnd: puller establishes the session via its segment
// GET; a production goroutine writes into that same session's cache; puller
// reassembles the full byte stream.
func TestDownSegPullerEndToEnd(t *testing.T) {
	h, base, client := newEndToEndServer(t)
	sid := h.config.GenerateSessionID()
	payload := bytes.Repeat([]byte{0x77}, downsegSize*2+500) // ~3 segments

	// Production goroutine: once the puller's first segment GET has upserted
	// the session (same source IP -> shared session), enter segment mode (the
	// GET already did) and write chunks + finalize.
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		// Wait for the session to appear (created by puller's GET).
		deadline := time.Now().Add(5 * time.Second)
		for {
			v, ok := h.sessions.Load(sid)
			if ok {
				sess := v.(*httpSession)
				if !sess.enterDownsegMode() {
					return
				}
				prod := &httpServerConn{Instance: done.New(), sess: sess}
				chunk := 64 << 10
				for off := 0; off < len(payload); off += chunk {
					end := off + chunk
					if end > len(payload) {
						end = len(payload)
					}
					if _, err := prod.Write(payload[off:end]); err != nil {
						return
					}
				}
				_ = prod.Close() // EOF finalize
				return
			}
			if time.Now().After(deadline) {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	puller := NewDownSegPuller(ctx, client, base, sid)
	defer puller.Close()

	var got bytes.Buffer
	buf := make([]byte, 8192)
	for {
		n, err := puller.Read(buf)
		got.Write(buf[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	<-doneCh
	if !bytes.Equal(got.Bytes(), payload) {
		t.Fatalf("reassembled mismatch: got %d bytes want %d", got.Len(), len(payload))
	}
}
