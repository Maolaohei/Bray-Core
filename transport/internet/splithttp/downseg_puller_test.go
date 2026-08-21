package splithttp

// 客户端段拉取端到端：真实 httptest server 跑服务端 handler；客户端 puller
// 先（由段 GET）在服务端建立 session（真实 IP），随后生产 goroutine 向同一
// session 写段，puller 拉取重组字节流。

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/signal/done"
)

type productionLegReader struct {
	err error
}

func (r *productionLegReader) Read([]byte) (int, error) { return 0, r.err }

func TestMonitorProductionLegReportsUnexpectedEOF(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got := make(chan error, 1)
	go monitorProductionLeg(ctx, &productionLegReader{err: io.ErrUnexpectedEOF}, func(err error) {
		got <- err
	})

	select {
	case err := <-got:
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("monitor error = %v, want unexpected EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("production-leg EOF was not reported")
	}
}

func TestMonitorProductionLegIgnoresLocalClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got := make(chan error, 1)
	go monitorProductionLeg(ctx, &productionLegReader{err: io.EOF}, func(err error) {
		got <- err
	})

	select {
	case err := <-got:
		t.Fatalf("monitor reported local shutdown as production failure: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestDownSegPullerProductionFailurePreemptsBufferedSegments(t *testing.T) {
	want := errors.New("production GET closed")
	p := &DownSegPuller{
		buf:   map[uint64][]byte{0: []byte("stale")},
		fatal: want,
		wake:  make(chan struct{}, 1),
	}

	_, err := p.Read(make([]byte, 16))
	if !errors.Is(err, want) {
		t.Fatalf("Read error = %v, want production failure %v", err, want)
	}
}

func TestDownSegCurrentRetryUsesShorterBaseThanPrefetch(t *testing.T) {
	if got := downSegRetryBase(7, 7); got >= downSegRetryInterval {
		t.Fatalf("current retry base = %v, want less than future base %v", got, downSegRetryInterval)
	}
	if got := downSegRetryBase(8, 7); got != downSegRetryInterval {
		t.Fatalf("future retry base = %v, want %v", got, downSegRetryInterval)
	}
}

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
	puller := NewDownSegPuller(ctx, client, base, sid, nil)
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
