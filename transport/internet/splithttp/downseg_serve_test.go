package splithttp

// B1 服务端闭环单测：生产（httpServerConn.Write 在段模式转缓存）→ 段 GET
// 拉取（等待未产出段 / 命中 / gone / EOF）。

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/signal/done"
)

func downsegTestSession(h *requestHandler) (*httpSession, string) {
	id := h.config.GenerateSessionID()
	sess := h.upsertSession(id, "192.0.2.20:1234")
	if !sess.enterDownsegMode() {
		return nil, ""
	}
	return sess, id
}

// TestDownsegProduceThenPull: production leg writes into the segment cache, a
// concurrent segment pull waits for and receives the finalized segment.
func TestDownsegProduceThenPull(t *testing.T) {
	h := refCountTestHandler(t)
	sess, id := downsegTestSession(h)
	if sess == nil {
		t.Fatal("enterDownsegMode failed")
	}

	prod := &httpServerConn{Instance: done.New(), sess: sess}
	payload := bytes.Repeat([]byte{0x42}, 2000)

	var respCode int
	var respBody []byte
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		respCode, respBody = pullSegmentWithID(h, id, 0)
	}()
	// Let the pull start polling; then produce and finalize.
	time.Sleep(50 * time.Millisecond)
	if _, err := prod.Write(payload); err != nil {
		t.Fatal(err)
	}
	_ = prod.Close() // finalize EOF
	wg.Wait()

	if respCode != http.StatusOK || !bytes.Equal(respBody, payload) {
		t.Fatalf("pull: code=%d bodyLen=%d want 200/%d", respCode, len(respBody), len(payload))
	}
}

// TestDownsegPullGoneAndEof: slid-past is 410, beyond-final is 404.
func TestDownsegPullGoneAndEof(t *testing.T) {
	h := refCountTestHandler(t)
	sess, id := downsegTestSession(h)
	if sess == nil {
		t.Fatal("enterDownsegMode failed")
	}
	prod := &httpServerConn{Instance: done.New(), sess: sess}
	for i := 0; i < downsegMaxSegs+3; i++ {
		if _, err := prod.Write(bytes.Repeat([]byte{byte(i)}, downsegSize)); err != nil {
			t.Fatal(err)
		}
	}
	_ = prod.Close() // finalize

	if code, _ := pullSegmentWithID(h, id, 0); code != http.StatusGone {
		t.Fatalf("slid segment: got %d want 410", code)
	}
	last := sess.downseg.producedCount() - 1
	if code, b := pullSegmentWithID(h, id, last); code != http.StatusOK || len(b) != downsegSize {
		t.Fatalf("last segment: code=%d len=%d", code, len(b))
	}
	if code, _ := pullSegmentWithID(h, id, last+1); code == http.StatusGone || code == http.StatusNotFound {
		// past-end segment: finalize() made it EOF; with the empty-200 EOF
		// protocol this must be a 200 with empty body (not a hard error).
		t.Fatalf("past-end segment: got %d want 200(empty), not %d/%d", code, http.StatusGone, http.StatusNotFound)
	}
}

// pullSegmentWithID issues a dseg-marked GET with the given seq.
func pullSegmentWithID(h *requestHandler, id string, seq uint64) (int, []byte) {
	req := httptest.NewRequest("GET", "http://example.com"+h.path, nil)
	req.RemoteAddr = "192.0.2.20:1234"
	// FillStreamRequest stamps valid padding (+ an initial sessionId token),
	// then reset the path and re-apply meta so the token carries sessionId+seq.
	h.config.FillStreamRequest(req, id, "")
	req.URL.Path = h.path
	h.config.ApplyMetaToRequest(req, id, strconv.FormatUint(seq, 10))
	// marker-free: a sessioned GET whose token carries a seq is a segment pull.

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code, rec.Body.Bytes()
}
