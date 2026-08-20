package splithttp

// B1 服务端闭环单测：生产（httpServerConn.Write 在段模式转缓存）→ 段 GET
// 拉取（命中 / EOF）。
//
// NOTE: the cache no longer evicts undelivered segments (bounded flow
// control / backpressure — see downseg_overflow_test.go). As a result there
// is no "oldest slid past -> 410" behavior to test anywhere; a segment is
// either present (200), not produced yet (404), or stream-over (200 empty =
// EOF). The old TestDownsegPullGoneAndEof asserted 410 for old segments and
// was removed because that behavior is gone.

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

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

// TestDownsegProduceThenPull: a brand-new session answers the first pull 404
// immediately (fast-path, no 2s poll - audit Finding-3); once produced, the
// pull returns the finalized segment.
func TestDownsegProduceThenPull(t *testing.T) {
	withZeroDownsegJitter(t)
	h := refCountTestHandler(t)
	sess, id := downsegTestSession(h)
	if sess == nil {
		t.Fatal("enterDownsegMode failed")
	}

	prod := &httpServerConn{Instance: done.New(), sess: sess}
	payload := bytes.Repeat([]byte{0x42}, 2000)

	// Fast-path: nothing produced yet, stream not over -> immediate 404.
	if code, _ := pullSegmentWithID(h, id, 0); code != http.StatusNotFound {
		t.Fatalf("brand-new session: got %d want immediate 404 (no 2s poll)", code)
	}
	// Produce + finalize, then the segment is available.
	if _, err := prod.Write(payload); err != nil {
		t.Fatal(err)
	}
	_ = prod.Close() // finalize EOF
	respCode, respBody := pullSegmentWithID(h, id, 0)
	if respCode != http.StatusOK || !bytes.Equal(respBody, payload) {
		t.Fatalf("pull after produce: code=%d bodyLen=%d want 200/%d", respCode, len(respBody), len(payload))
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
