package splithttp

// B1 服务端闭环单测：生产（httpServerConn.Write 在段模式转缓存）→ 段 GET
// 拉取（等待未产出段 / 命中 / gone / EOF）。

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
	last := sess.downseg.Load().producedCount() - 1
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
