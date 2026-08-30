package splithttp

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/xtls/xray-core/common/buf"
)

// POC for upstream 805d011f / dffc7ada ("XHTTP client: Define
// Request.GetBody() for packet-up so h2 can replay after GOAWAY").
//
// Why this matters: net/http's http2 transport may receive GOAWAY from the
// server after the request body has already been consumed and closed. It then
// retries on a fresh connection by calling req.GetBody(). Bray's packet-up body
// is a pooled MultiBuffer wrapper whose Close() releases the buffers back to
// the pool — so a naive replay would read an empty or, worse, a *recycled*
// buffer owned by another goroutine. That is silent upload corruption, not a
// visible error.
//
// Upstream's own Test_FillPacketRequest_GetBody replays the body while the
// first body is still open, which never exercises the release path. These tests
// use the real GOAWAY ordering: consume -> Close -> GetBody.

func newPacketUpRequest(t *testing.T) *http.Request {
	t.Helper()
	req, err := http.NewRequest("POST", "https://example.com/bray", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	return req
}

// requireGetBody fails the test cleanly when GetBody is undefined. A nil
// GetBody must not panic the binary, otherwise one missing assignment would
// mask the remaining cases in a negative-control run.
func requireGetBody(t *testing.T, req *http.Request) func() (io.ReadCloser, error) {
	t.Helper()
	if req.GetBody == nil {
		t.Fatal("GetBody is undefined: an h2 GOAWAY replay would send an empty body")
	}
	return req.GetBody
}

// TestPOC_HotPathReplayAfterClose covers FillPacketRequestBytes, the zero-copy
// hot path Bray uses for packet-up retries. Replay must work after the first
// body was consumed AND closed, and must not free or mutate the caller's
// durable snapshot.
func TestPOC_HotPathReplayAfterClose(t *testing.T) {
	data := []byte("bray-poc-hot-path-payload")

	cfg := &Config{Path: "/bray"}
	req := &http.Request{
		URL: &url.URL{Scheme: "https", Host: "example.com", Path: "/bray"},
	}
	if err := cfg.FillPacketRequestBytes(req, "sess", "0", data); err != nil {
		t.Fatalf("FillPacketRequestBytes: %v", err)
	}
	requireGetBody(t, req) // hot path must always expose a replayable body
	if req.ContentLength != int64(len(data)) {
		t.Fatalf("ContentLength = %d, want %d", req.ContentLength, len(data))
	}

	// Real GOAWAY ordering: attempt 1 consumes and closes the body.
	first, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read attempt 1: %v", err)
	}
	if !bytes.Equal(first, data) {
		t.Fatalf("attempt 1 body = %q, want %q", first, data)
	}
	if err := req.Body.Close(); err != nil {
		t.Fatalf("close attempt 1: %v", err)
	}

	// Attempt 2 after GOAWAY.
	body2, err := requireGetBody(t, req)()
	if err != nil {
		t.Fatalf("GetBody after close: %v", err)
	}
	second, err := io.ReadAll(body2)
	if err != nil {
		t.Fatalf("read attempt 2: %v", err)
	}
	if !bytes.Equal(second, data) {
		t.Fatalf("replay after close = %q, want %q", second, data)
	}
	body2.Close()

	// Zero-copy invariant: the durable snapshot stays intact and owned by the
	// caller for the whole retry window.
	if !bytes.Equal(data, []byte("bray-poc-hot-path-payload")) {
		t.Fatalf("durable snapshot mutated/freed: %q", data)
	}
}

// TestPOC_HotPathRepeatedReplays proves each GetBody() call yields an
// independent reader starting at offset 0 — a client may hit GOAWAY more than
// once while walking a connection pool.
func TestPOC_HotPathRepeatedReplays(t *testing.T) {
	data := bytes.Repeat([]byte("abcd"), 512)

	cfg := &Config{Path: "/bray"}
	req := &http.Request{
		URL: &url.URL{Scheme: "https", Host: "example.com", Path: "/bray"},
	}
	if err := cfg.FillPacketRequestBytes(req, "sess", "0", data); err != nil {
		t.Fatalf("FillPacketRequestBytes: %v", err)
	}

	for i := 1; i <= 3; i++ {
		b, err := requireGetBody(t, req)()
		if err != nil {
			t.Fatalf("GetBody #%d: %v", i, err)
		}
		got, err := io.ReadAll(b)
		if err != nil {
			t.Fatalf("read #%d: %v", i, err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("replay #%d = %d bytes (want %d), mismatch", i, len(got), len(data))
		}
		b.Close()
	}
}

// TestPOC_NonHotPathReplaySurvivesBufferRelease is the core regression guard.
//
// FillPacketRequest wraps a pooled MultiBuffer. Closing the body releases those
// buffers back to buf's pool, where another goroutine can immediately reuse
// them. If GetBody handed back a view onto the original MultiBuffer, the replay
// would read whatever the recycling goroutine wrote. This test forces that
// scenario by churning the pool between close and replay.
func TestPOC_NonHotPathReplaySurvivesBufferRelease(t *testing.T) {
	want := []byte("bray-poc-non-hot-payload")
	payload := buf.MergeBytes(nil, want)

	cfg := &Config{Path: "/bray"}
	req := newPacketUpRequest(t)
	if err := cfg.FillPacketRequest(req, "sess", "0", payload); err != nil {
		t.Fatalf("FillPacketRequest: %v", err)
	}
	requireGetBody(t, req) // non-hot path must always expose a replayable body

	// Attempt 1: consume and close, which releases the MultiBuffer.
	first, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read attempt 1: %v", err)
	}
	if !bytes.Equal(first, want) {
		t.Fatalf("attempt 1 body = %q, want %q", first, want)
	}
	if err := req.Body.Close(); err != nil {
		t.Fatalf("close attempt 1: %v", err)
	}

	// Churn the buffer pool so the released buffers are very likely recycled
	// and overwritten by the time we replay.
	for i := 0; i < 64; i++ {
		b := buf.New()
		b.Write(bytes.Repeat([]byte{0xFF}, 2048))
		b.Release()
	}

	// Attempt 2 must still see the original payload.
	body2, err := requireGetBody(t, req)()
	if err != nil {
		t.Fatalf("GetBody after release: %v", err)
	}
	second, err := io.ReadAll(body2)
	if err != nil {
		t.Fatalf("read attempt 2: %v", err)
	}
	if !bytes.Equal(second, want) {
		t.Fatalf("replay after buffer release = %q, want %q", second, want)
	}
	body2.Close()
}

// TestPOC_ReplayBodiesAreIndependent guards against a replay implementation
// that returns the same reader instance (offset already at EOF).
func TestPOC_ReplayBodiesAreIndependent(t *testing.T) {
	data := []byte("independent-replay")
	cfg := &Config{Path: "/bray"}
	req := &http.Request{
		URL: &url.URL{Scheme: "https", Host: "example.com", Path: "/bray"},
	}
	if err := cfg.FillPacketRequestBytes(req, "sess", "0", data); err != nil {
		t.Fatalf("FillPacketRequestBytes: %v", err)
	}

	b1, err := requireGetBody(t, req)()
	if err != nil {
		t.Fatalf("GetBody 1: %v", err)
	}
	if _, err := io.ReadAll(b1); err != nil {
		t.Fatalf("read 1: %v", err)
	}
	b1.Close()

	b2, err := requireGetBody(t, req)()
	if err != nil {
		t.Fatalf("GetBody 2: %v", err)
	}
	defer b2.Close()
	if b2 == b1 {
		t.Fatal("GetBody returned the same reader instance; a replay would read from EOF")
	}
	got, err := io.ReadAll(b2)
	if err != nil {
		t.Fatalf("read 2: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("second replay = %q, want %q", got, data)
	}
}

// TestPOC_PlacementVariantsDefineGetBody covers the non-body uplink placements
// too: the retry path is data-placement agnostic, so every placement that
// carries a body must expose a replayable one.
func TestPOC_PlacementVariantsDefineGetBody(t *testing.T) {
	cases := []struct {
		name      string
		placement string
		wantBody  bool
	}{
		{name: "body", placement: PlacementBody, wantBody: true},
		{name: "auto", placement: PlacementAuto, wantBody: true},
		{name: "header", placement: PlacementHeader, wantBody: false},
		{name: "cookie", placement: PlacementCookie, wantBody: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte("placement-payload")

			cfg := &Config{Path: "/bray", UplinkDataPlacement: tc.placement}
			req := &http.Request{
				URL: &url.URL{Scheme: "https", Host: "example.com", Path: "/bray"},
			}
			if err := cfg.FillPacketRequestBytes(req, "sess", "0", data); err != nil {
				t.Fatalf("FillPacketRequestBytes: %v", err)
			}

			if tc.wantBody && req.GetBody == nil {
				t.Fatalf("placement %q carries a body but defines no GetBody", tc.placement)
			}
			if req.GetBody != nil {
				b, err := req.GetBody()
				if err != nil {
					t.Fatalf("GetBody: %v", err)
				}
				got, err := io.ReadAll(b)
				if err != nil {
					t.Fatalf("read: %v", err)
				}
				if !bytes.Equal(got, data) {
					t.Fatalf("placement %q replay = %q, want %q", tc.placement, got, data)
				}
				b.Close()
			}
		})
	}
}
