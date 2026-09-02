package splithttp

import (
	"bytes"
	"net/http"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/signal/done"
)

// POC gate for the dseg delivery-latency floor (2026-09-02 audit).
//
// Measured on this repo, before this fix:
//
//	BenchmarkXHTTP_H2_DsegModes dseg_on  : 20.3 ms/op,  3.2 MB/s
//	BenchmarkXHTTP_H2_DsegModes dseg_off :  0.6 ms/op, 108 MB/s   (33x)
//	Read-phase histogram (150 ops): 100% > 3ms, 82% in the 10–21ms band.
//
// Root causes (all server-side):
//  1. handleDownSegment polled with time.Sleep(20ms) for in-flight
//     segments — a hard 20ms floor on every sub-1MiB response delivery.
//  2. The fast path 404'd any pull whose segment was not yet finalized,
//     even when its bytes were already in the cache, so the client burned
//     its 5ms retry cadence waiting out downsegCommitInterval (10ms).
//
// Fix (this change): segment-ready broadcast channel (close-and-replace)
// fired by append-commit / lazy-commit / finalize / shutdown; the pull
// handler holds the request on it (10ms backstop tick as missed-wakeup
// insurance + lazy-commit trigger), and the fast-path 404 now only fires
// when the segment NEVER started receiving.
//
// Gate: pulling a segment whose bytes were fully written by the producer
// must resolve within a small fraction of the old 20ms floor. Pre-fix this
// reliably measured ~20.5ms (POC FAIL); post-fix target < 5ms.

func TestPOCSegmentDeliveryWaitBoundedByEventNotPoll(t *testing.T) {
	withZeroDownsegJitter(t)
	h := refCountTestHandler(t)
	sess, id := downsegTestSession(h)
	if sess == nil {
		t.Fatal("enterDownsegMode failed")
	}

	prod := &httpServerConn{Instance: done.New(), sess: sess}
	payload := bytes.Repeat([]byte{0x42}, 32*1024) // interactive-size response

	// The producer writes the whole response in one shot, then goes QUIET
	// without finalizing (no Close): the segment stays in-flight, exactly
	// like a live origin mid-response. A real client pull for seq 0 now
	// arrives while the bytes are in the cache but unfinalized.
	if _, err := prod.Write(payload); err != nil {
		t.Fatal(err)
	}

	// Measure wall-clock from "producer quiet" to "pull resolved with the
	// full payload". handleDownSegment must hold this pull (bytes started,
	// producedCount==0 pre-commit) and deliver on the commit event.
	prodQuiet := time.Now()
	code, body := pullSegmentWithID(h, id, 0)
	wait := time.Since(prodQuiet)

	if code != http.StatusOK || !bytes.Equal(body, payload) {
		t.Fatalf("pull: code=%d bodyLen=%d want 200/%d (wait=%v)", code, len(body), len(payload), wait)
	}

	// GATE: pre-fix behaviour parks the pull behind the 20ms Sleep poll
	// (measured 20.5ms, FAIL at this gate). Event-driven delivery resolves
	// within the commit-interval order of magnitude; generous CI headroom
	// still separates the two designs by a clean margin.
	if wait >= 15*time.Millisecond {
		t.Fatalf("POC FAIL: segment pull waited %v — poll-bound, not event-bound (expected <15ms)", wait)
	}
	t.Logf("POC PASS: pull TTFB after producer quiet = %v (%d bytes)", wait, len(body))
}
