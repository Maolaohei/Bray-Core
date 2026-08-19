package splithttp

import (
	"bytes"
	"testing"
)

// withZeroDownsegJitter pins the per-segment size jitter to 0 for the
// duration of t/b so tests that assert exact segment boundaries (1 MiB) stay
// deterministic; restored via t.Cleanup.
func withZeroDownsegJitter(tb testing.TB) {
	tb.Helper()
	old := downsegSizeJitterFn
	downsegSizeJitterFn = func() int32 { return 0 }
	tb.Cleanup(func() { downsegSizeJitterFn = old })
}

func TestDownSegAppendGet(t *testing.T) {
	withZeroDownsegJitter(t)
	c := newDownSegCache()

	// Feed bytes larger than one segment: must split across segments.
	big := bytes.Repeat([]byte{0xAB}, downsegSize+1000)
	c.append(big)

	// Segment 0 finalized (full); segment 1 is in-flight (partial).
	s0, ok0, gone0 := c.get(0)
	if !ok0 || gone0 || len(s0) != downsegSize {
		t.Fatalf("seg0: ok=%v gone=%v len=%d want %d", ok0, gone0, len(s0), downsegSize)
	}
	if !bytes.Equal(s0, big[:downsegSize]) {
		t.Fatal("seg0 payload mismatch")
	}
	// Partial segment 1 is NOT readable until EOF finalizes it.
	if _, ok1, _ := c.get(1); ok1 {
		t.Fatal("partial segment 1 must not be readable before finalize")
	}
	c.finalize() // EOF
	s1, ok1, _ := c.get(1)
	if !ok1 || len(s1) != 1000 || !bytes.Equal(s1, big[downsegSize:]) {
		t.Fatalf("seg1 after finalize: ok=%v len=%d", ok1, len(s1))
	}
	// Beyond final is gone/not-produced and stream complete.
	if _, ok2, _ := c.get(2); ok2 {
		t.Fatal("seg2 should not exist after finalize")
	}
}

func TestDownSegAppendPartial(t *testing.T) {
	c := newDownSegCache()
	c.append([]byte("hello"))
	// Not finalized: the partial in-flight segment is not readable.
	if _, ok, _ := c.get(0); ok {
		t.Fatal("partial seg0 must not be readable before finalize")
	}
	// Second append continues segment 0.
	c.append([]byte(" world"))
	c.finalize()
	s, ok, _ := c.get(0)
	if !ok || string(s) != "hello world" {
		t.Fatalf("finalized seg: %v %q", ok, s)
	}
	// finalize must only finalize the one in-flight segment.
	if c.producedCount() != 1 {
		t.Fatalf("produced=%d want 1", c.producedCount())
	}
}

// TestDownSegSlidingGone: with the reader keeping up, the window stays at
// the steady-state bound and truly-old segments go 410.
func TestDownSegSlidingGone(t *testing.T) {
	withZeroDownsegJitter(t)
	c := newDownSegCache()
	// Produce more than the steady-state window.
	for i := 0; i < downsegMaxSegs+5; i++ {
		c.append(bytes.Repeat([]byte{byte(i)}, downsegSize))
	}
	// Simulate the client having consumed up to the newest-1 (steady state:
	// watermark near produced, window back at 8). Only then should the
	// truly oldest fall off the window -> 410.
	last := c.producedCount()
	for i := 0; i < int(last)-1; i++ {
		if _, ok, _ := c.get(uint64(i)); !ok {
			t.Fatalf("seg %d should be available", i)
		}
	}
	// Oldest-ever segments are gone (410), newest available.
	oldestEver := uint64(0)
	if _, _, gone := c.get(oldestEver); !gone {
		t.Fatalf("seg %d should be gone after slide", oldestEver)
	}
	firstKept := c.producedCount() - downsegMaxSegs
	if _, ok, gone := c.get(firstKept); !ok || gone {
		t.Fatalf("seg %d should still be available", firstKept)
	}
	last = c.producedCount() - 1
	if _, ok, _ := c.get(last); !ok {
		t.Fatalf("seg %d should be available", last)
	}
	// Cache must not exceed the adaptive bound (memory safety), even though
	// it legitimately grew beyond steady state while the reader lagged.
	c.mu.Lock()
	n := len(c.segs)
	c.mu.Unlock()
	if n > int(downsegAdaptiveSegs) {
		t.Fatalf("cache grew to %d segs (adaptive bound %d)", n, downsegAdaptiveSegs)
	}
}

// TestDownSegAdaptiveWindow: a reader that lags production keeps the window
// growing (up to the adaptive bound) so the stream does NOT 410 mid-flight —
// the regression for "偶发中断/转圈" (V2rayN video drops).
func TestDownSegAdaptiveWindow(t *testing.T) {
	withZeroDownsegJitter(t)
	c := newDownSegCache()

	// Produce far more than the steady-state window without the client
	// pulling (simulates a slow reader / burst production).
	for i := 0; i < downsegMaxSegs*3; i++ {
		c.append(bytes.Repeat([]byte{byte(i)}, downsegSize))
	}

	// The client eventually catches up: it should be able to pull segments
	// well beyond the fixed 8-segment bound — the window must have grown.
	// Without the adaptive window these would have been evicted -> 410.
	pulled := uint64(0)
	for seq := uint64(0); seq < c.producedCount(); seq++ {
		p, ok, gone := c.get(seq)
		if gone {
			t.Fatalf("seq %d gone 410 though client is catching up (lag=%d)", seq, c.producedCount()-seq)
		}
		if !ok {
			break // not yet produced
		}
		if len(p) == 0 {
			t.Fatalf("seq %d empty", seq)
		}
		pulled = seq + 1
	}
	if pulled < downsegMaxSegs*3-2 {
		t.Fatalf("expected to pull ~%d produced segments, got only %d (window didn't grow)", downsegMaxSegs*3, pulled)
	}
}

// TestDownSegAdaptiveWindowBounded: the adaptive window must not grow without
// bound even if production runs far ahead (memory bound).
func TestDownSegAdaptiveWindowBounded(t *testing.T) {
	withZeroDownsegJitter(t)
	c := newDownSegCache()
	for i := 0; i < downsegAdaptiveSegs+10; i++ {
		c.append(bytes.Repeat([]byte{byte(i)}, downsegSize))
	}
	c.mu.Lock()
	n := len(c.segs)
	c.mu.Unlock()
	if n > int(downsegAdaptiveSegs)+1 {
		t.Fatalf("cache grew to %d segs (adaptive bound %d)", n, downsegAdaptiveSegs)
	}
}
