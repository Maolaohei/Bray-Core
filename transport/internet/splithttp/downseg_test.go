package splithttp

import (
	"bytes"
	"testing"
)

func TestDownSegAppendGet(t *testing.T) {
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

func TestDownSegSlidingGone(t *testing.T) {
	c := newDownSegCache()
	// Produce more than the sliding window.
	for i := 0; i < downsegMaxSegs+5; i++ {
		c.append(bytes.Repeat([]byte{byte(i)}, downsegSize))
	}
	// Oldest segments are gone (410), newest available.
	oldestEver := uint64(0)
	if _, _, gone := c.get(oldestEver); !gone {
		t.Fatalf("seg %d should be gone after slide", oldestEver)
	}
	firstKept := c.producedCount() - downsegMaxSegs
	if _, ok, gone := c.get(firstKept); !ok || gone {
		t.Fatalf("seg %d should still be available", firstKept)
	}
	last := c.producedCount() - 1
	if _, ok, _ := c.get(last); !ok {
		t.Fatalf("seg %d should be available", last)
	}
	// Cache must not exceed the window bound.
	c.mu.Lock()
	n := len(c.segs)
	c.mu.Unlock()
	if n > downsegMaxSegs {
		t.Fatalf("cache grew to %d segs (window %d)", n, downsegMaxSegs)
	}
}
