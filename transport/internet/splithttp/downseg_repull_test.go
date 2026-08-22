package splithttp

import (
	"bytes"
	"testing"
	"time"
)

// TestDownSegRepullGrace: a delivered segment must remain re-pullable for
// downsegRepullGrace (lost GET response), then report gone after expiry.
func TestDownSegRepullGrace(t *testing.T) {
	c := newDownSegCache()
	payload := []byte("segment-zero-payload")
	c.append(payload)
	c.finalize()

	p, ok, gone := c.get(0)
	if !ok || gone || !bytes.Equal(p, payload) {
		t.Fatalf("first get: ok=%v gone=%v", ok, gone)
	}

	// Immediate re-pull (response was lost) must be served from the grace window.
	p, ok, gone = c.get(0)
	if !ok || gone || !bytes.Equal(p, payload) {
		t.Fatalf("repull within grace: ok=%v gone=%v", ok, gone)
	}

	// Expire the grace entry: the segment is now genuinely gone.
	c.mu.Lock()
	c.deliveredAtNs[0] = time.Now().UnixNano() - int64(downsegRepullGrace) - 1
	c.mu.Unlock()
	if _, _, gone := c.get(0); !gone {
		t.Fatal("repull after grace should report gone")
	}
}

// TestDownSegRepullByteCap: retained grace entries respect downsegRepullMaxBytes,
// dropping the oldest delivered entries first.
func TestDownSegRepullByteCap(t *testing.T) {
	oldCap := downsegRepullMaxBytes
	downsegRepullMaxBytes = 3 * 1024 // 3 x 1KiB
	defer func() { downsegRepullMaxBytes = oldCap }()

	c := newDownSegCache()
	seg := make([]byte, 1024)
	// Seed five finalized 1KiB segments directly so sizes are exact.
	for i := uint64(0); i < 5; i++ {
		p := make([]byte, len(seg))
		copy(p, seg)
		c.mu.Lock()
		c.segs[i] = p
		c.produced = i + 1
		c.mu.Unlock()
	}
	c.mu.Lock()
	c.final = true
	c.stopped = true
	c.mu.Unlock()

	for s := uint64(0); s < 5; s++ {
		if _, ok, _ := c.get(s); !ok {
			t.Fatalf("get(%d) failed", s)
		}
	}
	c.mu.Lock()
	got := len(c.deliveredSegs)
	bytesRetained := c.deliveredBytes
	c.mu.Unlock()
	if got != 3 || bytesRetained != 3*1024 {
		t.Fatalf("grace retained %d segs / %d bytes, want 3 / 3072", got, bytesRetained)
	}
	// Oldest entries dropped first; newest still re-pullable.
	if _, ok, _ := c.get(1); ok {
		t.Fatal("seq 1 should have been evicted (oldest first)")
	}
	if p, ok, _ := c.get(4); !ok || len(p) != 1024 {
		t.Fatal("seq 4 should still be re-pullable")
	}
}
