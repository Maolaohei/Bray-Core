package splithttp

import (
	"bytes"
	"testing"
	"time"
)

// F7: idleFor must reflect real inactivity (pull/produce both refresh it) and
// must NOT count a healthy transfer as idle.
func TestDownSegCacheIdleFor(t *testing.T) {
	c := newDownSegCache()

	// Freshly created: idle ~0.
	if d := c.idleFor(); d > 10*time.Second {
		t.Fatalf("fresh cache idle=%v want ~0", d)
	}

	// Simulate a long gap with no activity.
	past := time.Now().Add(-2 * time.Hour).UnixNano()
	c.writeAtNs.Store(past)
	c.pullAtNs.Store(past)
	if d := c.idleFor(); d < 90*time.Minute {
		t.Fatalf("idle after inactivity=%v want >90m", d)
	}

	// A segment pull refreshes it to ~0 even though no production happened.
	c.get(0)
	if d := c.idleFor(); d > 10*time.Second {
		t.Fatalf("idle after pull=%v want ~0", d)
	}

	// So does fresh production.
	past = time.Now().Add(-1 * time.Hour).UnixNano()
	c.pullAtNs.Store(past)
	if d := c.idleFor(); d < 30*time.Minute {
		t.Fatalf("idle after production gap=%v want >30m", d)
	}
	c.append([]byte("x"))
	if d := c.idleFor(); d > 10*time.Second {
		t.Fatalf("idle after append=%v want ~0", d)
	}
}

// F8: segment sizes vary under jitter (not a uniform 1 MiB row), every size
// respects the [min, max] band, and a payload splitting across several
// segments within the sliding window reassembles losslessly.
func TestDownSegSegmentJitter(t *testing.T) {
	// Force the production jitter source (a sibling test may have pinned it
	// to zero; these run sequentially so just restore it).
	old := downsegSizeJitterFn
	downsegSizeJitterFn = func() int32 { return biasedRangeRand(-downsegSizeJitterMax/2, downsegSizeJitterMax) }
	defer func() { downsegSizeJitterFn = old }()

	// 1) Sampled target sizes genuinely vary and stay inside the band.
	c := newDownSegCache()
	seen := map[int]bool{}
	for seq := uint64(0); seq < 64; seq++ {
		s := c.downsegSizeFor(seq)
		if s < downsegSizeMin || s > downsegSize+downsegSizeJitterMax {
			t.Fatalf("seq %d size %d outside [%d, %d]", seq, s, downsegSizeMin, downsegSize+downsegSizeJitterMax)
		}
		seen[s] = true
	}
	if len(seen) < 8 {
		t.Fatalf("jitter yielded only %d distinct sizes (jitter not applied?)", len(seen))
	}

	// 2) Lossless reassembly: 4 MiB (= 4-8 segments depending on jitter)
	// stays within the 8-segment window so nothing slides, and the full
	// stream must come back byte-identical under randomized sizes.
	c2 := newDownSegCache()
	payload := bytes.Repeat([]byte{0x7D}, downsegSize*4+777)
	c2.append(payload)
	c2.finalize()

	var got []byte
	for seq := uint64(0); ; seq++ {
		s, ok, gone := c2.get(seq)
		if ok {
			if len(s) == 0 {
				t.Fatalf("seg %d empty payload", seq)
			}
			// Only non-final segments are size-capped; the final segment is
			// whatever tail remained after the last full segment.
			if seq < c2.producedCount()-1 && len(s) < downsegSizeMin {
				t.Fatalf("seg %d len=%d below min %d", seq, len(s), downsegSizeMin)
			}
			got = append(got, s...)
			continue
		}
		if gone {
			t.Fatalf("seg %d unexpectedly slid past (window too small for test payload?)", seq)
		}
		break // not produced / finalized run ended
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("reassembled %d bytes want %d", len(got), len(payload))
	}
}
