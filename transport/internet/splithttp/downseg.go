package splithttp

// Downlink segment cache for Bray-paired downlink segmentation (M1).
//
// Server side: bytes produced toward the client (downlink, from the proxied
// target) are sliced into fixed-size segments and kept in a per-session
// sliding cache. The client pulls them with `GET ?seq=N` requests instead of
// one unbounded long GET — a much more browser-natural traffic shape (the
// download looks like a sequence of short video-segment GETs, HLS/DASH style).
//
// Design invariants:
//   - uploadQueue stays the uplink reorder backbone (unchanged).
//   - A session is "segment mode" once a dseg-marked GET(+seq) is seen;
//     legacy long-GET clients keep the plain streaming path untouched.
//   - Cache is bounded: at most maxSegs segments sliding forward; segments
//     already slid past are answered 410 (Gone) so the client re-establishes
//     or skips, never grows memory without bound.
//   - Producing > maxSegs ahead of consumption drops the oldest (the reader
//     is expected to keep up; a far-ahead producer would leak memory).

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	// downsegSize is the nominal segment payload size. 1 MiB: (a) amortizes
	// the fixed per-segment HTTP/framing overhead far better than 256KiB
	// (~81 -> ~205 MB/s on loopback) and (b) still reads as a natural HLS /
	// DASH video-segment download (1-6MiB typical), so the multi-short-GET
	// fingerprint benefit holds.
	downsegSize = 1 << 20 // 1 MiB

	// downsegMaxSegs bounds the sliding window of produced segments per
	// session. 8 x 1MiB = 8 MiB worst case per session (bounded).
	downsegMaxSegs = 8

	// downsegSizeJitterMax is the per-segment size jitter (±~10% of
	// 1 MiB, right-skewed) so a Bray download emits bitrate-variable
	// media-style segments instead of a perfectly uniform 1 MiB cadence.
	downsegSizeJitterMax = downsegSize / 10
	// downsegSizeMin floors segments so the pull overhead amortizes even
	// with jitter drawn low.
	downsegSizeMin = downsegSize / 2
)

// downSegCache holds produced downlink segments for one session.
type downSegCache struct {
	mu sync.Mutex

	produced uint64 // next segment index to finalize (first unwritten/full)
	// segs maps segment index -> payload. Sliding: we keep at most
	// downsegMaxSegs; seq < (produced - downsegMaxSegs) is 410 Gone.
	segs map[uint64][]byte
	// segSizeBySeq remembers the target size chosen for each segment
	// index so appends that span a boundary keep the same budget. The
	// sizes are randomized (per-segment jitter) so a download does not
	// emit a perfectly uniform 1 MiB cadence (fingerprint-clustering
	// risk; real HLS/DASH segments vary with bitrate).
	segSizeBySeq map[uint64]int
	// final is true once finalize() ran (stream complete; no more
	// segments will be produced).
	final bool

	// pullAtNs / writeAtNs stamp the last client-segment-pull and last
	// downlink-production activity (unix nanos). The production-leg
	// handler uses them to reap zombie sessions (client gone / TCP
	// half-open): no pull and no production for downsegProdIdleLimit
	// means nobody is consuming this stream.
	pullAtNs  atomic.Int64
	writeAtNs atomic.Int64
}

func newDownSegCache() *downSegCache {
	now := time.Now().UnixNano()
	c := &downSegCache{
		segs:         make(map[uint64][]byte, downsegMaxSegs),
		segSizeBySeq: make(map[uint64]int, downsegMaxSegs),
	}
	c.pullAtNs.Store(now)
	c.writeAtNs.Store(now)
	return c
}

// downsegSizeFor returns the target payload size for segment seq,
// randomized around downsegSize (right-skewed, ±~10%) so traffic does not
// form a uniform 1 MiB row. Deterministic for a given seq (fixed after
// first draw) so multi-append segments hold a stable budget.
func (c *downSegCache) downsegSizeFor(seq uint64) int {
	if s, ok := c.segSizeBySeq[seq]; ok {
		return s
	}
	// Right-skewed around 1 MiB: most segments land slightly below the
	// nominal size, mirroring bitrate-variable media segments.
	delta := int(downsegSizeJitterFn())
	s := downsegSize + delta
	if s < downsegSizeMin {
		s = downsegSizeMin
	}
	c.segSizeBySeq[seq] = s
	return s
}

// downsegSizeJitterFn yields the per-segment jitter delta. It is a package
// variable so tests can substitute a fixed value for deterministic segment
// sizes; the production source is right-skewed around zero.
var downsegSizeJitterFn = func() int32 {
	return biasedRangeRand(-downsegSizeJitterMax/2, downsegSizeJitterMax)
}

// append writes downlink bytes into segments, producing new segment slots as
// needed (sliding, evicting the oldest when over capacity).
func (c *downSegCache) append(b []byte) {
	if len(b) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeAtNs.Store(time.Now().UnixNano())
	off := 0
	for off < len(b) {
		idx := c.produced
		cur, ok := c.segs[idx]
		size := c.downsegSizeFor(idx)
		if !ok || len(cur) >= size {
			// Start/spill into a new segment; eject the oldest when the
			// sliding window is full.
			if idx >= downsegMaxSegs {
				delete(c.segs, idx-downsegMaxSegs)
			}
			// Pre-allocate the segment's full payload once. Growing from
			// nil via repeated append() reallocates/copies ~5x the segment
			// size (14 allocs vs 1, ~6x CPU on 1 MiB segments — see POC).
			// The final small segment overallocates its cap by at most
			// downsegSizeMin, a bounded one-off per session; the win is on
			// the fast path where full segments are the norm.
			cur = make([]byte, 0, size)
			c.segs[idx] = cur
		}
		cur = c.segs[idx]
		tail := size - len(cur)
		n := len(b) - off
		if n > tail {
			n = tail
		}
		c.segs[idx] = append(cur, b[off:off+n]...)
		off += n
		if len(c.segs[idx]) >= size {
			c.produced++
		} else {
			break
		}
	}
	if c.produced > downsegMaxSegs {
		// Trim any gaps older than the window (defensive).
		lo := c.produced - downsegMaxSegs
		for idx := range c.segs {
			if idx < lo {
				delete(c.segs, idx)
			}
		}
	}
}

// get returns the payload of a FINALIZED segment seq, and whether it is
// available. A produced-but-partial (in-flight) segment reports ok=false,
// gone=false: the client should wait; the producer finalizes it at EOF.
func (c *downSegCache) get(seq uint64) (payload []byte, ok bool, gone bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pullAtNs.Store(time.Now().UnixNano())
	// Only finalized segments (index < produced) are readable.
	if seq >= c.produced {
		return nil, false, false
	}
	lo := uint64(0)
	if c.produced > downsegMaxSegs {
		lo = c.produced - downsegMaxSegs
	}
	if seq < lo {
		return nil, false, true // slid past
	}
	p, ok := c.segs[seq]
	if !ok {
		return nil, false, true // evicted
	}
	return p, true, false
}

// finalize marks the stream complete: the current partial segment (if any)
// becomes finalized, and no more segments will be produced.
func (c *downSegCache) finalize() {
	c.mu.Lock()
	defer c.mu.Unlock()
	// produced points at the next finalized index; if a partial segment
	// exists there (in-flight), finalize it by advancing produced.
	if _, exists := c.segs[c.produced]; exists {
		c.produced++
	}
	c.final = true
}

// producedCount returns the highest produced index + 1 (for gap / EOF logic).
func (c *downSegCache) producedCount() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.produced
}

// over reports whether stream has ended (finalize ran) and no more segments
// are coming.
func (c *downSegCache) over() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.final
}

// idleFor reports how long neither a segment pull nor downlink production has
// occurred (used to reap zombie production legs whose client vanished).
func (c *downSegCache) idleFor() time.Duration {
	now := time.Now().UnixNano()
	last := c.pullAtNs.Load()
	if w := c.writeAtNs.Load(); w > last {
		last = w
	}
	d := now - last
	if d < 0 {
		return 0
	}
	return time.Duration(d)
}

// downsegHeader is intentionally NOT used: downlink segmentation is detected
// by "sessioned GET with a seq in the meta token" (dead-path reuse), so no
// extra header/label is ever placed on the wire.
