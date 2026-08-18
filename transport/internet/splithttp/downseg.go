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
)

const (
	// downsegSize is the nominal segment payload size. A little jitter on
	// the producer keeps sizes from being an exact constant (wire_audit).
	downsegSize = 256 << 10 // 256 KiB

	// downsegMaxSegs bounds the sliding window of produced segments per
	// session. 24 * 256KiB = 6 MiB worst case per session (bounded).
	downsegMaxSegs = 24
)

// downSegCache holds produced downlink segments for one session.
type downSegCache struct {
	mu sync.Mutex

	produced uint64 // next segment index to finalize (first unwritten/full)
	// segs maps segment index -> payload. Sliding: we keep at most
	// downsegMaxSegs; seq < (produced - downsegMaxSegs) is 410 Gone.
	segs map[uint64][]byte
	// final is true once finalize() ran (stream complete; no more
	// segments will be produced).
	final bool
}

func newDownSegCache() *downSegCache {
	return &downSegCache{segs: make(map[uint64][]byte, downsegMaxSegs)}
}

// append writes downlink bytes into segments, producing new segment slots as
// needed (sliding, evicting the oldest when over capacity).
func (c *downSegCache) append(b []byte) {
	if len(b) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	off := 0
	for off < len(b) {
		idx := c.produced
		cur, ok := c.segs[idx]
		if !ok || len(cur) >= downsegSize {
			// Start/spill into a new segment; eject the oldest when the
			// sliding window is full.
			if idx >= downsegMaxSegs {
				delete(c.segs, idx-downsegMaxSegs)
			}
			cur = nil
			c.segs[idx] = cur
		}
		tail := downsegSize - len(cur)
		n := len(b) - off
		if n > tail {
			n = tail
		}
		c.segs[idx] = append(cur, b[off:off+n]...)
		off += n
		if len(c.segs[idx]) >= downsegSize {
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

// downsegHeader is the client-local marker header that declares a GET as a
// downlink-segmentation request (segment pull or production leg). It is a
// request header only (never a fixed server header), enabling Bray-paired
// segment mode without touching legacy long-GET clients.
const downsegHeader = "X-Bray-Dseg"
