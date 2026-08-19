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
	// session at steady state. 8 x 1MiB = 8 MiB typical per session.
	downsegMaxSegs = 8
	// downsegAdaptiveSegs is the upper bound the window may grow to when the
	// client falls behind production (slow reader, burst, video throttling,
	// high-RTT bulk downloads). Growing the window keeps temporarily-lagging
	// consumers from getting 410 (which today tears the stream down); bounded
	// so a runaway producer (dead client / TCP half-open) cannot pin
	// unbounded memory — that case is additionally reaped by the
	// production-leg idle sweeper.
	// Set high enough to absorb the consumer lag of high-throughput file
	// downloads over lossy/high-RTT paths (V2rayN report: fixed 8 caused
	// 410 drops on file download).
	downsegAdaptiveSegs = 64 // 64 x 1MiB = 64 MiB worst case per session
	// downsegAdaptiveHeadroom is extra window (segments) kept beyond the
	// exact production-consumption gap so out-of-order/retried pulls of
	// already-produced segments do not spuriously 410.
	downsegAdaptiveHeadroom = 8

	// downsegSizeJitterMax is the per-segment size jitter (±~10% of
	// 1 MiB, right-skewed) so a Bray download emits bitrate-variable
	// media-style segments instead of a perfectly uniform 1 MiB cadence.
	downsegSizeJitterMax = downsegSize / 10
	// downsegSizeMin floors segments so the pull overhead amortizes even
	// with jitter drawn low.
	downsegSizeMin = downsegSize / 2
	// downsegCommitInterval bounds how long a segment may keep receiving
	// bytes before it is committed (made readable), even when far below
	// downsegSize. This is what makes a sub-1MiB stream (VLESS handshake,
	// first response bytes, small TUN packet) pullable instead of stranding
	// until 1 MiB fills — otherwise any short transaction deadlocks. Large
	// bursts still fill full segments and are committed by size, so the
	// sliding window does not flood with tiny segments on bulk traffic.
	// Bounded: a busy connection commits at most once per interval.
	downsegCommitInterval = 10 * time.Millisecond
)

// downSegCache holds produced downlink segments for one session.
type downSegCache struct {
	mu sync.Mutex

	produced uint64 // next segment index to finalize (first unwritten/full)
	// lastPulled is the highest segment index the client has successfully
	// pulled (+1), i.e. the consumption watermark. The sliding window grows
	// from the steady-state downsegMaxSegs up to downsegAdaptiveSegs so a
	// reader that lags production keeps the needed segments instead of
	// stranding them behind a fixed 8-segment cut (which caused 410 drop).
	lastPulled uint64
	// segs maps segment index -> payload. Sliding: we keep at most
	// downsegMaxSegs; seq < (produced - downsegMaxSegs) is 410 Gone.
	segs map[uint64][]byte
	// segSizeBySeq remembers the target size chosen for each segment
	// index so appends that span a boundary keep the same budget. The
	// sizes are randomized (per-segment jitter) so a download does not
	// emit a perfectly uniform 1 MiB cadence (fingerprint-clustering
	// risk; real HLS/DASH segments vary with bitrate).
	segSizeBySeq map[uint64]int
	// segStartedAt is the unix-nano time the in-flight (partial) segment
	// received its first byte. Used to commit a segment once it has been
	// receiving for downsegCommitInterval even if far below size, so a
	// sub-1MiB stream (VLESS handshake / first response bytes / small TUN
	// packet) becomes pullable without waiting to fill 1 MiB — otherwise
	// any sub-1MiB transaction deadlocks.
	segStartedAt int64
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

// windowSegs returns the sliding-window size (in segments) to keep. At
// steady state downsegMaxSegs; when the client lags production by more than
// the steady-state window, grow up to downsegAdaptiveSegs (with headroom) so
// the lagging reader does not 410. Caller must hold c.mu.
//
// lastPulled is the HIGHEST seq the client has pulled (worker goroutines pull
// out of order), so the window must additionally cushion the concurrency of
// the puller: an in-flight worker may still pull as far back as
// lastPulled - DownSegWindowSize. Otherwise a straggler worker 410s even
// though the client is overall keeping up (V2rayN large-download drop).
func (c *downSegCache) windowSegs() uint64 {
	// The puller reserves (takes) segment numbers monotonically but
	// completes them out of order. lastPulled is the highest COMPLETED seq,
	// yet in-flight workers may be taking up to DownSegWindowSize further
	// ahead, and the highest *completed* can lag what readers still need.
	// Keep enough so a storage worker isn't 410'd by a straggler.
	if c.produced <= c.lastPulled {
		return downsegMaxSegs
	}
	lag := c.produced - c.lastPulled
	// Cushion for out-of-order in-flight pulls of reserved-but-unfinished
	// segments. Without this a worker occasionally 410s even when the
	// client generally keeps up (V2rayN large-download drop).
	need := lag + DownSegWindowSize + downsegAdaptiveHeadroom
	if need > downsegAdaptiveSegs {
		need = downsegAdaptiveSegs
	}
	return need
}

// loIndex returns the lowest (produced, readable) segment index for the
// current window. seq < loIndex is Gone. Caller must hold c.mu.
func (c *downSegCache) loIndex() uint64 {
	win := c.windowSegs()
	if c.produced <= win {
		return 0
	}
	return c.produced - win
}

// append writes downlink bytes into segments. A segment is committed
// (produced++, readable) when it fills to its target size OR has been
// receiving bytes for downsegCommitInterval — the time bound is what makes a
// sub-1MiB stream pullable instead of stranding until 1 MiB fills.
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
			// (adaptive) sliding window is full.
			if lo := c.loIndex(); lo > 0 {
				// Defensive: the append loop itself can create gaps during
				// multi-segment appends; keep the exact oldest boundary.
				delete(c.segs, lo-1)
			}
			// Pre-allocate the segment's full payload once. Growing from
			// nil via repeated append() reallocates/copies ~5x the segment
			// size (14 allocs vs 1, ~6x CPU on 1 MiB segments — see POC).
			// The final small segment overallocates its cap by at most
			// downsegSizeMin, a bounded one-off per session; the win is on
			// the fast path where full segments are the norm.
			cur = make([]byte, 0, size)
			c.segs[idx] = cur
			c.segStartedAt = time.Now().UnixNano()
		}
		cur = c.segs[idx]
		tail := size - len(cur)
		n := len(b) - off
		if n > tail {
			n = tail
		}
		c.segs[idx] = append(cur, b[off:off+n]...)
		off += n
		filled := len(c.segs[idx]) >= size
		stale := !filled && time.Now().UnixNano()-c.segStartedAt >= int64(downsegCommitInterval)
		if filled || stale {
			c.produced++
		} else {
			break
		}
	}
	// Trim any segments older than the (adaptive) window. The producer may run
	// far ahead of a slow consumer; keeping only the window (which grows with
	// lag) bounds memory while preserving what the client still needs.
	if lo := c.loIndex(); lo > 0 {
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
	// Lazy commit: the producer may have written bytes into the in-flight
	// segment and gone quiet below the size threshold (VLESS handshake,
	// short response, small packet) with no further append to trigger the
	// time-based commit. If that segment has been receiving for
	// downsegCommitInterval, commit it now so this pull can see it —
	// otherwise a sub-1MiB transaction deadlocks forever (404 on a never
	// finalized segment).
	c.commitIfStaleLazy()
	// Only finalized segments (index < produced) are readable.
	if seq >= c.produced {
		return nil, false, false
	}
	if seq < c.loIndex() {
		return nil, false, true // slid past
	}
	p, ok := c.segs[seq]
	if !ok {
		return nil, false, true // evicted
	}
	// Advance the consumption watermark so the adaptive window tracks how
	// far the client has actually read (not just reserved).
	if seq+1 > c.lastPulled {
		c.lastPulled = seq + 1
	}
	return p, true, false
}

// commitIfStaleLazy commits the in-flight segment if it has been receiving
// bytes for at least downsegCommitInterval (regardless of size). Caller must
// hold c.mu.
func (c *downSegCache) commitIfStaleLazy() {
	if c.segStartedAt == 0 {
		return
	}
	if time.Now().UnixNano()-c.segStartedAt < int64(downsegCommitInterval) {
		return
	}
	if cur, ok := c.segs[c.produced]; ok && len(cur) > 0 {
		c.produced++
	}
	if lo := c.loIndex(); lo > 0 {
		for idx := range c.segs {
			if idx < lo {
				delete(c.segs, idx)
			}
		}
	}
}

// LazyCommitStale is the lock-taking wrapper used by the segment pull
// handler's fast path.
func (c *downSegCache) LazyCommitStale() {
	c.mu.Lock()
	c.commitIfStaleLazy()
	c.mu.Unlock()
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
