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
	"os"
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
	// NOTE: this is NOT the primary 410-protection mechanism — that is
	// deliver-on-get (see get()), which keeps every produced-but-not-yet-
	// delivered segment regardless of window math. The headroom only pads
	// the legacy window-SIZE bookkeeping that bounds total retained cache
	// entries (downsegAdaptiveSegs); 8 is sufficient padding given
	// deliver-on-get guarantees no gap below the last delivered seq.
	downsegAdaptiveHeadroom = 8

	// downsegSizeJitterMax is the per-segment size jitter (±~10% of
	// 1 MiB, right-skewed) so a Bray download emits bitrate-variable
	// media-style segments instead of a perfectly uniform 1 MiB cadence.
	downsegSizeJitterMax = downsegSize / 10
	// downsegSizeMin floors segments so the pull overhead amortizes even
	// with jitter drawn low.
	downsegSizeMin = downsegSize / 2
	// downsegInitialAllocFloor avoids allocating a full 1MiB backing array
	// for a short web/API response while retaining room for normal write bursts.
	downsegInitialAllocFloor = 64 << 10
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
	// spaceCond is the producer backpressure signal: append() blocks
	// (waiting on spaceCond) while undelivered segments are at the hard
	// bound, and get() broadcasts when it has delivered one. This turns
	// the cache into a bounded flow-controlled pipe instead of a
	// window-with-eviction: a produced-but-undelivered segment is NEVER
	// dropped, so a slow consumer (high-RTT link, video throttling) can
	// never be falsely 410'd — the producer simply stalls until the
	// client catches up, exactly like TCP flow control. The hard bound
	// caps memory (downsegAdaptiveSegs × ~1MiB worst case per session);
	// a vanished client is reaped by the production-leg idle sweeper
	// (finalize stays false, production-leg handler eventually times out).
	spaceCond *sync.Cond

	produced uint64 // next segment index to finalize (first unwritten/full)
	// lastPulled is the highest segment index the client has successfully
	// pulled (+1), i.e. the consumption watermark. Pure watermark for the
	// overflow accounting (undelivered = produced - lastPulled); NOT used
	// for eviction (see spaceCond backpressure doc).
	lastPulled uint64
	// segs maps segment index -> payload. Retains EXACTLY the produced-but-
	// not-yet-delivered segments (deliver-on-get removes on pull); bounded
	// by the backpressure bound above, never evicted through.
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

	// stopped is true once the session tears down (session close), or after
	// finalize with no room left. append() checks it before WAITING on
	// spaceCond so a blocked producer wakes up and errors out instead of
	// sleeping forever after the client vanished. Set under c.mu.
	stopped bool
}

func newDownSegCache() *downSegCache {
	now := time.Now().UnixNano()
	c := &downSegCache{
		segs:         make(map[uint64][]byte, downsegMaxSegs),
		segSizeBySeq: make(map[uint64]int, downsegMaxSegs),
	}
	c.spaceCond = sync.NewCond(&c.mu)
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

// NOTE: there is deliberately NO sliding-window/loIndex eviction in this
// cache. Eviction is "deliver-on-get" (get() deletes a segment once the
// client receives it) plus a hard overflow bound (evictOverflowLocked drops
// the oldest UNDELIVERED segments only when retained undelivered entries
// exceed downsegAdaptiveSegs). A produced-but-undelivered segment is never
// dropped while a straggler puller worker might still need it — window-size
// eviction keyed on lastPulled (a pre-fetch watermark) caused spurious 410s
// that tore down large downloads (V2rayN large-download drop).

// append writes downlink bytes into segments. A segment is committed
// (produced++, readable) when it fills to its target size OR has been
// receiving bytes for downsegCommitInterval — the time bound is what makes a
// sub-1MiB stream pullable instead of stranding until 1 MiB fills.
//
// Backpressure: when undelivered segments are at the hard bound
// (downsegAdaptiveSegs), append BLOCKS on spaceCond until get() delivers one
// (or the session shuts down). Produced-but-undelivered segments are never
// dropped — this is what makes a high-RTT / slow / throttled consumer immune
// to spurious 410s: the producer stalls like TCP flow control instead of the
// cache evicting the very segment the client is about to pull.
func (c *downSegCache) append(b []byte) {
	if len(b) == 0 || c.stopped {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeAtNs.Store(time.Now().UnixNano())
	off := 0
	for off < len(b) {
		// Backpressure gate BEFORE allocating a new segment: do not grow
		// the undelivered set past the bound. Block until a pull frees a
		// slot or the stream shuts down.
		for c.undeliveredCountLocked() >= downsegAdaptiveSegs && !c.stopped {
			c.spaceCond.Wait()
		}
		if c.stopped {
			return
		}
		idx := c.produced
		cur, ok := c.segs[idx]
		size := c.downsegSizeFor(idx)
		if !ok || len(cur) >= size {
			// Start/spill into a new segment. No window-size eviction:
			// deliver-on-get + backpressure are the memory policy (see
			// spaceCond doc); a produced-but-undelivered segment is never
			// dropped while a straggler might still need it.
			// Full preallocation is ideal for media segments but turns every
			// short web/API response into a 1MiB allocation. Reserve for the
			// available write with a modest floor; larger segments grow normally.
			initialCap := len(b) - off
			if initialCap < downsegInitialAllocFloor {
				initialCap = downsegInitialAllocFloor
			}
			if initialCap > size {
				initialCap = size
			}
			cur = make([]byte, 0, initialCap)
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
}

// undeliveredCountLocked returns the actual number of retained segments.
// Caller holds c.mu. lastPulled is a high watermark and can jump ahead when
// H2 pull workers finish out of order; using produced-lastPulled would then
// incorrectly report spare capacity while lower segments are still retained.
func (c *downSegCache) undeliveredCountLocked() uint64 {
	return uint64(len(c.segs))
}

// get returns the payload of a FINALIZED segment seq, and whether it is
// available. A produced-but-partial (in-flight) segment reports ok=false,
// gone=false: the client should wait; the producer finalizes it at EOF.
//
// Deliver-on-get: on a successful delivery the segment is removed from the
// cache immediately — the client now holds the payload, so retaining it only
// wastes memory. Keeping ONLY undelivered segments is what makes the cache
// correct under out-of-order pullers: a straggler worker may still need a
// LOW seq long after fast workers have been delivered seq numbers far
// ahead (lastPulled is a pre-fetch watermark, not the consumer position), so
// window-size eviction keyed on lastPulled would 410 that straggler even
// though the client overall keeps up. Undelivered segments are therefore
// never evicted below the hard bound in evictOverflow (only a runaway
// producer whose client vanished pushes retained undelivered segments past
// downsegAdaptiveSegs, and THAT is the 410 case).
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
	// Deliver-on-get: no window/low-watermark eviction is applied — the
	// retained set is exactly the undelivered finalized segments
	// ([lastPulled, produced)), so a missing entry means the segment was
	// already delivered (deleted on get) or dropped by the hard overflow
	// bound. Both are genuinely gone; a straggler's still-undelivered low
	// seq is always present here, which is what prevents the false-410
	// tear-down (previously loIndex could rise past undelivered segments
	// via out-of-order lastPulled, marking a needed-but-unfetched segment
	// Gone and killing the download).
	p, ok := c.segs[seq]
	if !ok {
		return nil, false, true // already delivered or overflow-evicted
	}
	// Advance the consumption watermark so the overflow accounting tracks
	// how far the client has actually read (not just reserved).
	if seq+1 > c.lastPulled {
		c.lastPulled = seq + 1
	}
	// Deliver-on-get: the payload is handed to the client; drop it from the
	// cache so retained entries equal undelivered segments only.
	delete(c.segs, seq)
	// Clean up the size bookkeeping now that the segment is gone.
	delete(c.segSizeBySeq, seq)
	// Wake a backpressured producer: a slot freed, so append() may resume.
	c.spaceCond.Broadcast()
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
	// No eviction here: memory is bounded by the append() backpressure
	// gate (spaceCond), and a just-produced segment must stay until
	// delivered (deliver-on-get). Wake a blocked producer in case the
	// commit just pushed us over the pop — actually the gate is checked
	// before allocating, so no wake needed; a slow consumer that frees a
	// slot is handled by get()'s broadcast.
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
	// produced points at the next finalized index; if a partial segment
	// exists there (in-flight), finalize it by advancing produced.
	if _, exists := c.segs[c.produced]; exists {
		c.produced++
	}
	c.final = true
	// Stop backpressure: stream over, a producer (if any) must not keep
	// waiting for a slot that will never free. finalize is the EOF path;
	// after it no more segments can be produced.
	c.stopped = true
	// Any producer blocked on backpressure must stop waiting: stream over.
	c.spaceCond.Broadcast()
	if dbgDownSeg {
		pc := c.produced
		c.mu.Unlock()
		println("[DBGFIN] finalize produced:", pc)
		return
	}
	c.mu.Unlock()
}

// shutdown terminates the cache: unblocks any backpressured producer and
// marks it so subsequent append() calls no-op. Called when the session is
// torn down (production-leg handler exit / uploadQueue close), so a producer
// that was waiting for the client to pull does not sleep forever if the
// client vanished.
func (c *downSegCache) shutdown() {
	c.mu.Lock()
	c.stopped = true
	c.final = true
	c.spaceCond.Broadcast()
	c.mu.Unlock()
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

// dbgDownSeg enables temporary per-session downlink-segmentation diagnostics
// (trace of finalize / production-leg exits). Enabled via the BRAY_DSEG_DEBUG
// environment variable (any non-empty value) so the dual-end e2e tests can
// opt in without shipping an API. Kept off otherwise; not compiled into
// production logs.
var dbgDownSeg = os.Getenv("BRAY_DSEG_DEBUG") != ""
