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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/randpool"
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
	// with jitter drawn low. Below the old 512KiB floor: the heavy-tail
	// size distribution occasionally produces small segments (real HLS
	// does too — scene changes / discontinuities), and 256KiB still
	// amortizes per-segment HTTP overhead acceptably while letting the
	// distribution's left side breathe.
	downsegSizeMin = downsegSize / 4
	// downsegInitialAllocFloor avoids allocating a full 1MiB backing array
	// for a short web/API response while retaining room for normal write bursts.
	downsegInitialAllocFloor = 64 << 10
	// downsegRepullGrace retains a DELIVERED segment for this long after its
	// successful get(), so the client can re-pull it if the GET response was
	// lost on the way back (client pull deadline exceeded, H2 stream reset
	// after the server handed over the payload). Without the grace window,
	// deliver-on-get deletes the segment immediately and the client's retry
	// observes 410 Gone for a segment it never received — which the puller
	// treats as a fatal protocol error and tears the whole download down
	// (observed as curl(18) under concurrent-load WAN contention).
	downsegRepullGrace = 30 * time.Second

	// downsegRepullMaxBytes caps the total payload bytes retained across
	// the repull grace window per session (oldest delivered entries are
	// dropped first when exceeded). Without this cap a fast download would
	// retain grace-periods' worth of payload (e.g. 10 MB/s x 30 s = 300 MB
	// per session); retries under contention arrive within a few seconds,
	// so a modest byte budget preserves the re-pull semantics where they
	// matter while bounding worst-case memory.
	downsegRepullMaxBytesDefault = 16 << 20 // 16 MiB
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

// downsegRepullMaxBytes is a var so tests can shrink the byte cap.
var downsegRepullMaxBytes = downsegRepullMaxBytesDefault

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
	// deliveredAtNs remembers when each delivered segment was handed out
	// (unix nanos). Delivered segments are kept for downsegRepullGrace so
	// the client can re-pull them if the GET response was lost in transit;
	// after the grace they are dropped. Bounded: a delivered segment costs
	// memory for at most the grace period, and the append() backpressure
	// gate counts undelivered entries only.
	deliveredAtNs map[uint64]int64
	// deliveredSegs holds the payload copies backing the repull grace
	// window above; entries are dropped together with deliveredAtNs.
	deliveredSegs map[uint64][]byte
	// deliveredBytes is the sum of payload bytes currently retained in
	// deliveredSegs (tracked to enforce downsegRepullMaxBytes).
	deliveredBytes int
	// deliveredSeq / deliveredOrder give a monotonic delivery order for
	// oldest-first eviction under the byte cap (nanosecond timestamps tie).
	deliveredSeq   uint64
	deliveredOrder map[uint64]uint64
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
		segs:           make(map[uint64][]byte, downsegMaxSegs),
		segSizeBySeq:   make(map[uint64]int, downsegMaxSegs),
		deliveredAtNs:  make(map[uint64]int64, downsegMaxSegs),
		deliveredSegs:  make(map[uint64][]byte, downsegMaxSegs),
		deliveredOrder: make(map[uint64]uint64, downsegMaxSegs),
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

// downsegSizeJitterFn yields the per-segment size delta (bytes added to
// downsegSize). It is a package variable so tests can substitute a fixed
// value for deterministic segment sizes.
//
// The production source draws from a lognormal-style heavy-tailed
// distribution (median ≈ -0.1 MiB below nominal, mean ≈ 0): most segments
// land moderately below the 1 MiB nominal, with occasional much larger and
// occasional small segments — mirroring how real HLS/DASH segment sizes
// vary with encoded bitrate rather than forming a tight uniform band.
// Methodology follows XMC finalmask padding presets: preserve the
// statistical shape of genuine traffic; never replay exact captured values.
var downsegSizeJitterFn = func() int32 {
	// Box-Muller: two uniform draws -> one standard normal.
	u1 := float64(randpool.Global.Uint32()) / (1 << 32)
	u2 := float64(randpool.Global.Uint32()) / (1 << 32)
	if u1 <= 0 { // guard log(0)
		u1 = 1.0 / (1 << 32)
	}
	z := math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
	// ln(size) = ln(median) + sigma*z with median=0.9MiB, sigma=0.45:
	// mean ≈ 1.0MiB, P(>2MiB) ≈ 4%, tiny segments rare (<0.3%).
	const medianBytes = 0.9 * 1024 * 1024
	const sigma = 0.45
	size := medianBytes * math.Exp(sigma*z)
	delta := int(size) - downsegSize
	if delta > math.MaxInt32 {
		delta = math.MaxInt32
	}
	if delta < -math.MaxInt32 {
		delta = -math.MaxInt32 + 1
	}
	return int32(delta)
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
	if len(b) == 0 {
		return
	}
	// stopped is written under mu by finalize/shutdown; read it under mu too.
	// The lock-free pre-check below was a data race with session teardown
	// (race detector: append vs shutdown, downseg.go:274 vs :530).
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return
	}
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
		// Repull grace: the segment may have been delivered moments ago
		// with the response lost on the way back (client pull deadline /
		// H2 reset). Within downsegRepullGrace we still hold a copy —
		// re-deliver it instead of reporting Gone (a false 410 here is
		// fatal for the client's puller). After the grace the copy is
		// dropped and Gone is genuinely correct.
		if at, was := c.deliveredAtNs[seq]; was {
			if time.Now().UnixNano()-at < int64(downsegRepullGrace) {
				if kept, have := c.deliveredSegs[seq]; have {
					if seq+1 > c.lastPulled {
						c.lastPulled = seq + 1
					}
					return kept, true, false
				}
			}
			delete(c.deliveredAtNs, seq)
			delete(c.deliveredSegs, seq)
		}
		return nil, false, true // already delivered or overflow-evicted
	}
	// Advance the consumption watermark so the overflow accounting tracks
	// how far the client has actually read (not just reserved).
	if seq+1 > c.lastPulled {
		c.lastPulled = seq + 1
	}
	// Deliver-on-get with repull grace: hand the payload to the client but
	// retain a copy for downsegRepullGrace so a lost-response retry can be
	// served. The retained copy does not count against the append()
	// backpressure bound (that gate tracks undelivered entries only), but
	// total retained bytes are capped by downsegRepullMaxBytes (oldest
	// entries dropped first) and each entry expires after the grace.
	c.retainDeliveredLocked(seq, p)
	delete(c.segs, seq)
	// Clean up the size bookkeeping now that the segment is gone.
	delete(c.segSizeBySeq, seq)
	// Wake a backpressured producer: a slot freed, so append() may resume.
	c.spaceCond.Broadcast()
	return p, true, false
}

// retainDeliveredLocked records a delivered payload in the repull grace
// window: expire stale entries, enforce the byte cap by dropping the oldest
// delivered entries first, then remember this one. Caller holds c.mu.
func (c *downSegCache) retainDeliveredLocked(seq uint64, p []byte) {
	now := time.Now().UnixNano()
	for s, at := range c.deliveredAtNs {
		if now-at >= int64(downsegRepullGrace) {
			c.dropDeliveredLocked(s)
		}
	}
	// If re-delivering an entry already retained (shouldn't normally
	// happen — a retained seq is served from deliveredSegs, not segs),
	// drop the old copy first so accounting stays exact.
	if _, dup := c.deliveredAtNs[seq]; dup {
		c.dropDeliveredLocked(seq)
	}
	c.deliveredAtNs[seq] = now
	// deliveredSeq is a monotonic delivery counter used as the eviction
	// order (raw timestamps can tie within the same nanosecond).
	c.deliveredSeq++
	c.deliveredOrder[seq] = c.deliveredSeq
	c.deliveredSegs[seq] = p
	c.deliveredBytes += len(p)
	for c.deliveredBytes > downsegRepullMaxBytes {
		oldest, oldestOrd := uint64(0), uint64(1<<63-1)
		found := false
		for s, ord := range c.deliveredOrder {
			if ord < oldestOrd {
				oldest, oldestOrd, found = s, ord, true
			}
		}
		if !found {
			break
		}
		c.dropDeliveredLocked(oldest)
	}
}

// dropDeliveredLocked removes one retained delivered segment. Caller holds c.mu.
func (c *downSegCache) dropDeliveredLocked(seq uint64) {
	if p, ok := c.deliveredSegs[seq]; ok {
		c.deliveredBytes -= len(p)
		delete(c.deliveredSegs, seq)
	}
	delete(c.deliveredAtNs, seq)
	delete(c.deliveredOrder, seq)
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
		dbgLog("[DBGFIN] finalize produced:", pc)
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
	// Free the repull grace window immediately: the session is gone, no
	// client can re-pull anything anymore.
	clear(c.deliveredAtNs)
	clear(c.deliveredSegs)
	clear(c.deliveredOrder)
	c.deliveredBytes = 0
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

// dbgDownSeg and dbgLog live in downseg_debug.go: the BRAY_DSEG_DEBUG gate
// plus the single stderr trace helper every [DBG*] point funnels through.
