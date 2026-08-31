package splithttp

// Client side of downlink segmentation (Bray-paired M1): instead of one
// unbounded long GET for the downlink, the client pulls the downlink as a
// sequence of finalized 256KiB segments (GET+seq). This shapes the download
// as a set of short, browser-natural segment GETs (HLS/DASH style) rather
// than one infinite GET, and gives per-segment retry.
//
// Wire: each segment pull is a GET on the stream path whose meta token
// carries (sessionId, seq) and whose request declares the dseg marker header.
// The server answers:
//   200 + body           -> finalized segment payload
//   200 + empty body     -> stream finalized, no more segments (EOF marker)
//   410                  -> segment slid past (skip)
//   404                  -> segment not yet produced: retry with backoff
//
// Concurrency: DownSegWindowSize worker goroutines each pull the next
// reserved segment concurrently (no window/wake signalling races). Results
// land in a map; Read consumes strictly in order. EOF is reached once the
// consumed position passes the EOF marker (all earlier segments are
// guaranteed to exist server-side).

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	errSegGone     = errorsNew("downlink segment slid past (410)")
	errSegNotFound = errorsNew("downlink segment not produced yet (404)")
)

// isTransientPullError reports whether a segment-pull error is a transient
// network-level fault worth retrying (connect timeout / reset / deadline /
// EOF / closed pipe), as opposed to a hard protocol error (410, bad status).
// On lossy/high-RTT paths these fire intermittently; fatal-ing on them would
// tear down the download (V2rayN "偶发中断").
func isTransientPullError(err error) bool {
	if err == nil {
		return false
	}
	if err == errSegGone || err == errSegNotFound {
		return false
	}
	return true
}

func errorsNew(s string) error { return &segError{s} }

type segError struct{ s string }

func (e *segError) Error() string { return e.s }

// DownSegWindowSize bounds concurrent in-flight segment pulls.
const DownSegWindowSize = 6

// downSegRetryInterval is the base backoff between 404 retries; the actual
// wait is jittered around it so the retry cadence does not form a uniform
// 20 ms row across clients (fingerprint clustering risk).
const downSegRetryInterval = 20 * time.Millisecond

// downsegStallGraceDefault is the production value of downsegStallGrace.
const downsegStallGraceDefault = 30 * time.Second

// downsegStallGrace is how long the puller keeps working after the production
// leg has ended without any forward progress (a segment pulled, the EOF marker
// found, or a segment handed to the reader). A live transfer always makes
// progress, so this only fires once the server is genuinely gone. It replaces
// the old "prod-leg EOF is instantly fatal" rule, which threw away up to
// prefetchAheadSegs of already-pulled data.
//
// It is a var so tests can shrink it (a 30s unit test is not acceptable).
var downsegStallGrace = downsegStallGraceDefault

// downSegCurrentRetryInterval is used only by the segment the reader is
// blocked on. It shortens page/API first-byte recovery without increasing the
// five future prefetch workers' 404 cadence or changing any wire shape.
const downSegCurrentRetryInterval = 5 * time.Millisecond

func downSegRetryBase(seq, consumedSeq uint64) time.Duration {
	if seq == consumedSeq {
		return downSegCurrentRetryInterval
	}
	return downSegRetryInterval
}

// PullSegment fetches one finalized downlink segment (200 body) or a
// 200-empty (EOF marker), distinguishing it from transient 404 and slipped 410.
func (c *DefaultDialerClient) PullSegment(ctx context.Context, base *url.URL, sessionId, seqStr string) ([]byte, error) {
	if base == nil {
		return nil, errorsNew("nil base URL")
	}
	u := new(url.URL)
	*u = *base
	req := &http.Request{
		Method:     http.MethodGet,
		URL:        u,
		Host:       u.Host,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
	}
	req = req.WithContext(ctx)
	// FillStreamRequest stamps padding + meta(sessionId, seq) (seq now
	// honored). No dseg marker header is needed: the server treats a
	// sessioned GET whose meta token carries a seq as a segment pull.
	c.transportConfig.FillStreamRequest(req, sessionId, seqStr)

	// Bound the pull with a hard timeout so a wedged transport (TLS/h2
	// handshake against a peer that never responds) surfaces as an error
	// instead of blocking the worker forever.
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req = req.WithContext(pctx)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		body, rerr := io.ReadAll(resp.Body)
		if rerr != nil {
			return nil, rerr
		}
		return body, nil
	case http.StatusGone:
		return nil, errSegGone
	case http.StatusNotFound:
		return nil, errSegNotFound
	default:
		return nil, errorsNew("unexpected segment status " + strconv.Itoa(resp.StatusCode))
	}
}

// DownSegPuller consumes the downlink as an ordered byte stream.
type DownSegPuller struct {
	client    *DefaultDialerClient
	base      *url.URL
	sessionId string
	ctx       context.Context
	cancel    context.CancelFunc

	// prod is the optional dseg production leg. Its response body has no
	// downlink payload (bytes are carried by segment pulls), so it is watched
	// solely for a peer-side close that would otherwise strand packet-up POSTs
	// on a deleted server session.
	prod io.Closer

	mu          sync.Mutex
	buf         map[uint64][]byte
	skip        map[uint64]bool
	eofAt       uint64 // first non-existent seq once known (0 = unknown)
	consumedSeq uint64 // next seq the stream needs
	fatal       error
	closed      bool

	// prodErr is the error the production leg ended with. It is NOT fatal on
	// its own: the server holds the session until the client has pulled every
	// produced segment and the EOF marker (see holdDrainLeg), so the tail is
	// still retrievable. Failing immediately here is what silently discarded
	// up to prefetchAheadSegs (~24 MiB at the 1 MiB segment size) of data the
	// client had already pulled. It turns fatal only after the stream stops
	// making progress for downsegStallGrace.
	prodErr      error
	lastProgress time.Time

	nextIssue atomic.Uint64
	wg        sync.WaitGroup
	wake      chan struct{}
}

// NewDownSegPuller creates a segment puller for sessionId over base.
func NewDownSegPuller(ctx context.Context, client *DefaultDialerClient, base *url.URL, sessionId string, prod io.Closer) *DownSegPuller {
	pctx, cancel := context.WithCancel(ctx)
	p := &DownSegPuller{
		client:       client,
		base:         base,
		sessionId:    sessionId,
		ctx:          pctx,
		cancel:       cancel,
		prod:         prod,
		buf:          make(map[uint64][]byte),
		skip:         make(map[uint64]bool),
		wake:         make(chan struct{}, 1),
		lastProgress: time.Now(),
	}
	for i := 0; i < DownSegWindowSize; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	if prodReader, ok := p.prod.(io.Reader); ok {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			monitorProductionLeg(p.ctx, prodReader, p.failProductionLeg)
		}()
	}
	return p
}

// monitorProductionLeg blocks until the production GET ends. A production
// GET carries no application bytes in segment mode; segment pulls carry the
// downlink, so the leg's only job is to signal the peer's view of the session.
//
// Its end is deliberately NOT an immediate failure (see failProductionLeg):
// in the normal case the server closes it exactly when the producer has
// reached EOF, which routinely happens while the client still owes tens of
// MiB of pulls. The server keeps the session alive until the client has them
// (holdDrainLeg), so the correct reaction is to keep draining, not to bail.
func monitorProductionLeg(ctx context.Context, prod io.Reader, fail func(error)) {
	var scratch [1]byte
	for {
		_, err := prod.Read(scratch[:])
		if err == nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		if ctx.Err() == nil {
			fail(fmt.Errorf("XHTTP dseg production leg closed: %w", err))
		}
		return
	}
}

// failProductionLeg records that the production leg ended. Unlike a hard
// protocol error this is DEFERRED, not fatal.
//
// The old behaviour (fail fast, and cancel the puller context right away)
// was lossy: Read() checks terminal conditions before it looks at p.buf, so
// every segment the workers had already prefetched but the application had
// not read yet was dropped on the floor. With a fast origin and a slow reader
// that is up to prefetchAheadSegs segments — measured 20.6 MiB missing from a
// 64 MiB download, with no error anywhere but the byte count.
//
// What we do instead: keep pulling. Two things bound it.
//
//   - The server holds the session until the client has pulled every produced
//     segment plus the EOF marker, so a live drain always completes and ends
//     in a clean io.EOF via eofAt.
//   - If the server really is gone, pulls stop resolving and the stream makes
//     no progress for downsegStallGrace, at which point we surface prodErr.
//
// The upload side is unaffected: while the drain is in progress the session
// still exists server-side, so packet-up POSTs are not going to a stale
// sessionId — which was the original reason for failing fast.
func (p *DownSegPuller) failProductionLeg(err error) {
	p.mu.Lock()
	if !p.closed && p.prodErr == nil {
		p.prodErr = err
		p.lastProgress = time.Now()
		if dbgDownSeg {
			dbgLog("[DBGPULL] prod leg ended (deferred, still draining):", err.Error(), "sid=", p.sessionId)
		}
	}
	p.mu.Unlock()
	p.notify()
}

// prefetchAheadSegs is how many segments may be reserved (fetched or
// in-flight) beyond the current consumption watermark. It is the
// prefetch-consumption alignment bound: enough in-flight segments to hide
// producer+network latency (so a sustained download stays at full speed
// even when each segment costs ~1 RTT), yet bounded so we never prefetch
// into the void or pile gigabytes of un-consumed buffers on a slow reader.
// 4 × DownSegWindowSize keeps up to a few full windows ahead; the server
// backpressure bound (downsegAdaptiveSegs=64) is far above this, so a
// reserved-but-not-yet-produced seq parks cleanly at 404 without stressing
// the cache bound. Without this bound every worker races ahead to
// brand-new seqs while the consumer stalls on an exact mid-window seq that
// no worker currently has in flight (single-segment serialization under
// high RTT). With it, reservation tracks the consumption watermark and the
// working set stays tight.
const prefetchAheadSegs = DownSegWindowSize * 4

// worker reserves the next segment and pulls it until it resolves, or stops
// once the stream end (EOF marker) is passed.
func (p *DownSegPuller) worker() {
	defer p.wg.Done()
	for {
		// Reserve the next segment to pull, bounded by the prefetch budget:
		// stop reserving once we are prefetchAheadSegs ahead of the
		// consumption watermark (the reader has plenty buffered; pulling
		// more would only prefetch into the void). Read() advances
		// consumedSeq and notifies via wake, so reservation re-arms.
		var seq uint64
		for {
			select {
			case <-p.ctx.Done():
				return
			default:
			}
			p.mu.Lock()
			if p.closed || (p.eofAt != 0 && p.nextIssue.Load() > p.eofAt) {
				p.mu.Unlock()
				return
			}
			if p.nextIssue.Load() <= p.consumedSeq+prefetchAheadSegs {
				seq = p.nextIssue.Add(1) - 1
				p.mu.Unlock()
				break
			}
			p.mu.Unlock()
			// We are ahead of the budget; wait for Read to consume.
			select {
			case <-p.wake:
			case <-p.ctx.Done():
				return
			case <-time.After(downSegRetryInterval):
			}
		}

		p.mu.Lock()
		if p.closed || (p.eofAt != 0 && seq > p.eofAt) {
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()

		// Retry loop: keep pulling the SAME reserved seq until it resolves
		// (a 404 means not-produced-yet; abandoning it would strand the
		// segment and deadlock the stream — audit Finding-2).
		for {
			seg, err := p.client.PullSegment(p.ctx, p.base, p.sessionId, strconv.FormatUint(seq, 10))

			p.mu.Lock()
			switch {
			case err == nil && len(seg) > 0:
				p.buf[seq] = seg
				p.lastProgress = time.Now()
				if pctxd := p.ctx.Err(); pctxd != nil && dbgDownSeg {
					dbgLog("[DBGPULL] seg", seq, "ok len", len(seg), "BUT ctx err", pctxd.Error())
				}
			case err == nil: // empty 200 -> EOF marker at seq (first wins)
				p.lastProgress = time.Now()
				if p.eofAt == 0 || seq < p.eofAt {
					p.eofAt = seq
					if dbgDownSeg {
						dbgLog("[DBGPULL] EOF marker at seq", seq, "sid=", p.sessionId)
					}
				}
			case err == errSegGone:
				// A segment slid past before this worker pulled it — the
				// byte stream would be silently corrupt (1MiB gap) without
				// error. Treat as protocol error, not a silent skip.
				if p.fatal == nil {
					p.fatal = errSegGone
					if dbgDownSeg {
						dbgLog("[DBGPULL] FATAL gone seq", seq, "sid=", p.sessionId)
					}
				}
			case err == errSegNotFound:
				// Not produced yet: retry after jittered backoff, keeping
				// seq. Only the segment Read is blocked on uses the shorter
				// base; future prefetches retain the normal jittered cadence.
				// This stays entirely client-local: no held server H2 stream.
				base := downSegRetryBase(seq, p.consumedSeq)
				// Once the production leg is gone the server has stopped
				// producing, so a 404 means "never coming" far more often
				// than "not yet": back off harder (no pull storm against a
				// possibly-deleted session) and give up entirely once the
				// stream has stalled for downsegStallGrace.
				var dead error
				if p.prodErr != nil {
					if p.fatal == nil && time.Since(p.lastProgress) > downsegStallGrace {
						dead = p.prodErr
						p.fatal = dead
					}
					base *= 4
				}
				p.mu.Unlock()
				if dead != nil {
					p.notify()
					return
				}
				wait := base + time.Duration(biasedRangeRand(-int32(base/time.Millisecond), int32(base/time.Millisecond)))*time.Millisecond
				if wait < 1*time.Millisecond {
					wait = 1 * time.Millisecond
				}
				select {
				case <-time.After(wait):
				case <-p.ctx.Done():
					return
				}
				continue
			case isTransientPullError(err):
				// Transient network error on the segment pull (connect
				// timeout / reset / deadline): retry the SAME seq instead
				// of killing the whole download. Real high-RTT/lossy paths
				// hit these intermittently; treating them as fatal caused
				// the "偶发中断" file-download drops.
				if dbgDownSeg {
					dbgLog("[DBGPULL] transient err seq", seq, ":", err.Error())
				}
				p.mu.Unlock()
				select {
				case <-time.After(downSegRetryInterval):
				case <-p.ctx.Done():
					return
				}
				continue
			default:
				if p.fatal == nil {
					p.fatal = err
					if dbgDownSeg {
						dbgLog("[DBGPULL] FATAL err seq", seq, ":", err.Error(), "sid=", p.sessionId)
					}
				}
			}
			p.mu.Unlock()
			p.notify()
			break
		}
	}
}

func (p *DownSegPuller) notify() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Read returns the next bytes of the reconstructed byte stream.
func (p *DownSegPuller) Read(b []byte) (int, error) {
	for {
		p.mu.Lock()
		c := p.consumedSeq
		// Data first, terminal conditions second.
		//
		// This ordering is deliberate and is the fix for a silent truncation:
		// the puller checks fatal/prodErr only once p.buf has nothing left for
		// the current seq. Bytes the client already owns are never discarded
		// because of something that happened on a different leg. The old order
		// (fatal first) threw away up to prefetchAheadSegs of prefetched
		// segments whenever the production leg closed, and it is not needed for
		// the stale-session concern it was written for: the server keeps the
		// session alive until the drain completes, so the upload side is not
		// POSTing into a deleted session while we drain (see holdDrainLeg).
		if seg, ok := p.buf[c]; ok {
			delete(p.buf, c)
			p.lastProgress = time.Now()
			p.mu.Unlock()
			n := copy(b, seg)
			if n < len(seg) {
				p.mu.Lock()
				p.consumedSeq = c // unchanged; put remainder back
				p.buf[c] = seg[n:]
				p.mu.Unlock()
			} else {
				p.mu.Lock()
				p.consumedSeq = c + 1
				p.mu.Unlock()
				p.notify() // consumption freed prefetch budget: re-arm workers
			}
			return n, nil
		}
		if p.skip[c] {
			delete(p.skip, c)
			p.consumedSeq = c + 1
			p.lastProgress = time.Now()
			p.mu.Unlock()
			p.notify() // consumption freed prefetch budget
			continue
		}
		// A known end of stream outranks any pending error: if the EOF marker
		// was found, every byte has been delivered and this is a clean finish
		// even if the production leg has since closed.
		if p.eofAt != 0 && c >= p.eofAt {
			p.mu.Unlock()
			return 0, io.EOF
		}
		// Hard protocol/transport error: the missing segment cannot appear by
		// waiting, so this one really is terminal.
		if p.fatal != nil {
			err := p.fatal
			p.fatal = nil
			p.mu.Unlock()
			return 0, err
		}
		// Production leg ended: keep draining while the stream still makes
		// progress, fail once it has stalled for downsegStallGrace.
		if p.prodErr != nil {
			if d := time.Until(p.lastProgress.Add(downsegStallGrace)); d > 0 {
				p.mu.Unlock()
				select {
				case <-p.wake:
				case <-p.ctx.Done():
					return 0, p.ctx.Err()
				case <-time.After(d):
				}
				continue
			}
			err := p.prodErr
			p.prodErr = nil
			p.mu.Unlock()
			p.cancel() // nothing more can arrive; stop the workers
			return 0, fmt.Errorf("XHTTP dseg stream stalled after production ended: %w", err)
		}
		p.mu.Unlock()
		select {
		case <-p.wake:
		case <-p.ctx.Done():
			return 0, p.ctx.Err()
		}
	}
}

// Close marks the puller finished and closes the production leg.
func (p *DownSegPuller) Close() error {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
	}
	p.mu.Unlock()
	p.notify()
	p.cancel()
	var err error
	if p.prod != nil {
		// The production-leg monitor is blocked in Read. Close before waiting
		// so local shutdown cannot deadlock behind that monitor.
		err = p.prod.Close()
	}
	p.wg.Wait()
	return err
}
