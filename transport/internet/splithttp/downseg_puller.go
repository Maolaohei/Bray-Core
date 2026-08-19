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

func errorsNew(s string) error { return &segError{s} }

type segError struct{ s string }

func (e *segError) Error() string { return e.s }

// DownSegWindowSize bounds concurrent in-flight segment pulls.
const DownSegWindowSize = 6

// downSegRetryInterval is the base backoff between 404 retries; the actual
// wait is jittered around it so the retry cadence does not form a uniform
// 20 ms row across clients (fingerprint clustering risk).
const downSegRetryInterval = 20 * time.Millisecond

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

	// prod is the optional dseg production leg closed on Close (EOF server-side).
	prod io.Closer

	mu          sync.Mutex
	buf         map[uint64][]byte
	skip        map[uint64]bool
	eofAt       uint64 // first non-existent seq once known (0 = unknown)
	consumedSeq uint64 // next seq the stream needs
	fatal       error
	closed      bool

	nextIssue atomic.Uint64
	wg        sync.WaitGroup
	wake      chan struct{}
}

// NewDownSegPuller creates a segment puller for sessionId over base.
func NewDownSegPuller(ctx context.Context, client *DefaultDialerClient, base *url.URL, sessionId string, prod io.Closer) *DownSegPuller {
	pctx, cancel := context.WithCancel(ctx)
	p := &DownSegPuller{
		client:    client,
		base:      base,
		sessionId: sessionId,
		ctx:       pctx,
		cancel:    cancel,
		prod:      prod,
		buf:       make(map[uint64][]byte),
		skip:      make(map[uint64]bool),
		wake:      make(chan struct{}, 1),
	}
	for i := 0; i < DownSegWindowSize; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

// worker reserves the next segment and pulls it until it resolves, or stops
// once the stream end (EOF marker) is passed.
func (p *DownSegPuller) worker() {
	defer p.wg.Done()
	for {
		// Stop when closed or when reserved seq is past the EOF marker.
		seq := p.nextIssue.Add(1) - 1
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
			case err == nil: // empty 200 -> EOF marker at seq (first wins)
				if p.eofAt == 0 || seq < p.eofAt {
					p.eofAt = seq
				}
			case err == errSegGone:
				// A segment slid past before this worker pulled it — the
				// byte stream would be silently corrupt (1MiB gap) without
				// error. Treat as protocol error, not a silent skip.
				if p.fatal == nil {
					p.fatal = errSegGone
				}
			case err == errSegNotFound:
				// Not produced yet: retry after jittered backoff, keeping
				// seq (jittered so retries don't form a fixed cadence).
				p.mu.Unlock()
				wait := downSegRetryInterval + time.Duration(biasedRangeRand(-int32(downSegRetryInterval/time.Millisecond), int32(downSegRetryInterval/time.Millisecond)))*time.Millisecond
				if wait < 1*time.Millisecond {
					wait = 1 * time.Millisecond
				}
				select {
				case <-time.After(wait):
				case <-p.ctx.Done():
					return
				}
				continue
			case p.fatal == nil:
				p.fatal = err
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
		if seg, ok := p.buf[c]; ok {
			delete(p.buf, c)
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
			}
			return n, nil
		}
		if p.skip[c] {
			delete(p.skip, c)
			p.consumedSeq = c + 1
			p.mu.Unlock()
			continue
		}
		if p.fatal != nil {
			err := p.fatal
			p.fatal = nil
			p.mu.Unlock()
			return 0, err
		}
		if p.eofAt != 0 && c >= p.eofAt {
			p.mu.Unlock()
			return 0, io.EOF
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
	p.wg.Wait()
	if p.prod != nil {
		return p.prod.Close()
	}
	return nil
}
