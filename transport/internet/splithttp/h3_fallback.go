package splithttp

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

// H3RaceWindow is the head-start window for H2 when racing H3 (Happy Eyeballs).
// Default 200ms per RFC 8305. Reduce for low-latency networks, increase for
// congested UDP links where H3 may be slow.
var H3RaceWindow = 200 * time.Millisecond

// happyEyeballsTransport implements an H3→H2 dual-track transport with
// Happy-Eyeballs-style racing on the first GET request:
//
//   - H3 starts immediately (preferred transport).
//   - H2 starts after a 200ms head-start window if H3 hasn't completed.
//   - Whichever succeeds first is cached and used for all subsequent requests.
//   - If the first request carries a body (stream-one), sequential failover is
//     used instead of racing (bodies cannot be replayed).
//   - Once H3 has been observed to fail, subsequent sessions skip the race and
//     go directly to H2 for 30 minutes. After that, H3 is retried once.
type happyEyeballsTransport struct {
	h3         http.RoundTripper
	h2         http.RoundTripper
	active     http.RoundTripper
	settled    bool
	mu         sync.Mutex
	h3FailedAt time.Time // zero = H3 has never failed
}

func newHappyEyeballsTransport(h3, h2 http.RoundTripper) *happyEyeballsTransport {
	return &happyEyeballsTransport{
		h3: h3,
		h2: h2,
	}
}

func (t *happyEyeballsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	if t.settled {
		active := t.active
		t.mu.Unlock()
		return active.RoundTrip(req)
	}
	h3Failed := !t.h3FailedAt.IsZero() && time.Since(t.h3FailedAt) < 30*time.Minute
	t.mu.Unlock()

	// If H3 has been observed to fail within the recovery window, skip it.
	if h3Failed {
		t.settle(t.h2)
		return t.h2.RoundTrip(req)
	}

	// Race only when there is no body (GET / stream-down).
	// For requests with a body, we can't replay the io.Reader, so use
	// sequential failover.
	if req.Body == nil {
		return t.race(req)
	}
	return t.failover(req)
}

type raceResult struct {
	resp *http.Response
	err  error
}

func (t *happyEyeballsTransport) race(req *http.Request) (*http.Response, error) {
	h3Ch := make(chan raceResult, 1)
	h2Ch := make(chan raceResult, 1)
	ctx := req.Context()

	// H3 is the preferred path — start it immediately.
	go func() {
		h3Req := req.Clone(ctx)
		resp, err := t.h3.RoundTrip(h3Req)
		h3Ch <- raceResult{resp, err}
	}()

	// H2 starts after H3RaceWindow, per Happy Eyeballs v2 (RFC 8305).
	// If H3 completes within the window, we skip H2 entirely.
	go func() {
		timer := time.NewTimer(H3RaceWindow)
		defer timer.Stop()
		select {
		case <-h3Ch:
			return // H3 already finished — don't waste a TCP handshake
		case <-timer.C:
		}

		h2Req := req.Clone(ctx)
		resp, err := t.h2.RoundTrip(h2Req)
		h2Ch <- raceResult{resp, err}
	}()

	select {
	case r := <-h3Ch:
		if r.err == nil {
			t.settle(t.h3)
			// Drain any late H2 response body so the connection can be reused.
			go t.drainLateResponse(h2Ch)
			return r.resp, nil
		}
		// H3 failed — record the failure and wait for H2.
		t.setH3Failed()
		errors.LogInfoInner(context.Background(), r.err, "H3 request failed, falling back to H2")
		select {
		case r2 := <-h2Ch:
			if r2.err == nil {
				t.settle(t.h2)
				return r2.resp, nil
			}
			return nil, r.err // both failed; return the H3 error
		case <-ctx.Done():
			return nil, ctx.Err()
		}

	case r := <-h2Ch:
		// H2 finished before H3 (H3 is slow / UDP-throttled).
		if r.err == nil {
			t.settle(t.h2)
			// Drain the late H3 response.
			go t.drainLateResponse(h3Ch)
			return r.resp, nil
		}
		// H2 also failed — wait for H3.
		select {
		case r3 := <-h3Ch:
			if r3.err == nil {
				t.settle(t.h3)
				return r3.resp, nil
			}
			return nil, r.err // both failed
		case <-ctx.Done():
			return nil, ctx.Err()
		}

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// failover tries H3 first, then H2 if H3 fails.
// Used when the request has a body (can't replay the reader).
func (t *happyEyeballsTransport) failover(req *http.Request) (*http.Response, error) {
	resp, err := t.h3.RoundTrip(req)
	if err == nil {
		t.settle(t.h3)
		return resp, nil
	}

	errors.LogInfoInner(context.Background(), err, "H3 request with body failed, falling back to H2")
	t.setH3Failed()
	t.settle(t.h2)
	return t.h2.RoundTrip(req)
}

// drainLateResponse reads and discards the response body from the losing
// transport so the underlying connection can be returned to the idle pool.
func (t *happyEyeballsTransport) drainLateResponse(ch <-chan raceResult) {
	select {
	case r := <-ch:
		if r.err == nil && r.resp != nil && r.resp.Body != nil {
			r.resp.Body.Close()
		}
	case <-time.After(10 * time.Second):
	}
}

func (t *happyEyeballsTransport) settle(transport http.RoundTripper) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.settled {
		t.active = transport
		t.settled = true
	}
}

func (t *happyEyeballsTransport) setH3Failed() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.h3FailedAt = time.Now()
}
