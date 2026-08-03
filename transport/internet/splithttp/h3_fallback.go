package splithttp

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

// H3RaceWindow is the head-start window for H2 when racing H3 (Happy Eyeballs).
// Default 200ms per RFC 8305. Reduce for low-latency networks, increase for
// congested UDP links where H3 may be slow.
var H3RaceWindow = 200 * time.Millisecond

// H3Cooldown is how long to skip H3 after an observed failure.
var H3Cooldown = 30 * time.Minute

// Package-level H3/H2 race metrics (process-wide, lock-free).
var h3TransportMetrics struct {
	H3Wins      atomic.Uint64
	H2Fallbacks atomic.Uint64
	H3Cooldowns atomic.Uint64
	H3FailMarks atomic.Uint64
	Races       atomic.Uint64
	Failovers   atomic.Uint64
}

// H3MetricsSnapshot is a point-in-time view of H3 race stats.
type H3MetricsSnapshot struct {
	H3Wins      uint64
	H2Fallbacks uint64
	H3Cooldowns uint64
	H3FailMarks uint64
	Races       uint64
	Failovers   uint64
}

// GetH3Metrics returns process-wide H3/H2 Happy Eyeballs counters.
func GetH3Metrics() H3MetricsSnapshot {
	return H3MetricsSnapshot{
		H3Wins:      h3TransportMetrics.H3Wins.Load(),
		H2Fallbacks: h3TransportMetrics.H2Fallbacks.Load(),
		H3Cooldowns: h3TransportMetrics.H3Cooldowns.Load(),
		H3FailMarks: h3TransportMetrics.H3FailMarks.Load(),
		Races:       h3TransportMetrics.Races.Load(),
		Failovers:   h3TransportMetrics.Failovers.Load(),
	}
}

// H3MetricsReport returns a compact one-line summary for logs.
func H3MetricsReport() string {
	m := GetH3Metrics()
	return fmt.Sprintf("H3 metrics: wins=%d h2_fallback=%d cooldowns=%d fail_marks=%d races=%d failovers=%d",
		m.H3Wins, m.H2Fallbacks, m.H3Cooldowns, m.H3FailMarks, m.Races, m.Failovers)
}

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
	h3Failed := !t.h3FailedAt.IsZero() && time.Since(t.h3FailedAt) < H3Cooldown
	t.mu.Unlock()

	// If H3 has been observed to fail within the recovery window, skip it.
	// Cooldown is its own metric; do not also count as a first-time H2 fallback settle.
	if h3Failed {
		h3TransportMetrics.H3Cooldowns.Add(1)
		t.mu.Lock()
		if !t.settled {
			t.active = t.h2
			t.settled = true
		}
		t.mu.Unlock()
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
	h3TransportMetrics.Races.Add(1)
	h3Ch := make(chan raceResult, 1)
	h2Ch := make(chan raceResult, 1)
	ctx := req.Context()

	// h2Started tracks whether the H2 racer actually launched (timer fired
	// before H3 finished). Only then does h2Ch ever receive, so only then do
	// we need a drain goroutine — otherwise every settled H3 race would leak
	// a 10s drain goroutine waiting on a channel that never fires.
	var h2Started atomic.Bool
	// Ensure response bodies are closed if we return early (e.g., context cancel)
	defer func() {
		go t.drainLateResponse(h3Ch)
		if h2Started.Load() {
			go t.drainLateResponse(h2Ch)
		}
	}()

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

		h2Started.Store(true)
		h2Req := req.Clone(ctx)
		resp, err := t.h2.RoundTrip(h2Req)
		h2Ch <- raceResult{resp, err}
	}()

	select {
	case r := <-h3Ch:
		if r.err == nil {
			t.settle(t.h3)
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

// failover tries H3 first, then H2 only when the body can be safely replayed.
// Used when the request has a body (stream-one / stream-up).
//
// Important: a generic io.Reader body cannot be rewound after a partial H3
// write. Replaying it on H2 would silently upload a truncated payload.
// Only GetBody (or a nil body) allows a correct fallback.
func (t *happyEyeballsTransport) failover(req *http.Request) (*http.Response, error) {
	h3TransportMetrics.Failovers.Add(1)
	resp, err := t.h3.RoundTrip(req)
	if err == nil {
		t.settle(t.h3)
		return resp, nil
	}

	t.setH3Failed()

	// Prefer settled H2 for subsequent requests after an H3 body failure.
	// For THIS request, only retry when the body is replayable.
	if req.Body != nil && req.GetBody == nil {
		errors.LogInfoInner(context.Background(), err,
			"H3 request with non-replayable body failed; not falling back to H2")
		t.settle(t.h2)
		return nil, err
	}

	if req.GetBody != nil {
		body, bodyErr := req.GetBody()
		if bodyErr != nil {
			errors.LogInfoInner(context.Background(), bodyErr,
				"H3 failed and GetBody could not rebuild request body for H2 fallback")
			t.settle(t.h2)
			return nil, err
		}
		req = req.Clone(req.Context())
		req.Body = body
	}

	errors.LogInfoInner(context.Background(), err, "H3 request with body failed, falling back to H2")
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
		if transport == t.h3 {
			h3TransportMetrics.H3Wins.Add(1)
		} else if transport == t.h2 {
			h3TransportMetrics.H2Fallbacks.Add(1)
		}
	}
}

func (t *happyEyeballsTransport) setH3Failed() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.h3FailedAt = time.Now()
	h3TransportMetrics.H3FailMarks.Add(1)
}

// Close shuts down both H3 and H2 transports.
func (t *happyEyeballsTransport) Close() {
	if closer, ok := t.h3.(interface{ Close() }); ok {
		closer.Close()
	}
	if closer, ok := t.h2.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}
