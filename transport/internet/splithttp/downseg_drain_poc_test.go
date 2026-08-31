package splithttp

// POC gates for the dseg slow-reader truncation.
//
// The bug: a 64 MiB download silently delivered only 46.47 MiB. Two
// independent defects composed:
//
//  1. Server: the production-leg handler returned as soon as the producer
//     closed (httpSC.Wait). Its defer dropped the last download leg, which
//     closes the session and shuts the segment cache down. But the cache may
//     legally hold up to downsegAdaptiveSegs (64) segments, so a fast origin
//     reaches EOF while tens of MiB are still undelivered — those bytes were
//     then unreachable. holdDrainLeg fixes this.
//
//  2. Client: Read() checked p.fatal before p.buf, and monitorProductionLeg
//     turned ANY production-leg EOF into a fatal. So even the segments the
//     workers had already pulled were discarded. Read() now drains first and
//     defers the production-leg error until the stream actually stalls.
//
// Measured before the fix: 46469066/67108864 bytes (20.6 MiB lost), while the
// legacy non-dseg path on the same harness delivered all 67108864 bytes.

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

// TestPOCProductionLegDeathDoesNotDiscardPrefetchedSegments is the client-side
// half of the fix, isolated from the network.
//
// Shape: the producer reached EOF (the server closed the production GET, the
// normal end-of-transfer signal), the puller had already prefetched segments
// 0 and 1 and knows the EOF marker sits at seq 2. Old code returned the
// production-leg error immediately and dropped both segments on the floor.
func TestPOCProductionLegDeathDoesNotDiscardPrefetchedSegments(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &DownSegPuller{
		buf:          map[uint64][]byte{0: []byte("hello "), 1: []byte("world")},
		skip:         map[uint64]bool{},
		eofAt:        2, // EOF marker already discovered by a worker
		wake:         make(chan struct{}, 1),
		ctx:          ctx,
		cancel:       cancel,
		lastProgress: time.Now(),
	}

	// The production GET just ended. This must NOT be fatal by itself.
	p.failProductionLeg(io.EOF)

	var got []byte
	b := make([]byte, 4)
	for {
		n, err := p.Read(b)
		got = append(got, b[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("stream died while draining prefetched segments after %d bytes (%q): %v",
				len(got), got, err)
		}
		if len(got) > 64 { // guard against a non-terminating loop
			t.Fatalf("runaway read: %q", got)
		}
	}
	if string(got) != "hello world" {
		t.Fatalf("reassembled %q, want %q", got, "hello world")
	}
}

// TestPOCProductionLegDeathStillFailsOnAStalledStream is the safety rail for
// the deferred-error design: a dead production leg must not turn into an
// infinite wait when the server really is gone. With no forward progress the
// puller must surface an error within a bounded time.
func TestPOCProductionLegDeathStillFailsOnAStalledStream(t *testing.T) {
	old := downsegStallGrace
	downsegStallGrace = 50 * time.Millisecond
	defer func() { downsegStallGrace = old }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &DownSegPuller{
		buf:          map[uint64][]byte{},
		skip:         map[uint64]bool{},
		wake:         make(chan struct{}, 1),
		ctx:          ctx,
		cancel:       cancel,
		lastProgress: time.Now().Add(-time.Second), // farther back than the (shrunken) grace
	}
	p.failProductionLeg(io.ErrUnexpectedEOF)

	_, err := p.Read(make([]byte, 16))
	if err == nil {
		t.Fatal("a stalled stream after a dead production leg must fail, not hang or return data")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("stalled")) {
		t.Fatalf("error = %v, want the stall-wrapped production error", err)
	}
}

// TestPOCDownSegCacheDrainedRequiresEofAndEmptyCache is the server-side half:
// the drain hold must not release the session while the client still owes
// pulls. Each of the three conditions is checked independently.
func TestPOCDownSegCacheDrainedRequiresEofAndEmptyCache(t *testing.T) {
	withZeroDownsegJitter(t)
	c := newDownSegCache()
	if c.drained() {
		t.Fatal("a fresh cache must not report drained")
	}

	c.append(bytes.Repeat([]byte{0x5A}, downsegSize))
	c.finalize() // producer reached EOF
	if c.drained() {
		t.Fatal("finalized with undelivered segments must NOT report drained (would truncate)")
	}

	// EOF marker served, but the segment is still sitting there: still held.
	c.noteEofServed()
	if c.drained() {
		t.Fatal("EOF served while a segment is undelivered must NOT report drained (would truncate)")
	}

	// Deliver every produced segment -> nothing left undelivered.
	delivered := 0
	for i := uint64(0); ; i++ {
		if _, ok, _ := c.get(i); !ok {
			break
		}
		delivered++
	}
	if delivered == 0 {
		t.Fatal("expected at least one deliverable segment")
	}
	if !c.drained() {
		t.Fatal("finalized + EOF served + no undelivered segments must report drained")
	}
}

// TestPOCHoldDrainLegWaitsForClientDrain locks the production-leg hold
// end-to-end at the unit level: the leg must stay open while the client still
// owes pulls, and release promptly once it has everything.
func TestPOCHoldDrainLegWaitsForClientDrain(t *testing.T) {
	withZeroDownsegJitter(t)
	c := newDownSegCache()
	c.append(bytes.Repeat([]byte{0x5A}, downsegSize))
	c.finalize()

	var sess httpSession
	sess.downseg.Store(c)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	released := make(chan struct{})
	go func() {
		defer close(released)
		holdDrainLeg(ctx, &sess, "poc-sid")
	}()

	select {
	case <-released:
		t.Fatal("drain hold released before the client pulled anything: the tail would be truncated")
	case <-time.After(150 * time.Millisecond):
	}

	// Client pulls every segment and the EOF marker.
	for i := uint64(0); ; i++ {
		if _, ok, _ := c.get(i); !ok {
			break
		}
	}
	c.noteEofServed()

	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Fatal("drain hold did not release after the client drained the cache")
	}
}

// TestPOCHoldDrainLegHonoursContextCancel bounds the hold: a cancelled request
// must not keep the leg (and the session cache) pinned.
func TestPOCHoldDrainLegHonoursContextCancel(t *testing.T) {
	withZeroDownsegJitter(t)
	c := newDownSegCache()
	c.append(bytes.Repeat([]byte{0x5A}, downsegSize))
	c.finalize()

	var sess httpSession
	sess.downseg.Store(c)

	ctx, cancel := context.WithCancel(context.Background())
	released := make(chan struct{})
	go func() {
		defer close(released)
		holdDrainLeg(ctx, &sess, "poc-sid")
	}()

	cancel()
	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Fatal("drain hold ignored request cancellation")
	}
}
