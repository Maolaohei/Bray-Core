package splithttp

// Real-network 410 regression (v7 report: sustained downloads on 195ms-RTT
// links kept tearing down with "downlink segment slid past (410)").
//
// Old (buggy) policy: when undelivered segments exceeded downsegAdaptiveSegs
// (64), evictOverflowLocked deleted the OLDEST undelivered segment — which is
// exactly the segment a slow consumer is about to pull (lastPulled). Under a
// high-RTT link the client pays ~1 RTT per pull while the server produces at
// local speed, so a sustained download naturally accumulates >64 undelivered
// segments; the eviction then 410'd the consumer's in-progress segment and
// tore the whole download. A pure dual-end test never tripped the 64-segment
// bound (32MiB = 32 segments), which is why it stayed green locally.
//
// New (correct) policy: bounded flow control. append() BLOCKS on spaceCond
// once undelivered == downsegAdaptiveSegs, and get() broadcasts when it
// delivers — a produced-but-undelivered segment is NEVER dropped, so a slow
// consumer can never be falsely 410'd. The stream ends via finalize() (EOF,
// which stops backpressure) or shutdown() (session teardown).

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

// TestDownSegBackpressureOrderedFollow is the real-world shape: a producer
// runs far ahead, a sequenced consumer follows at its own slower pace (the
// high-RTT client), and every segment the consumer asks for must be pullable.
// This is exactly what the buggy overflow eviction broke.
func TestDownSegBackpressureOrderedFollow(t *testing.T) {
	withZeroDownsegJitter(t)
	c := newDownSegCache()
	const total = downsegAdaptiveSegs + 32 // >> bound, forces backpressure

	var wg sync.WaitGroup
	wg.Add(2)

	// Producer: produce total segments, ends with finalize (stream EOF).
	go func() {
		defer wg.Done()
		for i := 0; i < total; i++ {
			c.append(bytes.Repeat([]byte{byte(i % 251)}, downsegSize))
		}
		c.finalize()
	}()

	// Consumer: pull every segment strictly in order (the puller's contract),
	// with a small real pacing to model RTT (the bug showed at high RTT).
	go func() {
		defer wg.Done()
		for seq := uint64(0); seq < total; seq++ {
			deadline := time.Now().Add(30 * time.Second)
			for {
				p, ok, gone := c.get(seq)
				if gone {
					t.Errorf("seq %d 410-gone under ordered follow — the false-410 bug", seq)
					return
				}
				if ok {
					if len(p) == 0 {
						t.Errorf("seq %d empty payload", seq)
						return
					}
					break
				}
				if time.Now().After(deadline) {
					t.Errorf("seq %d not produced within deadline", seq)
					return
				}
				// small paced wait models a slower-than-producer consumer
				time.Sleep(200 * time.Microsecond)
			}
		}
	}()

	wg.Wait()

	if !c.over() {
		t.Fatal("stream not finalized after full ordered consumption")
	}
}

// TestDownSegShutdownUnblocksProducer: if the session tears down while the
// producer is blocked at the bound (client vanished), shutdown() must unblock
// it so the goroutine is not wedged forever.
func TestDownSegShutdownUnblocksProducer(t *testing.T) {
	withZeroDownsegJitter(t)
	c := newDownSegCache()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < downsegAdaptiveSegs+32; i++ {
			c.append(bytes.Repeat([]byte{byte(i % 251)}, downsegSize))
		}
	}()

	select {
	case <-done:
		t.Fatal("producer finished without consumer (should be blocked)")
	case <-time.After(300 * time.Millisecond):
	}

	// Client vanished: session teardown unblocks the producer.
	c.shutdown()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not unblock the backpressured producer")
	}
}

func TestDownSegSessionCloseUnblocksProducer(t *testing.T) {
	withZeroDownsegJitter(t)
	s := &httpSession{uploadQueue: NewUploadQueue(1)}
	if !s.enterDownsegMode() {
		t.Fatal("failed to enter downseg mode")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < downsegAdaptiveSegs+32; i++ {
			s.downsegAppend(bytes.Repeat([]byte{byte(i % 251)}, downsegSize))
		}
	}()
	select {
	case <-done:
		t.Fatal("producer finished without a segment consumer")
	case <-time.After(300 * time.Millisecond):
	}
	s.close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("session close did not unblock the backpressured dseg producer")
	}
}
