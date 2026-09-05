package splithttp

// Regression for the v9 real-network report: sustained high-bitrate upload
// (YouTube video ACK + browser upstream through packet-up) filled the server
// uploadQueue (channel cap scMaxBufferedPosts=64); Push was non-blocking, so a
// full queue failed with "packet queue full" -> hub returned 404 -> client
// retried -> the upload leg thrashed and the user saw throughput collapse
// (20000 -> 1000 kbps) then recovery. This is the same "bounded queue without
// backpressure -> drop -> reconnect" class we fixed for downlink segments.

import (
	"context"
	"errors"
	"testing"
	"time"
)

func smallPacket(seq uint64) Packet {
	return Packet{Seq: seq, Payload: make([]byte, 128)}
}

// TestUploadQueueFullBackpressuresThenSucceeds proves the fixed behavior:
// Push past the cap waits (does not 404/drop), a reader drains one slot, and
// the exact same packet enters the queue successfully.
func TestUploadQueueFullBackpressuresThenSucceeds(t *testing.T) {
	q := NewUploadQueue(4)
	for i := 0; i < 4; i++ {
		if err := q.Push(smallPacket(uint64(i))); err != nil {
			t.Fatalf("push %d failed before cap: %v", i, err)
		}
	}

	pushed := make(chan error, 1)
	go func() { pushed <- q.Push(smallPacket(4)) }()

	// The fifth push must not fail immediately with the old "packet queue
	// full" 404 behavior; it waits for the reader to make room.
	select {
	case err := <-pushed:
		t.Fatalf("push past cap returned before drain: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	var buf [256]byte
	if n, err := q.Read(buf[:]); err != nil || n != 128 {
		t.Fatalf("drain first packet: n=%d err=%v", n, err)
	}

	select {
	case err := <-pushed:
		if err != nil {
			t.Fatalf("backpressured push failed after reader drained: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("backpressured push did not resume after reader drained")
	}
}

// TestUploadQueueFullBackpressureIsBounded retains the DoS backstop: if no
// consumer ever drains, Push eventually fails rather than pinning a handler
// goroutine forever. The wait is deliberately long enough to absorb a brief
// legitimate target/VLESS stall but bounded well below session lifetime.
func TestUploadQueueFullBackpressureIsBounded(t *testing.T) {
	// Shrink the production 4.5s backpressure grace (same technique as
	// upload_queue_internal_test.go): the test pins the bounded-wait property
	// via the uploadQueueBackpressureWait var itself, so shrinking it keeps
	// the assertions self-consistent. Safe: same-package tests run serially.
	oldWait := uploadQueueBackpressureWait
	uploadQueueBackpressureWait = 100 * time.Millisecond
	defer func() { uploadQueueBackpressureWait = oldWait }()
	q := NewUploadQueue(1)
	if err := q.Push(smallPacket(0)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err := q.Push(smallPacket(1))
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("push past full queue unexpectedly succeeded without consumer")
	}
	if elapsed < uploadQueueBackpressureWait/2 {
		t.Fatalf("full push returned too early (%v), expected bounded backpressure", elapsed)
	}
	if elapsed > uploadQueueBackpressureWait+time.Second {
		t.Fatalf("full push wait unbounded (%v)", elapsed)
	}
}

// TestUploadQueueCloseUnblocksBackpressuredPush proves session teardown wakes
// a full-queue Push before its timeout and never leaves a handler stuck.
func TestUploadQueueCloseUnblocksBackpressuredPush(t *testing.T) {
	q := NewUploadQueue(1)
	if err := q.Push(smallPacket(0)); err != nil {
		t.Fatal(err)
	}
	pushed := make(chan error, 1)
	go func() { pushed <- q.Push(smallPacket(1)) }()
	select {
	case err := <-pushed:
		t.Fatalf("push returned before Close: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-pushed:
		if err == nil {
			t.Fatal("closed queue accepted a backpressured push")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock a backpressured push")
	}
}

// TestUploadQueueContextCancelUnblocksPush proves the HTTP handler's request
// context terminates a backpressured body promptly rather than retaining it
// until the bounded full-queue grace period after the peer has disconnected.
func TestUploadQueueContextCancelUnblocksPush(t *testing.T) {
	q := NewUploadQueue(1)
	if err := q.Push(smallPacket(0)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pushed := make(chan error, 1)
	go func() { pushed <- q.PushContext(ctx, smallPacket(1)) }()
	select {
	case err := <-pushed:
		t.Fatalf("push returned before cancellation: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-pushed:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("push after cancellation: %v want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("context cancellation did not unblock a backpressured push")
	}
}

// TestUploadQueueWaiterCapBoundsBodies proves a full session cannot retain an
// unlimited number of decoded POST bodies/handler goroutines. Once the
// configured waiter budget is consumed, the next Push fails immediately and
// the session close wakes all admitted waiters.
func TestUploadQueueWaiterCapBoundsBodies(t *testing.T) {
	q := NewUploadQueue(64)
	for i := 0; i < 64; i++ {
		if err := q.Push(smallPacket(uint64(i))); err != nil {
			t.Fatalf("fill %d: %v", i, err)
		}
	}
	waiters := make([]chan error, uploadQueueMaxWaiters)
	for i := range waiters {
		waiters[i] = make(chan error, 1)
		go func(ch chan error, seq uint64) { ch <- q.Push(smallPacket(seq)) }(waiters[i], uint64(i+1))
	}
	deadline := time.Now().Add(time.Second)
	for {
		q.writeCloseMutex.Lock()
		n := q.waiters
		q.writeCloseMutex.Unlock()
		if n == uploadQueueMaxWaiters {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("waiters=%d want %d", n, uploadQueueMaxWaiters)
		}
		time.Sleep(time.Millisecond)
	}
	start := time.Now()
	err := q.Push(smallPacket(999))
	if err == nil {
		t.Fatal("push beyond waiter cap unexpectedly accepted")
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatalf("waiter-cap rejection was not immediate: %v", time.Since(start))
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	for i, ch := range waiters {
		select {
		case err := <-ch:
			if err == nil {
				t.Fatalf("waiter %d accepted after Close", i)
			}
		case <-time.After(time.Second):
			t.Fatalf("waiter %d not unblocked by Close", i)
		}
	}
}

// TestUploadQueueCloseUnblocksReader keeps the existing close behavior:
// closing an empty queue wakes its reader with EOF.
func TestUploadQueueCloseUnblocksReader(t *testing.T) {
	q := NewUploadQueue(4)
	done := make(chan error, 1)
	go func() {
		var buf [128]byte
		_, err := q.Read(buf[:])
		done <- err
	}()
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected EOF on closed empty queue, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reader not unblocked by Close")
	}
}

type closeSpyReader struct {
	closed chan struct{}
}

func (r *closeSpyReader) Read([]byte) (int, error) { return 0, nil }
func (r *closeSpyReader) Close() error {
	select {
	case <-r.closed:
	default:
		close(r.closed)
	}
	return nil
}

// TestUploadQueueCloseClosesQueuedStreamReader verifies Close cleans up an
// unread stream-up HTTP body as well as pooled packet bodies.
func TestUploadQueueCloseClosesQueuedStreamReader(t *testing.T) {
	q := NewUploadQueue(4)
	r := &closeSpyReader{closed: make(chan struct{})}
	if err := q.Push(Packet{Reader: r}); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-r.closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not close queued stream reader")
	}
}
