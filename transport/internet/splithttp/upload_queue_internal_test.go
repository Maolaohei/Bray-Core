package splithttp

// Locks the packet-up reorder queue's three stability guarantees (checklist:
// 稳定性 / 分段重组语义):
//  1. seq gap timeout is bounded and tears the session stream down (not a
//     silent stall, not a synthetic skip);
//  2. the reorder buffer has a hard memory cap (maxPackets) with an explicit
//     teardown strategy on overflow;
//  3. duplicate seq (client retransmit) is idempotent: dropped, no error, no
//     teardown, and the next ordered packet is served.

import (
	"strings"
	"testing"
	"time"
)

func TestUploadQueue_GapTimeout_TearsDownSession(t *testing.T) {
	old := maxSeqGapWait
	maxSeqGapWait = 50 * time.Millisecond
	defer func() { maxSeqGapWait = old }()

	q := NewUploadQueue(10)
	// seq=1 arrives; seq=0 never does (lost POST).
	if err := q.Push(Packet{Payload: []byte("b"), Seq: 1}); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 10)
	start := time.Now()
	_, err := q.Read(buf)
	if err == nil {
		t.Fatal("expected gap timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "gap timeout waiting for seq=0") {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("gap wait not bounded by configured wait: %v", elapsed)
	}
}

func TestUploadQueue_HeapCap_OverflowTearsDown(t *testing.T) {
	old := maxSeqGapWait
	maxSeqGapWait = time.Second
	defer func() { maxSeqGapWait = old }()

	q := NewUploadQueue(2)
	// nextSeq=0 is missing (lost POST). A reader sits in the misordered
	// branch draining the channel into the heap; the concurrent pusher keeps
	// feeding higher seqs until the heap breaches maxPackets -> overflow
	// teardown. Read must return "too large", never serve out of order.
	go func() {
		for seq := uint64(1); seq <= 4; seq++ {
			if err := q.Push(Packet{Payload: []byte{byte(seq)}, Seq: seq}); err != nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	buf := make([]byte, 10)
	start := time.Now()
	_, err := q.Read(buf)
	if err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("expected overflow teardown, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > maxSeqGapWait {
		t.Fatalf("overflow took longer than gap budget: %v", elapsed)
	}
}

func TestUploadQueue_DuplicateSeq_Idempotent(t *testing.T) {
	q := NewUploadQueue(10)
	if err := q.Push(Packet{Payload: []byte("a"), Seq: 0}); err != nil {
		t.Fatal(err)
	}
	// Duplicate of seq 0 (client retry after a lost response): must be
	// accepted without error and later dropped silently.
	if err := q.Push(Packet{Payload: []byte("a-dup"), Seq: 0}); err != nil {
		t.Fatalf("duplicate push must not fail: %v", err)
	}
	if err := q.Push(Packet{Payload: []byte("b"), Seq: 1}); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 10)
	n, err := q.Read(buf)
	if err != nil || string(buf[:n]) != "a" {
		t.Fatalf("first read = %q, %v", buf[:n], err)
	}
	n, err = q.Read(buf)
	if err != nil || string(buf[:n]) != "b" {
		t.Fatalf("second read = %q, %v (duplicate must be skipped, not served)", buf[:n], err)
	}
	q.Close()
}

func TestUploadQueue_PartialReadThenEOF(t *testing.T) {
	q := NewUploadQueue(10)
	if err := q.Push(Packet{Payload: []byte("hello"), Seq: 0}); err != nil {
		t.Fatal(err)
	}
	small := make([]byte, 2)
	n, err := q.Read(small)
	if err != nil || string(small[:n]) != "he" {
		t.Fatalf("partial read = %q, %v", small[:n], err)
	}
	n, err = q.Read(small)
	if err != nil || string(small[:n]) != "ll" {
		t.Fatalf("partial read 2 = %q, %v", small[:n], err)
	}
	n, err = q.Read(small)
	if err != nil || string(small[:n]) != "o" {
		t.Fatalf("partial read 3 = %q, %v", small[:n], err)
	}
	q.Close()
	if _, err := q.Read(small); err == nil {
		t.Fatal("expected EOF after Close")
	}
}

// TestUploadQueue_SessionRebuild_IndependentNumbering documents the rebuild
// edge case: a torn upload session is never resumed — a new session leg is a
// new queue whose numbering restarts at 0, and queues never share state.
func TestUploadQueue_SessionRebuild_IndependentNumbering(t *testing.T) {
	q1, q2 := NewUploadQueue(10), NewUploadQueue(10)
	for _, q := range []*uploadQueue{q1, q2} {
		if err := q.Push(Packet{Payload: []byte("first"), Seq: 0}); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 10)
		n, err := q.Read(buf)
		if err != nil || string(buf[:n]) != "first" {
			t.Fatalf("fresh queue must serve seq=0, got %q, %v", buf[:n], err)
		}
		q.Close()
	}
}
