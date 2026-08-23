package splithttp

import (
	"bytes"
	"runtime/debug"
	"testing"
	"time"
	"unsafe"
)

// alloc/free round-trip must return a buffer with len=n and cap >= n, and
// freePostBody must put the buffer back into the matching size class so a
// subsequent same-size alloc reuses it (verified by content survival, which
// holds without GC or concurrency between the two calls).
func TestPostBodyPool_RoundTrip(t *testing.T) {
	sizes := []int{1, 2047, 2048, 4096, 100000, 262144, 1000001, 512 * 1024}
	for _, n := range sizes {
		b := allocPostBody(n)
		if len(b) != n {
			t.Fatalf("allocPostBody(%d): len=%d", n, len(b))
		}
		if cap(b) < n {
			t.Fatalf("allocPostBody(%d): cap=%d < n", n, cap(b))
		}
		// fill every byte so reuse detection is content-based
		for i := range b {
			b[i] = byte(i & 0xff)
		}
		freePostBody(b)
		b2 := allocPostBody(n)
		if len(b2) != n {
			t.Fatalf("re-allocPostBody(%d): len=%d", n, len(b2))
		}
		// pooled reuse keeps the old content (same backing array); requests
		// below the smallest class or above the largest are plain make and
		// have no reuse guarantee. Skipped under -race: the detector clears
		// sync.Pool between Get/Put, so reuse cannot be observed.
		if !raceEnabled && n >= postBodyPoolSizes[0] && n <= postBodyPoolSizes[len(postBodyPoolSizes)-1] && !bytes.Equal(b2, b) {
			t.Fatalf("allocPostBody(%d): pool did not reuse buffer", n)
		}
		freePostBody(b2)
	}
}

// freePostBody must tolerate arbitrary slices (make-backed, resliced) without
// panicking or corrupting pool classes.
func TestPostBodyPool_FreeArbitrary(t *testing.T) {
	freePostBody(nil)
	freePostBody([]byte{})
	freePostBody(make([]byte, 100))         // below smallest class
	freePostBody(make([]byte, 100000))      // between classes
	freePostBody(allocPostBody(300000))     // pooled borrow
	freePostBody(make([]byte, 1<<23))       // above largest class
	freePostBody((allocPostBody(4096))[:0]) // resliced borrow
}

// assertPooledReuse verifies that payload has been returned to the pool by
// allocating the same size again and checking identity: same capacity class
// and the same backing array (pointer equality). Content checks are avoided:
// the pool is shared across tests, so an unrelated pooled buffer may come
// back first. Under -race the detector clears sync.Pool between Get/Put, so
// only len sanity is checked there. Exact-address reuse additionally relies
// on staying on the same P (sync.Pool per-P private slot); goroutine
// migration or a steal from another P's shared pool can return a different
// buffer even though ours WAS returned — so retry a bounded number of times
// before declaring failure.
func assertPooledReuse(t *testing.T, payload []byte) {
	t.Helper()
	if len(payload) == 0 {
		return
	}
	if raceEnabled {
		again := allocPostBody(len(payload))
		defer freePostBody(again)
		if len(again) != len(payload) {
			t.Fatalf("realloc len=%d want %d", len(again), len(payload))
		}
		return
	}
	for try := 0; try < 64; try++ {
		again := allocPostBody(len(payload))
		ok := cap(again) == cap(payload) && &again[0] == &payload[0]
		freePostBody(again)
		if ok {
			return
		}
	}
	t.Fatalf("payload never resurfaced from pool in 64 allocs (cap=%d)", cap(payload))
}

// Consumed packets must return their pooled payload; the queue must not touch
// non-pooled payloads.
func TestUploadQueue_PooledReturnOnConsume(t *testing.T) {
	q := NewUploadQueue(10)
	payload := allocPostBody(300000) // class 512K borrow
	for i := range payload {
		payload[i] = 0xAB
	}
	if err := q.Push(Packet{Payload: payload, Seq: 0, Pooled: true}); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 400000)
	n, err := q.Read(buf)
	if err != nil || n != len(payload) {
		t.Fatalf("Read: n=%d err=%v", n, err)
	}
	assertPooledReuse(t, payload)
}

// Duplicate packets (a late retry for an already-consumed seq) are skipped;
// their pooled payload must be returned.
func TestUploadQueue_PooledReturnOnDuplicate(t *testing.T) {
	q := NewUploadQueue(10)
	if err := q.Push(Packet{Payload: []byte("first"), Seq: 0}); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 32)
	n, err := q.Read(buf)
	if err != nil || string(buf[:n]) != "first" {
		t.Fatalf("Read: n=%d err=%v", n, err)
	}
	// Now a late retry of seq 0 arrives (dup), followed by the next packet.
	dup := allocPostBody(4096)
	if err := q.Push(Packet{Payload: dup, Seq: 0, Pooled: true}); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(Packet{Payload: []byte("second"), Seq: 1}); err != nil {
		t.Fatal(err)
	}
	n, err = q.Read(buf)
	if err != nil || string(buf[:n]) != "second" {
		t.Fatalf("Read: n=%d err=%v", n, err)
	}
	assertPooledReuse(t, dup)
}

// Push failure (queue full) must return the pooled payload.
func TestUploadQueue_PooledReturnOnPushFail(t *testing.T) {
	// The identity assertion below assumes sync.Pool LIFO reuse on this
	// goroutine. A GC between freePostBody and the next alloc drops the
	// pooled buffer (victim cache), making the next alloc return a different
	// address — a CI-load flake, not a product bug. Pin the GC off for the
	// critical section.
	oldGC := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(oldGC)
	q := NewUploadQueue(2)
	// Fill the channel: the first two pushes land in the channel buffer.
	if err := q.Push(Packet{Payload: allocPostBody(4096), Seq: 0, Pooled: true}); err != nil {
		t.Fatal(err)
	}
	// The heap hold does not consume channel capacity; push until full.
	for i := 1; ; i++ {
		payload := allocPostBody(4096)
		payload[0] = byte(i)
		err := q.Push(Packet{Payload: payload, Seq: uint64(i), Pooled: true})
		if err != nil {
			// Push failed: payload must already be back in the pool.
			assertPooledReuse(t, payload)
			break
		}
		if i > 100 {
			t.Fatal("queue never filled")
		}
	}
	q.Close()
}

// Aborting on a sequence gap must drain and return all queued pooled payloads
// (identity verified by TestUploadQueue_PooledDrainReturnsAll; here we pin the
// timeout behavior itself).
func TestUploadQueue_PooledReturnOnGapTimeout(t *testing.T) {
	q := NewUploadQueue(10)
	for i := 1; i <= 3; i++ {
		p := allocPostBody(4096)
		if err := q.Push(Packet{Payload: p, Seq: uint64(i), Pooled: true}); err != nil {
			t.Fatal(err)
		}
	}
	buf := make([]byte, 32)
	start := time.Now()
	_, err := q.Read(buf) // seq 0 never arrives -> gap timeout
	if err == nil {
		t.Fatal("expected gap timeout error")
	}
	if time.Since(start) > 6*time.Second {
		t.Fatal("gap timeout took too long")
	}
}

// TestUploadQueue_PooledDrainReturnsAll pins the drainAndFree abort path:
// every queued pooled payload must come back to the pool (verified by backing
// array identity, which survives pool reuse in non-race builds).
func TestUploadQueue_PooledDrainReturnsAll(t *testing.T) {
	q := NewUploadQueue(10)
	var orphans []uintptr
	for i := 1; i <= 3; i++ {
		p := allocPostBody(4096)
		orphans = append(orphans, uintptr(unsafe.Pointer(&p[0])))
		if err := q.Push(Packet{Payload: p, Seq: uint64(i), Pooled: true}); err != nil {
			t.Fatal(err)
		}
	}
	buf := make([]byte, 32)
	_, err := q.Read(buf) // seq 0 never arrives -> gap timeout
	if err == nil {
		t.Fatal("expected gap timeout error")
	}
	if raceEnabled {
		return // detector clears the pool; identity cannot be observed
	}
	// Collect the reallocations first, then match: freeing inside the loop
	// would return the same buffer and Get would hand it back repeatedly.
	var addrs []uintptr
	var bufs [][]byte
	for range orphans {
		again := allocPostBody(4096)
		bufs = append(bufs, again)
		addrs = append(addrs, uintptr(unsafe.Pointer(&again[0])))
	}
	for _, addr := range addrs {
		found := false
		for i, o := range orphans {
			if o == addr {
				orphans = append(orphans[:i], orphans[i+1:]...)
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("orphaned payload %x not returned to pool", addr)
		}
	}
	for _, b := range bufs {
		freePostBody(b)
	}
	if len(orphans) != 0 {
		t.Fatalf("%d orphaned payloads not returned", len(orphans))
	}
}

// TestUploadQueue_CloseDrainsBufferedPooledPackets guards the normal session
// teardown path: Close used to close the channel then make Read return EOF,
// leaving already-buffered Pooled payloads for GC instead of returning them
// to postBodyPool. Close must now drain those channel entries immediately.
func TestUploadQueue_CloseDrainsBufferedPooledPackets(t *testing.T) {
	q := NewUploadQueue(10)
	var orphans []uintptr
	for i := 0; i < 3; i++ {
		p := allocPostBody(4096)
		orphans = append(orphans, uintptr(unsafe.Pointer(&p[0])))
		if err := q.Push(Packet{Payload: p, Seq: uint64(i), Pooled: true}); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if raceEnabled {
		return // detector clears the pool; identity cannot be observed
	}
	var bufs [][]byte
	for range orphans {
		again := allocPostBody(4096)
		bufs = append(bufs, again)
		addr := uintptr(unsafe.Pointer(&again[0]))
		found := false
		for i, o := range orphans {
			if o == addr {
				orphans = append(orphans[:i], orphans[i+1:]...)
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Close-buffered payload %x not returned to pool", addr)
		}
	}
	for _, b := range bufs {
		freePostBody(b)
	}
	if len(orphans) != 0 {
		t.Fatalf("%d Close-buffered payloads not returned", len(orphans))
	}
}
