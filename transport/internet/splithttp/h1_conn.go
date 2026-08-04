package splithttp

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
)

// h1UploadMaxInflight bounds concurrent POSTs sharing one HTTP/1.1 upload conn.
// Depth 1 is fully serial. Depth >1 pipelines additional writes while earlier
// responses are still in flight; each caller still waits for its own 200 before
// returning (ordered read loop). Failures fail-closed and mark the conn dead.
// Depth 3 is the stability-first ceiling for product packet-up windows without
// unbounded H1 HOL risk on flaky middleboxes.
//
// Writes are batched: concurrent requests are coalesced into a single
// writev/WSASend via net.Buffers (double-buffered zero-alloc queue sized to
// the inflight ceiling), so N in-flight POSTs cost ~1 syscall instead of N.
const h1UploadMaxInflight = 3

type H1Conn struct {
	// UnreadResponsesCount is retained for idle-pool accounting / tests.
	// Live concurrent pipeline uses writeSeq/readSeq under mu.
	UnreadResponsesCount int
	RespBufReader        *bufio.Reader
	net.Conn

	mu       sync.Mutex
	cond     *sync.Cond
	writeSeq int
	readSeq  int
	dead     bool
	deadErr  error
	inflight int

	// Batched-write state (all under mu): producers append reqBytes to
	// *writeIn (a fixed array sized to the inflight ceiling — zero alloc);
	// the first producer becomes the writer and swaps the writeIn/writeOut
	// POINTERS, so it owns the old array exclusively — the batch slice keeps
	// referencing that array (the swap only rewrites the pointer fields),
	// while producers keep appending to the other array. Batch is flushed
	// with a single syscall.
	writePos     int // monotonically assigned write positions
	writtenPos   int // write watermark: positions <= writtenPos are flushed
	writeIn      *[h1UploadMaxInflight][]byte
	writeOut     *[h1UploadMaxInflight][]byte
	writeInCount int
	writing      bool

	activeUsers int
}

func NewH1Conn(conn net.Conn) *H1Conn {
	h := &H1Conn{
		RespBufReader: bufio.NewReaderSize(conn, 32*1024),
		Conn:          conn,
		writeIn:       new([h1UploadMaxInflight][]byte),
		writeOut:      new([h1UploadMaxInflight][]byte),
	}
	h.cond = sync.NewCond(&h.mu)
	return h
}

// pipelinePost writes one request and waits for the matching response in order.
// Multiple callers may share the same conn up to h1UploadMaxInflight.
func (h *H1Conn) pipelinePost(reqBytes []byte) error {
	h.mu.Lock()
	if h.dead {
		err := h.deadErr
		h.mu.Unlock()
		if err == nil {
			err = net.ErrClosed
		}
		return err
	}
	for h.inflight >= h1UploadMaxInflight && !h.dead {
		h.cond.Wait()
	}
	if h.dead {
		err := h.deadErr
		h.mu.Unlock()
		if err == nil {
			err = net.ErrClosed
		}
		return err
	}
	mySeq := h.writeSeq
	h.writeSeq++
	h.inflight++
	h.UnreadResponsesCount = h.inflight

	// Batched write: enqueue our bytes and either become the writer (draining
	// the queue with coalesced syscalls) or wait until our position is flushed.
	myWritePos := h.writePos
	h.writePos++
	h.writeIn[h.writeInCount] = reqBytes
	h.writeInCount++
	if h.writing {
		// Another goroutine is the writer; wait until our write is flushed.
		for !h.dead && h.writtenPos <= myWritePos {
			h.cond.Wait()
		}
		if h.dead {
			err := h.deadErr
			h.mu.Unlock()
			if err == nil {
				err = net.ErrClosed
			}
			return err
		}
		h.mu.Unlock()
	} else {
		h.writing = true
		h.mu.Unlock()

		// We are the writer. Take the batch from writeIn (the array producers
		// append to), then swap so we own the taken array exclusively while
		// producers keep appending to the other one.
		for {
			h.mu.Lock()
			if h.writeInCount == 0 {
				h.writing = false
				h.mu.Unlock()
				break
			}
			n := h.writeInCount
			batch := h.writeIn[:n]
			h.writeInCount = 0
			h.writeIn, h.writeOut = h.writeOut, h.writeIn
			h.mu.Unlock()

			werr := h.writeBatch(batch)

			h.mu.Lock()
			if werr != nil {
				h.writing = false
				h.mu.Unlock()
				h.failPipeline(werr)
				return werr
			}
			h.writtenPos += n
			h.cond.Broadcast()
			h.mu.Unlock()
		}
	}

	// Wait until it is our turn to read. Release mu before ReadResponse so a
	// peer can still acquire a write slot and pipeline the next request.
	h.mu.Lock()
	for !h.dead && h.readSeq != mySeq {
		h.cond.Wait()
	}
	if h.dead {
		err := h.deadErr
		h.mu.Unlock()
		if err == nil {
			err = net.ErrClosed
		}
		return err
	}
	h.mu.Unlock()

	resp, rerr := http.ReadResponse(h.RespBufReader, nil)
	if rerr != nil {
		h.failPipeline(rerr)
		return rerr
	}
	if resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	status := resp.StatusCode

	h.mu.Lock()
	h.readSeq++
	h.inflight--
	if h.inflight < 0 {
		h.inflight = 0
	}
	h.UnreadResponsesCount = h.inflight
	h.cond.Broadcast()
	h.mu.Unlock()

	if status != 200 {
		err := fmt.Errorf("got non-200 error response code: %d", status)
		h.failPipeline(err)
		return err
	}
	return nil
}

// writeBatch flushes one coalesced batch with a single writev/WSASend syscall
// when the batch holds multiple requests; a lone request goes through the
// plain Write fast path (identical cost to the pre-batching implementation).
func (h *H1Conn) writeBatch(batch [][]byte) error {
	if len(batch) == 1 {
		_, err := h.Conn.Write(batch[0])
		return err
	}
	bufs := net.Buffers(batch)
	_, err := bufs.WriteTo(h.Conn)
	return err
}

func (h *H1Conn) failPipeline(err error) {
	h.mu.Lock()
	if !h.dead {
		h.dead = true
		h.deadErr = err
	}
	h.cond.Broadcast()
	h.mu.Unlock()
	_ = h.Conn.Close()
}

func (h *H1Conn) isDead() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.dead
}

// tryAcquireShared marks one concurrent user. Returns false if dead.
func (h *H1Conn) tryAcquireShared() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.dead {
		return false
	}
	h.activeUsers++
	return true
}

// releaseShared drops one concurrent user. Returns true when idle+healthy.
func (h *H1Conn) releaseShared() (idleHealthy bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.activeUsers > 0 {
		h.activeUsers--
	}
	return h.activeUsers == 0 && !h.dead
}
