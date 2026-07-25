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
const h1UploadMaxInflight = 3

type H1Conn struct {
	// UnreadResponsesCount is retained for idle-pool accounting / tests.
	// Live concurrent pipeline uses writeSeq/readSeq under mu.
	UnreadResponsesCount int
	RespBufReader        *bufio.Reader
	net.Conn

	mu          sync.Mutex
	writeMu     sync.Mutex
	cond        *sync.Cond
	writeSeq    int
	readSeq     int
	dead        bool
	deadErr     error
	inflight    int
	activeUsers int
}

func NewH1Conn(conn net.Conn) *H1Conn {
	h := &H1Conn{
		RespBufReader: bufio.NewReaderSize(conn, 32*1024),
		Conn:          conn,
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
	h.mu.Unlock()

	h.writeMu.Lock()
	_, werr := h.Conn.Write(reqBytes)
	h.writeMu.Unlock()
	if werr != nil {
		h.failPipeline(werr)
		return werr
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
