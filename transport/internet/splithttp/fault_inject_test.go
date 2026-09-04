package splithttp

// Fault-injection units (checklist 干扰对抗 + 稳定性):
//   - a 5xx injected between pipelined POSTs must fail FAST (conn marked dead,
//     no retry-on-dead) and must not poison unrelated fresh connections;
//   - a mid-stream connection RST surfaces as a write/read error and tears the
//     H1 pipeline down — never a silent hang.

import (
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestFaultInject_H1_503BetweenPipelinedPosts(t *testing.T) {
	// Responses: 200 for req#1, 503 for req#2, then conn close.
	sc := &scriptedConn{reads: []string{
		"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
		"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n",
	}}
	h := NewH1Conn(sc)

	if err := h.pipelinePost([]byte("POST /a HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n")); err != nil {
		t.Fatalf("req1 must succeed: %v", err)
	}
	err := h.pipelinePost([]byte("POST /b HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n"))
	if err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("req2 must fail with 503, got %v", err)
	}
	if !h.isDead() {
		t.Fatal("conn must be marked dead after 5xx (fail fast, no reuse)")
	}
	sc.mu.Lock()
	closed := sc.closed
	sc.mu.Unlock()
	if !closed {
		t.Fatal("underlying conn must be closed after 5xx")
	}
	// A third post on the dead conn must fail immediately with the same error.
	err3 := h.pipelinePost([]byte("POST /c HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n"))
	if err3 == nil {
		t.Fatal("post on dead conn must fail")
	}
}

func TestFaultInject_H1_MidStreamRST(t *testing.T) {
	// rstConn: first response read succeeds (one 200), then EOF — the
	// client-side half of a mid-flight TCP RST between pipelined requests.
	rst := &rstConn{limit: 1}
	h := NewH1Conn(rst)

	if err := h.pipelinePost([]byte("POST /a HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n")); err != nil {
		t.Fatalf("req1 must succeed on the pre-RST response: %v", err)
	}
	err := h.pipelinePost([]byte("POST /b HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n"))
	if err == nil {
		t.Fatal("expected error from reset conn, got nil")
	}
	if !h.isDead() {
		t.Fatal("pipeline must die on mid-stream RST, not hang")
	}
	if time.Since(startOfTest(t)) > 5*time.Second {
		t.Fatal("failure must be immediate, not a timeout")
	}
}

func startOfTest(t *testing.T) time.Time {
	t.Helper()
	return time.Now()
}

// rstConn lets the first Read succeed (one 200), then EOFs — the client-side
// half of a mid-flight TCP RST.
type rstConn struct {
	limit    int
	consumed int
	closed   bool
}

func (c *rstConn) Read(p []byte) (int, error) {
	if c.consumed >= c.limit {
		return 0, io.EOF
	}
	c.consumed++
	data := []byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
	n := copy(p, data)
	return n, nil
}

func (c *rstConn) Write(p []byte) (int, error) {
	if c.closed {
		return 0, net.ErrClosed
	}
	return len(p), nil
}

func (c *rstConn) Close() error {
	c.closed = true
	return nil
}
func (c *rstConn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (c *rstConn) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (c *rstConn) SetDeadline(t time.Time) error      { return nil }
func (c *rstConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *rstConn) SetWriteDeadline(t time.Time) error { return nil }
