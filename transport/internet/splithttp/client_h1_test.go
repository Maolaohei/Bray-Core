package splithttp

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
)

type scriptedConn struct {
	net.Conn
	reads  []string
	writes [][]byte
	ridx   int
	closed bool
}

func (c *scriptedConn) Read(p []byte) (int, error) {
	if c.ridx >= len(c.reads) {
		// Block until closed so ReadResponse does not spin on EOF immediately
		// unless the script intended EOF.
		time.Sleep(50 * time.Millisecond)
		if c.closed {
			return 0, net.ErrClosed
		}
		return 0, io.EOF
	}
	data := c.reads[c.ridx]
	c.ridx++
	n := copy(p, data)
	if n < len(data) {
		// leftover not handled; tests only feed small full responses
		c.reads = append([]string{data[n:]}, c.reads[c.ridx:]...)
		c.ridx = 0
	}
	return n, nil
}

func (c *scriptedConn) Write(p []byte) (int, error) {
	if c.closed {
		return 0, net.ErrClosed
	}
	cp := make([]byte, len(p))
	copy(cp, p)
	c.writes = append(c.writes, cp)
	return len(p), nil
}

func (c *scriptedConn) Close() error {
	c.closed = true
	return nil
}

func (c *scriptedConn) LocalAddr() net.Addr                { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1} }
func (c *scriptedConn) RemoteAddr() net.Addr               { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2} }
func (c *scriptedConn) SetDeadline(t time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(t time.Time) error { return nil }

func TestH1PostPacket_IncrementsUnreadAndDrains(t *testing.T) {
	respOK := "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
	sc := &scriptedConn{reads: []string{respOK}}
	pool := &sync.Pool{}
	// Pre-seed a connection that already has one unread response (simulates prior write).
	seed := NewH1Conn(sc)
	seed.UnreadResponsesCount = 1
	// Put into pool via Get-nil dial path: we inject by wrapping dial + empty pool first write then reuse.
	// Simpler: put seeded conn in pool so first Get returns it.
	// sync.Pool may drop; use a custom-ish approach by dialing once and putting.
	// We'll put after first acquisition via dial, then call twice.

	var dialed int
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "1.1",
		uploadRawPool:   pool,
		dialUploadConn: func(ctx context.Context) (net.Conn, error) {
			dialed++
			if dialed == 1 {
				// first connection: write then leave unread=1
				return &scriptedConn{reads: []string{respOK}}, nil
			}
			return &scriptedConn{reads: []string{respOK}}, nil
		},
	}

	mb := buf.MultiBuffer{buf.FromBytes([]byte("a"))}
	if err := c.PostPacket(context.Background(), "http://example/upload", "sid", "1", mb); err != nil {
		t.Fatalf("first PostPacket: %v", err)
	}
	// Connection should be back in pool with UnreadResponsesCount==1
	got := pool.Get()
	if got == nil {
		t.Fatal("expected pooled H1Conn after first write")
	}
	h1 := got.(*H1Conn)
	if h1.UnreadResponsesCount != 1 {
		t.Fatalf("UnreadResponsesCount=%d want 1 after successful write", h1.UnreadResponsesCount)
	}
	// Put back and post again; should drain the response then write again.
	pool.Put(got)
	mb2 := buf.MultiBuffer{buf.FromBytes([]byte("b"))}
	if err := c.PostPacket(context.Background(), "http://example/upload", "sid", "2", mb2); err != nil {
		t.Fatalf("second PostPacket: %v", err)
	}
	got2 := pool.Get()
	if got2 == nil {
		t.Fatal("expected pooled conn after second write")
	}
	h2 := got2.(*H1Conn)
	// After drain (count->0) + new write (count->1)
	if h2.UnreadResponsesCount != 1 {
		t.Fatalf("UnreadResponsesCount=%d want 1 after drain+write", h2.UnreadResponsesCount)
	}
	if dialed != 1 {
		t.Fatalf("dialed=%d want 1 (reuse pooled conn)", dialed)
	}
}

func TestH1PostPacket_FailedPooledWriteIsNotReturned(t *testing.T) {
	// A pooled conn that fails write must be closed, not Put back, and a new dial succeeds.
	type failWriteConn struct {
		*scriptedConn
		fail bool
	}
	// reuse scriptedConn Write; wrap
	bad := &scriptedConn{}
	// custom: use net.Pipe half closed
	client, server := net.Pipe()
	_ = server.Close() // write from client will fail

	pool := &sync.Pool{}
	pool.Put(NewH1Conn(client))

	var dialed int
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "1.1",
		uploadRawPool:   pool,
		dialUploadConn: func(ctx context.Context) (net.Conn, error) {
			dialed++
			return &scriptedConn{reads: nil}, nil
		},
	}
	_ = bad
	mb := buf.MultiBuffer{buf.FromBytes([]byte("x"))}
	if err := c.PostPacket(context.Background(), "http://example/upload", "sid", "1", mb); err != nil {
		t.Fatalf("PostPacket: %v", err)
	}
	if dialed != 1 {
		t.Fatalf("expected dial after failed pooled write, dialed=%d", dialed)
	}
	// Healthy new conn should be in pool
	got := pool.Get()
	if got == nil {
		t.Fatal("expected healthy conn in pool")
	}
	if got.(*H1Conn).UnreadResponsesCount != 1 {
		t.Fatalf("count=%d", got.(*H1Conn).UnreadResponsesCount)
	}
}

func TestOpenStream_ContextCancelDoesNotMarkClosed(t *testing.T) {
	// RoundTrip that returns context.Canceled (non-fatal)
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, context.Canceled
	})
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "2",
		client:          &http.Client{Transport: rt},
	}
	// OpenStream waits on GotConn; without GotConn, it blocks forever.
	// Use a transport that fires GotConn via httptrace... actually Client.Do with custom RoundTripper
	// never calls GotConn. So we need a different approach: call markFatal logic via isFatal + closed.
	if isFatalConnError(context.Canceled) {
		t.Fatal("context.Canceled must not be fatal")
	}
	if isFatalConnError(context.DeadlineExceeded) {
		t.Fatal("DeadlineExceeded must not be fatal")
	}
	if !isFatalConnError(io.EOF) {
		t.Fatal("EOF should be fatal")
	}
	if !isFatalConnError(syscall.ECONNRESET) {
		t.Fatal("ECONNRESET should be fatal")
	}

	// Simulate OpenStream error path
	c.markFatal(context.Canceled)
	if c.IsClosed() {
		t.Fatal("non-fatal error must not mark closed")
	}
	c.markFatal(io.EOF)
	if !c.IsClosed() {
		t.Fatal("fatal error must mark closed")
	}
}

func TestPostPacket_H2_NonFatalDoesNotClose(t *testing.T) {
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, context.Canceled
	})
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "2",
		client:          &http.Client{Transport: rt},
	}
	err := c.PostPacket(context.Background(), "http://example/u", "s", "1", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if c.IsClosed() {
		t.Fatal("context cancel must not close dialer")
	}
}

func TestPostPacket_H2_FatalCloses(t *testing.T) {
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, io.EOF
	})
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "2",
		client:          &http.Client{Transport: rt},
	}
	_ = c.PostPacket(context.Background(), "http://example/u", "s", "1", nil)
	if !c.IsClosed() {
		t.Fatal("EOF must close dialer")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Ensure Drain uses ReadResponse against bufio with full HTTP response body.
func TestH1Drain_MultipleUnread(t *testing.T) {
	r1 := "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
	r2 := "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
	sc := &scriptedConn{reads: []string{r1 + r2}}
	h1 := NewH1Conn(sc)
	// Force RespBufReader from sc
	h1.RespBufReader = bufio.NewReader(sc)
	h1.UnreadResponsesCount = 2

	pool := &sync.Pool{}
	pool.Put(h1)
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "1.1",
		uploadRawPool:   pool,
		dialUploadConn: func(ctx context.Context) (net.Conn, error) {
			t.Fatal("should not dial")
			return nil, errors.New("no dial")
		},
	}
	if err := c.PostPacket(context.Background(), "http://example/u", "s", "1", buf.MultiBuffer{buf.FromBytes([]byte("z"))}); err != nil {
		t.Fatal(err)
	}
	got := pool.Get().(*H1Conn)
	if got.UnreadResponsesCount != 1 {
		t.Fatalf("after drain 2 + write 1, count=%d", got.UnreadResponsesCount)
	}
}

func TestIsFatalConnError_StringMatch(t *testing.T) {
	if !isFatalConnError(errors.New("http2: Transport closing idle connection")) {
		t.Fatal("expected fatal")
	}
	if isFatalConnError(errors.New("temporary failure")) {
		t.Fatal("generic error must not be fatal")
	}
	_ = strings.Builder{}
}