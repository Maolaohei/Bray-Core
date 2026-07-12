package splithttp

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
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

func (c *scriptedConn) LocalAddr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1} }
func (c *scriptedConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2}
}
func (c *scriptedConn) SetDeadline(t time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(t time.Time) error { return nil }

func TestH1PostPacket_ImmediateReadAndReuse(t *testing.T) {
	respOK := "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
	pool := newH1ConnPool(defaultH1UploadPoolCap)

	var dialed int
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "1.1",
		uploadRawPool:   pool,
		dialUploadConn: func(ctx context.Context) (net.Conn, error) {
			dialed++
			return &scriptedConn{reads: []string{respOK, respOK}}, nil
		},
	}

	mb := buf.MultiBuffer{buf.FromBytes([]byte("a"))}
	if err := c.PostPacket(context.Background(), "http://example/upload", "sid", "1", mb); err != nil {
		t.Fatalf("first PostPacket: %v", err)
	}
	h1 := pool.Get()
	if h1 == nil {
		t.Fatal("expected pooled H1Conn after first write")
	}
	if h1.UnreadResponsesCount != 0 {
		t.Fatalf("UnreadResponsesCount=%d want 0 after immediate response drain", h1.UnreadResponsesCount)
	}
	pool.Put(h1)
	mb2 := buf.MultiBuffer{buf.FromBytes([]byte("b"))}
	if err := c.PostPacket(context.Background(), "http://example/upload", "sid", "2", mb2); err != nil {
		t.Fatalf("second PostPacket: %v", err)
	}
	h2 := pool.Get()
	if h2 == nil {
		t.Fatal("expected pooled conn after second write")
	}
	if h2.UnreadResponsesCount != 0 {
		t.Fatalf("UnreadResponsesCount=%d want 0 after second immediate response drain", h2.UnreadResponsesCount)
	}
	if dialed != 1 {
		t.Fatalf("dialed=%d want 1 (reuse pooled conn)", dialed)
	}
}

func TestH1PostPacket_FailedPooledWriteIsNotReturned(t *testing.T) {
	client, server := net.Pipe()
	_ = server.Close()

	pool := newH1ConnPool(defaultH1UploadPoolCap)
	pool.Put(NewH1Conn(client))

	var dialed int
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "1.1",
		uploadRawPool:   pool,
		dialUploadConn: func(ctx context.Context) (net.Conn, error) {
			dialed++
			return &scriptedConn{reads: []string{"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"}}, nil
		},
	}
	mb := buf.MultiBuffer{buf.FromBytes([]byte("x"))}
	if err := c.PostPacket(context.Background(), "http://example/upload", "sid", "1", mb); err != nil {
		t.Fatalf("PostPacket: %v", err)
	}
	if dialed != 1 {
		t.Fatalf("expected dial after failed pooled write, dialed=%d", dialed)
	}
	got := pool.Get()
	if got == nil {
		t.Fatal("expected healthy conn in pool")
	}
	if got.UnreadResponsesCount != 0 {
		t.Fatalf("count=%d want 0 after immediate response drain", got.UnreadResponsesCount)
	}
}

func TestOpenStream_ContextCancelDoesNotMarkClosed(t *testing.T) {
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

	c := &DefaultDialerClient{transportConfig: &Config{}, httpVersion: "2"}
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

func TestH1Drain_MultipleUnread(t *testing.T) {
	r1 := "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
	r2 := "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
	r3 := "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
	sc := &scriptedConn{reads: []string{r1 + r2 + r3}}
	h1 := NewH1Conn(sc)
	h1.RespBufReader = bufio.NewReader(sc)
	h1.UnreadResponsesCount = 2

	pool := newH1ConnPool(defaultH1UploadPoolCap)
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
	got := pool.Get()
	if got == nil {
		t.Fatal("expected pooled conn")
	}
	if got.UnreadResponsesCount != 0 {
		t.Fatalf("after drain 2 + write+read 1, count=%d want 0", got.UnreadResponsesCount)
	}
}

func TestH1ConnPool_BoundedCapClosesExcess(t *testing.T) {
	pool := newH1ConnPool(2)
	c1 := NewH1Conn(&scriptedConn{})
	c2 := NewH1Conn(&scriptedConn{})
	c3 := NewH1Conn(&scriptedConn{})
	pool.Put(c1)
	pool.Put(c2)
	pool.Put(c3) // excess should be closed, not retained
	if !c3.Conn.(*scriptedConn).closed {
		t.Fatal("excess Put must close conn")
	}
	g1 := pool.Get()
	g2 := pool.Get()
	g3 := pool.Get()
	if g1 == nil || g2 == nil {
		t.Fatal("cap-2 pool should yield two conns")
	}
	if g3 != nil {
		t.Fatal("pool must not retain more than cap")
	}
}

func TestIsFatalConnError_StringMatch(t *testing.T) {
	if !isFatalConnError(errors.New("http2: Transport closing idle connection")) {
		t.Fatal("expected fatal")
	}
	if isFatalConnError(errors.New("temporary failure")) {
		t.Fatal("generic error must not be fatal")
	}
}
