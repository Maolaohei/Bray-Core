package splithttp

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
)

type scriptedConn struct {
	net.Conn
	mu     sync.Mutex
	reads  []string
	writes [][]byte
	ridx   int
	closed bool
}

func (c *scriptedConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	if c.ridx >= len(c.reads) {
		c.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		c.mu.Lock()
		if c.ridx >= len(c.reads) {
			if c.closed {
				return 0, net.ErrClosed
			}
			return 0, io.EOF
		}
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	cp := make([]byte, len(p))
	copy(cp, p)
	c.writes = append(c.writes, cp)
	return len(p), nil
}

func (c *scriptedConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *scriptedConn) LocalAddr() net.Addr  { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1} }
func (c *scriptedConn) RemoteAddr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2} }
func (c *scriptedConn) SetDeadline(t time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(t time.Time) error { return nil }

// TestH1PostPacket_HotReuse serial posts share one dialed hot conn (not idle pool hop).
func TestH1PostPacket_HotReuse(t *testing.T) {
	respOK := "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
	pool := newH1ConnPool(defaultH1UploadPoolCap)

	var dialed int
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "1.1",
		uploadRawPool:   pool,
		dialUploadConn: func(ctx context.Context) (net.Conn, error) {
			dialed++
			return &scriptedConn{reads: []string{respOK, respOK, respOK}}, nil
		},
	}

	for i, body := range []string{"a", "b", "c"} {
		mb := buf.MultiBuffer{buf.FromBytes([]byte(body))}
		if err := c.PostPacket(context.Background(), "http://example/upload", "sid", "1", mb); err != nil {
			t.Fatalf("PostPacket #%d: %v", i+1, err)
		}
	}
	if dialed != 1 {
		t.Fatalf("dialed=%d want 1 (hot reuse)", dialed)
	}
	c.hotH1Mu.Lock()
	hot := c.hotH1
	c.hotH1Mu.Unlock()
	if hot == nil {
		t.Fatal("expected healthy hotH1 after serial posts")
	}
	if hot.UnreadResponsesCount != 0 {
		t.Fatalf("UnreadResponsesCount=%d want 0 (each post waits for its 200)", hot.UnreadResponsesCount)
	}
	// Idle pool may be empty while hot holds the only conn — that is intentional.
	if pool.Get() != nil {
		t.Fatal("did not expect spare idle conn while single hot is retained")
	}
}

// TestH1PostPacket_PipelineConcurrentDepth2: two concurrent posts share one dial
// and both observe 200. Server withholds responses until both requests are fully
// read so write-side pipeline depth is forced.
func TestH1PostPacket_PipelineConcurrentDepth2(t *testing.T) {
	if h1UploadMaxInflight < 2 {
		t.Skip("pipeline depth disabled")
	}

	clientEnd, serverEnd := net.Pipe()
	var reqSeen atomic.Int32
	var dialed atomic.Int32
	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)
		defer serverEnd.Close()
		br := bufio.NewReader(serverEnd)
		// Read two full HTTP requests before answering either — proves pipelining.
		for i := 0; i < 2; i++ {
			req, err := http.ReadRequest(br)
			if err != nil {
				t.Errorf("server ReadRequest #%d: %v", i+1, err)
				return
			}
			if req.Body != nil {
				_, _ = io.Copy(io.Discard, req.Body)
				req.Body.Close()
			}
			reqSeen.Add(1)
		}
		// Respond twice (order preserved).
		for i := 0; i < 2; i++ {
			_, _ = io.WriteString(serverEnd, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
		}
	}()

	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "1.1",
		uploadRawPool:   newH1ConnPool(defaultH1UploadPoolCap),
		dialUploadConn: func(ctx context.Context) (net.Conn, error) {
			dialed.Add(1)
			return clientEnd, nil
		},
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			mb := buf.MultiBuffer{buf.FromBytes([]byte{byte('a' + n)})}
			errs <- c.PostPacket(context.Background(), "http://example/u", "s", "1", mb)
		}(i)
	}

	// Bound hang: if pipeline is broken (serial wait-for-200 before next write),
	// server never sees 2 requests and this times out.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		_ = clientEnd.Close()
		_ = serverEnd.Close()
		t.Fatalf("timed out; reqSeen=%d dialed=%d (pipeline likely serializing on response)", reqSeen.Load(), dialed.Load())
	}
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("PostPacket: %v", err)
		}
	}
	<-serverDone
	if dialed.Load() != 1 {
		t.Fatalf("dialed=%d want 1", dialed.Load())
	}
	if reqSeen.Load() != 2 {
		t.Fatalf("reqSeen=%d want 2", reqSeen.Load())
	}
}

// TestH1PostPacket_PipelineConcurrentDepth3: three concurrent posts share one
// dial; server withholds all responses until all three requests are fully read.
func TestH1PostPacket_PipelineConcurrentDepth3(t *testing.T) {
	if h1UploadMaxInflight < 3 {
		t.Skip("pipeline depth < 3")
	}

	clientEnd, serverEnd := net.Pipe()
	var reqSeen atomic.Int32
	var dialed atomic.Int32
	serverDone := make(chan struct{})
	const n = 3

	go func() {
		defer close(serverDone)
		defer serverEnd.Close()
		br := bufio.NewReader(serverEnd)
		for i := 0; i < n; i++ {
			req, err := http.ReadRequest(br)
			if err != nil {
				t.Errorf("server ReadRequest #%d: %v", i+1, err)
				return
			}
			if req.Body != nil {
				_, _ = io.Copy(io.Discard, req.Body)
				req.Body.Close()
			}
			reqSeen.Add(1)
		}
		for i := 0; i < n; i++ {
			_, _ = io.WriteString(serverEnd, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
		}
	}()

	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "1.1",
		uploadRawPool:   newH1ConnPool(defaultH1UploadPoolCap),
		dialUploadConn: func(ctx context.Context) (net.Conn, error) {
			dialed.Add(1)
			return clientEnd, nil
		},
	}

	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			mb := buf.MultiBuffer{buf.FromBytes([]byte{byte('a' + idx)})}
			errs <- c.PostPacket(context.Background(), "http://example/u", "s", "1", mb)
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		_ = clientEnd.Close()
		_ = serverEnd.Close()
		t.Fatalf("timed out; reqSeen=%d dialed=%d", reqSeen.Load(), dialed.Load())
	}
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("PostPacket: %v", err)
		}
	}
	<-serverDone
	if dialed.Load() != 1 {
		t.Fatalf("dialed=%d want 1", dialed.Load())
	}
	if reqSeen.Load() != int32(n) {
		t.Fatalf("reqSeen=%d want %d", reqSeen.Load(), n)
	}
}

func TestH1PostPacket_FailedPooledWriteRetriesFreshDial(t *testing.T) {
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
	// Healthy conn stays as hotH1, not necessarily returned to idle pool.
	c.hotH1Mu.Lock()
	hot := c.hotH1
	c.hotH1Mu.Unlock()
	if hot == nil {
		t.Fatal("expected healthy hotH1 after retry dial")
	}
	if hot.isDead() {
		t.Fatal("hotH1 should be healthy")
	}
}

func TestOpenStream_ContextCancelDoesNotMarkClosed(t *testing.T) {
	if isFatalConnError(context.Canceled) {
		t.Fatal("context.Canceled must not be fatal")
	}
	if isFatalConnError(context.DeadlineExceeded) {
		t.Fatal("DeadlineExceeded must not be fatal")
	}
	// Bare EOF is stream-scoped on H2 multiplex; must NOT force-close the pooled socket.
	if isFatalConnError(io.EOF) {
		t.Fatal("bare io.EOF must not be fatal (would mid-kill sibling streams)")
	}
	if isFatalConnError(io.ErrUnexpectedEOF) {
		t.Fatal("UnexpectedEOF must not be fatal")
	}
	if !isFatalConnError(syscall.ECONNRESET) {
		t.Fatal("ECONNRESET should be fatal")
	}
	if !isFatalConnError(net.ErrClosed) {
		t.Fatal("net.ErrClosed should be fatal")
	}
	if !isFatalConnError(errors.New("http2: client connection lost")) {
		t.Fatal("client connection lost should be fatal")
	}
	if isFatalConnError(errors.New("http2: stream closed")) {
		t.Fatal("stream-scoped error must not be fatal")
	}

	c := &DefaultDialerClient{transportConfig: &Config{}, httpVersion: "2"}
	c.markFatal(context.Canceled)
	if c.IsClosed() {
		t.Fatal("non-fatal error must not mark closed")
	}
	c.markFatal(io.EOF)
	if c.IsClosed() {
		t.Fatal("bare EOF must not mark closed")
	}
	c.markFatal(syscall.ECONNRESET)
	if !c.IsClosed() {
		t.Fatal("hard conn death must mark closed")
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
		return nil, net.ErrClosed
	})
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "2",
		client:          &http.Client{Transport: rt},
	}
	_ = c.PostPacket(context.Background(), "http://example/u", "s", "1", nil)
	if !c.IsClosed() {
		t.Fatal("hard conn death must close dialer")
	}
}

func TestPostPacket_H2_BareEOFDoesNotClose(t *testing.T) {
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, io.EOF
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
		t.Fatal("bare EOF must not close dialer")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

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

func TestWaitForReady_FastPathDoesNotBlockAfterProbe(t *testing.T) {
	c := &XmuxClient{ready: make(chan struct{})}
	close(c.ready)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := c.WaitForReady(ctx); err != nil {
		t.Fatalf("WaitForReady after close: %v", err)
	}
}

func TestDefaultDialerClient_CloseClearsHotH1(t *testing.T) {
	respOK := "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
	sc := &scriptedConn{reads: []string{respOK}}
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "1.1",
		uploadRawPool:   newH1ConnPool(defaultH1UploadPoolCap),
		dialUploadConn: func(ctx context.Context) (net.Conn, error) {
			return sc, nil
		},
	}
	if err := c.PostPacket(context.Background(), "http://example/u", "s", "1", buf.MultiBuffer{buf.FromBytes([]byte("z"))}); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c.hotH1Mu.Lock()
	hot := c.hotH1
	c.hotH1Mu.Unlock()
	if hot != nil {
		t.Fatal("Close must clear hotH1")
	}
	if !sc.closed {
		t.Fatal("Close must close hot conn")
	}
}
