package splithttp

import (
	"context"
	"io"
	stdnet "net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOpenStream_DownloadWaitsForHeadersAndReturnsBody(t *testing.T) {
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("payload")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "2",
		client:          &http.Client{Transport: rt},
	}
	rc, _, _, err := c.OpenStream(context.Background(), "http://example/d", "sid", nil, false)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(b) != "payload" {
		t.Fatalf("body=%q", string(b))
	}
}

func TestOpenStream_Non200ReturnsError(t *testing.T) {
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 404,
			Body:       io.NopCloser(strings.NewReader("nope")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "2",
		client:          &http.Client{Transport: rt},
	}
	rc, _, _, err := c.OpenStream(context.Background(), "http://example/d", "sid", nil, false)
	if err == nil {
		if rc != nil {
			rc.Close()
		}
		t.Fatal("expected non-200 error")
	}
	if c.IsClosed() {
		t.Fatal("non-200 must not mark connection fatal by itself")
	}
}

func TestOpenStream_DoErrorSurfacesAndFatalMarks(t *testing.T) {
	// Hard connection loss (not bare EOF) must mark the dialer closed for eviction.
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, stdnet.ErrClosed
	})
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "2",
		client:          &http.Client{Transport: rt},
	}
	_, _, _, err := c.OpenStream(context.Background(), "http://example/d", "sid", nil, false)
	if err == nil {
		t.Fatal("expected error")
	}
	if !c.IsClosed() {
		t.Fatal("hard conn death must mark dialer closed")
	}
}

func TestOpenStream_BareEOFDoesNotMarkClosed(t *testing.T) {
	// Multiplexed H2: one stream EOF must not force-close sibling streams.
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, io.EOF
	})
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "2",
		client:          &http.Client{Transport: rt},
	}
	_, _, _, err := c.OpenStream(context.Background(), "http://example/d", "sid", nil, false)
	if err == nil {
		t.Fatal("expected error")
	}
	if c.IsClosed() {
		t.Fatal("bare EOF must not mark dialer closed")
	}
}

func TestOpenStream_UploadOnlySuccessKeepsDialerOpen(t *testing.T) {
	bodySeen := make(chan struct{})
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		// stream-up: 200 with empty body; request body may still be open.
		go func() {
			if req.Body != nil {
				_, _ = io.Copy(io.Discard, req.Body)
			}
			close(bodySeen)
		}()
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "2",
		client:          &http.Client{Transport: rt},
	}
	pr, pw := io.Pipe()
	rc, _, _, err := c.OpenStream(context.Background(), "http://example/u", "sid", pr, true)
	if err != nil {
		t.Fatalf("OpenStream uploadOnly: %v", err)
	}
	if rc != nil {
		t.Fatal("uploadOnly should not return a response body reader")
	}
	_, _ = pw.Write([]byte("up"))
	_ = pw.Close()
	select {
	case <-bodySeen:
	case <-time.After(2 * time.Second):
		t.Fatal("request body was not consumed")
	}
	if c.IsClosed() {
		t.Fatal("successful uploadOnly must not close dialer")
	}
}

func TestOpenStream_UploadOnlyFatalMarksClosed(t *testing.T) {
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, stdnet.ErrClosed
	})
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "2",
		client:          &http.Client{Transport: rt},
	}
	pr, pw := io.Pipe()
	defer pw.Close()
	_, _, _, err := c.OpenStream(context.Background(), "http://example/u", "sid", pr, true)
	if err == nil {
		t.Fatal("expected fatal error")
	}
	if !c.IsClosed() {
		t.Fatal("uploadOnly hard fault must mark dialer closed")
	}
}

func TestOpenStream_RecordsTTFBOnSuccess(t *testing.T) {
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		time.Sleep(15 * time.Millisecond)
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("payload")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})
	var samples atomic.Int32
	var lastTTFB atomic.Int64
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "2",
		client:          &http.Client{Transport: rt},
	}
	c.SetOnTTFB(func(d time.Duration) {
		samples.Add(1)
		lastTTFB.Store(int64(d))
	})
	rc, _, _, err := c.OpenStream(context.Background(), "http://example/d", "sid", nil, false)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	defer rc.Close()
	if samples.Load() != 1 {
		t.Fatalf("TTFB samples=%d want 1", samples.Load())
	}
	if lastTTFB.Load() < int64(10*time.Millisecond) {
		t.Fatalf("TTFB too small: %v", time.Duration(lastTTFB.Load()))
	}
}

func TestOpenStream_DoesNotRecordTTFBOnNon200(t *testing.T) {
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 503,
			Body:       io.NopCloser(strings.NewReader("nope")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})
	var samples atomic.Int32
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "2",
		client:          &http.Client{Transport: rt},
	}
	c.SetOnTTFB(func(d time.Duration) {
		samples.Add(1)
	})
	_, _, _, err := c.OpenStream(context.Background(), "http://example/d", "sid", nil, false)
	if err == nil {
		t.Fatal("expected non-200 error")
	}
	if samples.Load() != 0 {
		t.Fatalf("TTFB must not record failures, samples=%d", samples.Load())
	}
}

func TestOpenStream_ParentCancelAbortsInFlightDo(t *testing.T) {
	// Blocking RoundTripper: returns only when req.Context is canceled.
	entered := make(chan struct{})
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		close(entered)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "2",
		client:          &http.Client{Transport: rt},
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, _, _, err := c.OpenStream(ctx, "http://example/d", "sid", nil, false)
		errCh <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("RoundTrip did not start")
	}
	cancel()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected cancel error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OpenStream did not return after cancel")
	}
	if c.IsClosed() {
		t.Fatal("cancel must not mark dialer fatal")
	}
}

func TestOpenStream_SuccessKeepsStreamAfterDialContextDone(t *testing.T) {
	// Dial ctx ends after headers; stream-one/download must stay open.
	// We simulate by canceling parent after OpenStream returns.
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("alive")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "2",
		client:          &http.Client{Transport: rt},
	}
	ctx, cancel := context.WithCancel(context.Background())
	rc, _, _, err := c.OpenStream(ctx, "http://example/d", "sid", nil, false)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	cancel() // Dial would return and cancel parent here
	b, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ReadAll after dial cancel: %v", err)
	}
	if string(b) != "alive" {
		t.Fatalf("body=%q", string(b))
	}
}

func TestTrackConn_CloseForceClosesLive(t *testing.T) {
	c := &DefaultDialerClient{transportConfig: &Config{}, httpVersion: "2"}
	fc := &fakeNetConn{}
	wrapped := c.trackConn(fc)
	if wrapped == nil {
		t.Fatal("trackConn returned nil")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if fc.closed.Load() == 0 {
		t.Fatal("tracked conn must be force-closed on Close")
	}
	// Untrack path: second Close is safe.
	_ = wrapped.Close()
}

type fakeNetConn struct {
	closed atomic.Int32
}

func (f *fakeNetConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (f *fakeNetConn) Write(b []byte) (int, error)      { return len(b), nil }
func (f *fakeNetConn) Close() error                     { f.closed.Add(1); return nil }
func (f *fakeNetConn) LocalAddr() stdnet.Addr           { return &stdnet.TCPAddr{} }
func (f *fakeNetConn) RemoteAddr() stdnet.Addr          { return &stdnet.TCPAddr{} }
func (f *fakeNetConn) SetDeadline(time.Time) error      { return nil }
func (f *fakeNetConn) SetReadDeadline(time.Time) error  { return nil }
func (f *fakeNetConn) SetWriteDeadline(time.Time) error { return nil }
