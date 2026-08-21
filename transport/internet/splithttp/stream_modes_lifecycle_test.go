package splithttp

import (
	"context"
	"io"
	stdnet "net"
	"net/http"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/signal/done"
)

// The hard cap is a deliberate unauthenticated stream-one DoS backstop. This
// test keeps its close behavior explicit while production keeps the 4h default.
func TestStreamOneHardCapClosesActiveConnection(t *testing.T) {
	c := &httpServerConn{Instance: done.New()}
	c.hardCapTimer = time.AfterFunc(25*time.Millisecond, func() { _ = c.Close() })
	defer c.hardCapTimer.Stop()

	c.touch() // activity must not reset the absolute cap
	select {
	case <-c.Wait():
	case <-time.After(time.Second):
		t.Fatal("hard cap did not close an active stream-one connection")
	}
}

func TestStreamUpEOFDoesNotEvictSiblingTransport(t *testing.T) {
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, io.ErrUnexpectedEOF
	})
	c := &DefaultDialerClient{
		transportConfig: &Config{},
		httpVersion:     "2",
		client:          &http.Client{Transport: rt},
	}
	pr, pw := io.Pipe()
	defer pw.Close()
	_, _, _, err := c.OpenStream(context.Background(), mustParseURL(t, "http://example/upload"), "sid", pr, true)
	if err == nil {
		t.Fatal("expected upload EOF")
	}
	if c.IsClosed() {
		t.Fatal("stream-scoped upload EOF evicted the shared H2 transport")
	}
}

func TestStreamUpGOAWAYEvictsBrokenTransport(t *testing.T) {
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
	_, _, _, err := c.OpenStream(context.Background(), mustParseURL(t, "http://example/upload"), "sid", pr, true)
	if err == nil || !c.IsClosed() {
		t.Fatalf("hard upload transport failure: err=%v closed=%v", err, c.IsClosed())
	}
}
