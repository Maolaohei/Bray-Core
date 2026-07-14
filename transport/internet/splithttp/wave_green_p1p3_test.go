package splithttp

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestParseExtraEndpoints_Cap(t *testing.T) {
	// MaxMultiEndpoints=4 => at most 3 extras (primary fills the 4th slot).
	eps := ParseExtraEndpoints(map[string]string{
		"x-bray-endpoints": "a:443,b:443,c:443,d:443,e:443",
	})
	if len(eps) != MaxMultiEndpoints-1 {
		t.Fatalf("extras len=%d want %d got=%v", len(eps), MaxMultiEndpoints-1, eps)
	}
}

func TestBuildEndpointList_Cap(t *testing.T) {
	got := BuildEndpointList("p:443", []string{"a:443", "b:443", "c:443", "d:443", "e:443"})
	if len(got) > MaxMultiEndpoints {
		t.Fatalf("list len=%d > MaxMultiEndpoints=%d: %v", len(got), MaxMultiEndpoints, got)
	}
	if got[0] != "p:443" {
		t.Fatalf("primary not first: %v", got)
	}
	// primary + 3 extras
	if len(got) != MaxMultiEndpoints {
		t.Fatalf("want %d endpoints, got %d %v", MaxMultiEndpoints, len(got), got)
	}
}

func TestNoteStickyEndpointFailure_Invalidates(t *testing.T) {
	ClearStickyEndpointForTest()
	old := StickyEndpointTTL
	StickyEndpointTTL = time.Hour
	defer func() { StickyEndpointTTL = old }()

	key := stickyEndpointKey("1.2.3.4:443", "cdn.example.com")
	RememberStickyEndpoint(key, "5.6.7.8:443")
	if _, ok := LookupStickyEndpoint(key); !ok {
		t.Fatal("expected sticky after remember")
	}
	// Different endpoint failure must not clear.
	NoteStickyEndpointFailure(key, "9.9.9.9:443")
	if _, ok := LookupStickyEndpoint(key); !ok {
		t.Fatal("cleared on non-sticky endpoint")
	}
	// Sticky endpoint failure clears (failHits threshold = 1).
	NoteStickyEndpointFailure(key, "5.6.7.8:443")
	if _, ok := LookupStickyEndpoint(key); ok {
		t.Fatal("expected sticky cleared after sticky-EP failure")
	}
}

func TestRaceDialEndpoints_NilConn(t *testing.T) {
	_, _, err := RaceDialEndpoints(context.Background(), []string{"only"}, func(ctx context.Context, endpoint string) (net.Conn, error) {
		return nil, nil
	})
	if !errors.Is(err, ErrNilMultiEndpointConn) {
		t.Fatalf("want ErrNilMultiEndpointConn, got %v", err)
	}

	// Multi-path: all dials return nil conn => error, not success.
	_, _, err = RaceDialEndpoints(context.Background(), []string{"a", "b"}, func(ctx context.Context, endpoint string) (net.Conn, error) {
		return nil, nil
	})
	if err == nil {
		t.Fatal("expected error when all dials return nil conn")
	}
	if !errors.Is(err, ErrNilMultiEndpointConn) && !strings.Contains(err.Error(), "nil conn") {
		// firstErr should be ErrNilMultiEndpointConn
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestIsFatalOpenTransportError_Typed(t *testing.T) {
	if !isFatalOpenTransportError(io.EOF) {
		t.Fatal("io.EOF should be fatal")
	}
	if !isFatalOpenTransportError(net.ErrClosed) {
		t.Fatal("net.ErrClosed should be fatal")
	}
	op := &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}
	if !isFatalOpenTransportError(op) {
		t.Fatal("net.OpError should be fatal")
	}
	// Non-transport status-style errors remain non-fatal (string path).
	if isFatalOpenTransportError(errors.New("unexpected status 403")) {
		t.Fatal("403 should not be fatal")
	}
	// Still match string needles for http2 pool faults.
	if !isFatalOpenTransportError(errors.New("http2: client conn not usable")) {
		t.Fatal("http2 unusable should be fatal")
	}
}

func TestWaitCascadeStepJitter_JoinPreservesOpenErr(t *testing.T) {
	// Unit-level: stderrors.Join behavior used by dialer cascade path.
	openErr := errors.New("open stream failed: status 502")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	werr := WaitCascadeStepJitter(ctx)
	if werr == nil {
		t.Fatal("expected cancel err from WaitCascadeStepJitter")
	}
	joined := errors.Join(openErr, werr)
	if !errors.Is(joined, openErr) {
		t.Fatalf("Join lost openErr: %v", joined)
	}
	if !errors.Is(joined, context.Canceled) && !errors.Is(joined, werr) {
		t.Fatalf("Join lost cancel: %v", joined)
	}
}
