package splithttp

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type memConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *memConn) Close() error {
	c.closed.Store(true)
	return nil
}

func TestMultiEndpointOptInAndParse(t *testing.T) {
	if MultiEndpointEnabled(nil) {
		t.Fatal("default off")
	}
	if !MultiEndpointEnabled(map[string]string{"X-Bray-Multi-Endpoint": "on"}) {
		t.Fatal("header on")
	}
	eps := ParseExtraEndpoints(map[string]string{"x-bray-endpoints": "a.example:443, b.example:443;a.example:443"})
	if len(eps) != 2 || eps[0] != "a.example:443" || eps[1] != "b.example:443" {
		t.Fatalf("parse=%v", eps)
	}
}

func TestRaceDialEndpoints_FirstWin(t *testing.T) {
	oldWin := MultiEndpointRaceWindow
	oldTO := MultiEndpointProbeTimeout
	MultiEndpointRaceWindow = 20 * time.Millisecond
	MultiEndpointProbeTimeout = 500 * time.Millisecond
	defer func() {
		MultiEndpointRaceWindow = oldWin
		MultiEndpointProbeTimeout = oldTO
	}()

	dial := func(ctx context.Context, ep string) (net.Conn, error) {
		switch ep {
		case "fast":
			return &memConn{}, nil
		case "slow":
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(200 * time.Millisecond):
				return &memConn{}, nil
			}
		default:
			return nil, errors.New("unknown")
		}
	}
	c, ep, err := RaceDialEndpoints(context.Background(), []string{"slow", "fast"}, dial)
	if err != nil {
		t.Fatal(err)
	}
	if ep != "fast" {
		t.Fatalf("want fast, got %s", ep)
	}
	if c == nil {
		t.Fatal("nil conn")
	}
	_ = c.Close()
}

func TestRaceDialEndpoints_SingleNoRace(t *testing.T) {
	calls := 0
	c, ep, err := RaceDialEndpoints(context.Background(), []string{"only"}, func(ctx context.Context, endpoint string) (net.Conn, error) {
		calls++
		return &memConn{}, nil
	})
	if err != nil || ep != "only" || c == nil || calls != 1 {
		t.Fatalf("single path failed: err=%v ep=%s calls=%d", err, ep, calls)
	}
	_ = c.Close()
}

func TestBuildEndpointList(t *testing.T) {
	got := BuildEndpointList("a:443", []string{"b:443", "a:443", " c:443 "})
	if len(got) != 3 || got[0] != "a:443" || got[1] != "b:443" || got[2] != "c:443" {
		t.Fatalf("%v", got)
	}
}
