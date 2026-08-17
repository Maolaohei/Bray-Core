package freedom

// POC for suspected MED-HIGH: preconnPool sync.Map keeps a queue entry
// per distinct destination forever — even after the queue drains — so
// an operator's proxy being asked to reach many distinct targets grows
// the map unboundedly (slow memory exhaustion, client-reachable DoS).

import (
	stdnet "net"
	"testing"

	"github.com/xtls/xray-core/common/net"
)

func preconnPoolKeyCount() int {
	n := 0
	preconnPool.Range(func(_, _ any) bool { n++; return true })
	return n
}

// TestPreconnPoolKeyRemovedAfterDrain is the POC assertion: after a
// take drains the queue, the destination key must be removed from the
// pool (with no expiry sweep, a permanent per-target entry means the
// map grows without bound across distinct destinations).
func TestPreconnPoolKeyRemovedAfterDrain(t *testing.T) {
	start := preconnPoolKeyCount()

	freshConn := func() stdnet.Conn {
		_, c := stdnet.Pipe()
		return c
	}

	d := net.TCPDestination(net.IPAddress([]byte{203, 0, 113, 1}), 443)
	preconnOffer(d, freshConn())
	if n := preconnPoolKeyCount() - start; n != 1 {
		t.Fatalf("expected 1 new pool key after offer, got %d", n)
	}

	got := preconnTake(d)
	if got == nil {
		t.Fatal("take failed")
	}
	_ = got.Close()

	if n := preconnPoolKeyCount() - start; n != 0 {
		t.Fatalf("BUG: pool key leaked after queue drained (existing=%d). "+
			"Distinct destinations grow the map unboundedly", n)
	}
}
