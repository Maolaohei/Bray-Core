package freedom

import (
	stdnet "net"
	"testing"

	"github.com/xtls/xray-core/common/net"
)

func TestIsFakeIP(t *testing.T) {
	cases := []struct {
		ip   string
		want bool
	}{
		{"198.18.0.1", true},
		{"198.18.255.255", true},
		{"198.19.100.50", true},
		{"198.17.255.255", false}, // just outside the /15
		{"198.20.0.1", false},
		{"172.217.116.4", false},
		{"8.8.8.8", false},
	}
	for _, c := range cases {
		addr := net.ParseAddress(c.ip)
		if got := isFakeIP(addr); got != c.want {
			t.Errorf("isFakeIP(%s) = %v, want %v", c.ip, got, c.want)
		}
	}
	// Domain addresses are never fake IPs.
	if isFakeIP(net.DomainAddress("example.com")) {
		t.Error("domain must not be treated as fake IP")
	}
}

// TestPreconnPoolSkipsFakeIP verifies the pool never stores or returns
// connections for FakeDNS synthetic addresses (198.18.0.0/15): those
// keys collide across domains and the addresses are not routable.
func TestPreconnPoolSkipsFakeIP(t *testing.T) {
	l, err := stdnet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()

	fakeDest := net.TCPDestination(net.ParseAddress("198.18.1.1"), 443)
	realDest := net.TCPDestination(net.ParseAddress("127.0.0.1"), net.Port(l.Addr().(*stdnet.TCPAddr).Port))

	conn, err := stdnet.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	preconnOffer(fakeDest, conn) // must close, not store
	if v, ok := preconnPool.Load(preconnKey(fakeDest)); ok {
		q := v.(*preconnQueue)
		q.mu.Lock()
		n := len(q.items)
		q.mu.Unlock()
		if n != 0 {
			t.Fatalf("fake IP destination stored %d pooled connections", n)
		}
	}

	conn2, err := stdnet.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	preconnOffer(realDest, conn2)
	if got := preconnTake(fakeDest); got != nil {
		got.Close()
		t.Fatal("preconnTake returned a connection for a fake IP")
	}
	if got := preconnTake(realDest); got == nil {
		t.Fatal("preconnTake missed the real destination")
	} else {
		got.Close()
	}
}
