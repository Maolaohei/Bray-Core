package freedom

// Pre-dial pool (B14): frequently used TCP targets get their next
// connection dialed ahead of time, so a subsequent request to the same
// destination skips the dial RTT (the largest single TTFB component on
// the server side). The proxy already has "connect anywhere" semantics,
// so this adds no new SSRF capability — it only front-loads connections
// for destinations the proxy has been asked to reach.

import (
	stdnet "net"
	"strconv"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/net"
)

// preconnTTL bounds pooled connections. Servers and firewalls drop idle
// TCP connections well inside 90s (typical Keep-Alive timeouts are
// 30-60s), and handing a dead socket to the next request fails it —
// exactly what the pool was supposed to avoid. 10s keeps the pool fresh
// for the bursty case (request N's pre-dial serves request N+1, which
// typically follows within seconds) while making stale-take rare.
const preconnTTL = 10 * time.Second

// preconnMaxPerTarget caps idle pre-dials per destination (resource
// bound; two is enough to cover a request racing its own pre-dial).
const preconnMaxPerTarget = 2

// preconnDialTimeout bounds the background pre-dial (never block the
// request path beyond the normal dial).
const preconnDialTimeout = 5 * time.Second

type preconnEntry struct {
	conn   stdnet.Conn
	expire time.Time
}

type preconnQueue struct {
	mu    sync.Mutex
	items []preconnEntry
}

var preconnPool sync.Map // destKey string -> *preconnQueue

// preconnSweepInterval bounds how long an expired pre-dial may sit in the
// pool before its socket is actively closed (fd bound): TTL is 10s, a 15s
// sweep keeps worst-case staleness to ~25s, far inside typical server
// keep-alive timeouts.
const preconnSweepInterval = 15 * time.Second

// sweepPreconnPool closes expired pre-dials and drops drained destination
// keys, bounding both file descriptors and pool memory. Runs once per
// interval in the background; cheap (only active keys are scanned).
func sweepPreconnPool() {
	preconnPool.Range(func(key, value any) bool {
		q := value.(*preconnQueue)
		q.mu.Lock()
		now := time.Now()
		keep := q.items[:0]
		for _, it := range q.items {
			if it.expire.After(now) {
				keep = append(keep, it)
			} else {
				it.conn.Close()
			}
		}
		q.items = keep
		empty := len(q.items) == 0
		q.mu.Unlock()
		if empty {
			preconnPool.Delete(key)
		}
		return true
	})
}

func init() {
	go func() {
		ticker := time.NewTicker(preconnSweepInterval)
		defer ticker.Stop()
		for range ticker.C {
			sweepPreconnPool()
		}
	}()
}

func preconnKey(d net.Destination) string {
	return d.Network.String() + "|" + d.Address.String() + "|" + strconv.Itoa(int(d.Port))
}

// fakeIPNets are the FakeDNS synthetic ranges: 198.18.0.0/15 (IPv4) and
// fc00::/18 (IPv6, matching features/dns FakeIPv6Pool). Pooled
// connections keyed by synthetic IPs collide across domains that share
// the same fake address (pool reuse), and a fake IP is not routable —
// pre-dialing or taking one only manufactures wrong-target connections.
var fakeIPNets = func() []*stdnet.IPNet {
	var out []*stdnet.IPNet
	for _, cidr := range []string{"198.18.0.0/15", "fc00::/18"} {
		if _, n, err := stdnet.ParseCIDR(cidr); err == nil {
			out = append(out, n)
		}
	}
	return out
}()

// isFakeIP reports whether the address falls in a FakeDNS range
// (IPv4 198.18.0.0/15 or IPv6 fc00::/18).
func isFakeIP(addr net.Address) bool {
	if !addr.Family().IsIP() {
		return false
	}
	ip := addr.IP()
	for _, n := range fakeIPNets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// preconnTake returns a pooled connection for dest (nil when empty).
// The caller owns the connection and must close it.
func preconnTake(d net.Destination) stdnet.Conn {
	if d.Network != net.Network_TCP || isFakeIP(d.Address) {
		return nil
	}
	v, ok := preconnPool.Load(preconnKey(d))
	if !ok {
		return nil
	}
	q := v.(*preconnQueue)
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	for len(q.items) > 0 {
		it := q.items[len(q.items)-1]
		q.items = q.items[:len(q.items)-1]
		if it.expire.After(now) {
			if len(q.items) == 0 {
				// Last item taken: drop the destination key so the pool
				// cannot grow unboundedly across distinct destinations
				// (no expiry sweep exists). A concurrent offer re-creates
				// it via LoadOrStore — the only cost of the tiny race is
				// losing one speculative pre-dial.
				preconnPool.Delete(preconnKey(d))
			}
			return it.conn
		}
		it.conn.Close()
	}
	preconnPool.Delete(preconnKey(d))
	return nil
}

// preconnOffer stores conn for a future request to dest. Best-effort:
// when the pool is full or the connection is expired it is closed.
func preconnOffer(d net.Destination, conn stdnet.Conn) {
	if d.Network != net.Network_TCP || isFakeIP(d.Address) {
		conn.Close()
		return
	}
	key := preconnKey(d)
	v, _ := preconnPool.LoadOrStore(key, &preconnQueue{})
	q := v.(*preconnQueue)
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	keep := q.items[:0]
	for _, it := range q.items {
		if it.expire.After(now) {
			keep = append(keep, it)
		} else {
			it.conn.Close()
		}
	}
	q.items = keep
	if len(q.items) >= preconnMaxPerTarget {
		conn.Close()
		return
	}
	q.items = append(q.items, preconnEntry{conn: conn, expire: now.Add(preconnTTL)})
}
