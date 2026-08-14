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

func preconnKey(d net.Destination) string {
	return d.Network.String() + "|" + d.Address.String() + "|" + strconv.Itoa(int(d.Port))
}

// preconnTake returns a pooled connection for dest (nil when empty).
// The caller owns the connection and must close it.
func preconnTake(d net.Destination) stdnet.Conn {
	if d.Network != net.Network_TCP {
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
			return it.conn
		}
		it.conn.Close()
	}
	return nil
}

// preconnOffer stores conn for a future request to dest. Best-effort:
// when the pool is full or the connection is expired it is closed.
func preconnOffer(d net.Destination, conn stdnet.Conn) {
	if d.Network != net.Network_TCP {
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
