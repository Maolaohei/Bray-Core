package dns

import (
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"golang.org/x/net/dns/dnsmessage"
)

func TestAllowServeStale_Semantics(t *testing.T) {
	// grace 30s stored as -30
	c := NewCacheController("test", false, true, 30)
	if c.serveExpiredTTL != -30 {
		t.Fatalf("serveExpiredTTL=%d want -30", c.serveExpiredTTL)
	}

	// fresh remaining TTL positive: allowServeStale not used for hits, but if called with +5:
	// -30 < 5 is true (would allow); production only calls after ttl<=0.
	if !c.allowServeStale(-10) { // expired 10s ago, grace 30
		t.Fatal("within grace should allow")
	}
	if c.allowServeStale(-40) { // expired 40s ago, grace 30
		t.Fatal("outside grace should deny")
	}
	if !c.allowServeStale(-29) {
		t.Fatal("edge within grace")
	}
	// Exactly at grace boundary: -30 < -30 is false
	if c.allowServeStale(-30) {
		t.Fatal("exactly at grace boundary should deny (strict <)")
	}

	// unlimited stale when grace==0
	c0 := NewCacheController("test0", false, true, 0)
	if c0.serveExpiredTTL != 0 {
		t.Fatalf("want 0 got %d", c0.serveExpiredTTL)
	}
	if !c0.allowServeStale(-99999) {
		t.Fatal("unlimited stale must allow any remainingTTL")
	}

	// disabled
	cOff := NewCacheController("off", false, false, 30)
	if cOff.allowServeStale(-1) {
		t.Fatal("serveStale=false must deny")
	}
}

func TestGetIPs_TTLSignForExpired(t *testing.T) {
	rec := &IPRecord{
		IP:     []net.IP{net.IP{1, 2, 3, 4}},
		Expire: time.Now().Add(-10 * time.Second),
		RCode:  dnsmessage.RCodeSuccess,
	}
	ips, ttl, err := rec.getIPs()
	if err != nil || len(ips) != 1 {
		t.Fatalf("ips=%v err=%v", ips, err)
	}
	if ttl >= 0 {
		t.Fatalf("expired record ttl should be negative, got %d", ttl)
	}
	// roughly -10
	if ttl > -5 || ttl < -15 {
		t.Fatalf("ttl=%d expected around -10", ttl)
	}
}

func TestAllowServeStale_MatchesProductionCondition(t *testing.T) {
	// Documented equivalence:
	// old: serveStale && (serveExpiredTTL == 0 || serveExpiredTTL < ttl)
	// new: allowServeStale(ttl)
	c := NewCacheController("eq", false, true, 60)
	for _, ttl := range []int32{10, 0, -1, -30, -60, -61, -100} {
		old := c.serveStale && (c.serveExpiredTTL == 0 || c.serveExpiredTTL < ttl)
		nw := c.allowServeStale(ttl)
		if old != nw {
			t.Fatalf("ttl=%d old=%v new=%v", ttl, old, nw)
		}
	}
}