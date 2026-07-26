package splithttp_test

import (
	"context"
	"testing"
	"time"

	. "github.com/xtls/xray-core/transport/internet/splithttp"
)

// TestMultiPoolUnlockScanPrefersLowRTT ensures multi-conn selection still
// quality-ranks after the unlock-then-scan change (pool_8+ path).
//
// Pool is force-filled (GetXmuxClient alone reuses the best client once the
// steady target is met, so unique-client collection cannot fill 8 slots).
func TestMultiPoolUnlockScanPrefersLowRTT(t *testing.T) {
	m := NewXmuxManager(&XmuxConfig{
		MaxConnections: &RangeConfig{From: 8, To: 8},
		MaxConcurrency: &RangeConfig{From: 0, To: 0}, // unlimited streams
		CMaxReuseTimes: &RangeConfig{From: 0, To: 0}, // unlimited reuse (leftUsage=-1)
	}, func() XmuxConn {
		return &fakeRoundTripper{}
	})
	defer m.Close()

	// preConnect may already add 1-2; force exact steady size then snapshot.
	// Trim/overfill is fine for ranking: we only need >=2 distinct clients.
	for {
		clients := m.PoolClientsForTest()
		if len(clients) >= 8 {
			break
		}
		m.ForceAddClientsForTest(8 - len(clients))
	}
	clients := m.PoolClientsForTest()
	if len(clients) < 8 {
		t.Fatalf("pool size=%d want >=8", len(clients))
	}
	// Use first 8 for RTT assignment.
	clients = clients[:8]
	low := clients[0]
	for i, c := range clients {
		if i == 0 {
			c.UpdateRTT(5 * time.Millisecond)
		} else {
			c.UpdateRTT(200 * time.Millisecond)
		}
	}
	time.Sleep(5 * time.Millisecond)

	var lowN, highN int
	for i := 0; i < 400; i++ {
		c, err := m.GetXmuxClient(context.Background())
		if err != nil || c == nil {
			t.Fatalf("GetXmuxClient loop: %v", err)
		}
		if c == low {
			lowN++
		} else {
			highN++
		}
	}
	t.Logf("lowRTT=%d highRTT=%d pool=%d", lowN, highN, len(clients))
	// With unlimited concurrency/reuse and pure score scan, low-RTT should dominate.
	if lowN < 300 {
		t.Fatalf("expected low-RTT client strongly preferred, low=%d high=%d", lowN, highN)
	}
}
