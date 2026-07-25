package splithttp_test

import (
	"context"
	"testing"
	"time"

	. "github.com/xtls/xray-core/transport/internet/splithttp"
)

// TestMultiPoolUnlockScanPrefersLowRTT ensures multi-conn selection still
// quality-ranks after the unlock-then-scan change (pool_8+ path).
func TestMultiPoolUnlockScanPrefersLowRTT(t *testing.T) {
	m := NewXmuxManager(&XmuxConfig{
		MaxConnections: &RangeConfig{From: 8, To: 8},
		MaxConcurrency: &RangeConfig{From: 0, To: 0}, // unlimited streams
		CMaxReuseTimes: &RangeConfig{From: 0, To: 0}, // unlimited reuse (leftUsage=-1)
	}, func() XmuxConn {
		return &fakeRoundTripper{}
	})
	defer m.Close()

	seen := make(map[*XmuxClient]struct{})
	var clients []*XmuxClient
	deadline := time.Now().Add(2 * time.Second)
	for len(clients) < 8 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout filling pool, got %d", len(clients))
		}
		c, err := m.GetXmuxClient(context.Background())
		if err != nil || c == nil {
			t.Fatalf("GetXmuxClient: %v", err)
		}
		if _, ok := seen[c]; !ok {
			seen[c] = struct{}{}
			clients = append(clients, c)
		}
	}
	for i, c := range clients {
		if i == 0 {
			c.UpdateRTT(5 * time.Millisecond)
		} else {
			c.UpdateRTT(200 * time.Millisecond)
		}
	}
	time.Sleep(5 * time.Millisecond)

	var low, high int
	for i := 0; i < 400; i++ {
		c, err := m.GetXmuxClient(context.Background())
		if err != nil || c == nil {
			t.Fatalf("GetXmuxClient loop: %v", err)
		}
		if c == clients[0] {
			low++
		} else {
			high++
		}
	}
	t.Logf("lowRTT=%d highRTT=%d", low, high)
	// With unlimited concurrency/reuse and pure score scan, low-RTT should dominate.
	if low < 300 {
		t.Fatalf("expected low-RTT client strongly preferred, low=%d high=%d", low, high)
	}
}
