package splithttp

import (
	"context"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common"
	fstats "github.com/xtls/xray-core/features/stats"
)

func TestApplyStickyEndpoints(t *testing.T) {
	list := []string{"a:443", "b:443", "c:443"}
	got := ApplyStickyEndpoints(list, "b:443")
	if len(got) != 3 || got[0] != "b:443" || got[1] != "a:443" || got[2] != "c:443" {
		t.Fatalf("reorder %v", got)
	}
	got = ApplyStickyEndpoints(list, "a:443")
	if len(got) != 3 || got[0] != "a:443" {
		t.Fatalf("already first %v", got)
	}
	got = ApplyStickyEndpoints(list, "missing:443")
	if len(got) != 3 || got[0] != "a:443" {
		t.Fatalf("unknown sticky ignored %v", got)
	}
	got = ApplyStickyEndpoints([]string{"only:443"}, "only:443")
	if len(got) != 1 {
		t.Fatalf("single %v", got)
	}
}

func TestStickyEndpointRememberLookupTTL(t *testing.T) {
	ClearStickyEndpointForTest()
	old := StickyEndpointTTL
	StickyEndpointTTL = time.Hour
	defer func() { StickyEndpointTTL = old }()

	key := stickyEndpointKey("1.2.3.4:443", "cdn.example.com")
	if _, ok := LookupStickyEndpoint(key); ok {
		t.Fatal("empty")
	}
	RememberStickyEndpoint(key, "5.6.7.8:443")
	ep, ok := LookupStickyEndpoint(key)
	if !ok || ep != "5.6.7.8:443" {
		t.Fatalf("lookup %v %v", ep, ok)
	}

	StickyEndpointTTL = time.Millisecond
	time.Sleep(5 * time.Millisecond)
	if _, ok := LookupStickyEndpoint(key); ok {
		t.Fatal("expired sticky should miss")
	}
}

func TestStickyEndpointOptOut(t *testing.T) {
	if !StickyEndpointEnabled(nil) {
		t.Fatal("default on")
	}
	if StickyEndpointEnabled(map[string]string{"x-bray-sticky-endpoint": "off"}) {
		t.Fatal("opt-out")
	}
	if !StickyEndpointEnabled(map[string]string{"X-Bray-Sticky-Endpoint": "yes"}) {
		t.Fatal("explicit on")
	}
}

func TestBrayV2EndpointStickyMetrics(t *testing.T) {
	before := GetBrayV2Metrics()
	recordEndpointStickyHit()
	recordEndpointStickyRemember()
	after := GetBrayV2Metrics()
	if after.EndpointStickyHits < before.EndpointStickyHits+1 {
		t.Fatal("hit")
	}
	if after.EndpointStickyRemembers < before.EndpointStickyRemembers+1 {
		t.Fatal("remember")
	}
	if BrayV2MetricsReport() == "" {
		t.Fatal("report")
	}
}

func TestPublishBrayV2MetricsToStats(t *testing.T) {
	// Noop manager must not panic
	PublishBrayV2MetricsToStats(nil)
	PublishBrayV2MetricsToStats(fstats.NoopManager{})

	raw, err := common.CreateObject(context.Background(), &stats.Config{})
	if err != nil {
		t.Fatal(err)
	}
	m := raw.(fstats.Manager)
	recordModeAttempt()
	recordEndpointStickyHit()
	PublishBrayV2MetricsToStats(m)

	names := BrayV2StatsCounterNames()
	if len(names) < 11 {
		t.Fatalf("names %d", len(names))
	}
	c := m.GetCounter("bray-v2>>>mode_attempts")
	if c == nil || c.Value() <= 0 {
		t.Fatalf("mode_attempts counter missing or zero: %v", c)
	}
	c2 := m.GetCounter("bray-v2>>>endpoint_sticky_hits")
	if c2 == nil || c2.Value() <= 0 {
		t.Fatalf("endpoint sticky counter missing: %v", c2)
	}

	BindBrayV2StatsManager(m)
	PublishBoundBrayV2Metrics()
	BindBrayV2StatsManager(nil)
}
