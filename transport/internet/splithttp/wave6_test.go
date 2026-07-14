package splithttp

import (
	"context"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common"
	fstats "github.com/xtls/xray-core/features/stats"
)

func TestParseStickyTTLDuration(t *testing.T) {
	d, ok := ParseStickyTTLDuration("10m")
	if !ok || d != 10*time.Minute {
		t.Fatalf("10m -> %v %v", d, ok)
	}
	d, ok = ParseStickyTTLDuration("15")
	if !ok || d != 15*time.Minute {
		t.Fatalf("plain minutes -> %v %v", d, ok)
	}
	d, ok = ParseStickyTTLDuration("30s")
	if !ok || d != 30*time.Second {
		t.Fatalf("30s -> %v %v", d, ok)
	}
	if _, ok := ParseStickyTTLDuration(""); ok {
		t.Fatal("empty")
	}
	if _, ok := ParseStickyTTLDuration("nope"); ok {
		t.Fatal("invalid")
	}
	if _, ok := ParseStickyTTLDuration("0"); ok {
		t.Fatal("zero")
	}
	d, ok = ParseStickyTTLDuration("100h")
	if !ok || d != 24*time.Hour {
		t.Fatalf("clamp 24h got %v", d)
	}
}

func TestApplyStickyTTLFromHeaders(t *testing.T) {
	oldM, oldE := StickyModeTTL, StickyEndpointTTL
	defer func() {
		StickyModeTTL, StickyEndpointTTL = oldM, oldE
	}()
	StickyModeTTL = time.Hour
	StickyEndpointTTL = time.Hour

	// Wave-7: ApplyStickyTTLFromHeaders is a process-global no-op.
	ApplyStickyTTLFromHeaders(nil)
	if StickyModeTTL != time.Hour || StickyEndpointTTL != time.Hour {
		t.Fatal("nil headers must no-op")
	}

	ApplyStickyTTLFromHeaders(map[string]string{
		"x-bray-sticky-mode-ttl":     "5m",
		"X-Bray-Sticky-Endpoint-TTL": "2m",
	})
	if StickyModeTTL != time.Hour || StickyEndpointTTL != time.Hour {
		t.Fatalf("globals must stay default; got mode=%v ep=%v", StickyModeTTL, StickyEndpointTTL)
	}

	// invalid still no-op on globals
	ApplyStickyTTLFromHeaders(map[string]string{"x-bray-sticky-mode-ttl": "bad"})
	if StickyModeTTL != time.Hour {
		t.Fatal("invalid must not mutate globals")
	}

	modeTTL, epTTL := StickyTTLFromHeaders(map[string]string{
		"x-bray-sticky-mode-ttl":     "5m",
		"X-Bray-Sticky-Endpoint-TTL": "2m",
	})
	if modeTTL != 5*time.Minute || epTTL != 2*time.Minute {
		t.Fatalf("StickyTTLFromHeaders mode=%v ep=%v", modeTTL, epTTL)
	}
}

func TestComputeBrayV2Rates(t *testing.T) {
	r := ComputeBrayV2Rates(BrayV2MetricsSnapshot{})
	if r.ModeSuccessRate != 0 || r.CascadeWinRate != 0 {
		t.Fatalf("zero denoms: %+v", r)
	}
	r = ComputeBrayV2Rates(BrayV2MetricsSnapshot{
		ModeAttempts:         10,
		ModeSuccesses:        8,
		ModeCascadeWins:      2,
		StickyHits:           4,
		MultiEndpointRaces:   5,
		MultiEndpointAltWins: 1,
		EndpointStickyHits:   3,
	})
	if r.ModeSuccessRate != 0.8 {
		t.Fatalf("mode ok %v", r.ModeSuccessRate)
	}
	if r.CascadeWinRate != 0.25 {
		t.Fatalf("cascade %v", r.CascadeWinRate)
	}
	if r.StickyHitRate != 0.4 {
		t.Fatalf("sticky %v", r.StickyHitRate)
	}
	if r.MultiAltWinRate != 0.2 {
		t.Fatalf("alt %v", r.MultiAltWinRate)
	}
	if r.EndpointStickyHitRate != 0.6 {
		t.Fatalf("ep sticky %v", r.EndpointStickyHitRate)
	}
	if BrayV2RatesReport() == "" {
		t.Fatal("report")
	}
}

func TestBrayV2StatsAutoMirror(t *testing.T) {
	// Keep tests fast and isolated
	oldAuto := BrayV2StatsAutoMirror
	oldIV := BrayV2StatsMirrorInterval
	BrayV2StatsAutoMirror = true
	BrayV2StatsMirrorInterval = 50 * time.Millisecond
	defer func() {
		BindBrayV2StatsManager(nil)
		BrayV2StatsAutoMirror = oldAuto
		BrayV2StatsMirrorInterval = oldIV
	}()

	raw, err := common.CreateObject(context.Background(), &stats.Config{})
	if err != nil {
		t.Fatal(err)
	}
	m := raw.(fstats.Manager)
	recordModeAttempt()
	recordModeSuccess(false)

	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	// Give mirror a tick
	deadline := time.Now().Add(500 * time.Millisecond)
	var c fstats.Counter
	for time.Now().Before(deadline) {
		c = m.GetCounter("bray-v2>>>mode_attempts")
		if c != nil && c.Value() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if c == nil || c.Value() <= 0 {
		t.Fatalf("auto mirror did not publish: c=%v active=%v", c, brayStatsMirrorActiveForTest())
	}

	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	// Close should stop mirror
	time.Sleep(30 * time.Millisecond)
	if brayStatsMirrorActiveForTest() {
		t.Fatal("mirror should stop on Close")
	}
}

func TestPublishSkipsNoopManager(t *testing.T) {
	// must not panic
	PublishBrayV2MetricsToStats(fstats.NoopManager{})
}
