package conf

import (
	"testing"

	"github.com/xtls/xray-core/transport/internet/splithttp"
)

// TestXmuxZeroRangeBecomesNil pins the "0 = no value" mapping: an unset
// XMUX range must become a nil pointer so the runtime falls back to the
// process-stable jittered defaults (2-4 conns etc.) instead of a hard 6/6
// (which was synced from upstream 18b85adb in 41de9a38 and removed
// 2026-08-14 because it overrode the fork's anti-fleet jitter design).
func TestXmuxZeroRangeBecomesNil(t *testing.T) {
	c := &SplitHTTPConfig{}
	config, err := c.Build()
	if err != nil {
		t.Fatal(err)
	}
	sc := config.(*splithttp.Config)
	if sc.Xmux == nil {
		t.Fatal("expected non-nil Xmux config shell")
	}
	if sc.Xmux.MaxConnections != nil {
		t.Fatalf("unset MaxConnections must map to nil, got %+v", sc.Xmux.MaxConnections)
	}
	if sc.Xmux.MaxConcurrency != nil {
		t.Fatalf("unset MaxConcurrency must map to nil, got %+v", sc.Xmux.MaxConcurrency)
	}
	if sc.Xmux.HMaxRequestTimes != nil {
		t.Fatalf("unset HMaxRequestTimes must map to nil, got %+v", sc.Xmux.HMaxRequestTimes)
	}
	// Jittered default must actually be reachable through the runtime getter.
	if got := sc.Xmux.GetNormalizedMaxConnections(); got == nil || got.To == 0 {
		t.Fatalf("GetNormalizedMaxConnections must fall back to jittered default, got %+v", got)
	}
}

// TestXmuxExplicitRangeWins: an explicitly configured range passes through
// and wins over the jittered defaults.
func TestXmuxExplicitRangeWins(t *testing.T) {
	c := &SplitHTTPConfig{}
	c.Xmux.MaxConnections.From = 3
	c.Xmux.MaxConnections.To = 3
	config, err := c.Build()
	if err != nil {
		t.Fatal(err)
	}
	sc := config.(*splithttp.Config)
	if sc.Xmux.MaxConnections == nil || sc.Xmux.MaxConnections.From != 3 || sc.Xmux.MaxConnections.To != 3 {
		t.Fatalf("explicit range must win, got %+v", sc.Xmux.MaxConnections)
	}
}

// TestXmuxExplicitRangeClamped: out-of-band explicit values are clamped
// into the same band as the jittered defaults so custom config cannot
// create an out-of-band connection-count fingerprint.
func TestXmuxExplicitRangeClamped(t *testing.T) {
	c := &SplitHTTPConfig{}
	c.Xmux.MaxConnections.From = 100
	c.Xmux.MaxConnections.To = 200
	config, err := c.Build()
	if err != nil {
		t.Fatal(err)
	}
	sc := config.(*splithttp.Config)
	got := sc.Xmux.MaxConnections
	if got == nil || got.From > splithttp.XmuxClampConnectionsToMax || got.To > splithttp.XmuxClampConnectionsToMax {
		t.Fatalf("explicit range must be clamped into band, got %+v", got)
	}
	// In-band explicit values stay untouched.
	c2 := &SplitHTTPConfig{}
	c2.Xmux.MaxConnections.From = 4
	c2.Xmux.MaxConnections.To = 4
	config2, err := c2.Build()
	if err != nil {
		t.Fatal(err)
	}
	sc2 := config2.(*splithttp.Config)
	if sc2.Xmux.MaxConnections.From != 4 || sc2.Xmux.MaxConnections.To != 4 {
		t.Fatalf("in-band explicit range must pass through, got %+v", sc2.Xmux.MaxConnections)
	}
}
