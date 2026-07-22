package reality_test

import (
	"testing"
	"time"

	"github.com/xtls/xray-core/transport/internet/reality"
)

func TestGetREALITYConfig_DefaultMinClientVer(t *testing.T) {
	cfg := &reality.Config{
		PrivateKey: make([]byte, 32),
		// MinClientVer intentionally empty
	}
	rc := cfg.GetREALITYConfig()
	if rc == nil {
		t.Fatal("nil REALITY config")
	}
	if len(rc.MinClientVer) != 3 {
		t.Fatalf("MinClientVer len=%d want 3", len(rc.MinClientVer))
	}
	if rc.MinClientVer[0] != 26 || rc.MinClientVer[1] != 3 || rc.MinClientVer[2] != 27 {
		t.Fatalf("MinClientVer=%v want [26 3 27]", rc.MinClientVer)
	}
}

func TestGetREALITYConfig_ExplicitMinClientVerPreserved(t *testing.T) {
	cfg := &reality.Config{
		PrivateKey:   make([]byte, 32),
		MinClientVer: []byte{1, 2, 3},
	}
	rc := cfg.GetREALITYConfig()
	if rc.MinClientVer[0] != 1 || rc.MinClientVer[1] != 2 || rc.MinClientVer[2] != 3 {
		t.Fatalf("MinClientVer=%v want [1 2 3]", rc.MinClientVer)
	}
}

// An unset MaxTimeDiff must resolve to the same 90s the REALITY library would
// apply implicitly, so we make the anti-replay window explicit and suppress the
// library's startup warning without changing the accept/reject decision.
func TestGetREALITYConfig_DefaultMaxTimeDiff(t *testing.T) {
	cfg := &reality.Config{
		PrivateKey: make([]byte, 32),
		// MaxTimeDiff intentionally left 0 (unset)
	}
	rc := cfg.GetREALITYConfig()
	if rc.MaxTimeDiff != 90*time.Second {
		t.Fatalf("MaxTimeDiff=%v want 90s", rc.MaxTimeDiff)
	}
}

// An explicit positive MaxTimeDiff (milliseconds in Bray config) must be
// preserved and converted to a duration. The field is unsigned, so disabling
// replay protection is intentionally not exposed here.
func TestGetREALITYConfig_ExplicitMaxTimeDiffPreserved(t *testing.T) {
	cfg := &reality.Config{
		PrivateKey:  make([]byte, 32),
		MaxTimeDiff: 30000, // 30s in ms
	}
	if rc := cfg.GetREALITYConfig(); rc.MaxTimeDiff != 30*time.Second {
		t.Fatalf("MaxTimeDiff=%v want 30s", rc.MaxTimeDiff)
	}
}
