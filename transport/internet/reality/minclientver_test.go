package reality_test

import (
	"testing"

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
