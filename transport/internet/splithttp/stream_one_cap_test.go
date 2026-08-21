package splithttp

import (
	"testing"
	"time"
)

func TestStreamOneHardCapControl(t *testing.T) {
	if got := (&Config{}).GetStreamOneHardCap(); got != streamOneHardCapLifetime {
		t.Fatalf("default cap=%v want %v", got, streamOneHardCapLifetime)
	}
	cfg := &Config{Headers: map[string]string{"x-bray-stream-one-hard-cap": "21600"}}
	if got := cfg.GetStreamOneHardCap(); got != 6*time.Hour {
		t.Fatalf("configured cap=%v want 6h", got)
	}
	for _, value := range []string{"bad", "10", "90000"} {
		cfg.Headers["x-bray-stream-one-hard-cap"] = value
		if got := cfg.GetStreamOneHardCap(); got != streamOneHardCapLifetime {
			t.Fatalf("invalid cap %q=%v want default", value, got)
		}
	}
}
