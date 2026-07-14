package splithttp

import (
	"testing"
	"time"
)

func TestH3MetricsAndCooldownConst(t *testing.T) {
	if H3Cooldown != 30*time.Minute {
		t.Fatalf("H3Cooldown=%v", H3Cooldown)
	}
	before := GetH3Metrics()
	h3TransportMetrics.H3Wins.Add(1)
	h3TransportMetrics.H2Fallbacks.Add(1)
	after := GetH3Metrics()
	if after.H3Wins < before.H3Wins+1 {
		t.Fatalf("H3Wins not incremented: before=%d after=%d", before.H3Wins, after.H3Wins)
	}
	if after.H2Fallbacks < before.H2Fallbacks+1 {
		t.Fatalf("H2Fallbacks not incremented")
	}
	if H3MetricsReport() == "" {
		t.Fatal("empty report")
	}
}
