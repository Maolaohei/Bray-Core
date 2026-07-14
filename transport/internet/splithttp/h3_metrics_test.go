package splithttp

import (
	"errors"
	"net/http"
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

func TestH3CooldownDoesNotDoubleCountH2Fallback(t *testing.T) {
	oldCooldown := H3Cooldown
	H3Cooldown = time.Hour
	defer func() { H3Cooldown = oldCooldown }()

	h2 := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: http.NoBody, Request: req}, nil
	})
	h3 := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("h3 down")
	})
	tr := newHappyEyeballsTransport(h3, h2)

	// Force cooldown path: mark H3 failed first.
	tr.setH3Failed()
	before := GetH3Metrics()
	req, _ := http.NewRequest(http.MethodGet, "https://example.com/", nil)
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	after := GetH3Metrics()
	if after.H3Cooldowns < before.H3Cooldowns+1 {
		t.Fatalf("cooldown not counted: before=%d after=%d", before.H3Cooldowns, after.H3Cooldowns)
	}
	if after.H2Fallbacks != before.H2Fallbacks {
		t.Fatalf("cooldown path must not increment H2Fallbacks: before=%d after=%d", before.H2Fallbacks, after.H2Fallbacks)
	}
}

// roundTripperFunc is a tiny adapter for unit tests.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
