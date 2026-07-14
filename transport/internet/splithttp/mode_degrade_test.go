package splithttp

import "testing"

func TestResolveInitialMode(t *testing.T) {
	if got := ResolveInitialMode("", false, false); got != "packet-up" {
		t.Fatalf("no reality auto=%s", got)
	}
	if got := ResolveInitialMode("auto", true, false); got != "stream-one" {
		t.Fatalf("reality auto=%s", got)
	}
	if got := ResolveInitialMode("auto", true, true); got != "stream-up" {
		t.Fatalf("reality+download auto=%s", got)
	}
	if got := ResolveInitialMode("packet-up", true, true); got != "packet-up" {
		t.Fatalf("explicit wins=%s", got)
	}
}

func TestNextDegradedModeLadder(t *testing.T) {
	if NextDegradedMode("stream-one") != "stream-up" {
		t.Fatal("stream-one ladder")
	}
	if NextDegradedMode("stream-up") != "packet-up" {
		t.Fatal("stream-up ladder")
	}
	if NextDegradedMode("packet-up") != "" {
		t.Fatal("packet-up is terminal")
	}
	if !CanDegradeMode("stream-one") || CanDegradeMode("packet-up") {
		t.Fatal("CanDegradeMode mismatch")
	}
}

func TestModeDegradeOptIn(t *testing.T) {
	if !ShouldAttemptModeDegrade("auto", nil) {
		t.Fatal("auto should degrade")
	}
	if ShouldAttemptModeDegrade("stream-one", nil) {
		t.Fatal("explicit stream-one without header must not degrade")
	}
	if !ShouldAttemptModeDegrade("stream-one", map[string]string{"X-Bray-Mode-Degrade": "true"}) {
		t.Fatal("explicit + header should degrade")
	}
	if !ModeDegradeEnabled(map[string]string{"x-bray-mode-degrade": "1"}) {
		t.Fatal("truthy header")
	}
}
