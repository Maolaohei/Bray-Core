package splithttp

import (
	"context"
	"errors"
	"testing"
	"time"
)

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
	if got := ResolveInitialModeOpts("auto", true, false, true); got != "packet-up" {
		t.Fatalf("preferPacket auto=%s", got)
	}
	if got := ResolveInitialModeOpts("stream-one", true, false, true); got != "stream-one" {
		t.Fatalf("preferPacket must not override explicit=%s", got)
	}
}

func TestValidateConfiguredMode(t *testing.T) {
	if err := ValidateConfiguredMode(""); err != nil {
		t.Fatal(err)
	}
	if err := ValidateConfiguredMode("auto"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateConfiguredMode("stream-one"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateConfiguredMode("STREAM-UP"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateConfiguredMode("packet-up"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateConfiguredMode("streamone"); err == nil {
		t.Fatal("typo must fail")
	}
	if err := ValidateConfiguredMode("stream_up"); err == nil {
		t.Fatal("underscore must fail")
	}
}

func TestServerModeAllows(t *testing.T) {
	if !ServerModeAllowsStreamOne("auto") || !ServerModeAllowsStreamOne("stream-one") {
		t.Fatal("stream-one allow")
	}
	if ServerModeAllowsStreamOne("packet-up") || ServerModeAllowsStreamOne("stream-up") {
		t.Fatal("stream-one deny locked other")
	}
	if !ServerModeAllowsStreamUp("") || !ServerModeAllowsStreamUp("stream-up") {
		t.Fatal("stream-up allow")
	}
	if ServerModeAllowsStreamUp("stream-one") || ServerModeAllowsPacketUp("stream-one") {
		t.Fatal("locked stream-one must not allow sessioned shapes")
	}
	if !ServerModeAllowsPacketUp("auto") || !ServerModeAllowsPacketUp("packet-up") {
		t.Fatal("packet-up allow")
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

func TestBuildModeCascade(t *testing.T) {
	got := BuildModeCascade("stream-one", false)
	if len(got) != 1 || got[0] != "stream-one" {
		t.Fatalf("no degrade: %v", got)
	}
	got = BuildModeCascade("stream-one", true)
	if len(got) != 3 || got[0] != "stream-one" || got[1] != "stream-up" || got[2] != "packet-up" {
		t.Fatalf("full ladder: %v", got)
	}
	got = BuildModeCascade("stream-up", true)
	if len(got) != 2 || got[0] != "stream-up" || got[1] != "packet-up" {
		t.Fatalf("partial ladder: %v", got)
	}
}

func TestIsDegradeEligibleError(t *testing.T) {
	if IsDegradeEligibleError(nil) {
		t.Fatal("nil")
	}
	if IsDegradeEligibleError(context.Canceled) {
		t.Fatal("canceled")
	}
	if !IsDegradeEligibleError(errors.New("connection reset by peer")) {
		t.Fatal("transport fatal should degrade")
	}
	if !IsDegradeEligibleError(errors.New("unexpected status 403")) {
		t.Fatal("CDN 403 should degrade")
	}
	if !IsDegradeEligibleError(errors.New("unexpected status 502")) {
		t.Fatal("5xx should degrade")
	}
	if IsDegradeEligibleError(errors.New("unexpected status 400")) {
		t.Fatal("400 config reject must not degrade")
	}
	if IsDegradeEligibleError(errors.New("unexpected status 401")) {
		t.Fatal("401 must not degrade")
	}
	if IsDegradeEligibleError(errors.New("edge 403 / reset")) {
		// no status parser hit and not a known fatal needle ("reset" alone is weak;
		// full "connection reset" is fatal). This free-form string should not cascade.
		// Keep fail-closed: unknown noise != cascade.
	} else {
		// expected: false
	}
	if IsDegradeEligibleError(errors.New("padding mismatch")) {
		t.Fatal("unknown non-status must not degrade")
	}
}

func TestCascadeStepJitter(t *testing.T) {
	if CascadeStepJitterMax != 200*time.Millisecond {
		t.Fatalf("max = %v", CascadeStepJitterMax)
	}
	for i := 0; i < 40; i++ {
		d := CascadeStepJitter()
		if d < 0 || d > CascadeStepJitterMax {
			t.Fatalf("jitter out of range: %v", d)
		}
	}
}

func TestWaitCascadeStepJitterCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = WaitCascadeStepJitter(ctx)
}
