package splithttp

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestApplyStickyMode(t *testing.T) {
	cascade := []string{"stream-one", "stream-up", "packet-up"}
	got := ApplyStickyMode(cascade, "stream-up")
	if len(got) != 2 || got[0] != "stream-up" || got[1] != "packet-up" {
		t.Fatalf("got %v", got)
	}
	got = ApplyStickyMode(cascade, "packet-up")
	if len(got) != 1 || got[0] != "packet-up" {
		t.Fatalf("terminal sticky %v", got)
	}
	got = ApplyStickyMode(cascade, "stream-one")
	if len(got) != 3 {
		t.Fatalf("first sticky keeps full %v", got)
	}
	got = ApplyStickyMode(cascade, "unknown")
	if len(got) != 3 {
		t.Fatalf("unknown sticky ignored %v", got)
	}
}

func TestStickyModeRememberLookupTTL(t *testing.T) {
	ClearStickyModeForTest()
	old := StickyModeTTL
	StickyModeTTL = time.Hour
	defer func() { StickyModeTTL = old }()

	key := stickyDestKey("1.2.3.4:443", "cdn.example.com")
	if _, ok := LookupStickyMode(key); ok {
		t.Fatal("empty")
	}
	RememberStickyMode(key, "packet-up")
	mode, ok := LookupStickyMode(key)
	if !ok || mode != "packet-up" {
		t.Fatalf("lookup %v %v", mode, ok)
	}

	StickyModeTTL = time.Millisecond
	time.Sleep(5 * time.Millisecond)
	if _, ok := LookupStickyMode(key); ok {
		t.Fatal("expired sticky should miss")
	}
}

func TestStickyModeOptOut(t *testing.T) {
	if !StickyModeEnabled(nil) {
		t.Fatal("default on")
	}
	if StickyModeEnabled(map[string]string{"x-bray-sticky-mode": "off"}) {
		t.Fatal("opt-out")
	}
	if !StickyModeEnabled(map[string]string{"X-Bray-Sticky-Mode": "yes"}) {
		t.Fatal("explicit on")
	}
}

func TestBrayV2MetricsReport(t *testing.T) {
	before := GetBrayV2Metrics()
	recordModeAttempt()
	recordModeSuccess(true)
	recordModeCascadeStep()
	recordStickyHit()
	recordMultiEndpointRace(true)
	after := GetBrayV2Metrics()
	if after.ModeAttempts < before.ModeAttempts+1 {
		t.Fatal("attempt")
	}
	if after.ModeCascadeWins < before.ModeCascadeWins+1 {
		t.Fatal("cascade win")
	}
	if after.MultiEndpointAltWins < before.MultiEndpointAltWins+1 {
		t.Fatal("alt win")
	}
	if BrayV2MetricsReport() == "" {
		t.Fatal("report")
	}
}

func TestIsFatalOpenTransportError(t *testing.T) {
	if isFatalOpenTransportError(errors.New("unexpected status 403")) {
		t.Fatal("403 must not thrash pool")
	}
	if !isFatalOpenTransportError(errors.New("read: connection reset by peer")) {
		t.Fatal("reset")
	}
	if MaybeEvictXmuxAfterOpenFailure(nil, errors.New("eof")); false {
		t.Fatal("nil client ok")
	}
	// cancel never fatal path via MaybeEvict
	c := &XmuxClient{}
	MaybeEvictXmuxAfterOpenFailure(c, context.Canceled)
}

func TestBuildModeCascadeWithSticky(t *testing.T) {
	// integration-ish: cascade + sticky apply
	c := BuildModeCascade("stream-one", true)
	c = ApplyStickyMode(c, "stream-up")
	if len(c) != 2 || c[0] != "stream-up" {
		t.Fatalf("%v", c)
	}
}
