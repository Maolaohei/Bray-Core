package splithttp

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/xtls/xray-core/common/net"
)

func TestDestinationFromEndpoint(t *testing.T) {
	primary := net.TCPDestination(net.ParseAddress("1.2.3.4"), 443)

	d, err := destinationFromEndpoint("cdn.example.com:8443", primary)
	if err != nil {
		t.Fatal(err)
	}
	if d.Address.String() != "cdn.example.com" || d.Port != 8443 {
		t.Fatalf("host:port parse = %v", d)
	}
	if d.Network != net.Network_TCP {
		t.Fatalf("network want TCP got %v", d.Network)
	}

	// bare host inherits primary port
	d, err = destinationFromEndpoint("edge.example.com", primary)
	if err != nil {
		t.Fatal(err)
	}
	if d.Port != 443 {
		t.Fatalf("inherit port: %v", d.Port)
	}

	// explicit tcp: prefix
	d, err = destinationFromEndpoint("tcp:backup.example.com:443", primary)
	if err != nil {
		t.Fatal(err)
	}
	if d.Address.String() != "backup.example.com" || d.Port != 443 {
		t.Fatalf("tcp prefix: %v", d)
	}

	// UDP primary without scheme keeps UDP for H3-style dials
	primaryUDP := net.UDPDestination(net.ParseAddress("9.9.9.9"), 443)
	d, err = destinationFromEndpoint("h3.example.com:443", primaryUDP)
	if err != nil {
		t.Fatal(err)
	}
	if d.Network != net.Network_UDP {
		t.Fatalf("want UDP retained, got %v", d.Network)
	}

	if _, err := destinationFromEndpoint("", primary); err == nil {
		t.Fatal("empty endpoint should fail")
	}
}

func TestBrayControlHeadersStrippedFromWire(t *testing.T) {
	if !isBrayControlHeader("x-bray-mode-degrade") {
		t.Fatal("prefix match")
	}
	if !isBrayControlHeader("X-Bray-Endpoints") {
		t.Fatal("case fold")
	}
	if isBrayControlHeader("x-custom-user") {
		t.Fatal("non-bray must pass")
	}

	c := &Config{
		Headers: map[string]string{
			"User-Agent":            "TestAgent/1.0",
			"x-bray-mode-degrade":   "true",
			"X-Bray-Multi-Endpoint": "on",
			"x-bray-endpoints":      "a:443,b:443",
			"X-Custom-App":          "keep-me",
		},
	}
	h := c.GetRequestHeader()
	for _, key := range []string{"x-bray-mode-degrade", "X-Bray-Multi-Endpoint", "x-bray-endpoints"} {
		if v := h.Get(key); v != "" {
			t.Fatalf("control header leaked on wire: %s=%s", key, v)
		}
	}
	// Ensure no x-bray-* keys remain regardless of canonicalization.
	for k := range h {
		if isBrayControlHeader(k) {
			t.Fatalf("leaked key %q", k)
		}
	}
	if h.Get("X-Custom-App") != "keep-me" {
		t.Fatalf("user header dropped: %v", h)
	}
	// GetRequestHeaderWithPayload must also strip (builds on GetRequestHeader).
	c.UplinkDataKey = "x-data"
	c.UplinkChunkSize = &RangeConfig{From: 100, To: 100}
	hp := c.GetRequestHeaderWithPayload([]byte("abc"))
	for k := range hp {
		if isBrayControlHeader(k) {
			t.Fatalf("payload header leaked %q", k)
		}
	}
}

func TestIsDegradeEligibleError_WrappedContext(t *testing.T) {
	wrapped := fmt.Errorf("open: %w", context.Canceled)
	if IsDegradeEligibleError(wrapped) {
		t.Fatal("wrapped cancel must not cascade")
	}
	wrappedDL := fmt.Errorf("open: %w", context.DeadlineExceeded)
	if IsDegradeEligibleError(wrappedDL) {
		t.Fatal("wrapped deadline must not cascade")
	}
	if IsDegradeEligibleError(context.DeadlineExceeded) {
		t.Fatal("deadline")
	}
	if !IsDegradeEligibleError(errors.New("stream reset")) {
		t.Fatal("reset should cascade")
	}
}

func TestBuildModeCascade_EmptyInitial(t *testing.T) {
	got := BuildModeCascade("", true)
	if len(got) != 1 || got[0] != "packet-up" {
		t.Fatalf("empty initial with degrade: %v", got)
	}
	got = BuildModeCascade("packet-up", true)
	if len(got) != 1 || got[0] != "packet-up" {
		t.Fatalf("terminal: %v", got)
	}
}
