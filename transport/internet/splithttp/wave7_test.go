package splithttp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
)

func TestDestinationFromEndpoint_IPv6(t *testing.T) {
	primary := net.TCPDestination(net.ParseAddress("1.2.3.4"), 443)

	// bracketed IPv6 with port
	d, err := destinationFromEndpoint("[2001:db8::1]:8443", primary)
	if err != nil {
		t.Fatal(err)
	}
	if d.Address.String() != "[2001:db8::1]" || d.Port != 8443 {
		t.Fatalf("bracketed ipv6:port = %v", d)
	}

	// bare IPv6 inherits primary port
	d, err = destinationFromEndpoint("2001:db8::2", primary)
	if err != nil {
		t.Fatal(err)
	}
	if d.Address.String() != "[2001:db8::2]" || d.Port != 443 {
		t.Fatalf("bare ipv6 inherit port: %v", d)
	}

	// bracketed IPv6 without port inherits primary port
	d, err = destinationFromEndpoint("[2001:db8::3]", primary)
	if err != nil {
		t.Fatal(err)
	}
	if d.Address.String() != "[2001:db8::3]" || d.Port != 443 {
		t.Fatalf("bracket bare inherit: %v", d)
	}

	// tcp: prefix with IPv6
	d, err = destinationFromEndpoint("tcp:[2001:db8::4]:443", primary)
	if err != nil {
		t.Fatal(err)
	}
	if d.Address.String() != "[2001:db8::4]" || d.Port != 443 || d.Network != net.Network_TCP {
		t.Fatalf("tcp ipv6: %v", d)
	}
}

func TestStickyTTLFromHeaders_NoGlobalMutation(t *testing.T) {
	oldM, oldE := StickyModeTTL, StickyEndpointTTL
	defer func() {
		StickyModeTTL, StickyEndpointTTL = oldM, oldE
	}()
	StickyModeTTL = time.Hour
	StickyEndpointTTL = time.Hour

	// Compat wrapper must not mutate globals (Wave-7).
	ApplyStickyTTLFromHeaders(map[string]string{
		"x-bray-sticky-mode-ttl":     "5m",
		"X-Bray-Sticky-Endpoint-TTL": "2m",
	})
	if StickyModeTTL != time.Hour || StickyEndpointTTL != time.Hour {
		t.Fatalf("globals mutated: mode=%v ep=%v", StickyModeTTL, StickyEndpointTTL)
	}

	modeTTL, epTTL := StickyTTLFromHeaders(map[string]string{
		"x-bray-sticky-mode-ttl":     "5m",
		"X-Bray-Sticky-Endpoint-TTL": "2m",
	})
	if modeTTL != 5*time.Minute || epTTL != 2*time.Minute {
		t.Fatalf("parsed ttls mode=%v ep=%v", modeTTL, epTTL)
	}
}

func TestRememberStickyModeTTL_PerEntryExpiry(t *testing.T) {
	ClearStickyModeForTest()
	old := StickyModeTTL
	StickyModeTTL = time.Hour
	defer func() { StickyModeTTL = old }()

	key := stickyDestKey("9.9.9.9:443", "cdn.example.com")
	RememberStickyModeTTL(key, "packet-up", 5*time.Millisecond)
	if mode, ok := LookupStickyMode(key); !ok || mode != "packet-up" {
		t.Fatalf("lookup %v %v", mode, ok)
	}
	time.Sleep(12 * time.Millisecond)
	if _, ok := LookupStickyMode(key); ok {
		t.Fatal("per-entry TTL should expire even when global default is long")
	}
}

func TestNoteStickyModeFailure_Invalidates(t *testing.T) {
	ClearStickyModeForTest()
	old := StickyModeTTL
	StickyModeTTL = time.Hour
	defer func() { StickyModeTTL = old }()

	key := stickyDestKey("8.8.8.8:443", "edge.example.com")
	RememberStickyMode(key, "stream-up")
	// Failure of a different mode should not clear sticky.
	NoteStickyModeFailure(key, "stream-one")
	if mode, ok := LookupStickyMode(key); !ok || mode != "stream-up" {
		t.Fatalf("unrelated fail cleared sticky: %v %v", mode, ok)
	}
	// Failure of sticky mode itself clears it.
	NoteStickyModeFailure(key, "stream-up")
	if _, ok := LookupStickyMode(key); ok {
		t.Fatal("sticky mode failure should invalidate")
	}
}

func TestShouldRefreshXmuxBeforeCascade(t *testing.T) {
	if ShouldRefreshXmuxBeforeCascade(errors.New("connection reset by peer"), false) {
		t.Fatal("no more modes")
	}
	if !ShouldRefreshXmuxBeforeCascade(errors.New("connection reset by peer"), true) {
		t.Fatal("fatal + more modes should refresh")
	}
	if ShouldRefreshXmuxBeforeCascade(errors.New("unexpected status 403"), true) {
		t.Fatal("CDN status must not refresh/evict")
	}
	if ShouldRefreshXmuxBeforeCascade(context.Canceled, true) {
		t.Fatal("cancel must not refresh")
	}
	if !ShouldEvictXmuxOnOpenFailure(errors.New("http2: client conn not usable")) {
		t.Fatal("fatal should evict")
	}
}

func TestRaceDialEndpoints_EmptyErrors(t *testing.T) {
	_, _, err := RaceDialEndpoints(context.Background(), nil, func(ctx context.Context, endpoint string) (net.Conn, error) {
		return nil, errors.New("unused")
	})
	if !errors.Is(err, ErrNoMultiEndpoints) {
		t.Fatalf("empty list: %v", err)
	}
	_, _, err = RaceDialEndpoints(context.Background(), []string{"a:443"}, nil)
	if !errors.Is(err, ErrNilMultiEndpointDial) {
		t.Fatalf("nil dial: %v", err)
	}
}
