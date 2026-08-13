package splithttp_test

import (
	"net/http"
	"net/url"
	"testing"

	. "github.com/xtls/xray-core/transport/internet/splithttp"
)

func newMetaTestConfig() *Config {
	return &Config{
		Path: "/x/",
	}
}

func TestMetaTokenRoundTrip(t *testing.T) {
	cases := []struct {
		sessionId string
		seqStr    string
	}{
		{"550e8400-e29b-41d4-a716-446655440000.abc12345", "123"},
		{"550e8400-e29b-41d4-a716-446655440000.abc12345", "999999"},
		{"", "42"},
		{"550e8400-e29b-41d4-a716-446655440000.abc12345", ""},
	}
	for _, tc := range cases {
		cfg := newMetaTestConfig()
		req := &http.Request{URL: &url.URL{Path: "/x/"}}
		cfg.ApplyMetaToRequest(req, tc.sessionId, tc.seqStr)
		gotSid, gotSeq := cfg.ExtractMetaFromRequest(req, "/x/")
		if gotSid != tc.sessionId || gotSeq != tc.seqStr {
			t.Errorf("round trip (%q,%q) -> (%q,%q)", tc.sessionId, tc.seqStr, gotSid, gotSeq)
		}
	}
}

func TestMetaTokenLegacyFallback(t *testing.T) {
	// Legacy wire: /x/<sessionId>/<seq> must still parse.
	cfg := newMetaTestConfig()
	req := &http.Request{URL: &url.URL{Path: "/x/550e8400-e29b-41d4-a716-446655440000.abc12345/77"}}
	sid, seq := cfg.ExtractMetaFromRequest(req, "/x/")
	if sid != "550e8400-e29b-41d4-a716-446655440000.abc12345" || seq != "77" {
		t.Fatalf("legacy fallback got (%q,%q)", sid, seq)
	}
}

func TestMetaTokenNotCollidingWithSessionId(t *testing.T) {
	// A legacy sessionId contains dots but never a colon; make sure a dotted
	// segment is NOT treated as a token (no seq consumed from it).
	cfg := newMetaTestConfig()
	req := &http.Request{URL: &url.URL{Path: "/x/550e8400-e29b-41d4-a716-446655440000.abc12345"}}
	sid, seq := cfg.ExtractMetaFromRequest(req, "/x/")
	if sid == "" || seq != "" {
		t.Fatalf("dotted sessionId misparsed: sid=%q seq=%q", sid, seq)
	}
}

func TestMetaTokenOpaqueShape(t *testing.T) {
	// The wire segment must carry no dots and no colon (regex-hostile).
	cfg := newMetaTestConfig()
	req := &http.Request{URL: &url.URL{Path: "/x/"}}
	cfg.ApplyMetaToRequest(req, "550e8400-e29b-41d4-a716-446655440000.abc12345", "123")
	seg := req.URL.Path[len("/x/"):]
	for _, ch := range seg {
		if ch == '.' || ch == ':' {
			t.Fatalf("token leaks structure: %q", seg)
		}
	}
}
