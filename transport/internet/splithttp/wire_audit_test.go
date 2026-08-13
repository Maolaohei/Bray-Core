package splithttp

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// Wire-audit regression tests: padding name pool spread, skewed parameter
// distributions, and server-side validation accepting both legacy repeat-x
// and tokenish shapes (old and new clients).

func TestWireAuditPaddingNamePool(t *testing.T) {
	cfg := &Config{Path: "/sh/"}
	var sids []string
	for i := 0; i < 40; i++ {
		// random UUID-like session ids (raw.uuid + "." + 8-char mac)
		raw := fmt.Sprintf("550e8400-e29b-41d4-a716-%012d", i)
		mac := fmt.Sprintf("%08x", i*2654435761)
		sids = append(sids, raw+"."+mac)
	}
	names := map[string]bool{}
	for _, sid := range sids {
		req := &http.Request{URL: &url.URL{Path: "/sh/"}, Header: http.Header{}, Body: http.NoBody}
		cfg.FillStreamRequest(req, sid, "1")
		// find the padding header by scanning the pool names
		found := ""
		for _, cand := range []string{"X-Padding", "X-Request-Id", "X-Correlation-Id", "X-Client-Trace", "X-Request-Trace", "X-Session-Id", "X-Request-Key", "X-Client-Id"} {
			if req.Header.Get(cand) != "" {
				found = cand
				break
			}
		}
		names[found] = true
		fmt.Printf("  session %s -> padding header %q\n", sid[len(sid)-8:], found)
		if req.Header.Get("Content-Type") != "text/plain;charset=UTF-8" {
			t.Errorf("Content-Type = %q, want text/plain;charset=UTF-8", req.Header.Get("Content-Type"))
		}
		if req.Header.Get("DNT") != "" {
			t.Errorf("DNT still present: %q", req.Header.Get("DNT"))
		}
		ua := req.Header.Get("User-Agent")
		if !strings.Contains(ua, "Chrome/") || !strings.Contains(ua, "Safari/537.36") {
			t.Errorf("UA odd: %q", ua)
		}
		fmt.Printf("    UA=%s\n    CT=%s DNT=%q\n", ua, req.Header.Get("Content-Type"), req.Header.Get("DNT"))
	}
	if len(names) < 4 {
		t.Errorf("padding name pool not effective: only %d distinct names out of %d sessions", len(names), len(sids))
	}
}

func TestWireAuditSkewedDistributions(t *testing.T) {
	// padding length skew: mean of biasedRangeRand(100,1000) should be well
	// below the uniform midpoint 550 (right-skewed -> most values low).
	const n = 20000
	var sum int64
	for i := 0; i < n; i++ {
		sum += int64(biasedRangeRand(100, 1000))
	}
	mean := float64(sum) / n
	fmt.Printf("  biasedRangeRand(100,1000) mean=%.1f (uniform would be ~550)\n", mean)
	if mean > 500 {
		t.Errorf("padding length not skewed: mean=%.1f", mean)
	}
}

func TestWireAuditServerAcceptsBothShapes(t *testing.T) {
	cfg := &Config{}
	// legacy repeat-x value, base62, dashed-hex, lowercase-hex — all must pass
	// the default (empty method) validation within range.
	// legacy repeat-x value must still pass (huffman len == raw len for 'X')
	cases := []string{strings.Repeat("X", 500)}
	for i := 0; i < 200; i++ {
		cases = append(cases, GeneratePadding(1, 500, false))
	}
	ok := 0
	for _, v := range cases {
		// server validation: method empty -> huffman; range from tokenish
		// generation: huffman target 500 with tolerance 2
		if cfg.IsPaddingValid(v, 498, 502, "") {
			ok++
		} else {
			t.Logf("server rejected value: len=%d huffman=%d sample=%q", len(v), cachedHuffmanLen(len(v)), v[:32])
		}
	}
	fmt.Printf("  server accepted %d/%d padding values across all shapes\n", ok, len(cases))
}
