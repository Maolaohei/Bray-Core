package splithttp

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// TestStrictPaddingRange_AlwaysWithinBase verifies that StrictPaddingRange
// returns [baseFrom, baseTo] unchanged regardless of payload size.
func TestStrictPaddingRange_AlwaysWithinBase(t *testing.T) {
	baseFrom, baseTo := int32(100), int32(1000)
	for _, payload := range []int{0, 1, 100, 255, 256, 512, 1023, 1024, 4096} {
		from, to := StrictPaddingRange(baseFrom, baseTo)
		if from != baseFrom || to != baseTo {
			t.Fatalf("StrictPaddingRange payload=%d: got [%d,%d] want [%d,%d]",
				payload, from, to, baseFrom, baseTo)
		}
	}
}

// TestStrictPaddingRange_DegenerateInputs verifies clamp on swapped/negative
// bounds.
func TestStrictPaddingRange_DegenerateInputs(t *testing.T) {
	// Swapped bounds: should auto-swap.
	from, to := StrictPaddingRange(1000, 100)
	if from != 100 || to != 1000 {
		t.Fatalf("swapped bounds: got [%d,%d] want [100,1000]", from, to)
	}
	// Negative from: should clamp to 0.
	from, to = StrictPaddingRange(-50, 100)
	if from != 0 || to != 100 {
		t.Fatalf("negative from: got [%d,%d] want [0,100]", from, to)
	}
}

// TestAdaptivePaddingRange_ClampInvariant verifies the key invariant:
// returned to is always <= baseTo, and from <= to.
func TestAdaptivePaddingRange_ClampInvariant(t *testing.T) {
	cases := []struct {
		baseFrom, baseTo int32
		payload          int
	}{
		{100, 1000, 0},
		{100, 1000, 1},
		{100, 1000, 100},
		{100, 1000, 255},
		{100, 1000, 256},
		{100, 1000, 512},
		{100, 1000, 1023},
		{100, 1000, 1024},
		{100, 1000, 4096},
		// Tight range where jitter could overflow without clamp.
		{100, 105, 0},
		{100, 105, 100},
		{100, 105, 255},
		{100, 105, 1024},
		// Very small range.
		{0, 5, 0},
		{0, 5, 100},
		{0, 5, 255},
		{0, 5, 1024},
		// Equal bounds.
		{500, 500, 0},
		{500, 500, 255},
		{500, 500, 1024},
	}
	for _, c := range cases {
		from, to := AdaptivePaddingRange(c.baseFrom, c.baseTo, c.payload)
		if to > c.baseTo {
			t.Errorf("base=[%d,%d] payload=%d: to=%d > baseTo (invariant broken)",
				c.baseFrom, c.baseTo, c.payload, to)
		}
		if from > to {
			t.Errorf("base=[%d,%d] payload=%d: from=%d > to=%d",
				c.baseFrom, c.baseTo, c.payload, from, to)
		}
		if from < 0 {
			t.Errorf("base=[%d,%d] payload=%d: from=%d < 0",
				c.baseFrom, c.baseTo, c.payload, from)
		}
	}
}

// TestAdaptivePaddingRange_JitterBreaksBucket verifies that adjacent payload
// sizes do not always map to the exact same sub-range — the jitter factor
// must produce observable variation.
func TestAdaptivePaddingRange_JitterBreaksBucket(t *testing.T) {
	baseFrom, baseTo := int32(100), int32(1000)
	seen := map[[2]int32]int{}
	// Sample 100 payload sizes in the small-packet band.
	for payload := 0; payload < 100; payload++ {
		from, to := AdaptivePaddingRange(baseFrom, baseTo, payload)
		seen[[2]int32{from, to}]++
	}
	if len(seen) < 2 {
		t.Fatalf("jitter did not break bucket determinism: only %d distinct ranges observed", len(seen))
	}
}

// TestGeneratePadding_StrictModeByteVariation verifies that strict mode
// returns different byte patterns for the same length across multiple calls
// (at least for tokenish, where samples differ).
func TestGeneratePadding_StrictModeByteVariation(t *testing.T) {
	length := 200
	seen := map[string]int{}
	for i := 0; i < 32; i++ {
		s := GeneratePadding(methodIndex(PaddingMethodTokenish), length, true)
		seen[s]++
	}
	if len(seen) < 2 {
		t.Fatalf("strict tokenish produced only %d distinct sample(s) across 32 calls", len(seen))
	}
}

// TestGeneratePadding_StrictModeLengthCorrect verifies strict mode returns
// padding of the correct length. For repeat-x the raw string length matches
// the target; for tokenish the Huffman-encoded length matches (the raw
// string is longer because Huffman compresses base62 ~20%).
func TestGeneratePadding_StrictModeLengthCorrect(t *testing.T) {
	for _, length := range []int{1, 10, 100, 500, 1024} {
		// repeat-x: raw length == target.
		s := GeneratePadding(methodIndex(PaddingMethodRepeatX), length, true)
		if len(s) != length {
			t.Errorf("repeat-x length=%d strict: got len=%d", length, len(s))
		}
		// tokenish: Huffman-encoded length within tolerance of target.
		s = GeneratePadding(methodIndex(PaddingMethodTokenish), length, true)
		hlen := cachedHuffmanLen(len(s))
		diff := hlen - length
		if diff < 0 {
			diff = -diff
		}
		if diff > validationTolerance {
			t.Errorf("tokenish length=%d strict: huffman len=%d (diff=%d > tolerance=%d)",
				length, hlen, diff, validationTolerance)
		}
	}
}

// TestExtractXPadding_ObfsFallback verifies the three obfs-mode scenarios:
//  1. obfs hit -> returns obfs location
//  2. obfs miss + standard has value -> falls back to standard location
//  3. both empty -> returns ("", "")
func TestExtractXPadding_ObfsFallback(t *testing.T) {
	cfg := &Config{
		XPaddingObfsMode:  true,
		XPaddingKey:       "obfs_key",
		XPaddingHeader:    "X-Obfs",
		XPaddingPlacement: PlacementQueryInHeader,
	}

	// Case 1: obfs cookie carries the padding -> obfs location returned.
	req1, _ := http.NewRequest("GET", "https://example.com/path", nil)
	req1.AddCookie(&http.Cookie{Name: "obfs_key", Value: "COOKIEVAL"})
	v, p := cfg.ExtractXPaddingFromRequest(req1, true)
	if v != "COOKIEVAL" || !strings.Contains(p, "cookie") {
		t.Fatalf("case1: got (%q,%q) want obfs cookie hit", v, p)
	}

	// Case 2: obfs miss + Referer?x_padding present -> standard fallback.
	req2, _ := http.NewRequest("GET", "https://example.com/path", nil)
	req2.Header.Set("Referer", "https://example.com/ref?x_padding=STDVAL")
	v, p = cfg.ExtractXPaddingFromRequest(req2, true)
	if v != "STDVAL" || !strings.Contains(p, "Referer") {
		t.Fatalf("case2: got (%q,%q) want standard Referer fallback", v, p)
	}

	// Case 3: both empty -> ("", "").
	req3, _ := http.NewRequest("GET", "https://example.com/path", nil)
	v, p = cfg.ExtractXPaddingFromRequest(req3, true)
	if v != "" || p != "" {
		t.Fatalf("case3: got (%q,%q) want empty", v, p)
	}
}

// TestExtractXPadding_StandardModeUnchanged verifies non-obfs mode still
// reads only from standard locations.
func TestExtractXPadding_StandardModeUnchanged(t *testing.T) {
	cfg := &Config{
		XPaddingObfsMode:  false,
		XPaddingKey:       "obfs_key",
		XPaddingHeader:    "X-Obfs",
		XPaddingPlacement: PlacementQueryInHeader,
	}

	// Standard location (URL query) carries padding.
	u, _ := url.Parse("https://example.com/path?x_padding=URLVAL")
	req, _ := http.NewRequest("GET", u.String(), nil)
	v, p := cfg.ExtractXPaddingFromRequest(req, false)
	if v != "URLVAL" {
		t.Fatalf("standard URL query: got %q want URLVAL", v)
	}
	_ = p

	// Obfs cookie present but should be ignored in standard mode.
	req.AddCookie(&http.Cookie{Name: "obfs_key", Value: "COOKIEVAL"})
	v, _ = cfg.ExtractXPaddingFromRequest(req, false)
	if v != "URLVAL" {
		t.Fatalf("standard mode must ignore obfs cookie: got %q want URLVAL", v)
	}
}

// BenchmarkGeneratePadding_StrictVsNonStrict compares the throughput of
// GeneratePadding in strict (cache-pick) vs non-strict (legacy) mode for
// tokenish padding at a hot small-packet length.
func BenchmarkGeneratePadding_StrictVsNonStrict(b *testing.B) {
	for _, strict := range []bool{false, true} {
		name := "nonstrict"
		if strict {
			name = "strict"
		}
		b.Run(name+"_tokenish_200", func(b *testing.B) {
			b.ReportAllocs()
			mi := methodIndex(PaddingMethodTokenish)
			for i := 0; i < b.N; i++ {
				_ = GeneratePadding(mi, 200, strict)
			}
		})
		b.Run(name+"_repeatX_200", func(b *testing.B) {
			b.ReportAllocs()
			mi := methodIndex(PaddingMethodRepeatX)
			for i := 0; i < b.N; i++ {
				_ = GeneratePadding(mi, 200, strict)
			}
		})
	}
}
