package splithttp

import (
	"hash/fnv"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/xtls/xray-core/common/randpool"
	"golang.org/x/net/http2/hpack"
)

type PaddingMethod string

const (
	PaddingMethodRepeatX  PaddingMethod = "repeat-x"
	PaddingMethodTokenish PaddingMethod = "tokenish"
)

const charsetBase62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// Huffman encoding gives ~20% size reduction for base62 sequences
const avgHuffmanBytesPerCharBase62 = 0.8

const validationTolerance = 2

type XPaddingPlacement struct {
	Placement string
	Key       string
	Header    string
	RawURL    string
}

type XPaddingConfig struct {
	Length    int
	Placement XPaddingPlacement
	Method    PaddingMethod
	// Strict: when true, GeneratePadding picks from a pre-generated pool of
	// byte-pattern variants for the chosen length, defeating byte-pattern
	// fingerprinting at O(1) cost. Also implies the length was drawn from the
	// full [baseFrom, baseTo] range (see StrictPaddingRange).
	Strict bool
	// methodIdx is the strictCache row index for Method (0 = repeat-x,
	// 1 = tokenish). Cached at config-assembly time so the hot
	// GeneratePadding path never pays for a string comparison.
	methodIdx int
}

// randBufPool reuses the 256-byte buffer used in randStringFromCharset.
var randBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 256)
		return &b
	},
}

// repeatXArray is a static, init-populated table of "XXX...X" strings for
// every length in [1, repeatXCacheMaxLen]. Lookups on the hot path become a
// single array-index operation — no sync.Map, no CAS, no interface boxing,
// no allocation. Memory cost: ~8 KB of slice headers plus the underlying
// string data, all of which sits in L1 cache.
var repeatXArray [repeatXCacheMaxLen + 1]string

// repeatXCacheMaxLen is the largest length pre-computed in repeatXArray.
// Matches the strictCacheMaxLen so both tables share the same coverage.
const repeatXCacheMaxLen = 1024

// paddingBytePool reuses byte slices for RepeatX padding generation.
// Sizing: common padding range is 100-1000 bytes; 8192 covers edge cases.
var paddingBytePool = sync.Pool{
	New: func() any {
		b := make([]byte, 8192)
		for i := range b {
			b[i] = 'X'
		}
		return &b
	},
}

// paddingResultPool reuses result buffers for randStringFromCharset
// to avoid per-request heap allocation.
var paddingResultPool = sync.Pool{
	New: func() any {
		b := make([]byte, 8192)
		return &b
	},
}

// parsedURLCache caches url.Parse results for PlacementQueryInHeader
// to avoid repeated parsing of the same RawURL across requests.
// Bounded to 256 entries to prevent unbounded memory growth.
var parsedURLCache struct {
	sync.RWMutex
	entries map[string]*url.URL
	order   []string
	maxSize int
}

func init() {
	parsedURLCache.entries = make(map[string]*url.URL)
	parsedURLCache.maxSize = 256
}

func cachedParseURL(rawURL string) (*url.URL, bool) {
	parsedURLCache.RLock()
	if u, ok := parsedURLCache.entries[rawURL]; ok {
		parsedURLCache.RUnlock()
		return u, true
	}
	parsedURLCache.RUnlock()

	u, err := url.Parse(rawURL)
	if err != nil || u == nil {
		return nil, false
	}

	parsedURLCache.Lock()
	// Evict oldest if at capacity
	if len(parsedURLCache.entries) >= parsedURLCache.maxSize {
		oldest := parsedURLCache.order[0]
		parsedURLCache.order = parsedURLCache.order[1:]
		delete(parsedURLCache.entries, oldest)
	}
	parsedURLCache.entries[rawURL] = u
	parsedURLCache.order = append(parsedURLCache.order, rawURL)
	parsedURLCache.Unlock()

	return u, true
}

// huffmanLenCache caches HuffmanEncodeLength results by string length.
// For base62 charset, encoding length depends only on string length, not content.
// Security loss: 0 — this is a pure compute optimization.
var huffmanLenCache sync.Map

func cachedHuffmanLen(n int) int {
	if v, ok := huffmanLenCache.Load(n); ok {
		return v.(int)
	}
	s := strings.Repeat("A", n)
	realLen := int(hpack.HuffmanEncodeLength(s))
	huffmanLenCache.Store(n, realLen)
	return realLen
}

func randStringFromCharset(n int, charset string) (string, bool) {
	if n <= 0 || len(charset) == 0 {
		return "", false
	}

	m := len(charset)
	limit := byte(256 - (256 % m))

	rp := paddingResultPool.Get().(*[]byte)
	defer paddingResultPool.Put(rp)
	result := (*rp)[:n]
	i := 0

	sp := randBufPool.Get().(*[]byte)
	defer randBufPool.Put(sp)
	buf := *sp
	for i < n {
		for j := range buf {
			buf[j] = byte(randpool.Global.IntN(256))
		}
		for _, rb := range buf {
			if rb >= limit {
				continue
			}
			result[i] = charset[int(rb)%m]
			i++
			if i == n {
				break
			}
		}
	}

	s := string(result)
	return s, true
}

func absInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// tokenishCacheArray is a static, init-populated table of Tokenish padding
// strings for every length in [1, tokenishCacheMaxLen]. Replaces the old
// RWMutex-protected map: the table is fully written during init() and is
// read-only afterwards, so concurrent access is safe without any lock.
// Lookups on the hot path become a single array-index operation.
//
// Note: the non-strict fast-path only queries lengths in 1..31 and multiples
// of 32 up to 1024, but we fill every slot so the table is uniform and the
// compiler can prove index safety (BCE). Memory cost: ~16 KB + string data.
var tokenishCacheArray [tokenishCacheMaxLen + 1]string

// tokenishCacheMaxLen is the largest length pre-computed in tokenishCacheArray.
const tokenishCacheMaxLen = 1024

// strictCache is a read-only-after-init cache of multiple pre-generated
// padding samples per (method, length). In strict mode, GeneratePadding
// picks one sample at random via randpool, so the same length yields
// different bytes on every call — defeating byte-pattern fingerprinting.
//
// Concurrency: no lock is needed because init() fully populates the cache
// before any reader can observe it, and Go arrays/slices are safe for
// concurrent reads once writes stop (happens-before via init ordering).
//
// Layout: a fixed-size 2-D array indexed by [methodIndex][length]. Using a
// direct array (instead of a map) eliminates hash computation and bucket
// traversal on every lookup — the hot path becomes two pointer-chases plus
// a bounds check, all of which stay inside L1 cache. Memory cost:
// 2 methods × 1025 slots × 24 bytes/slice-header ≈ 48 KB.
var strictCache struct {
	// items[mi][length] holds strictSamplesPerLength pre-generated variants.
	// mi: 0 = repeat-x, 1 = tokenish (see methodIndex).
	// length: 1..strictCacheMaxLen (slot 0 is unused).
	items [2][strictCacheMaxLen + 1][]string
}

// strictSamplesPerLength is the number of pre-generated byte-pattern
// variants kept per (method, length). 8 (power of two, AND-mask selectable)
// lowers the repeat-cycle observability of identical-length padding across
// requests without blowing memory.
const strictSamplesPerLength = 8

// strictCacheMaxLen is the largest length pre-cached. Matches the upper
// band of the existing tokenishCache multiples-of-32 sweep.
const strictCacheMaxLen = 1024

func init() {
	// Fill repeatXArray: every length in [1, repeatXCacheMaxLen] gets a
	// pre-computed "XXX...X" string. Hot-path lookups become a single array
	// index — no sync.Map, no allocation.
	for length := 1; length <= repeatXCacheMaxLen; length++ {
		repeatXArray[length] = strings.Repeat("X", length)
	}

	// Fill tokenishCacheArray: every length in [1, tokenishCacheMaxLen] gets
	// a pre-computed Tokenish padding string. The old implementation only
	// covered 1..31 and multiples-of-32 up to 1024 (sparse map); filling
	// every slot makes the table uniform, eliminates the map hash lookup,
	// and lets the compiler prove index safety (BCE). Shapes are mixed
	// (base62 / dashed-hex / lowercase-hex) so values look like real
	// request-id headers rather than generator output.
	for length := 1; length <= tokenishCacheMaxLen; length++ {
		tokenishCacheArray[length] = generateTokenishPadding(length)
	}

	// Fill strictCache: every (method, length) in [1, strictCacheMaxLen] gets
	// strictSamplesPerLength pre-generated variants. This runs once at process
	// startup and is fully read-only afterwards.
	methods := []PaddingMethod{PaddingMethodRepeatX, PaddingMethodTokenish}
	for mi, method := range methods {
		for length := 1; length <= strictCacheMaxLen; length++ {
			samples := make([]string, strictSamplesPerLength)
			for s := 0; s < strictSamplesPerLength; s++ {
				samples[s] = generatePaddingForMethod(method, length)
			}
			strictCache.items[mi][length] = samples
		}
	}
}

// methodIndex maps a PaddingMethod to its strictCache.items slot.
// Returns 0 (repeat-x) for unknown methods as a safe default.
func methodIndex(method PaddingMethod) int {
	if method == PaddingMethodTokenish {
		return 1
	}
	return 0
}

// generatePaddingForMethod produces one padding string for the given method
// and length. Used by init() to populate strictCache.
func generatePaddingForMethod(method PaddingMethod, length int) string {
	switch method {
	case PaddingMethodTokenish:
		return generateTokenishPadding(length)
	default:
		return generateRepeatX(length)
	}
}

// hexChars is the lowercase hex alphabet used by the realistic token shapes.
const hexChars = "0123456789abcdef"

// generateTokenishPadding produces a tokenish padding string that mixes
// realistic header-value shapes with the legacy base62 look. Real-world
// X-Request-Id / X-Correlation-Id / trace values are UUIDs, lowercase hex
// tokens, or mixed alnum — a pure uniform base62 string is a generator
// signature. Roughly a third each: base62, dashed-hex (UUID-like),
// lowercase-hex (API-token-like). Init-time only (cache prefill), so this
// costs nothing on the hot path; the server's tokenish validation only
// checks huffman-encoded length, so all three shapes pass unchanged.
// targetHuffmanBytes is the huffman-encoded length the value must match
// (the wire-visible length), like the base62 raw generator.
func generateTokenishPadding(targetHuffmanBytes int) string {
	switch randpool.Global.IntN(3) {
	case 1:
		return generateTokenishShape(targetHuffmanBytes, generateDashedHex)
	case 2:
		return generateTokenishShape(targetHuffmanBytes, generateLowerHex)
	default:
		return generateTokenishPaddingBase62Raw(targetHuffmanBytes)
	}
}

// generateTokenishShape produces a shape string whose huffman-encoded
// length matches targetHuffmanBytes, iterating the character count until it
// converges (hex chars average ~0.75 bytes/char under hpack huffman).
func generateTokenishShape(targetHuffmanBytes int, gen func(n int) string) string {
	if targetHuffmanBytes <= 0 {
		return ""
	}
	n := (targetHuffmanBytes*4 + 2) / 3
	for i := 0; i < 8; i++ {
		s := gen(n)
		if d := cachedHuffmanLen(len(s)) - targetHuffmanBytes; d == 0 {
			return s
		} else if d < 0 {
			n++
		} else {
			n--
		}
	}
	return gen(n) // best effort after bounded iterations
}

// generateDashedHex builds a UUID-like dashed-hex string of the requested
// length: the classic 8-4-4-4-12 grouping for length 36, a scalable
// dashed-hex shape otherwise. '-' is a 6-bit huffman symbol (same order as
// base62 letters), so validation tolerance is unaffected.
func generateDashedHex(length int) string {
	if length <= 0 {
		return ""
	}
	buf := make([]byte, 0, length+8)
	group := 0
	for len(buf) < length {
		var g int
		switch group {
		case 0:
			g = 8
		case 1, 2, 3:
			g = 4
		case 4:
			g = 12
		default:
			g = 4
		}
		group++
		if g > length-len(buf) {
			g = length - len(buf)
		}
		for i := 0; i < g; i++ {
			buf = append(buf, hexChars[randpool.Global.IntN(16)])
		}
		if len(buf) < length {
			buf = append(buf, '-')
		}
	}
	return string(buf)
}

// generateLowerHex builds a lowercase-hex string of the requested length
// (common API-token / request-id shape).
func generateLowerHex(length int) string {
	if length <= 0 {
		return ""
	}
	buf := make([]byte, length)
	for i := range buf {
		buf[i] = hexChars[randpool.Global.IntN(16)]
	}
	return string(buf)
}

func generateTokenishPaddingBase62Raw(targetHuffmanBytes int) string {
	n := int(math.Ceil(float64(targetHuffmanBytes) / avgHuffmanBytesPerCharBase62))
	if n < 1 {
		n = 1
	}

	randBase62Str, ok := randStringFromCharset(n, charsetBase62)
	if !ok {
		return ""
	}

	const maxIter = 150
	adjustChar := byte('X')

	// Adjust until close enough
	for iter := 0; iter < maxIter; iter++ {
		currentLength := cachedHuffmanLen(len(randBase62Str))
		diff := currentLength - targetHuffmanBytes

		if absInt(diff) <= validationTolerance {
			return randBase62Str
		}

		if diff < 0 {
			// Too small -> append padding char(s)
			randBase62Str += string(adjustChar)

			// Avoid a long run of identical chars
			if adjustChar == 'X' {
				adjustChar = 'Z'
			} else {
				adjustChar = 'X'
			}
		} else {
			// Too big -> remove from the end
			if len(randBase62Str) <= 1 {
				return randBase62Str
			}
			randBase62Str = randBase62Str[:len(randBase62Str)-1]
		}
	}

	return randBase62Str
}

func generateRepeatX(length int) string {
	if length <= 0 {
		return ""
	}
	// Hot-path: static array direct-index for lengths covered by init.
	// Replaces the old sync.Map.Load + interface-unbox path — no CAS, no
	// allocation, no interface boxing. Array access is a single base+offset
	// instruction and stays inside L1 cache.
	if length <= repeatXCacheMaxLen {
		return repeatXArray[length]
	}
	// Cold fallback for lengths beyond the pre-cache ceiling (>1024).
	// paddingBytePool is still useful here for one-off large allocations.
	if length <= 8192 {
		bp := paddingBytePool.Get().(*[]byte)
		s := string((*bp)[:length])
		paddingBytePool.Put(bp)
		return s
	}
	return strings.Repeat("X", length)
}

// GeneratePadding produces a padding string of approximately the requested
// length. methodIdx is the strictCache row index (0 = repeat-x, 1 = tokenish)
// — pre-computed by the caller via methodIndex(Method) at config-assembly
// time, so the hot path never pays for a string comparison.
//
// When strict is true, the returned bytes are randomly picked from a
// pre-generated pool of variants for that length, defeating byte-pattern
// fingerprinting at O(1) cost:
//
//   - Array lookup: strictCache.items[mi][length] — direct base+offset,
//     no hash, no bucket traversal.
//   - Bounds-check elimination: the compound guard below lets the Go 1.26
//     compiler prove all subsequent indices are in-range, so the emitted
//     assembly has no TEST/JB instructions on the hot path.
//   - Bitmask sampling: Uint32() & 3 replaces IntN(4), compiling down to a
//     single AND instruction (~1 cycle vs ~10-15 for a division).
//
// When strict is false, the legacy fast-path cache is used (one
// deterministic sample per length).
func GeneratePadding(methodIdx int, length int, strict bool) string {
	if length <= 0 {
		return ""
	}

	// Strict mode: pick a random pre-generated sample for (method, length).
	// The compound guard is shaped so the Go compiler's BCE pass can prove
	// every subsequent index is in-range — emitted assembly has no bounds
	// checks on the hot path.
	if strict {
		if uint(methodIdx) < uint(len(strictCache.items)) &&
			length >= 1 && length <= strictCacheMaxLen {
			samples := strictCache.items[methodIdx][length]
			if n := len(samples); n > 0 {
				var idx int
				if n&(n-1) == 0 {
					// Power-of-two fast-path: single AND instruction.
					idx = int(randpool.Global.Uint32()) & (n - 1)
				} else {
					idx = randpool.Global.IntN(n)
				}
				// len(samples) == strictSamplesPerLength (>= 1) and idx is
				// in [0, n), so this index is always valid.
				return samples[idx]
			}
		}
		// Length beyond the pre-cache ceiling or unknown methodIdx: fall
		// through to on-demand generation. Rare in practice.
	}

	// https://www.rfc-editor.org/rfc/rfc7541.html#appendix-B
	// h2's HPACK Header Compression feature employs a huffman encoding using a static table.
	// 'X' and 'Z' are assigned an 8 bit code, so HPACK compression won't change actual padding length on the wire.
	// https://www.rfc-editor.org/rfc/rfc9204.html#section-4.1.2-2
	// h3's similar QPACK feature uses the same huffman table.

	// Non-strict path: legacy dispatch by PaddingMethod string. This path
	// is only taken when strict=false (non-default configuration), so the
	// string comparison here is cold and does not affect the hot path.
	switch {
	case methodIdx == 1: // tokenish
		return tokenishOrRepeatX(length)
	case methodIdx == 0: // repeat-x
		return generateRepeatX(length)
	default:
		return generateRepeatX(length)
	}
}

// tokenishOrRepeatX is the legacy tokenish fast-path, factored out so the
// non-strict branch stays readable.
func tokenishOrRepeatX(length int) string {
	// Now that tokenishCacheArray is fully populated for every length in
	// [1, tokenishCacheMaxLen] (not just the old 1..31 + multiples-of-32
	// sparse coverage), the fast-path condition collapses to a single
	// upper-bound check. Every length in this range is a single array
	// direct-index — no huffman-adjust iteration.
	if length >= 1 && length <= tokenishCacheMaxLen {
		return tokenishCacheArray[length]
	}
	return generateTokenishPaddingBase62Raw(length)
}

func ApplyPaddingToCookie(req *http.Request, name, value string) {
	if req == nil || name == "" || value == "" {
		return
	}
	req.AddCookie(&http.Cookie{
		Name:  name,
		Value: value,
		Path:  "/",
	})
}

func ApplyPaddingToResponseCookie(writer http.ResponseWriter, name, value string) {
	if name == "" || value == "" {
		return
	}
	http.SetCookie(writer, &http.Cookie{
		Name:  name,
		Value: value,
		Path:  "/",
	})
}

func ApplyPaddingToQuery(u *url.URL, key, value string) {
	if u == nil || key == "" || value == "" {
		return
	}
	q := u.Query()
	q.Set(key, value)
	u.RawQuery = q.Encode()
}

// Pre-allocated default RangeConfig for XPaddingBytes.
var defaultRangeConfigXPaddingBytes = &RangeConfig{
	From: 100,
	To:   1000,
}

// AdaptivePaddingRange returns a padding range adjusted for the given payload size.
// Small payloads get smaller padding to reduce bandwidth waste; large payloads
// get proportionally larger padding to maintain traffic analysis resistance.
//
// Invariant (strictly enforced via clamp): the returned [from, to] is always
// a subset of [0, baseTo]. This guarantees that padding lengths drawn from
// this range never exceed the base upper bound, which is the acceptance
// ceiling of both Bray and stock Xray servers.
//
// The bucket boundaries are softened with a small payload-dependent jitter so
// that observers cannot deterministically map a wire padding length back to a
// specific payload-size bucket. The jitter is bounded to never break the
// invariant above.
//
// Note: the lower bound intentionally does NOT clamp to baseFrom — the
// original adaptive floors (e.g. 10 for small packets, 20 for medium) are
// preserved so that AcceptedPaddingRange() below remains wire-compatible with
// stock Xray peers. Use StrictPaddingRange() when you need padding to stay
// within [baseFrom, baseTo] for traffic-analysis resistance.
func AdaptivePaddingRange(baseFrom, baseTo int32, payloadSize int) (from, to int32) {
	// Degenerate inputs: keep the range well-formed.
	if baseTo < baseFrom {
		baseFrom, baseTo = baseTo, baseFrom
	}
	if baseFrom < 0 {
		baseFrom = 0
	}
	if baseTo < 0 {
		baseTo = 0
	}

	// Soften bucket boundaries with a bounded random jitter factor. The old
	// payloadSize%7 derivation leaked the payload size class through the
	// padding length (7 distinct sub-ranges); a random jitter breaks that
	// correlation while keeping the range inside [0, baseTo].
	jitter := int32(randpool.Global.IntN(7)) // 0..6, decoupled from payload size

	switch {
	case payloadSize < 256:
		// Small packets (SSH, MQTT, RPC): reduce padding to ~20-25% of base.
		from = max(10, baseFrom/5)
		to = max(50, baseTo/5) + jitter
	case payloadSize < 1024:
		// Medium packets: reduce padding to ~40-70% of base.
		from = max(20, baseFrom*2/5)
		to = max(100, baseTo*7/10) + jitter
	case payloadSize < 8192:
		// Large packets: keep full base range for camouflage.
		return baseFrom, baseTo
	default:
		// Bray-only bulk path (full scMaxEachPostBytes chunks): keep a
		// non-trivial floor so traffic still looks padded, but cap the upper
		// bound so 100KB+ posts are not inflated by hundreds of padding
		// bytes on every POST. Still within [0, baseTo].
		from = max(baseFrom, max(32, baseFrom/2))
		to = max(from, min(baseTo, max(from+64, baseTo/4)+jitter))
		if to > baseTo {
			to = baseTo
		}
		if from > to {
			from = to
		}
		return from, to
	}

	// Clamp: preserve the invariant to ⊆ [0, baseTo]. The max(...) floors
	// above can exceed baseTo for very small configured ranges (e.g.
	// baseTo=5); in that case the floor is meaningless and we must clamp.
	if to > baseTo {
		to = baseTo
	}
	// from must also be within [0, baseTo] — if the floor exceeded baseTo
	// we collapse the range to [baseTo, baseTo] so rand() still works.
	if from > baseTo {
		from = baseTo
	}
	// Defensive: rand() requires from ≤ to.
	if to < from {
		to = from
	}
	return from, to
}

// StrictPaddingRange returns [baseFrom, baseTo] unchanged, regardless of
// payload size. Use this when padding must stay within the base configured
// range for traffic-analysis resistance (the payload-size information is
// fully hidden from observers) and wire-compatibility with stock Xray peers
// (padding is always >= the configured base minimum).
func StrictPaddingRange(baseFrom, baseTo int32) (from, to int32) {
	if baseTo < baseFrom {
		baseFrom, baseTo = baseTo, baseFrom
	}
	if baseFrom < 0 {
		baseFrom = 0
	}
	return baseFrom, baseTo
}

// AcceptedPaddingRange returns the full padding length window a server must accept.
// Clients may shrink padding via AdaptivePaddingRange for small payloads, so the
// accepted lower bound is the adaptive floor while the upper bound stays at base.To.
func AcceptedPaddingRange(baseFrom, baseTo int32) (from, to int32) {
	from, _ = AdaptivePaddingRange(baseFrom, baseTo, 0)
	if baseFrom < from {
		from = baseFrom
	}
	to = baseTo
	if to < from {
		to = from
	}
	return from, to
}

func (c *Config) GetNormalizedXPaddingBytes() *RangeConfig {
	if c.XPaddingBytes == nil || c.XPaddingBytes.To == 0 {
		return defaultRangeConfigXPaddingBytes
	}

	return c.XPaddingBytes
}

func (c *Config) ApplyXPaddingToHeader(h http.Header, config XPaddingConfig) {
	if h == nil {
		return
	}

	paddingValue := GeneratePadding(config.methodIdx, config.Length, config.Strict)

	switch p := config.Placement; p.Placement {
	case PlacementHeader:
		h.Set(p.Header, paddingValue)
	case PlacementQueryInHeader:
		var baseURL *url.URL
		if cached, ok := cachedParseURL(p.RawURL); ok {
			baseURL = cached
		} else {
			return
		}
		// Build Referer-like URL without re-encoding the full query map.
		base := baseURL.Scheme + "://" + baseURL.Host + baseURL.Path
		if baseURL.Opaque != "" && baseURL.Scheme != "" {
			// Fall back to clone path for rare opaque URLs.
			clone := *baseURL
			clone.RawQuery = p.Key + "=" + paddingValue
			h.Set(p.Header, clone.String())
			return
		}
		h.Set(p.Header, base+"?"+p.Key+"="+paddingValue)
	}
}

func (c *Config) ApplyXPaddingToRequest(req *http.Request, config XPaddingConfig) {
	if req == nil {
		return
	}
	if req.Header == nil {
		req.Header = make(http.Header)
	}

	placement := config.Placement.Placement

	if placement == PlacementHeader || placement == PlacementQueryInHeader {
		c.ApplyXPaddingToHeader(req.Header, config)
		return
	}

	paddingValue := GeneratePadding(config.methodIdx, config.Length, config.Strict)

	switch placement {
	case PlacementCookie:
		ApplyPaddingToCookie(req, config.Placement.Key, paddingValue)
	case PlacementQuery:
		ApplyPaddingToQuery(req.URL, config.Placement.Key, paddingValue)
	}
}

func (c *Config) ApplyXPaddingToResponse(writer http.ResponseWriter, config XPaddingConfig) {
	placement := config.Placement.Placement

	if placement == PlacementHeader || placement == PlacementQueryInHeader {
		c.ApplyXPaddingToHeader(writer.Header(), config)
		return
	}

	paddingValue := GeneratePadding(config.methodIdx, config.Length, config.Strict)

	switch placement {
	case PlacementCookie:
		ApplyPaddingToResponseCookie(writer, config.Placement.Key, paddingValue)
	}
}

// brayPaddingHeaderNames is the pool of wire header names for the Bray-only
// default padding placement. The client derives one name per session (stable
// within a connection), the server accepts any pool member. A single fixed
// "X-Padding" would be a one-rule DPI fingerprint; the pool spreads traffic
// across names without adding any wire state.
var brayPaddingHeaderNames = [...]string{
	"X-Padding",
	"X-Request-Id",
	"X-Correlation-Id",
	"X-Client-Trace",
	"X-Request-Trace",
	"X-Session-Id",
	"X-Request-Key",
	"X-Client-Id",
}

// paddingHeaderNameForSession picks the padding header name for a session.
// The choice is deterministic per sessionId (so a connection keeps one stable
// name, like a real client) and stateless on the server side (it accepts any
// pool member). Operator-configured business headers are excluded so the
// padding Set never clobbers a user header.
func (c *Config) paddingHeaderNameForSession(sessionId string) string {
	h := fnv.New32a()
	h.Write([]byte(sessionId))
	idx := int(h.Sum32() % uint32(len(brayPaddingHeaderNames)))
	for i := range brayPaddingHeaderNames {
		name := brayPaddingHeaderNames[(idx+i)%len(brayPaddingHeaderNames)]
		if _, used := c.Headers[name]; !used {
			return name
		}
	}
	return brayPaddingHeaderNames[idx]
}

// hasBrayPaddingHeader reports whether the request carries a padding header
// from the Bray default pool (any member — the server does not need to know
// which name the client derived).
func hasBrayPaddingHeader(req *http.Request) bool {
	if req == nil {
		return false
	}
	for _, name := range brayPaddingHeaderNames {
		if req.Header.Get(name) != "" {
			return true
		}
	}
	return false
}

// extractBrayDefaultXPadding reads padding from Bray-only default locations.
// Primary: any header from the brayPaddingHeaderNames pool. No stock Xray
// Referer?x_padding path.
func extractBrayDefaultXPadding(req *http.Request) (string, string) {
	for _, name := range brayPaddingHeaderNames {
		if v := req.Header.Get(name); v != "" {
			return v, PlacementHeader + "=" + name
		}
	}
	// Rare alternate: query ?xb= for middleboxes that strip custom headers.
	if v := req.URL.Query().Get("xb"); v != "" {
		return v, PlacementQuery + ", key=xb"
	}
	return "", ""
}

// ExtractXPaddingFromRequest extracts the padding value and its placement
// descriptor from an incoming request.
//
// Bray-only (obfsMode=false): header X-Padding (or query xb).
// obfsMode=true: operator-configured locations first, then Bray default.
// Stock Xray Referer?x_padding is intentionally NOT accepted.
func (c *Config) ExtractXPaddingFromRequest(req *http.Request, obfsMode bool) (string, string) {
	if req == nil {
		return "", ""
	}

	if !obfsMode {
		return extractBrayDefaultXPadding(req)
	}

	// obfsMode: try operator-configured locations first.
	key := c.XPaddingKey
	header := c.XPaddingHeader

	if cookie, err := req.Cookie(key); err == nil {
		if cookie != nil && cookie.Value != "" {
			return cookie.Value, PlacementCookie + ", key=" + key
		}
	}

	if headerValue := req.Header.Get(header); headerValue != "" {
		if c.XPaddingPlacement == PlacementHeader {
			return headerValue, PlacementHeader + "=" + header
		}
		if parsedURL, err := url.Parse(headerValue); err == nil {
			if v := parsedURL.Query().Get(key); v != "" {
				return v, PlacementQueryInHeader + "=" + header + ", key=" + key
			}
		}
	}

	if queryValue := req.URL.Query().Get(key); queryValue != "" {
		return queryValue, PlacementQuery + ", key=" + key
	}

	return extractBrayDefaultXPadding(req)
}

func (c *Config) IsPaddingValid(paddingValue string, from, to int32, method PaddingMethod) bool {
	if paddingValue == "" {
		return false
	}
	if to <= 0 {
		r := c.GetNormalizedXPaddingBytes()
		from, to = r.From, r.To
	}

	switch method {
	case PaddingMethodRepeatX:
		n := int32(len(paddingValue))
		return n >= from && n <= to
	case PaddingMethodTokenish:
		const tolerance = int32(validationTolerance)

		n := int32(cachedHuffmanLen(len(paddingValue)))
		f := from - tolerance
		t := to + tolerance
		if f < 0 {
			f = 0
		}
		return n >= f && n <= t
	default:
		n := int32(len(paddingValue))
		return n >= from && n <= to
	}
}
