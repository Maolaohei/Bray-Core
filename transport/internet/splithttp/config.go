package splithttp

import (
	"context"
	"encoding/base64"
	"io"
	"math/rand/v2"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/bytespool"
	"github.com/xtls/xray-core/common/crypto"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/randpool"
	"github.com/xtls/xray-core/common/utils"
	"github.com/xtls/xray-core/common/uuid"
	"github.com/xtls/xray-core/transport/internet"
)

// requestHeaderBaseCache stores a cloneable base header map per Config pointer.
// Headers map is immutable after start in normal use; rebuild on miss only.
var requestHeaderBaseCache sync.Map // *Config -> http.Header

// multiBufferContainerPool reuses packet-up request body wrappers.
// Hot path previously allocated &buf.MultiBufferContainer per PostPacket.
var multiBufferContainerPool = sync.Pool{
	New: func() any {
		return &buf.MultiBufferContainer{}
	},
}

// packetBody is a one-shot io.ReadCloser over a pooled MultiBufferContainer.
// The shell is NOT pooled: net/http.Request.Write always Close()s Body, and
// callers may hold the same pointer afterward. Reusing the shell via sync.Pool
// caused concurrent packet-up posts to steal each other's body (CL != body).
// Only the MultiBufferContainer is pooled.
type packetBody struct {
	c *buf.MultiBufferContainer
}

func (p *packetBody) Read(b []byte) (int, error) {
	if p == nil || p.c == nil {
		return 0, io.EOF
	}
	return p.c.Read(b)
}

func (p *packetBody) Close() error {
	if p == nil || p.c == nil {
		return nil
	}
	// Close releases buffers via ReleaseMulti and keeps MultiBuffer[:0] capacity.
	_ = p.c.Close()
	multiBufferContainerPool.Put(p.c)
	p.c = nil
	return nil
}

func acquirePacketBody(payload buf.MultiBuffer) *packetBody {
	c := multiBufferContainerPool.Get().(*buf.MultiBufferContainer)
	// Reuse container MultiBuffer capacity for the common 1-buffer wrap so the
	// temporary MultiBuffer{FromBytes(...)} outer slice can be GC'd while the
	// Buffer element lives on the container until body Close/ReleaseMulti.
	if len(payload) == 1 {
		mb := c.MultiBuffer[:0]
		if cap(mb) < 1 {
			mb = make(buf.MultiBuffer, 0, 1)
		}
		c.MultiBuffer = append(mb, payload[0])
	} else {
		c.MultiBuffer = payload
	}
	return &packetBody{c: c}
}

// durableBody is a one-shot io.ReadCloser over an external durable []byte.
// Used by FillPacketRequestBytes so retries skip MultiBuffer/FromBytes shells.
// Shell is not pooled (same concurrent Close/reuse hazard as packetBody).
type durableBody struct {
	b []byte
	i int
}

func (p *durableBody) Read(b []byte) (int, error) {
	if p == nil || p.i >= len(p.b) {
		return 0, io.EOF
	}
	n := copy(b, p.b[p.i:])
	p.i += n
	if p.i >= len(p.b) {
		return n, io.EOF
	}
	return n, nil
}

func (p *durableBody) Close() error {
	if p == nil {
		return nil
	}
	// Idempotent: drop the view; caller owns the durable snapshot bytes.
	p.b = nil
	p.i = 0
	return nil
}

func acquireDurableBody(data []byte) *durableBody {
	return &durableBody{b: data}
}

// Pre-allocated default RangeConfig values to avoid per-request heap allocations.
// XMUX nil fields use process-stable jittered copies (green-zone); explicit config wins.
var (
	defaultRangeConfigMaxPostBytes         = &RangeConfig{From: 1000000, To: 1000000}
	defaultRangeConfigMinPostInterval      = &RangeConfig{From: 30, To: 30}
	defaultRangeConfigStreamUpSecs         = &RangeConfig{From: 20, To: 80}
	defaultRangeConfigUplinkChunkCookie    = &RangeConfig{From: 2 * 1024, To: 3 * 1024}
	defaultRangeConfigUplinkChunkHeader    = &RangeConfig{From: 3 * 1000, To: 4 * 1000}
	defaultRangeConfigXmuxMaxConcurrency   = &RangeConfig{From: 8, To: 16}   // browser-like stream concurrency
	defaultRangeConfigXmuxMaxConnections   = &RangeConfig{From: 2, To: 4}    // small connection pool
	defaultRangeConfigXmuxCMaxReuseTimes   = &RangeConfig{From: 64, To: 128} // rotate before endless reuse
	defaultRangeConfigXmuxHMaxRequestTimes = &RangeConfig{From: 400, To: 800}
	defaultRangeConfigXmuxHMaxReusableSecs = &RangeConfig{From: 600, To: 1200} // 10-20 min lifecycle

	// Process-stable jittered XMUX defaults (green-zone anti-fleet fingerprint).
	// Initialized once; getters return these when the operator left the field nil.
	defaultXmuxJitteredMaxConcurrency   *RangeConfig
	defaultXmuxJitteredMaxConnections   *RangeConfig
	defaultXmuxJitteredCMaxReuseTimes   *RangeConfig
	defaultXmuxJitteredHMaxRequestTimes *RangeConfig
	defaultXmuxJitteredHMaxReusableSecs *RangeConfig
	defaultXmuxJitterOnce               sync.Once
)

// Browser-band clamps for process-stable XMUX default jitter (±10% of base).
const (
	xmuxJitterPct = 10 // percent of base From/To applied as delta bound

	xmuxClampConcurrencyFromMin = 4
	xmuxClampConcurrencyToMax   = 32
	xmuxClampConnectionsFromMin = 1
	xmuxClampConnectionsToMax   = 8
	xmuxClampReuseFromMin       = 32
	xmuxClampReuseToMax         = 256
	xmuxClampReqTimesFromMin    = 200
	xmuxClampReqTimesToMax      = 1600
	xmuxClampSecsFromMin        = 300
	xmuxClampSecsToMax          = 1800
)

func ensureDefaultXmuxJittered() {
	defaultXmuxJitterOnce.Do(func() {
		seed := uint64(crypto.RandBetween(1, 1<<62))
		defaultXmuxJitteredMaxConcurrency = jitterDefaultRange(
			defaultRangeConfigXmuxMaxConcurrency, seed, 1,
			xmuxClampConcurrencyFromMin, xmuxClampConcurrencyToMax)
		defaultXmuxJitteredMaxConnections = jitterDefaultRange(
			defaultRangeConfigXmuxMaxConnections, seed, 2,
			xmuxClampConnectionsFromMin, xmuxClampConnectionsToMax)
		defaultXmuxJitteredCMaxReuseTimes = jitterDefaultRange(
			defaultRangeConfigXmuxCMaxReuseTimes, seed, 3,
			xmuxClampReuseFromMin, xmuxClampReuseToMax)
		defaultXmuxJitteredHMaxRequestTimes = jitterDefaultRange(
			defaultRangeConfigXmuxHMaxRequestTimes, seed, 4,
			xmuxClampReqTimesFromMin, xmuxClampReqTimesToMax)
		defaultXmuxJitteredHMaxReusableSecs = jitterDefaultRange(
			defaultRangeConfigXmuxHMaxReusableSecs, seed, 5,
			xmuxClampSecsFromMin, xmuxClampSecsToMax)
	})
}

// jitterDefaultRange returns a process-stable ±pct copy of base, clamped to [minV,maxV].
// Explicit operator ranges are never routed here.
func jitterDefaultRange(base *RangeConfig, seed uint64, paramID uint64, minV, maxV int32) *RangeConfig {
	if base == nil {
		return &RangeConfig{From: 0, To: 0}
	}
	r := rand.New(rand.NewPCG(seed^paramID, seed+paramID*0x9e3779b97f4a7c15))
	from := jitterBound(base.From, r, minV, maxV)
	to := jitterBound(base.To, r, minV, maxV)
	if from > to {
		from, to = to, from
	}
	baseSpan := base.To - base.From
	if baseSpan < 0 {
		baseSpan = -baseSpan
	}
	if to-from < baseSpan {
		need := baseSpan - (to - from)
		roomHi := maxV - to
		if roomHi > need {
			to += need
			need = 0
		} else {
			to = maxV
			need -= roomHi
		}
		if need > 0 {
			roomLo := from - minV
			if roomLo > need {
				from -= need
			} else {
				from = minV
			}
		}
		if from > to {
			from, to = to, from
		}
	}
	return &RangeConfig{From: from, To: to}
}

func jitterBound(v int32, r *rand.Rand, minV, maxV int32) int32 {
	if v <= 0 {
		return v
	}
	delta := v * xmuxJitterPct / 100
	if delta < 1 {
		delta = 1
	}
	span := int32(2*delta + 1)
	off := r.Int32N(span) - delta
	out := v + off
	if out < minV {
		out = minV
	}
	if out > maxV {
		out = maxV
	}
	return out
}

func (c *Config) GetNormalizedPath() string {
	pathAndQuery := strings.SplitN(c.Path, "?", 2)
	path := pathAndQuery[0]

	if path == "" || path[0] != '/' {
		path = "/" + path
	}

	if c.GetNormalizedSessionPlacement() == PlacementPath ||
		c.GetNormalizedSeqPlacement() == PlacementPath {
		if path[len(path)-1] != '/' {
			path = path + "/"
		}
	}

	return path
}

func (c *Config) GetNormalizedQuery() string {
	pathAndQuery := strings.SplitN(c.Path, "?", 2)
	query := ""

	if len(pathAndQuery) > 1 {
		query = pathAndQuery[1]
	}

	/*
		if query != "" {
			query += "&"
		}
		query += "x_version=" + core.Version()
	*/

	return query
}

// headerMapPool reuses outer http.Header maps for packet/stream request fill.
// Values are cleared on Put; single-value slices may still share immutable base.
var headerMapPool = sync.Pool{
	New: func() any {
		return make(http.Header, 8)
	},
}

func acquireHeaderMap(sizeHint int) http.Header {
	h := headerMapPool.Get().(http.Header)
	if sizeHint > 0 && len(h) == 0 {
		// Grow by inserting then deleting is pointless; just return pooled map.
		_ = sizeHint
	}
	return h
}

// releaseHeaderMap returns a request-local header map to the pool after the
// HTTP round-trip finished. Safe only when no transport retains the map.
func releaseHeaderMap(h http.Header) {
	if h == nil {
		return
	}
	for k := range h {
		delete(h, k)
	}
	headerMapPool.Put(h)
}

// cloneHeaderShallow builds a request-local http.Header from an immutable base.
// Outer map comes from headerMapPool. Single-value non-Cookie slices are shared
// (Set replaces the entry). Multi-value and Cookie slices are copied so Add/append
// cannot poison the cache. Avoids http.Header.Clone's deeper bookkeeping.
func cloneHeaderShallow(src http.Header) http.Header {
	if src == nil {
		return acquireHeaderMap(0)
	}
	dst := acquireHeaderMap(len(src))
	for k, vv := range src {
		switch len(vv) {
		case 0:
			dst[k] = nil
		case 1:
			// Cookie may be Header.Add'd (AddCookie / multi cookies); give it
			// exclusive capacity so append never mutates the cached base.
			if k == "Cookie" {
				cp := make([]string, 1, 2)
				cp[0] = vv[0]
				dst[k] = cp
			} else {
				// Set replaces the whole entry; sharing the 1-elem slice is safe.
				dst[k] = vv
			}
		default:
			cp := make([]string, len(vv))
			copy(cp, vv)
			dst[k] = cp
		}
	}
	return dst
}

// knownBrayControlKeys is the allowlist of x-bray-* local control headers.
// Anything else with the x-bray- prefix is a typo that today silently falls
// back to defaults — warn once so operators notice.
var knownBrayControlKeys = map[string]struct{}{
	"x-bray-session-secret":      {},
	"x-bray-session-uuid":        {},
	"x-bray-mode-degrade":        {},
	"x-bray-sticky-mode":         {},
	"x-bray-sticky-mode-ttl":     {},
	"x-bray-sticky-endpoint":     {},
	"x-bray-sticky-endpoint-ttl": {},
	"x-bray-multi-endpoint":      {},
	"x-bray-endpoints":           {},
	"x-bray-sse":                 {},
	"x-bray-x-accel":             {},
}

func validateBrayControlHeaders(headers map[string]string) {
	if headers == nil {
		return
	}
	for k := range headers {
		lk := strings.ToLower(strings.TrimSpace(k))
		if strings.HasPrefix(lk, "x-bray-") {
			if _, ok := knownBrayControlKeys[lk]; !ok {
				errors.LogWarning(context.Background(), "unknown x-bray-* control header: ", k, " (typo? silently falls back to defaults)")
			}
		}
	}
}

func (c *Config) GetRequestHeader() http.Header {
	if c != nil {
		if cached, ok := requestHeaderBaseCache.Load(c); ok {
			return cloneHeaderShallow(cached.(http.Header))
		}
	}
	header := http.Header{}
	if c != nil {
		// Warn about unknown x-bray-* control headers (typos fall back silently).
		validateBrayControlHeaders(c.Headers)
		// Deterministic header order: map iteration order is random, and a
		// per-request shuffled header order is a recognizable bot fingerprint
		// (real browsers send stable orders). Sorted once per config thanks to
		// requestHeaderBaseCache.
		keys := make([]string, 0, len(c.Headers))
		for k := range c.Headers {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			// Bray control headers (x-bray-*) are client-local only; never send on wire.
			if isBrayControlHeader(k) {
				continue
			}
			header.Add(k, c.Headers[k])
		}
	}
	utils.TryDefaultHeadersWith(header, "fetch")
	// DisableCompression=true on the http2.Transport suppresses Go's automatic
	// Accept-Encoding header; real browsers always send it, so its absence is
	// a machine fingerprint. Emit a browser-like value — the XHTTP server
	// never compresses (binary packet bodies), so this is purely cosmetic.
	if c != nil && header.Get("Accept-Encoding") == "" {
		header.Set("Accept-Encoding", "gzip, deflate, br, zstd")
	}
	if c != nil {
		// Store a frozen clone so callers mutating the returned header never
		// poison the cache.
		requestHeaderBaseCache.Store(c, header.Clone())
	}
	return header
}

// isBrayControlHeader reports client-local Bray-V2 control keys used for
// opt-in features (mode degrade, multi-endpoint). They must not leave the process.

// requestURLString returns a stable URL string for padding placement.
// Prefer already-materialized RequestURI/URL.String only once.
func requestURLString(req *http.Request) string {
	if req == nil || req.URL == nil {
		return ""
	}
	u := req.URL
	// Prefer manual Scheme://Host/Path?query to avoid url.URL.String re-encoding.
	if u.Opaque != "" {
		return u.String()
	}
	if u.Scheme != "" && u.Host != "" {
		path := u.EscapedPath()
		if path == "" {
			path = "/"
		}
		if u.RawQuery != "" {
			return u.Scheme + "://" + u.Host + path + "?" + u.RawQuery
		}
		return u.Scheme + "://" + u.Host + path
	}
	return u.String()
}

func isBrayControlHeader(key string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(key)), "x-bray-")
}

func (c *Config) GetRequestHeaderWithPayload(payload []byte) http.Header {
	header := c.GetRequestHeader()

	key := c.UplinkDataKey
	encodedData := base64.RawURLEncoding.EncodeToString(payload)

	for i := 0; len(encodedData) > 0; i++ {
		chunkSize := min(int(c.GetNormalizedUplinkChunkSize().rand()), len(encodedData))
		chunk := encodedData[:chunkSize]
		encodedData = encodedData[chunkSize:]
		headerKey := key + "-" + strconv.Itoa(i)
		header.Set(headerKey, chunk)
	}

	return header
}

func (c *Config) GetRequestCookiesWithPayload(payload []byte) []*http.Cookie {
	cookies := []*http.Cookie{}

	key := c.UplinkDataKey
	encodedData := base64.RawURLEncoding.EncodeToString(payload)

	for i := 0; len(encodedData) > 0; i++ {
		chunkSize := min(int(c.GetNormalizedUplinkChunkSize().rand()), len(encodedData))
		chunk := encodedData[:chunkSize]
		encodedData = encodedData[chunkSize:]
		cookieName := key + "_" + strconv.Itoa(i)
		cookies = append(cookies, &http.Cookie{Name: cookieName, Value: chunk})
	}

	return cookies
}

func (c *Config) WriteResponseHeader(writer http.ResponseWriter, requestMethod string, requestHeader http.Header) {
	// Only emit CORS headers when the request actually carries an Origin (a real
	// cross-origin browser request via the browser dialer). Non-browser clients
	// never inspect CORS, and a real static origin / CDN would not stamp an
	// unconditional "Access-Control-Allow-Origin: *" onto every response — doing
	// so is a constant, camouflage-breaking fingerprint. Stay silent otherwise.
	origin := requestHeader.Get("Origin")
	if origin == "" {
		return
	}

	// Chrome says: The value of the 'Access-Control-Allow-Origin' header in the response must not be the wildcard '*' when the request's credentials mode is 'include'.
	// Echo the request Origin instead of the wildcard.
	writer.Header().Set("Access-Control-Allow-Origin", origin)

	if c.GetNormalizedSessionPlacement() == PlacementCookie ||
		c.GetNormalizedSeqPlacement() == PlacementCookie ||
		c.XPaddingPlacement == PlacementCookie ||
		c.GetNormalizedUplinkDataPlacement() == PlacementCookie {
		writer.Header().Set("Access-Control-Allow-Credentials", "true")
	}

	if requestMethod == "OPTIONS" {
		requestedMethod := requestHeader.Get("Access-Control-Request-Method")
		if requestedMethod != "" {
			writer.Header().Set("Access-Control-Allow-Methods", requestedMethod)
		} else {
			writer.Header().Set("Access-Control-Allow-Methods", "*")
		}

		requestedHeaders := requestHeader.Get("Access-Control-Request-Headers")
		if requestedHeaders == "" {
			writer.Header().Set("Access-Control-Allow-Headers", "*")
		} else {
			writer.Header().Set("Access-Control-Allow-Headers", requestedHeaders)
		}
	}
}

func (c *Config) GetNormalizedUplinkHTTPMethod() string {
	if c.UplinkHTTPMethod == "" {
		return "POST"
	}

	return c.UplinkHTTPMethod
}

func (c *Config) GetNormalizedScMaxEachPostBytes() *RangeConfig {
	if c.ScMaxEachPostBytes == nil || c.ScMaxEachPostBytes.To == 0 {
		return defaultRangeConfigMaxPostBytes
	}

	return c.ScMaxEachPostBytes
}

func (c *Config) GetNormalizedScMinPostsIntervalMs() *RangeConfig {
	if c.ScMinPostsIntervalMs == nil || c.ScMinPostsIntervalMs.To == 0 {
		return defaultRangeConfigMinPostInterval
	}

	return c.ScMinPostsIntervalMs
}

func (c *Config) GetNormalizedScMaxBufferedPosts() int {
	if c.ScMaxBufferedPosts == 0 {
		return 64
	}

	return int(c.ScMaxBufferedPosts)
}

func (c *Config) GetNormalizedScSessionTtlSecs() int32 {
	if c.ScSessionTtlSecs <= 0 {
		return 45
	}
	return c.ScSessionTtlSecs
}

func (c *Config) GetNormalizedScStreamUpServerSecs() *RangeConfig {
	if c.ScStreamUpServerSecs == nil || c.ScStreamUpServerSecs.To == 0 {
		return defaultRangeConfigStreamUpSecs
	}

	return c.ScStreamUpServerSecs
}

func (c *Config) GetNormalizedUplinkChunkSize() *RangeConfig {
	if c.UplinkChunkSize == nil || c.UplinkChunkSize.To == 0 {
		switch c.UplinkDataPlacement {
		case PlacementCookie:
			return defaultRangeConfigUplinkChunkCookie
		case PlacementHeader:
			return defaultRangeConfigUplinkChunkHeader
		default:
			return c.GetNormalizedScMaxEachPostBytes()
		}
	} else if c.UplinkChunkSize.From < 64 {
		return &RangeConfig{
			From: 64,
			To:   max(64, c.UplinkChunkSize.To),
		}
	}

	return c.UplinkChunkSize
}

func (c *Config) GetNormalizedServerMaxHeaderBytes() int {
	if c.ServerMaxHeaderBytes <= 0 {
		return 8192
	} else {
		return int(c.ServerMaxHeaderBytes)
	}
}

func (c *Config) GetNormalizedSessionPlacement() string {
	if c.SessionIDPlacement == "" {
		return PlacementPath
	}
	return c.SessionIDPlacement
}

func (c *Config) GetNormalizedSeqPlacement() string {
	if c.SeqPlacement == "" {
		return PlacementPath
	}
	return c.SeqPlacement
}

func (c *Config) GetNormalizedUplinkDataPlacement() string {
	if c.UplinkDataPlacement == "" {
		return PlacementBody
	}
	return c.UplinkDataPlacement
}

func (c *Config) GetNormalizedSessionKey() string {
	if c.SessionIDKey != "" {
		return c.SessionIDKey
	}
	switch c.GetNormalizedSessionPlacement() {
	case PlacementHeader:
		return "X-Session"
	case PlacementCookie, PlacementQuery:
		return "x_session"
	default:
		return ""
	}
}

func (c *Config) GetNormalizedSeqKey() string {
	if c.SeqKey != "" {
		return c.SeqKey
	}
	switch c.GetNormalizedSeqPlacement() {
	case PlacementHeader:
		return "X-Seq"
	case PlacementCookie, PlacementQuery:
		return "x_seq"
	default:
		return ""
	}
}

func (c *Config) ApplyMetaToRequest(req *http.Request, sessionId string, seqStr string) {
	sessionPlacement := c.GetNormalizedSessionPlacement()
	seqPlacement := c.GetNormalizedSeqPlacement()

	// Default wire (session+seq both path): one path rewrite, no Query/Cookie work.
	if sessionPlacement == PlacementPath && (seqStr == "" || seqPlacement == PlacementPath) {
		if sessionId != "" || seqStr != "" {
			// Bray-only obfuscated wire: merge into a single opaque token
			// segment. The legacy layout exposed "raw.mac" dotted sessionId
			// and a bare incrementing seq as two fixed path segments — a
			// regex-level fingerprint. The token has no structure until
			// decoded; the server accepts both formats.
			req.URL.Path = appendToPath2(req.URL.Path, encodeMetaToken(sessionId, seqStr), "")
		}
		return
	}

	sessionKey := c.GetNormalizedSessionKey()
	seqKey := c.GetNormalizedSeqKey()

	if sessionId != "" {
		switch sessionPlacement {
		case PlacementPath:
			req.URL.Path = appendToPath(req.URL.Path, sessionId)
		case PlacementQuery:
			q := req.URL.Query()
			q.Set(sessionKey, sessionId)
			req.URL.RawQuery = q.Encode()
		case PlacementHeader:
			req.Header.Set(sessionKey, sessionId)
		case PlacementCookie:
			req.AddCookie(&http.Cookie{Name: sessionKey, Value: sessionId})
		}
	}

	if seqStr != "" {
		switch seqPlacement {
		case PlacementPath:
			req.URL.Path = appendToPath(req.URL.Path, seqStr)
		case PlacementQuery:
			q := req.URL.Query()
			q.Set(seqKey, seqStr)
			req.URL.RawQuery = q.Encode()
		case PlacementHeader:
			req.Header.Set(seqKey, seqStr)
		case PlacementCookie:
			req.AddCookie(&http.Cookie{Name: seqKey, Value: seqStr})
		}
	}
}

func (c *Config) FillStreamRequest(request *http.Request, sessionId string, seqStr string) {
	request.Header = c.GetRequestHeader()
	basePad := c.GetNormalizedXPaddingBytes()
	// Stream open has no payload size yet; keep configured range (skewed).
	length := int(biasedRangeRand(basePad.From, basePad.To))
	config := XPaddingConfig{Length: length}

	if c.XPaddingObfsMode {
		// Query/Referer placements need a stable URL string; header-only does not.
		rawURL := ""
		if c.XPaddingPlacement == PlacementQueryInHeader || c.XPaddingPlacement == PlacementQuery {
			rawURL = requestURLString(request)
		}
		config.Placement = XPaddingPlacement{
			Placement: c.XPaddingPlacement,
			Key:       c.XPaddingKey,
			Header:    c.XPaddingHeader,
			RawURL:    rawURL,
		}
		config.Method = PaddingMethod(c.XPaddingMethod)
		config.methodIdx = methodIndex(config.Method)
	} else {
		// Bray-only default wire: avoid stock Xray Referer?x_padding fingerprint.
		// Both ends use header from the padding-name pool (not query/Referer)
		// unless obfs mode. Tokenish shapes (base62/UUID/hex mix) — NOT the
		// legacy repeat-x: an all-"X" value is a one-glance fake to any
		// observer. Server validates tokenish via huffman length.
		config.Method = PaddingMethodTokenish
		config.methodIdx = methodIndex(config.Method)
		config.Placement = XPaddingPlacement{
			Placement: PlacementHeader,
			Key:       "X-Padding",
			Header:    "X-Padding",
		}
	}

	c.ApplyXPaddingToRequest(request, config)
	c.ApplyMetaToRequest(request, sessionId, "")

	if request.Body != nil && !c.NoGRPCHeader { // stream-up/one
		// Bray-only: application/grpc is a common XHTTP stream fingerprint,
		// and application/octet-stream is not something browsers upload via
		// fetch. sendBeacon-style text/plain is the browser-natural choice;
		// the server never inspects Content-Type.
		request.Header.Set("Content-Type", "text/plain;charset=UTF-8")
	}
}

func (c *Config) FillPacketRequest(request *http.Request, sessionId string, seqStr string, payload buf.MultiBuffer) error {
	dataPlacement := c.GetNormalizedUplinkDataPlacement()
	payloadLen := 0
	if payload != nil {
		payloadLen = int(payload.Len())
	}

	if dataPlacement == PlacementBody || dataPlacement == PlacementAuto {
		request.Header = c.GetRequestHeader()
		// Pooled body wrapper: Close releases MultiBuffer and returns container.
		request.Body = acquirePacketBody(payload)
		request.ContentLength = int64(payloadLen)
	} else {
		dataLen := payload.Len()
		data := bytespool.Alloc(int32(dataLen))
		data = data[:dataLen]
		payload.Copy(data)
		buf.ReleaseMulti(payload)
		switch dataPlacement {
		case PlacementHeader:
			request.Header = c.GetRequestHeaderWithPayload(data)
		case PlacementCookie:
			request.Header = c.GetRequestHeader()
			for _, cookie := range c.GetRequestCookiesWithPayload(data) {
				request.AddCookie(cookie)
			}
		}
		bytespool.Free(data)
	}

	return c.finishPacketRequest(request, sessionId, seqStr, payloadLen)
}

// FillPacketRequestBytes is the zero-copy body path for durable packet-up
// snapshots (retry source already materialised as []byte). Body Close never
// frees data; ownership stays with the caller for the whole retry window.
func (c *Config) FillPacketRequestBytes(request *http.Request, sessionId string, seqStr string, data []byte) error {
	dataPlacement := c.GetNormalizedUplinkDataPlacement()
	payloadLen := len(data)

	if dataPlacement == PlacementBody || dataPlacement == PlacementAuto {
		request.Header = c.GetRequestHeader()
		request.Body = acquireDurableBody(data)
		request.ContentLength = int64(payloadLen)
	} else {
		switch dataPlacement {
		case PlacementHeader:
			request.Header = c.GetRequestHeaderWithPayload(data)
		case PlacementCookie:
			request.Header = c.GetRequestHeader()
			for _, cookie := range c.GetRequestCookiesWithPayload(data) {
				request.AddCookie(cookie)
			}
		default:
			request.Header = c.GetRequestHeader()
			request.Body = acquireDurableBody(data)
			request.ContentLength = int64(payloadLen)
		}
	}

	return c.finishPacketRequest(request, sessionId, seqStr, payloadLen)
}

func (c *Config) finishPacketRequest(request *http.Request, sessionId string, seqStr string, payloadLen int) error {
	basePad := c.GetNormalizedXPaddingBytes()
	var from, to int32
	var strict bool
	if c.XPaddingStrictMinPadding {
		// Strict mode: padding is always drawn from the full [base.From,
		// base.To] range regardless of payload size. This hides
		// payload-size information from observers and is wire-compatible
		// with stock Xray peers (padding is always >= the configured
		// base minimum).
		from, to = StrictPaddingRange(basePad.From, basePad.To)
		strict = true
	} else {
		// Non-strict (default): adaptive range shrinks padding for small
		// payloads to save bandwidth. The lower bound may dip below
		// base.From, so the server must accept the adaptive floor — this
		// is NOT wire-compatible with stock Xray peers that only know
		// about [base.From, base.To].
		from, to = AdaptivePaddingRange(basePad.From, basePad.To, payloadLen)
	}
	// Avoid heap-escaping &RangeConfig{...} per packet-up POST.
	// Skewed draw: short paddings dominate, occasional long tails.
	length := int(biasedRangeRand(from, to))
	config := XPaddingConfig{Length: length, Strict: strict}

	if c.XPaddingObfsMode {
		rawURL := ""
		if c.XPaddingPlacement == PlacementQueryInHeader || c.XPaddingPlacement == PlacementQuery {
			rawURL = requestURLString(request)
		}
		config.Placement = XPaddingPlacement{
			Placement: c.XPaddingPlacement,
			Key:       c.XPaddingKey,
			Header:    c.XPaddingHeader,
			RawURL:    rawURL,
		}
		config.Method = PaddingMethod(c.XPaddingMethod)
		config.methodIdx = methodIndex(config.Method)
	} else {
		// Bray-only default wire: session-derived header name from the
		// padding-name pool (server accepts any member). Avoids the stock
		// Xray Referer?x_padding fingerprint AND a fixed "X-Padding" rule.
		// Header placement needs no RawURL string build. Tokenish shapes,
		// not repeat-x (see FillStreamRequest).
		config.Method = PaddingMethodTokenish
		config.methodIdx = methodIndex(config.Method)
		name := c.paddingHeaderNameForSession(sessionId)
		config.Placement = XPaddingPlacement{
			Placement: PlacementHeader,
			Key:       name,
			Header:    name,
		}
	}

	c.ApplyXPaddingToRequest(request, config)
	c.ApplyMetaToRequest(request, sessionId, seqStr)

	return nil
}

func (c *Config) ExtractMetaFromRequest(req *http.Request, path string) (sessionId string, seqStr string) {
	sessionPlacement := c.GetNormalizedSessionPlacement()
	seqPlacement := c.GetNormalizedSeqPlacement()
	sessionKey := c.GetNormalizedSessionKey()
	seqKey := c.GetNormalizedSeqKey()

	var subpath []string
	pathPart := 0
	tokenConsumed := false // obfuscated single-token format carried both fields
	if sessionPlacement == PlacementPath || seqPlacement == PlacementPath {
		subpath = strings.Split(req.URL.Path[len(path):], "/")
	}

	switch sessionPlacement {
	case PlacementPath:
		if len(subpath) > pathPart {
			// Bray-only: try the obfuscated single-token format first, then
			// fall back to the legacy two-segment layout.
			if sid, seq, ok := decodeMetaToken(subpath[pathPart]); ok {
				sessionId = sid
				seqStr = seq
				tokenConsumed = true
			} else {
				sessionId = subpath[pathPart]
			}
			pathPart += 1
		}
	case PlacementQuery:
		sessionId = req.URL.Query().Get(sessionKey)
	case PlacementHeader:
		sessionId = req.Header.Get(sessionKey)
	case PlacementCookie:
		if cookie, e := req.Cookie(sessionKey); e == nil {
			sessionId = cookie.Value
		}
	}

	switch seqPlacement {
	case PlacementPath:
		if !tokenConsumed && len(subpath) > pathPart {
			seqStr = subpath[pathPart]
			pathPart += 1
		}
	case PlacementQuery:
		seqStr = req.URL.Query().Get(seqKey)
	case PlacementHeader:
		seqStr = req.Header.Get(seqKey)
	case PlacementCookie:
		if cookie, e := req.Cookie(seqKey); e == nil {
			seqStr = cookie.Value
		}
	}

	return sessionId, seqStr
}

// encodeMetaToken merges sessionId and seq into a single opaque path segment
// (base64url of "sessionId:seq"). See ApplyMetaToRequest for the rationale.
// Deliberately no length padding: a random pad would cost ~10% more per-
// request allocation (longer base64 output) to hide only a weak 53-58 char
// length signature — not worth it under the perf budget.
func encodeMetaToken(sessionId, seqStr string) string {
	if sessionId == "" && seqStr == "" {
		return ""
	}
	raw := make([]byte, 0, len(sessionId)+1+len(seqStr))
	raw = append(raw, sessionId...)
	raw = append(raw, ':')
	raw = append(raw, seqStr...)
	return base64.RawURLEncoding.EncodeToString(raw)
}

// decodeMetaToken reverses encodeMetaToken. The ":" separator cannot collide
// with legacy values: the old sessionId is "uuid.mac" (dotted, no colon) and
// the old seq is a bare decimal string.
func decodeMetaToken(token string) (sessionId, seqStr string, ok bool) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", "", false
	}
	s := string(raw)
	idx := strings.IndexByte(s, ':')
	if idx < 0 {
		return "", "", false
	}
	return s[:idx], s[idx+1:], true
}

func (m *XmuxConfig) GetNormalizedMaxConcurrency() *RangeConfig {
	if m.MaxConcurrency == nil {
		ensureDefaultXmuxJittered()
		return defaultXmuxJitteredMaxConcurrency
	}

	return m.MaxConcurrency
}

func (m *XmuxConfig) GetNormalizedMaxConnections() *RangeConfig {
	if m.MaxConnections == nil {
		ensureDefaultXmuxJittered()
		return defaultXmuxJitteredMaxConnections
	}

	return m.MaxConnections
}

func (m *XmuxConfig) GetNormalizedCMaxReuseTimes() *RangeConfig {
	if m.CMaxReuseTimes == nil {
		ensureDefaultXmuxJittered()
		return defaultXmuxJitteredCMaxReuseTimes
	}

	return m.CMaxReuseTimes
}

func (m *XmuxConfig) GetNormalizedHMaxRequestTimes() *RangeConfig {
	if m.HMaxRequestTimes == nil {
		ensureDefaultXmuxJittered()
		return defaultXmuxJitteredHMaxRequestTimes
	}

	return m.HMaxRequestTimes
}

func (m *XmuxConfig) GetNormalizedHMaxReusableSecs() *RangeConfig {
	if m.HMaxReusableSecs == nil {
		ensureDefaultXmuxJittered()
		return defaultXmuxJitteredHMaxReusableSecs
	}

	return m.HMaxReusableSecs
}

func init() {
	common.Must(internet.RegisterProtocolConfigCreator(protocolName, func() interface{} {
		return new(Config)
	}))
}

func (c *RangeConfig) rand() int32 {
	if c == nil {
		return 0
	}
	return int32(crypto.RandBetween(int64(c.From), int64(c.To)))
}

// biasedRangeRand returns a right-skewed value in [from, to]: most draws land
// near the low end with occasional long tails. Uniform draws on wire-visible
// parameters (padding lengths, connection lifetimes) are a machine signature;
// the skewed shape is closer to real client behavior. Purely a client-side
// generation choice — validation ranges are unchanged.
func biasedRangeRand(from, to int32) int32 {
	if to <= from {
		return from
	}
	r := int64(randpool.Global.IntN(1024))
	return from + int32((int64(to)-int64(from))*r*r/(1024*1024))
}

// predefined
var PredefinedTable = map[string]string{
	"ALPHABET": "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
	"Alphabet": "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
	"BASE36":   "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ",
	"Base62":   "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
	"HEX":      "0123456789ABCDEF",
	"alphabet": "abcdefghijklmnopqrstuvwxyz",
	"base36":   "0123456789abcdefghijklmnopqrstuvwxyz",
	"hex":      "0123456789abcdef",
	"number":   "0123456789",
}

func (c *Config) GenerateSessionID() string {
	length := c.SessionIDLength.rand()
	table := c.SessionIDTable
	if predefined, ok := PredefinedTable[table]; ok {
		table = predefined
	}
	if table != "" && length > 0 {
		id := make([]byte, length)
		for i := range id {
			id[i] = table[rand.N(len(table))]
		}
		return signSessionID(string(id), c.sessionSecret())
	}
	uuid := uuid.New()
	return signSessionID(uuid.String(), c.sessionSecret())
}

func appendToPath(path, value string) string {
	if value == "" {
		return path
	}
	if strings.HasSuffix(path, "/") {
		return path + value
	}
	return path + "/" + value
}

// appendToPath2 appends up to two path segments in a single allocation.
// Used by the default session+seq PlacementPath packet-up meta path.
// Built with one []byte buffer (no strings.Builder indirection).
func appendToPath2(path, a, b string) string {
	if a == "" && b == "" {
		return path
	}
	needSlash := !strings.HasSuffix(path, "/")
	extra := 0
	if needSlash {
		extra++
	}
	extra += len(a)
	if a != "" && b != "" {
		extra++ // '/' between a and b
	}
	extra += len(b)
	buf := make([]byte, 0, len(path)+extra)
	buf = append(buf, path...)
	if needSlash {
		buf = append(buf, '/')
	}
	if a != "" {
		buf = append(buf, a...)
		if b != "" {
			buf = append(buf, '/')
		}
	}
	if b != "" {
		buf = append(buf, b...)
	}
	return string(buf)
}
