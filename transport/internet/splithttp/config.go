package splithttp

import (
	"encoding/base64"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/bytespool"
	"github.com/xtls/xray-core/common/crypto"
	"github.com/xtls/xray-core/common/utils"
	"github.com/xtls/xray-core/common/uuid"
	"github.com/xtls/xray-core/transport/internet"
)

// Pre-allocated default RangeConfig values to avoid per-request heap allocations.
var (
	defaultRangeConfigMaxPostBytes         = &RangeConfig{From: 1000000, To: 1000000}
	defaultRangeConfigMinPostInterval      = &RangeConfig{From: 30, To: 30}
	defaultRangeConfigStreamUpSecs         = &RangeConfig{From: 20, To: 80}
	defaultRangeConfigUplinkChunkCookie    = &RangeConfig{From: 2 * 1024, To: 3 * 1024}
	defaultRangeConfigUplinkChunkHeader    = &RangeConfig{From: 3 * 1000, To: 4 * 1000}
	defaultRangeConfigZero                 = &RangeConfig{From: 0, To: 0}
	defaultRangeConfigXmuxMaxConcurrency   = &RangeConfig{From: 8, To: 16}   // browser-like stream concurrency
	defaultRangeConfigXmuxMaxConnections   = &RangeConfig{From: 2, To: 4}    // small connection pool
	defaultRangeConfigXmuxCMaxReuseTimes   = &RangeConfig{From: 64, To: 128} // rotate before endless reuse
	defaultRangeConfigXmuxHMaxRequestTimes = &RangeConfig{From: 400, To: 800}
	defaultRangeConfigXmuxHMaxReusableSecs = &RangeConfig{From: 600, To: 1200} // 10-20 min lifecycle
)

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

func (c *Config) GetRequestHeader() http.Header {
	header := http.Header{}
	for k, v := range c.Headers {
		// Bray control headers (x-bray-*) are client-local only; never send on wire.
		if isBrayControlHeader(k) {
			continue
		}
		header.Add(k, v)
	}
	utils.TryDefaultHeadersWith(header, "fetch")
	return header
}

// isBrayControlHeader reports client-local Bray-V2 control keys used for
// opt-in features (mode degrade, multi-endpoint). They must not leave the process.
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
	// CORS headers for the browser dialer
	if origin := requestHeader.Get("Origin"); origin == "" {
		writer.Header().Set("Access-Control-Allow-Origin", "*")
	} else {
		// Chrome says: The value of the 'Access-Control-Allow-Origin' header in the response must not be the wildcard '*' when the request's credentials mode is 'include'.
		writer.Header().Set("Access-Control-Allow-Origin", origin)
	}

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
	// Stream open has no payload size yet; keep configured range.
	length := int(basePad.rand())
	config := XPaddingConfig{Length: length}

	if c.XPaddingObfsMode {
		config.Placement = XPaddingPlacement{
			Placement: c.XPaddingPlacement,
			Key:       c.XPaddingKey,
			Header:    c.XPaddingHeader,
			RawURL:    request.URL.String(),
		}
		config.Method = PaddingMethod(c.XPaddingMethod)
	} else {
		config.Placement = XPaddingPlacement{
			Placement: PlacementQueryInHeader,
			Key:       "x_padding",
			Header:    "Referer",
			RawURL:    request.URL.String(),
		}
	}

	c.ApplyXPaddingToRequest(request, config)
	c.ApplyMetaToRequest(request, sessionId, "")

	if request.Body != nil && !c.NoGRPCHeader { // stream-up/one
		request.Header.Set("Content-Type", "application/grpc")
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
		request.Body = io.NopCloser(&buf.MultiBufferContainer{MultiBuffer: payload})
		request.ContentLength = int64(payload.Len())
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

	basePad := c.GetNormalizedXPaddingBytes()
	from, to := AdaptivePaddingRange(basePad.From, basePad.To, payloadLen)
	length := int((&RangeConfig{From: from, To: to}).rand())
	config := XPaddingConfig{Length: length}

	if c.XPaddingObfsMode {
		config.Placement = XPaddingPlacement{
			Placement: c.XPaddingPlacement,
			Key:       c.XPaddingKey,
			Header:    c.XPaddingHeader,
			RawURL:    request.URL.String(),
		}
		config.Method = PaddingMethod(c.XPaddingMethod)
	} else {
		config.Placement = XPaddingPlacement{
			Placement: PlacementQueryInHeader,
			Key:       "x_padding",
			Header:    "Referer",
			RawURL:    request.URL.String(),
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
	if sessionPlacement == PlacementPath || seqPlacement == PlacementPath {
		subpath = strings.Split(req.URL.Path[len(path):], "/")
	}

	switch sessionPlacement {
	case PlacementPath:
		if len(subpath) > pathPart {
			sessionId = subpath[pathPart]
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
		if len(subpath) > pathPart {
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

func (m *XmuxConfig) GetNormalizedMaxConcurrency() *RangeConfig {
	if m.MaxConcurrency == nil {
		return defaultRangeConfigXmuxMaxConcurrency
	}

	return m.MaxConcurrency
}

func (m *XmuxConfig) GetNormalizedMaxConnections() *RangeConfig {
	if m.MaxConnections == nil {
		return defaultRangeConfigXmuxMaxConnections
	}

	return m.MaxConnections
}

func (m *XmuxConfig) GetNormalizedCMaxReuseTimes() *RangeConfig {
	if m.CMaxReuseTimes == nil {
		return defaultRangeConfigXmuxCMaxReuseTimes
	}

	return m.CMaxReuseTimes
}

func (m *XmuxConfig) GetNormalizedHMaxRequestTimes() *RangeConfig {
	if m.HMaxRequestTimes == nil {
		return defaultRangeConfigXmuxHMaxRequestTimes
	}

	return m.HMaxRequestTimes
}

func (m *XmuxConfig) GetNormalizedHMaxReusableSecs() *RangeConfig {
	if m.HMaxReusableSecs == nil {
		return defaultRangeConfigXmuxHMaxReusableSecs
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
		return string(id)
	} else {
		uuid := uuid.New()
		return uuid.String()
	}
}

func appendToPath(path, value string) string {
	if strings.HasSuffix(path, "/") {
		return path + value
	}
	return path + "/" + value
}
