package splithttp

import (
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
}

// randBufPool reuses the 256-byte buffer used in randStringFromCharset.
var randBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 256)
		return &b
	},
}

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
var parsedURLCache = sync.Map{}

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
	result := (*rp)[:n]
	i := 0

	sp := randBufPool.Get().(*[]byte)
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
	randBufPool.Put(sp)

	s := string(result)
	paddingResultPool.Put(rp)
	return s, true
}

func absInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// tokenishPools per-length sync.Pool for pre-generated Tokenish padding.
// Covers 32~1024 every 32 bytes. Init fills once; GC reclaims naturally.
var tokenishPools [32]sync.Pool

func init() {
	for i := range tokenishPools {
		huffmanLen := 32 + i*32
		s := generateTokenishPaddingBase62Raw(huffmanLen)
		tokenishPools[i].Put(&s)
	}
}

func tokenishPoolIndex(huffmanLen int) int {
	return (huffmanLen - 32) / 32
}

func getOrGenTokenish(huffmanLen int) string {
	idx := tokenishPoolIndex(huffmanLen)
	if item := tokenishPools[idx].Get(); item != nil {
		return *item.(*string)
	}
	return generateTokenishPaddingBase62Raw(huffmanLen)
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
	if length <= 8192 {
		bp := paddingBytePool.Get().(*[]byte)
		s := string((*bp)[:length])
		paddingBytePool.Put(bp)
		return s
	}
	return strings.Repeat("X", length)
}

func GeneratePadding(method PaddingMethod, length int) string {
	if length <= 0 {
		return ""
	}

	// https://www.rfc-editor.org/rfc/rfc7541.html#appendix-B
	// h2's HPACK Header Compression feature employs a huffman encoding using a static table.
	// 'X' and 'Z' are assigned an 8 bit code, so HPACK compression won't change actual padding length on the wire.
	// https://www.rfc-editor.org/rfc/rfc9204.html#section-4.1.2-2
	// h3's similar QPACK feature uses the same huffman table.

	switch method {
	case PaddingMethodRepeatX:
		return generateRepeatX(length)
	case PaddingMethodTokenish:
		if length >= 32 && length <= 1024 && (length-32)%32 == 0 {
			return getOrGenTokenish(length)
		}
		paddingValue := generateTokenishPaddingBase62Raw(length)
		if paddingValue == "" {
			return generateRepeatX(length)
		}
		return paddingValue
	default:
		return generateRepeatX(length)
	}
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

	paddingValue := GeneratePadding(config.Method, config.Length)

	switch p := config.Placement; p.Placement {
	case PlacementHeader:
		h.Set(p.Header, paddingValue)
	case PlacementQueryInHeader:
		var baseURL *url.URL
		if cached, ok := parsedURLCache.Load(p.RawURL); ok {
			baseURL = cached.(*url.URL)
		} else {
			u, err := url.Parse(p.RawURL)
			if err != nil || u == nil {
				return
			}
			baseURL = u
			parsedURLCache.Store(p.RawURL, baseURL)
		}
		clone := *baseURL
		clone.RawQuery = p.Key + "=" + paddingValue
		h.Set(p.Header, clone.String())
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

	paddingValue := GeneratePadding(config.Method, config.Length)

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

	paddingValue := GeneratePadding(config.Method, config.Length)

	switch placement {
	case PlacementCookie:
		ApplyPaddingToResponseCookie(writer, config.Placement.Key, paddingValue)
	}
}

func (c *Config) ExtractXPaddingFromRequest(req *http.Request, obfsMode bool) (string, string) {
	if req == nil {
		return "", ""
	}

	if !obfsMode {
		referrer := req.Header.Get("Referer")

		if referrer != "" {
			if referrerURL, err := url.Parse(referrer); err == nil {
				paddingValue := referrerURL.Query().Get("x_padding")
				paddingPlacement := PlacementQueryInHeader + "=Referer, key=x_padding"
				return paddingValue, paddingPlacement
			}
		} else {
			paddingValue := req.URL.Query().Get("x_padding")
			return paddingValue, PlacementQuery + ", key=x_padding"
		}
	}

	key := c.XPaddingKey
	header := c.XPaddingHeader

	if cookie, err := req.Cookie(key); err == nil {
		if cookie != nil && cookie.Value != "" {
			paddingValue := cookie.Value
			paddingPlacement := PlacementCookie + ", key=" + key
			return paddingValue, paddingPlacement
		}
	}

	headerValue := req.Header.Get(header)

	if headerValue != "" {
		if c.XPaddingPlacement == PlacementHeader {
			paddingPlacement := PlacementHeader + "=" + header
			return headerValue, paddingPlacement
		}

		if parsedURL, err := url.Parse(headerValue); err == nil {
			paddingPlacement := PlacementQueryInHeader + "=" + header + ", key=" + key

			return parsedURL.Query().Get(key), paddingPlacement
		}
	}

	queryValue := req.URL.Query().Get(key)

	if queryValue != "" {
		paddingPlacement := PlacementQuery + ", key=" + key
		return queryValue, paddingPlacement
	}

	return "", ""
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
