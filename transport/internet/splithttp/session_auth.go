package splithttp

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"strings"
	"sync"
)

// Bray session IDs are rawID + "." + base64url(HMAC-SHA256(secret, rawID)[:8]).
// Clients always generate signed IDs; servers reject unsigned or bad MACs so
// unauthenticated session creation cannot fill the hub map.
const (
	sessionMACDomain   = "bray-xhttp-session-v1"
	sessionMACTagBytes = 8
	sessionMACSep      = "."

	// BraySessionSecretHeader is the client-local control header (never sent on wire).
	// Client and server must share the same value so session MAC verifies.
	BraySessionSecretHeader = "x-bray-session-secret"

	// DefaultBraySessionSecret is injected when the header is absent.
	// Bray-only zero-config: both ends use this unless operators override.
	DefaultBraySessionSecret = "bray-default-session-key"
)

// fixedBraySessionSecret is the derived key for DefaultBraySessionSecret.
// Host/path intentionally NOT mixed in: CDN multi-host and client/server
// host field asymmetry must not desync MAC verification.
var fixedBraySessionSecret = deriveSessionSecret([]byte(DefaultBraySessionSecret), "", "")

var sessionSecretCache sync.Map // *Config -> []byte

// ensureDefaultSessionSecret writes DefaultBraySessionSecret into c.Headers when
// no non-empty x-bray-session-secret is present. Safe to call on Dial/Listen and
// from sessionSecret(); never overwrites an explicit operator value.
func (c *Config) ensureDefaultSessionSecret() {
	if c == nil {
		return
	}
	if lookupSessionSecretHeader(c.Headers) != "" {
		return
	}
	if c.Headers == nil {
		c.Headers = make(map[string]string, 1)
	}
	c.Headers[BraySessionSecretHeader] = DefaultBraySessionSecret
}

func lookupSessionSecretHeader(headers map[string]string) string {
	if headers == nil {
		return ""
	}
	if s, ok := headers[BraySessionSecretHeader]; ok {
		if t := strings.TrimSpace(s); t != "" {
			return t
		}
	}
	if s, ok := headers["X-Bray-Session-Secret"]; ok {
		if t := strings.TrimSpace(s); t != "" {
			return t
		}
	}
	// Case-insensitive fallback for odd JSON casing.
	for k, v := range headers {
		if strings.EqualFold(k, BraySessionSecretHeader) {
			if t := strings.TrimSpace(v); t != "" {
				return t
			}
		}
	}
	return ""
}

func (c *Config) sessionSecret() []byte {
	if c == nil {
		return fixedBraySessionSecret
	}
	c.ensureDefaultSessionSecret()
	if v, ok := sessionSecretCache.Load(c); ok {
		return v.([]byte)
	}
	var secret []byte
	if s := lookupSessionSecretHeader(c.Headers); s != "" {
		if s == DefaultBraySessionSecret {
			secret = fixedBraySessionSecret
		} else {
			secret = deriveSessionSecret([]byte(s), "", "")
		}
	}
	if secret == nil {
		secret = fixedBraySessionSecret
	}
	actual, _ := sessionSecretCache.LoadOrStore(c, secret)
	return actual.([]byte)
}

func deriveSessionSecret(explicit []byte, host, path string) []byte {
	h := sha256.New()
	h.Write([]byte(sessionMACDomain))
	h.Write([]byte{0})
	if len(explicit) > 0 {
		h.Write(explicit)
	} else {
		h.Write([]byte(host))
		h.Write([]byte{0})
		h.Write([]byte(path))
	}
	return h.Sum(nil)
}

func signSessionID(raw string, secret []byte) string {
	if raw == "" {
		return ""
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(raw))
	sum := mac.Sum(nil)
	tag := base64.RawURLEncoding.EncodeToString(sum[:sessionMACTagBytes])
	return raw + sessionMACSep + tag
}

// verifySessionID returns true when sessionId carries a valid Bray MAC for secret.
// Empty sessionId is allowed only for stream-one (caller already gates that).
func verifySessionID(sessionId string, secret []byte) bool {
	if sessionId == "" {
		return true
	}
	dot := strings.LastIndexByte(sessionId, '.')
	if dot <= 0 || dot >= len(sessionId)-1 {
		return false
	}
	raw := sessionId[:dot]
	tag := sessionId[dot+1:]
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(raw))
	sum := mac.Sum(nil)
	want := base64.RawURLEncoding.EncodeToString(sum[:sessionMACTagBytes])
	if len(tag) != len(want) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(tag), []byte(want)) == 1
}

// appendSeqTag binds packet-up seq into a short MAC for optional anti-injection.
// Kept for future wire upgrade; currently unused on the public path.
func appendSeqTag(seq uint64, secret []byte) string {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], seq)
	mac := hmac.New(sha256.New, secret)
	mac.Write(buf[:])
	sum := mac.Sum(nil)
	return base64.RawURLEncoding.EncodeToString(sum[:4])
}

// VerifySessionIDExported is a test/helper wrapper around verifySessionID.
func VerifySessionIDExported(sessionId string, c *Config) bool {
	if c == nil {
		return verifySessionID(sessionId, fixedBraySessionSecret)
	}
	return verifySessionID(sessionId, c.sessionSecret())
}