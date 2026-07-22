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
//
// Secret resolution (Bray-only, never on wire):
//  1. explicit headers["x-bray-session-secret"]  (operator override)
//  2. headers["x-bray-session-uuid"] one or more UUIDs (comma-separated),
//     each derived via SHA-256(domain||uuid) — injected from VLESS id at conf build
//  3. DefaultBraySessionSecret (non-VLESS / missing UUID zero-config fallback)
const (
	sessionMACDomain   = "bray-xhttp-session-v1"
	sessionMACTagBytes = 8
	sessionMACSep      = "."

	// BraySessionSecretHeader is the client-local control header (never sent on wire).
	// Client and server must share the same value so session MAC verifies.
	BraySessionSecretHeader = "x-bray-session-secret"

	// BraySessionUUIDHeader carries one or more VLESS UUIDs used to derive the
	// default session MAC key(s). Local-only; never sent on wire.
	// Multiple UUIDs (inbound multi-user) are comma-separated; verify accepts any.
	BraySessionUUIDHeader = "x-bray-session-uuid"

	// DefaultBraySessionSecret is injected when neither explicit secret nor UUID
	// seed is present. Bray-only zero-config for non-VLESS or incomplete configs.
	DefaultBraySessionSecret = "bray-default-session-key"
)

// fixedBraySessionSecret is the derived key for DefaultBraySessionSecret.
// Host/path intentionally NOT mixed in: CDN multi-host and client/server
// host field asymmetry must not desync MAC verification.
var fixedBraySessionSecret = deriveSessionSecret([]byte(DefaultBraySessionSecret), "", "")

var sessionSecretCache sync.Map // *Config -> [][]byte

// ensureDefaultSessionSecret fills zero-config auth material when the operator
// did not set an explicit secret or UUID seed. Safe to call on Dial/Listen and
// from sessionSecrets(); never overwrites operator values.
func (c *Config) ensureDefaultSessionSecret() {
	if c == nil {
		return
	}
	if lookupSessionSecretHeader(c.Headers) != "" {
		return
	}
	if len(lookupSessionUUIDSeeds(c.Headers)) > 0 {
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

func lookupSessionUUIDSeeds(headers map[string]string) []string {
	if headers == nil {
		return nil
	}
	raw := ""
	if s, ok := headers[BraySessionUUIDHeader]; ok {
		raw = s
	} else if s, ok := headers["X-Bray-Session-Uuid"]; ok {
		raw = s
	} else {
		for k, v := range headers {
			if strings.EqualFold(k, BraySessionUUIDHeader) {
				raw = v
				break
			}
		}
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, p := range parts {
		u := normalizeSessionUUID(p)
		if u == "" {
			continue
		}
		if _, ok := seen[u]; ok {
			continue
		}
		seen[u] = struct{}{}
		out = append(out, u)
	}
	return out
}

func normalizeSessionUUID(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return ""
	}
	// Keep UUID-looking strings as-is after lowercasing; still accept non-UUID
	// seeds so conf-layer can pass any stable id string.
	return s
}

// sessionSecrets returns all MAC keys accepted for verify. The first entry is
// the primary key used for signing new session IDs on the client.
func (c *Config) sessionSecrets() [][]byte {
	if c == nil {
		return [][]byte{fixedBraySessionSecret}
	}
	c.ensureDefaultSessionSecret()
	if v, ok := sessionSecretCache.Load(c); ok {
		return v.([][]byte)
	}
	secrets := resolveSessionSecrets(c.Headers)
	actual, _ := sessionSecretCache.LoadOrStore(c, secrets)
	return actual.([][]byte)
}

func resolveSessionSecrets(headers map[string]string) [][]byte {
	if s := lookupSessionSecretHeader(headers); s != "" {
		if s == DefaultBraySessionSecret {
			return [][]byte{fixedBraySessionSecret}
		}
		return [][]byte{deriveSessionSecret([]byte(s), "", "")}
	}
	if uuids := lookupSessionUUIDSeeds(headers); len(uuids) > 0 {
		out := make([][]byte, 0, len(uuids))
		for _, u := range uuids {
			out = append(out, deriveSessionSecretFromUUID(u))
		}
		return out
	}
	return [][]byte{fixedBraySessionSecret}
}

func (c *Config) sessionSecret() []byte {
	secrets := c.sessionSecrets()
	if len(secrets) == 0 {
		return fixedBraySessionSecret
	}
	return secrets[0]
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

// deriveSessionSecretFromUUID binds the MAC key to a VLESS account id so
// operators need not configure a second shared secret. Domain tag differs from
// explicit secrets so a raw UUID string used as x-bray-session-secret does not
// collide with UUID-seed derivation.
func deriveSessionSecretFromUUID(uuidStr string) []byte {
	h := sha256.New()
	h.Write([]byte(sessionMACDomain))
	h.Write([]byte{0})
	h.Write([]byte("uuid"))
	h.Write([]byte{0})
	h.Write([]byte(normalizeSessionUUID(uuidStr)))
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

// verifySessionIDAny accepts a sessionId signed by any of the provided secrets.
// Used on multi-user inbounds where each VLESS UUID yields a distinct key.
func verifySessionIDAny(sessionId string, secrets [][]byte) bool {
	if sessionId == "" {
		return true
	}
	if len(secrets) == 0 {
		return verifySessionID(sessionId, fixedBraySessionSecret)
	}
	for _, secret := range secrets {
		if verifySessionID(sessionId, secret) {
			return true
		}
	}
	return false
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

// VerifySessionIDExported is a test/helper wrapper around verifySessionIDAny.
func VerifySessionIDExported(sessionId string, c *Config) bool {
	if c == nil {
		return verifySessionID(sessionId, fixedBraySessionSecret)
	}
	return verifySessionIDAny(sessionId, c.sessionSecrets())
}
