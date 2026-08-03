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
// SECURITY: the wire tag is only 8 bytes of the HMAC, and the sessionId is
// transmitted in plaintext on the request path. An operator-configured
// x-bray-session-secret should therefore be high-entropy (≥ 32 random bytes /
// 64 hex chars); a weak secret can be brute-forced offline from a captured
// sessionId. Prefer the x-bray-session-uuid (VLESS account id) derivation,
// which inherits the account's 128-bit entropy.
//
// Secret resolution (Bray-only, never on wire):
//  1. explicit headers["x-bray-session-secret"]  (operator override)
//  2. headers["x-bray-session-uuid"] one or more UUIDs (comma-separated),
//     each derived via SHA-256(domain||uuid) — injected from VLESS id at conf build
//
// Fail-closed: with neither configured, no MAC key exists. GenerateSessionID
// returns an empty (stream-one) session id and the server rejects any non-empty
// sessionId, so the session wire modes (stream-up/packet-up) require an explicit
// secret or UUID seed.
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

)

var sessionSecretCache sync.Map // *Config -> [][]byte

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
		return nil
	}
	if v, ok := sessionSecretCache.Load(c); ok {
		return v.([][]byte)
	}
	secrets := resolveSessionSecrets(c.Headers)
	actual, _ := sessionSecretCache.LoadOrStore(c, secrets)
	return actual.([][]byte)
}

func resolveSessionSecrets(headers map[string]string) [][]byte {
	if s := lookupSessionSecretHeader(headers); s != "" {
		return [][]byte{deriveSessionSecret([]byte(s), "", "")}
	}
	if uuids := lookupSessionUUIDSeeds(headers); len(uuids) > 0 {
		out := make([][]byte, 0, len(uuids))
		for _, u := range uuids {
			out = append(out, deriveSessionSecretFromUUID(u))
		}
		return out
	}
	return nil
}

func (c *Config) sessionSecret() []byte {
	secrets := c.sessionSecrets()
	if len(secrets) == 0 {
		return nil
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
	if raw == "" || secret == nil {
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
// Empty sessionId is never accepted here: stream-one (no session) requests are
// gated by the caller's mode check, not by session verification.
func verifySessionIDAny(sessionId string, secrets [][]byte) bool {
	if sessionId == "" {
		return false
	}
	if len(secrets) == 0 {
		return false
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
		return false
	}
	return verifySessionIDAny(sessionId, c.sessionSecrets())
}
