package splithttp_test

import (
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
)

func TestSessionAuth_SignVerifyRoundTrip(t *testing.T) {
	secret := map[string]string{BraySessionSecretHeader: "roundtrip-secret"}
	c := &Config{Host: "example.com", Path: "/xhttp", Headers: secret}
	id := c.GenerateSessionID()
	if id == "" || !strings.Contains(id, ".") {
		t.Fatalf("expected signed session id, got %q", id)
	}
	if !VerifySessionIDExported(id, c) {
		t.Fatal("verify should accept GenerateSessionID output")
	}
	// Different host/path still verifies with same explicit secret
	c2 := &Config{Host: "other.cdn", Path: "/else", Headers: secret}
	if !VerifySessionIDExported(id, c2) {
		t.Fatal("explicit secret must match across host/path")
	}
	// Tamper tag
	parts := strings.Split(id, ".")
	if len(parts) != 2 {
		t.Fatalf("want raw.tag, got %q", id)
	}
	bad := parts[0] + ".AAAAAAAA"
	if VerifySessionIDExported(bad, c) {
		t.Fatal("tampered tag must fail")
	}
	// Unsigned raw UUID must fail
	if VerifySessionIDExported("550e8400-e29b-41d4-a716-446655440000", c) {
		t.Fatal("unsigned id must fail")
	}
}

func TestSessionAuth_ExplicitSecret(t *testing.T) {
	c1 := &Config{
		Host: "a.example",
		Path: "/p",
		Headers: map[string]string{
			"x-bray-session-secret": "s3cret",
		},
	}
	c2 := &Config{
		Host: "b.example",
		Path: "/q",
		Headers: map[string]string{
			"x-bray-session-secret": "s3cret",
		},
	}
	c3 := &Config{
		Headers: map[string]string{
			"x-bray-session-secret": "other",
		},
	}
	id := c1.GenerateSessionID()
	if !VerifySessionIDExported(id, c2) {
		t.Fatal("same secret should verify across configs")
	}
	if VerifySessionIDExported(id, c3) {
		t.Fatal("different secret must reject")
	}
	// Zero-config cannot sign (fail-closed): no default-signed id exists.
	cDef := &Config{}
	defID := cDef.GenerateSessionID()
	if defID != "" {
		t.Fatalf("zero-config must not produce a signed id, got %q", defID)
	}
	if VerifySessionIDExported(defID, c1) {
		t.Fatal("empty zero-config id must not verify as a session id")
	}
}

func TestSessionAuth_UUIDDerived(t *testing.T) {
	const uuidA = "550e8400-e29b-41d4-a716-446655440000"
	const uuidB = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	client := &Config{
		Headers: map[string]string{
			BraySessionUUIDHeader: uuidA,
		},
	}
	// Server multi-user: both UUIDs accepted
	server := &Config{
		Headers: map[string]string{
			BraySessionUUIDHeader: uuidA + "," + uuidB,
		},
	}
	id := client.GenerateSessionID()
	if !VerifySessionIDExported(id, server) {
		t.Fatal("server must accept client UUID-derived MAC")
	}
	// Peer with only other UUID must reject
	other := &Config{Headers: map[string]string{BraySessionUUIDHeader: uuidB}}
	if VerifySessionIDExported(id, other) {
		t.Fatal("different UUID seed must reject")
	}
	// Case / whitespace normalized
	serverCase := &Config{Headers: map[string]string{BraySessionUUIDHeader: "  " + strings.ToUpper(uuidA) + "  "}}
	if !VerifySessionIDExported(id, serverCase) {
		t.Fatal("UUID seed must be case-insensitive")
	}
	// Explicit secret still wins over UUID seed
	mixed := &Config{Headers: map[string]string{
		BraySessionUUIDHeader:   uuidA,
		BraySessionSecretHeader: "override",
	}}
	idMixed := mixed.GenerateSessionID()
	if VerifySessionIDExported(idMixed, client) {
		t.Fatal("explicit secret must not verify under UUID-only peer")
	}
	sameOverride := &Config{Headers: map[string]string{BraySessionSecretHeader: "override"}}
	if !VerifySessionIDExported(idMixed, sameOverride) {
		t.Fatal("explicit secret peers must match")
	}
	// Wire strip: UUID control header must not leave process
	reqH := client.GetRequestHeader()
	if reqH.Get(BraySessionUUIDHeader) != "" || reqH.Get(BraySessionSecretHeader) != "" {
		t.Fatal("session control headers must not be sent on wire")
	}
}

func TestUploadQueue_GapTimeout(t *testing.T) {
	q := NewUploadQueue(10)
	common.Must(q.Push(Packet{Payload: []byte("b"), Seq: 1}))

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 8)
		_, err := q.Read(buf)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected gap timeout error")
		}
		if !strings.Contains(err.Error(), "gap timeout") {
			t.Fatalf("want gap timeout, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read did not return within gap timeout window")
	}
}

func TestUploadQueue_OrderedOk(t *testing.T) {
	q := NewUploadQueue(10)
	common.Must(q.Push(Packet{Payload: []byte("a"), Seq: 0}))
	common.Must(q.Push(Packet{Payload: []byte("b"), Seq: 1}))
	buf := make([]byte, 8)
	n, err := q.Read(buf)
	common.Must(err)
	if n != 1 || buf[0] != 'a' {
		t.Fatalf("got n=%d b=%q", n, buf[:n])
	}
	n, err = q.Read(buf)
	common.Must(err)
	if n != 1 || buf[0] != 'b' {
		t.Fatalf("got n=%d b=%q", n, buf[:n])
	}
}

func TestSessionAuth_ZeroConfigFailClosed(t *testing.T) {
	c := &Config{Host: "example.com", Path: "/xhttp"}
	// Zero-config: no secret injected, no session MAC capability (fail-closed).
	id := c.GenerateSessionID()
	if id != "" {
		t.Fatalf("zero-config GenerateSessionID must be empty (fail-closed), got %q", id)
	}
	if c.Headers[BraySessionSecretHeader] != "" {
		t.Fatalf("zero-config must not inject a default secret header: %q", c.Headers[BraySessionSecretHeader])
	}
	// Explicit secret must produce a signed session id and be preserved.
	c2 := &Config{Headers: map[string]string{BraySessionSecretHeader: "custom-secret"}}
	if c2.GenerateSessionID() == "" {
		t.Fatal("explicit secret must produce a signed session id")
	}
	if c2.Headers[BraySessionSecretHeader] != "custom-secret" {
		t.Fatalf("explicit secret overwritten: %q", c2.Headers[BraySessionSecretHeader])
	}
	// Control header must never appear on wire request headers.
	reqH := c2.GetRequestHeader()
	if reqH.Get(BraySessionSecretHeader) != "" || reqH.Get("X-Bray-Session-Secret") != "" {
		t.Fatal("session secret must not be sent on wire")
	}
}

func TestSessionAuth_SharedSecretMatchesBothEnds(t *testing.T) {
	client := &Config{Host: "cdn.example", Path: "/a", Headers: map[string]string{BraySessionSecretHeader: "shared-secret"}}
	server := &Config{Host: "origin.internal", Path: "/b", Headers: map[string]string{BraySessionSecretHeader: "shared-secret"}}
	id := client.GenerateSessionID()
	if id == "" {
		t.Fatal("client with explicit secret must sign session id")
	}
	if !VerifySessionIDExported(id, server) {
		t.Fatal("client and server with same explicit secret must verify")
	}
	// Mismatched secrets must fail.
	other := &Config{Headers: map[string]string{BraySessionSecretHeader: "other-secret"}}
	if VerifySessionIDExported(id, other) {
		t.Fatal("mismatched secret must not verify")
	}
	// Zero-config server must reject signed ids (fail-closed).
	zero := &Config{Host: "origin.internal", Path: "/b"}
	if VerifySessionIDExported(id, zero) {
		t.Fatal("zero-config server must reject signed session ids (fail-closed)")
	}
	// UUID seed also derives a working key.
	cUUID := &Config{Headers: map[string]string{BraySessionUUIDHeader: "550e8400-e29b-41d4-a716-446655440000"}}
	idU := cUUID.GenerateSessionID()
	if idU == "" {
		t.Fatal("UUID seed must produce a signed session id")
	}
	sUUID := &Config{Headers: map[string]string{BraySessionUUIDHeader: "550e8400-e29b-41d4-a716-446655440000"}}
	if !VerifySessionIDExported(idU, sUUID) {
		t.Fatal("UUID-seed client and server must verify")
	}
}
