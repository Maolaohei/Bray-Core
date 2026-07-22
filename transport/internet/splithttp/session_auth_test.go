package splithttp_test

import (
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
)

func TestSessionAuth_SignVerifyRoundTrip(t *testing.T) {
	c := &Config{Host: "example.com", Path: "/xhttp"}
	id := c.GenerateSessionID()
	if id == "" || !strings.Contains(id, ".") {
		t.Fatalf("expected signed session id, got %q", id)
	}
	if !VerifySessionIDExported(id, c) {
		t.Fatal("verify should accept GenerateSessionID output")
	}
	// Different host/path still verifies with fixed default secret
	c2 := &Config{Host: "other.cdn", Path: "/else"}
	if !VerifySessionIDExported(id, c2) {
		t.Fatal("default secret must match across host/path")
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
	// Explicit secret must not accept default-signed id
	cDef := &Config{}
	defID := cDef.GenerateSessionID()
	if VerifySessionIDExported(defID, c1) {
		t.Fatal("default-signed id must fail under explicit secret")
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
		BraySessionUUIDHeader:  uuidA,
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

func TestSessionAuth_DefaultHeaderInjected(t *testing.T) {
	c := &Config{Host: "example.com", Path: "/xhttp"}
	// GenerateSessionID -> sessionSecret injects default header for zero-config.
	_ = c.GenerateSessionID()
	if c.Headers == nil {
		t.Fatal("Headers map should be created")
	}
	got, ok := c.Headers[BraySessionSecretHeader]
	if !ok || got != DefaultBraySessionSecret {
		t.Fatalf("want default header %q=%q, got ok=%v val=%q", BraySessionSecretHeader, DefaultBraySessionSecret, ok, got)
	}
	// UUID seed present: do NOT inject default secret
	cUUID := &Config{Headers: map[string]string{BraySessionUUIDHeader: "550e8400-e29b-41d4-a716-446655440000"}}
	_ = cUUID.GenerateSessionID()
	if cUUID.Headers[BraySessionSecretHeader] == DefaultBraySessionSecret {
		t.Fatal("default secret must not be injected when UUID seed is present")
	}
	// Explicit secret must be preserved (sessionSecret path must not overwrite).
	c2 := &Config{Headers: map[string]string{BraySessionSecretHeader: "custom-secret"}}
	_ = c2.GenerateSessionID()
	if c2.Headers[BraySessionSecretHeader] != "custom-secret" {
		t.Fatalf("explicit secret overwritten: %q", c2.Headers[BraySessionSecretHeader])
	}
	// Control header must never appear on wire request headers.
	reqH := c.GetRequestHeader()
	if reqH.Get(BraySessionSecretHeader) != "" || reqH.Get("X-Bray-Session-Secret") != "" {
		t.Fatal("session secret must not be sent on wire")
	}
}

func TestSessionAuth_DefaultMatchesBothEnds(t *testing.T) {
	client := &Config{Host: "cdn.example", Path: "/a"}
	server := &Config{Host: "origin.internal", Path: "/b"}
	id := client.GenerateSessionID()
	if !VerifySessionIDExported(id, server) {
		t.Fatal("zero-config client and server must share default session secret")
	}
	if client.Headers[BraySessionSecretHeader] != DefaultBraySessionSecret {
		t.Fatalf("client default missing: %q", client.Headers[BraySessionSecretHeader])
	}
	if server.Headers[BraySessionSecretHeader] != DefaultBraySessionSecret {
		_ = VerifySessionIDExported(id, server)
	}
	if server.Headers[BraySessionSecretHeader] != DefaultBraySessionSecret {
		t.Fatalf("server default missing: %q", server.Headers[BraySessionSecretHeader])
	}
}
