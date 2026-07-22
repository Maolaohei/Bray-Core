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
		// server may not have been forced through GenerateSessionID; verify via Verify is enough
		// but VerifySessionIDExported calls sessionSecret which injects
		_ = VerifySessionIDExported(id, server)
	}
	if server.Headers[BraySessionSecretHeader] != DefaultBraySessionSecret {
		t.Fatalf("server default missing: %q", server.Headers[BraySessionSecretHeader])
	}
}
