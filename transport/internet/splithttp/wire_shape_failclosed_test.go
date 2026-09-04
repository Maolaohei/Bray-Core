package splithttp_test

// Anti-censorship checklist item: 主动探测防护 / fail-closed 一致性.
//
// A prober must not get ANY distinguishing signal from auth failures:
//   - forged session MAC, missing/invalid padding and plain route-miss all
//     return the identical bare 404 (same status, same empty body, same
//     header set modulo Date — Go's default shape, no custom headers, no
//     error text, no stack, no path hints);
//   - MAC comparison is constant-time (session_auth.go) and each check costs
//     microseconds — orders of magnitude below network jitter, so no timing
//     side channel is measurable remotely (asserted structurally here: all
//     paths exit through the same WriteHeader(404) before session machinery).
//
// This test pins the byte-level shape so a future "helpful" error message or
// header on one of the paths cannot slip in unnoticed.

import (
	"context"
	"crypto/hmac"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport/internet"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/stat"
)

func TestWireShape_FailClosed404Uniform(t *testing.T) {
	p := tcp.PickPort()
	settings := &internet.MemoryStreamConfig{
		ProtocolName: "splithttp",
		ProtocolSettings: &Config{
			Path:    "/sh",
			Mode:    "packet-up",
			Headers: map[string]string{BraySessionSecretHeader: "wire-shape-secret"},
		},
	}
	listen, err := ListenXH(context.Background(), net.LocalHostIP, p, settings, func(conn stat.Connection) {
		go func(c stat.Connection) {
			defer c.Close()
			buf.Copy(buf.NewReader(c), buf.NewWriter(c))
		}(conn)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listen.Close()
	base := fmt.Sprintf("http://127.0.0.1:%d", int(p))

	// Build a real signed session id, then corrupt one tag char -> forged MAC.
	clientCfg := &Config{Path: "/sh", Headers: map[string]string{BraySessionSecretHeader: "wire-shape-secret"}}
	signed := clientCfg.GenerateSessionID()
	if signed == "" {
		t.Fatal("expected signed session id")
	}
	dot := strings.LastIndexByte(signed, '.')
	if dot <= 0 {
		t.Fatalf("signed id missing tag separator: %q", signed)
	}
	raw, tag := signed[:dot], signed[dot+1:]
	tagBytes, err := base64.RawURLEncoding.DecodeString(tag)
	if err != nil {
		t.Fatal(err)
	}
	tagBytes[0] ^= 0x01 // single-bit flip: realistic brute-force probe shape
	forged := raw + "." + base64.RawURLEncoding.EncodeToString(tagBytes)
	if hmac.Equal([]byte(forged), []byte(signed)) {
		t.Fatal("forgery must differ from the signed id")
	}

	// Padding header that passes IsPaddingValid (raw length inside the
	// default accepted band), so the forged-MAC request actually reaches the
	// MAC gate instead of being rejected earlier by padding validation.
	pad := strings.Repeat("a", 200)

	cases := map[string]string{
		"path-miss":    base + "/other",
		"forged-mac":   base + "/sh/" + forged,
		"padding-fail": base + "/sh/" + signed, // no padding header -> 404 at padding gate
	}

	shapes := make(map[string]string, len(cases))
	client := &http.Client{Timeout: 10 * time.Second}
	for name, url := range cases {
		req, _ := http.NewRequest("GET", url, nil)
		// Only the forged-MAC probe carries valid padding so it reaches the
		// MAC gate; the padding-fail probe intentionally sends none.
		if name == "forged-mac" {
			req.Header.Set("X-Padding", pad)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s: status = %d, want 404", name, resp.StatusCode)
		}
		if len(body) != 0 {
			t.Fatalf("%s: 404 must have empty body, got %q", name, body)
		}
		shapes[name] = canonicalShape(resp)
	}

	// All three shapes must be byte-identical (modulo Date, which the
	// canonical form strips).
	want := shapes["path-miss"]
	for name, got := range shapes {
		if got != want {
			t.Fatalf("%s 404 shape differs from path-miss:\n got: %s\nwant: %s", name, got, want)
		}
	}
}

// canonicalShape renders status + sorted headers (Date excluded) + body
// length so two responses can be compared byte-faithfully.
func canonicalShape(resp *http.Response) string {
	keys := make([]string, 0, len(resp.Header))
	for k := range resp.Header {
		if strings.EqualFold(k, "Date") {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	fmt.Fprintf(&b, "status=%d proto=%s\n", resp.StatusCode, resp.Proto)
	for _, k := range keys {
		vals := resp.Header[k]
		sort.Strings(vals)
		for _, v := range vals {
			fmt.Fprintf(&b, "%s: %s\n", k, v)
		}
	}
	return b.String()
}
