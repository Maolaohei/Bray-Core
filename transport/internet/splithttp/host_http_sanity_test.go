package splithttp_test

// Host HTTP sanity probe: some endpoint security products (observed: Huorong
// 火绒 WFP driver hrwfpdrv.sys) install an HTTP-aware callout on loopback TCP
// and REWRITE the stream — injecting request headers ("DNT: 1", "Sec-GPC: 1")
// and re-serializing responses. Any test that asserts byte-exact HTTP/1.1
// framing over real loopback sockets is then subject to environment noise
// that looks like a product bug (misframed pipelined requests, truncated
// 400s, mid-stream header injection) and wastes bisect time.
//
// skipIfHostLoopbackHTTPRewrite(t) runs one 461-byte canned request through
// a raw pair of loopback sockets (no product code involved) and fails the
// invariant check when the stream is touched. Call it at the top of tests
// that depend on byte-exact wire framing.

import (
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func skipIfHostLoopbackHTTPRewrite(t *testing.T) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("loopback probe: cannot listen: %v", err)
	}
	defer ln.Close()

	type verdict struct {
		injected bool
		respOK   bool
		detail   string
	}
	done := make(chan verdict, 1)

	go func() {
		c, err := ln.Accept()
		if err != nil {
			done <- verdict{detail: "accept: " + err.Error()}
			return
		}
		defer c.Close()
		got, err := io.ReadAll(c)
		if err != nil && err != io.EOF {
			done <- verdict{detail: "server read: " + err.Error()}
			return
		}
		injected := strings.Contains(string(got), "DNT: 1") ||
			strings.Contains(string(got), "Sec-GPC: 1")
		// Server-side full-form 400, byte-identical to Go's readRequest path.
		resp := "HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n\r\n400 Bad Request"
		if _, werr := c.Write([]byte(resp)); werr == nil {
			done <- verdict{injected: injected, respOK: true}
			return
		}
		done <- verdict{injected: injected}
	}()

	c, err := net.DialTimeout("tcp", ln.Addr().String(), 3*time.Second)
	if err != nil {
		t.Skipf("loopback probe: dial: %v", err)
	}
	defer c.Close()
	req := "POST /sh/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA HTTP/1.1\r\n" +
		"Host: localhost\r\nUser-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36\r\n" +
		"Content-Length: 16\r\nAccept: */*\r\n" +
		"X-Request-Trace: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n\r\n0123456789abcdef"
	if _, err := c.Write([]byte(req)); err != nil {
		t.Skipf("loopback probe: write: %v", err)
	}
	// Half-close so the server side's io.ReadAll terminates even when the
	// host stack does not; without this the probe can deadlock symmetrically
	// (server waits EOF, client waits response) exactly like a real app.
	if tc, ok := c.(*net.TCPConn); ok {
		tc.CloseWrite()
	}
	buf := make([]byte, 4096)
	respLen := 0
	for {
		n, rerr := c.Read(buf)
		respLen += n
		if rerr != nil {
			break
		}
	}
	select {
	case v := <-done:
		if v.injected {
			t.Skipf("host loopback stack rewrites HTTP (header injection detected: %s); wire-exact assertions are not meaningful here", v.detail)
		}
		if respLen != len("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n\r\n400 Bad Request") {
			t.Skipf("host loopback stack rewrote the response stream (%d/%d bytes delivered); wire-exact assertions are not meaningful here", respLen, 111)
		}
	case <-time.After(5 * time.Second):
		t.Skipf("loopback probe inconclusive (no verdict in 5s, resp=%d bytes); host TCP stack behaves abnormally", respLen)
	}
	_ = fmt.Sprintf // keep fmt imported for future probes
}
