package splithttp_test

// Host HTTP sanity probe: some endpoint security products (observed: Huorong
// 火绒 WFP driver hrwfpdrv.sys) install an HTTP-aware callout on loopback TCP
// and REWRITE the stream — injecting request headers ("DNT: 1", "Sec-GPC: 1")
// and re-serializing responses. Any test that asserts byte-exact HTTP/1.1
// framing over real loopback sockets is then subject to environment noise
// that looks like a product bug (misframed pipelined requests, truncated
// 400s, mid-stream header injection) and wastes bisect time.
//
// skipIfHostLoopbackHTTPRewrite(t) probes a raw pair of sockets (no product
// code involved) and skips when the stream is touched. Call it at the top of
// tests that depend on byte-exact wire framing.
//
// testBindIP(t) goes one step further for e2e tests: when loopback is
// rewritten but a LAN IPv4 is clean (Huorong's callout covers loopback
// only), it returns the LAN IP so tests bind there and stay meaningful
// instead of being skipped on every run.

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

// probeWireFidelity runs one canned HTTP round trip over a raw socket pair
// bound to ip and reports whether the stream arrived untouched.
func probeWireFidelity(ip string) (clean bool, detail string) {
	ln, err := net.Listen("tcp", net.JoinHostPort(ip, "0"))
	if err != nil {
		return false, "cannot listen: " + err.Error()
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
		injDetail := ""
		if injected {
			injDetail = "DNT/Sec-GPC"
		}
		// Server-side full-form 400, byte-identical to Go's readRequest path.
		resp := "HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n\r\n400 Bad Request"
		if _, werr := c.Write([]byte(resp)); werr == nil {
			done <- verdict{injected: injected, respOK: true, detail: injDetail}
			return
		}
		done <- verdict{injected: injected, detail: injDetail}
	}()

	c, err := net.DialTimeout("tcp", ln.Addr().String(), 3*time.Second)
	if err != nil {
		return false, "dial: " + err.Error()
	}
	defer c.Close()
	req := "POST /sh/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA HTTP/1.1\r\n" +
		"Host: localhost\r\nUser-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36\r\n" +
		"Content-Length: 16\r\nAccept: */*\r\n" +
		"X-Request-Trace: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n\r\n0123456789abcdef"
	if _, err := c.Write([]byte(req)); err != nil {
		return false, "write: " + err.Error()
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
			return false, "header injection (" + v.detail + ")"
		}
		if v.respOK && respLen != len("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n\r\n400 Bad Request") {
			return false, fmt.Sprintf("response stream rewritten (%d/111 bytes)", respLen)
		}
		if !v.respOK {
			return false, "server write failed: " + v.detail
		}
		return true, ""
	case <-time.After(5 * time.Second):
		return false, fmt.Sprintf("no verdict in 5s (resp=%d bytes)", respLen)
	}
}

func skipIfHostLoopbackHTTPRewrite(t *testing.T) {
	t.Helper()

	// Escape hatch: BRAY_TRUST_LOOPBACK=1 disables the guard for one-off
	// debugging on a host whose interceptor has been turned off (or when the
	// operator has whitelisted the test process in the security product).
	if os.Getenv("BRAY_TRUST_LOOPBACK") == "1" {
		return
	}

	clean, detail := probeWireFidelity("127.0.0.1")
	if !clean {
		t.Skipf("host loopback stack rewrites HTTP (%s); wire-exact assertions are not meaningful here", detail)
	}
}

// testBindIP returns an IP that e2e tests can bind servers to with intact
// wire fidelity: loopback when clean, else the first clean LAN IPv4
// (vswitch adapters included — traffic stays on-host), else t.Skip.
func testBindIP(t *testing.T) string {
	t.Helper()
	if os.Getenv("BRAY_TRUST_LOOPBACK") != "1" {
		if clean, _ := probeWireFidelity("127.0.0.1"); clean {
			return "127.0.0.1"
		}
		ifaces, err := net.Interfaces()
		if err == nil {
			for _, ifc := range ifaces {
				if ifc.Flags&net.FlagLoopback != 0 || ifc.Flags&net.FlagUp == 0 {
					continue
				}
				addrs, _ := ifc.Addrs()
				for _, a := range addrs {
					ipnet, ok := a.(*net.IPNet)
					if !ok {
						continue
					}
					ip4 := ipnet.IP.To4()
					if ip4 == nil || ip4.IsLinkLocalUnicast() || ip4.IsLoopback() {
						continue
					}
					ip := ip4.String()
					if clean, detail := probeWireFidelity(ip); clean {
						t.Logf("loopback rewritten by host stack; binding tests to clean LAN IP %s (%s)", ip, ifc.Name)
						return ip
					} else {
						t.Logf("LAN candidate %s (%s) dirty: %s", ip, ifc.Name, detail)
					}
				}
			}
		}
		t.Skip("no clean bind IP: loopback rewritten and every LAN IPv4 probe dirty (host HTTP interceptor active)")
	}
	return "127.0.0.1"
}
