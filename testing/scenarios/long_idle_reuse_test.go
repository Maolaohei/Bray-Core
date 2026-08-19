package scenarios

// Long-idle connection reuse regression: does a sustained connection that
// goes quiet for longer than the client XMUX idle-eviction band
// (clientIdleTimeout 90-240s) survive and keep carrying traffic afterwards,
// or does it silently die requiring a reconnect (the "page hangs, refresh
// fixes it" symptom)?
//
// We use the real dual-end docked (dokodemo-in -> VLESS XHTTP client ->
// server -> freedom -> XOR echo) so a quiescent period exercises the
// connection-pool / idle-eviction / keep-alive path exactly as a browser
// tab does when you leave a page open and come back minutes later.

import (
	"io"
	stdnet "net"
	"strconv"
	"testing"
	"time"

	"github.com/xtls/xray-core/testing/servers/tcp"
)

func TestVLESSXHTTP_LongIdleConnectionReuse(t *testing.T) {
	if testing.Short() {
		t.Skip("long-idle regression skipped in short mode")
	}
	ep := &dsegEndpoints{serverPort: tcp.PickPort()}
	startServerOnly(t, ep)
	startClientOnly(t, ep, "127.0.0.1", ep.serverPort)

	addr := "127.0.0.1:" + strconv.Itoa(int(ep.clientPort))
	conn, err := stdnet.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	payload := make([]byte, 32<<10)
	for i := range payload {
		payload[i] = 0x5A
	}
	// The upstream is the XOR echo (xor(b) = b ^ 'c'); echo back expects the
	// XOR-transformed payload.
	want := make([]byte, len(payload))
	for i, v := range payload {
		want[i] = v ^ 'c'
	}

	// Phase 1: verify the connection carries traffic now.
	roundTrip := func(phase string) error {
		_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
		defer conn.SetDeadline(time.Time{})
		if _, err := conn.Write(payload); err != nil {
			return err
		}
		buf := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, buf); err != nil {
			return err
		}
		for i := range buf {
			if buf[i] != want[i] {
				return &longIdleErr{phase + " echo mismatch"}
			}
		}
		return nil
	}
	if err := roundTrip("warmup"); err != nil {
		t.Fatal("warmup round trip failed:", err)
	}

	// Phase 2: go quiet past the primary client-side idle-reclamation
	// boundary at the low end of the eviction band (XMUX clientIdleTimeout
	// min 90s). The connection must survive this without needing a
	// browser refresh. (Crossing the full 300s http2.Transport
	// IdleConnTimeout boundary is a separate, slower regression; see
	// the -timeout note above the test.)
	t.Log("idle for 100s...")
	time.Sleep(100 * time.Second)

	// Phase 3: the SAME connection must still carry traffic.
	if err := roundTrip("post-idle"); err != nil {
		t.Fatalf("connection died after long idle (page would need refresh): %v", err)
	}
	t.Log("post-idle round trip OK")
}

type longIdleErr struct{ s string }

func (e *longIdleErr) Error() string { return e.s }
