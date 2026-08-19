package scenarios

// Multi-connection session-sharing regression: in packet-up + dseg, two
// logical connections from the same client share one XMUX transport
// connection. If closing one logical connection tears down the shared
// server-side session (uploadQueue closed), the other connection's
// packet-up POSTs start failing 404 — the "page hangs, reactivate node"
// symptom reported on X-style sites. This test opens connA and connB,
// transfers on both, closes connA while connB stays open, then keeps
// transferring on connB to assert its session survives.
//
// XOR echo upstream expected response = payload ^ 'c'.

import (
	"io"
	stdnet "net"
	"strconv"
	"testing"
	"time"

	"github.com/xtls/xray-core/testing/servers/tcp"
)

func TestVLESSXHTTP_ConnCloseDoesNotKillSharedSession(t *testing.T) {
	if testing.Short() {
		t.Skip("session-sharing regression skipped in short mode")
	}
	ep := &dsegEndpoints{serverPort: tcp.PickPort()}
	startServerOnly(t, ep)
	startClientOnly(t, ep, "127.0.0.1", ep.serverPort)
	addr := "127.0.0.1:" + strconv.Itoa(int(ep.clientPort))

	payload := make([]byte, 16<<10)
	for i := range payload {
		payload[i] = 0x3C
	}
	want := make([]byte, len(payload))
	for i, v := range payload {
		want[i] = v ^ 'c'
	}

	roundTrip := func(conn stdnet.Conn, phase string, i int) error {
		_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
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
				return &sessShareErr{phase + " echo mismatch"}
			}
		}
		return nil
	}

	connA, err := stdnet.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	connB, err := stdnet.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer connB.Close()

	// Both carry traffic first (forces both legs to be established).
	for i := 0; i < 3; i++ {
		if err := roundTrip(connA, "A", i); err != nil {
			t.Fatalf("connA warmup i=%d: %v", i, err)
		}
		if err := roundTrip(connB, "B", i); err != nil {
			t.Fatalf("connB warmup i=%d: %v", i, err)
		}
	}

	// Close connA; connB must keep its session usable.
	_ = connA.Close()
	time.Sleep(500 * time.Millisecond)

	for i := 0; i < 8; i++ {
		if err := roundTrip(connB, "B-post-close", i); err != nil {
			t.Fatalf("connB failed after connA close (shared session torn down?): i=%d: %v", i, err)
		}
	}
	t.Log("connB survived connA close")
}

type sessShareErr struct{ s string }

func (e *sessShareErr) Error() string { return e.s }
