package scenarios

// Session-churn stress: packet-up clients rapidly open + close connections
// against the same server (browser multi-tab churn, X-style sites), while
// intermittent long-lived connections carry sustained traffic. Ate the
// server session-map correctness — if closing one leg tears down a session
// a sibling leg still uses, packet-up POSTs start getting 404
// ("XHTTP packet-up POST retry seq=N > 404" in user logs). This drives the
// real dual-end hard and asserts no 404-induced upload loss across N rounds.

import (
	"io"
	stdnet "net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/testing/servers/tcp"
)

func TestVLESSXHTTP_SessionChurnNo404(t *testing.T) {
	if testing.Short() {
		t.Skip("session-churn regression skipped in short mode")
	}
	ep := &dsegEndpoints{serverPort: tcp.PickPort()}
	startServerOnly(t, ep)
	startClientOnly(t, ep, "127.0.0.1", ep.serverPort)
	addr := "127.0.0.1:" + strconv.Itoa(int(ep.clientPort))

	payload := make([]byte, 8<<10)
	for i := range payload {
		payload[i] = 0x7A
	}
	want := make([]byte, len(payload))
	for i, v := range payload {
		want[i] = v ^ 'c'
	}

	roundTrip := func(conn stdnet.Conn) error {
		_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
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
				return &churnErr{"echo mismatch"}
			}
		}
		return nil
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var errCount int

	// One long-lived connection continuously transferring.
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := stdnet.Dial("tcp", addr)
		if err != nil {
			mu.Lock()
			errCount++
			mu.Unlock()
			return
		}
		defer conn.Close()
		for i := 0; i < 30; i++ {
			if err := roundTrip(conn); err != nil {
				mu.Lock()
				errCount++
				mu.Unlock()
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	// Many churning short connections: connect, one round trip, close.
	for w := 0; w < 40; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				conn, err := stdnet.Dial("tcp", addr)
				if err != nil {
					mu.Lock()
					errCount++
					mu.Unlock()
					return
				}
				if err := roundTrip(conn); err != nil {
					conn.Close()
					mu.Lock()
					errCount++
					mu.Unlock()
					return
				}
				_ = conn.Close()
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}
	wg.Wait()

	if errCount > 0 {
		t.Fatalf("session churn produced %d errors (goose 404s / torn-down shared sessions?)", errCount)
	}
	t.Log("session churn OK: 0 errors")
}

// churnErr avoids clash with stdlib error vars.
type churnErr struct{ s string }

func (e *churnErr) Error() string { return e.s }
