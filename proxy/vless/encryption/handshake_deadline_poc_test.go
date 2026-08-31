package encryption

// POC for REALITY 专项 R1 (HIGH Slowloris DoS): ServerInstance.Handshake()
// performs multiple io.ReadFull(conn, ...) calls (server.go:130, 197, 224,
// 280, 355) with NO read deadline set on conn. The inbound path
// (proxy/vless/inbound/inbound.go:274) calls h.decryption.Handshake(connection,
// nil) BEFORE setting the read deadline (inbound.go:280), so a client that
// completes REALITY auth but then stalls on the ML-KEM exchange blocks the
// server goroutine (and holds the FD) forever -> unbounded goroutine + socket
// exhaustion under many stalled clients.
//
// This test drives the REAL ServerInstance.Handshake:
//   - NEGATIVE (pre-fix behavior): invoke Handshake on a conn with NO deadline
//     and a client that sends nothing -> Handshake blocks indefinitely (the
//     Slowloris hang / goroutine leak is reproduced).
//   - POSITIVE (post-fix mechanism): set a short read deadline on the conn
//     BEFORE Handshake (exactly what inbound.go must do) -> Handshake returns
//     within the deadline instead of hanging.
//
// The production fix lives in proxy/vless/inbound/inbound.go: move the
// policy fetch + connection.SetReadDeadline(...) to BEFORE the
// h.decryption.Handshake(...) call so the handshake is deadline-bounded.

import (
	"crypto/mlkem"
	"net"
	"testing"
	"time"
)

func newTestServerInstance(t *testing.T) *ServerInstance {
	t.Helper()
	key, err := mlkem.GenerateKey768()
	if err != nil {
		t.Fatalf("mlkem.GenerateKey768: %v", err)
	}
	decap := key.Bytes()
	i := &ServerInstance{}
	if err := i.Init([][]byte{decap}, 0, 0, 0, ""); err != nil {
		t.Fatalf("ServerInstance.Init: %v", err)
	}
	return i
}

func TestR1_ServerHandshakeNoDeadlineHang(t *testing.T) {
	i := newTestServerInstance(t)

	// --- NEGATIVE: no deadline -> Handshake blocks (Slowloris hang) ---
	clientConn, serverConn := net.Pipe()
	errCh := make(chan error, 1)
	go func() {
		_, e := i.Handshake(serverConn, nil)
		errCh <- e
	}()
	// client sends nothing; the handshake must block on io.ReadFull.

	select {
	case e := <-errCh:
		t.Fatalf("R1 NOT reproduced: Handshake returned without a deadline (got %v)", e)
	case <-time.After(2 * time.Second):
		// blocked -> R1 reproduced (goroutine + socket leak). Release it.
	}
	clientConn.Close()
	select {
	case <-errCh:
		// unblocked after the peer closed; confirms it was blocked, not exited.
	case <-time.After(2 * time.Second):
		t.Fatal("Handshake did not unblock after the conn was closed")
	}

	// --- POSITIVE: a read deadline set BEFORE Handshake bounds it (the fix) ---
	clientConn2, serverConn2 := net.Pipe()
	if err := serverConn2.SetReadDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	errCh2 := make(chan error, 1)
	go func() {
		_, e := i.Handshake(serverConn2, nil)
		errCh2 <- e
	}()
	select {
	case e := <-errCh2:
		// bounded by the deadline -> fix is effective.
		_ = e
	case <-time.After(2 * time.Second):
		t.Fatal("R1 fix ineffective: Handshake ignored the read deadline and hung")
	}
	clientConn2.Close()
}
