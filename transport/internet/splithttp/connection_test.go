package splithttp

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestSplitConnSatisfiesNetConn(t *testing.T) {
	r, w := io.Pipe()
	defer w.Close()
	defer r.Close()

	var _ net.Conn = &splitConn{
		reader: r,
		writer: w,
	}
}

func TestSplitConnDeadlineMethodsReturnNil(t *testing.T) {
	r, w := io.Pipe()
	defer w.Close()
	defer r.Close()

	conn := &splitConn{
		reader:     r,
		writer:     w,
		remoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234},
		localAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5678},
	}

	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Errorf("SetDeadline returned error: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Errorf("SetReadDeadline returned error: %v", err)
	}
	if err := conn.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Errorf("SetWriteDeadline returned error: %v", err)
	}
}

func TestSplitConnDeadlineZeroValue(t *testing.T) {
	r, w := io.Pipe()
	defer w.Close()
	defer r.Close()

	conn := &splitConn{reader: r, writer: w}

	if err := conn.SetDeadline(time.Time{}); err != nil {
		t.Errorf("SetDeadline with zero time returned error: %v", err)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Errorf("SetReadDeadline with zero time returned error: %v", err)
	}
	if err := conn.SetWriteDeadline(time.Time{}); err != nil {
		t.Errorf("SetWriteDeadline with zero time returned error: %v", err)
	}
}

func TestSplitConnReadDeadlineTimesOut(t *testing.T) {
	// Reader is a pipe that never gets data; deadline must surface net.Error timeout.
	// This is the IdleRecovery hang class: H2 body is not net.Conn so SetReadDeadline
	// used to be a silent no-op and Read blocked forever.
	r, w := io.Pipe()
	defer w.Close()
	defer r.Close()

	conn := &splitConn{reader: r, writer: w}
	if err := conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	buf := make([]byte, 8)
	start := time.Now()
	n, err := conn.Read(buf)
	elapsed := time.Since(start)
	if n != 0 {
		t.Fatalf("n=%d want 0", n)
	}
	if err == nil {
		t.Fatal("expected timeout error")
	}
	ne, ok := err.(net.Error)
	if !ok || !ne.Timeout() {
		t.Fatalf("err=%v type=%T; want net.Error Timeout", err, err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("timeout took %v; expected ~50ms", elapsed)
	}

	// After timeout, delivering data must unblock subsequent Read (pending delivery).
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		_, _ = w.Write([]byte("ok"))
	}()
	n, err = conn.Read(buf)
	if err != nil {
		t.Fatalf("Read after timeout: %v", err)
	}
	if string(buf[:n]) != "ok" {
		t.Fatalf("got %q", string(buf[:n]))
	}
}

func TestSplitConnReadDelegation(t *testing.T) {
	r, w := io.Pipe()
	defer w.Close()
	defer r.Close()

	conn := &splitConn{reader: r, writer: w}

	go func() {
		w.Write([]byte("hello"))
	}()

	buf := make([]byte, 5)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(buf[:n]) != "hello" {
		t.Errorf("Read returned %q, want %q", string(buf[:n]), "hello")
	}
}

func TestSplitConnWriteDelegation(t *testing.T) {
	r, w := io.Pipe()
	defer w.Close()
	defer r.Close()

	conn := &splitConn{reader: r, writer: w}

	out := make([]byte, 5)
	go io.ReadFull(r, out)

	n, err := conn.Write([]byte("world"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != 5 {
		t.Errorf("Write returned n=%d, want 5", n)
	}
	if string(out) != "world" {
		t.Errorf("Read after Write got %q, want %q", string(out), "world")
	}
}

func TestSplitConnAddr(t *testing.T) {
	local := &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 1111}
	remote := &net.TCPAddr{IP: net.IPv4(10, 0, 0, 2), Port: 2222}
	conn := &splitConn{localAddr: local, remoteAddr: remote}

	if conn.LocalAddr() != local {
		t.Errorf("LocalAddr mismatch")
	}
	if conn.RemoteAddr() != remote {
		t.Errorf("RemoteAddr mismatch")
	}
}

func TestSplitConnCloseNilOnClose(t *testing.T) {
	r, w := io.Pipe()
	conn := &splitConn{reader: r, writer: w, onClose: nil}

	if err := conn.Close(); err != nil {
		t.Errorf("Close with nil onClose returned error: %v", err)
	}
}

func TestSplitConnCloseCallsOnClose(t *testing.T) {
	r, w := io.Pipe()
	called := false
	conn := &splitConn{
		reader:  r,
		writer:  w,
		onClose: func() { called = true },
	}

	conn.Close()
	if !called {
		t.Error("onClose was not called")
	}
}

type errCloser struct {
	err error
}

func (c *errCloser) Read(p []byte) (int, error)  { return 0, io.EOF }
func (c *errCloser) Write(p []byte) (int, error) { return len(p), nil }
func (c *errCloser) Close() error                { return c.err }

type errWriter struct {
	err error
}

func (w *errWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *errWriter) Close() error                { return w.err }

func TestSplitConnCloseWriterErrorPriority(t *testing.T) {
	writerErr := errors.New("writer close failed")
	conn := &splitConn{
		reader: &errCloser{nil},
		writer: &errWriter{writerErr},
	}

	err := conn.Close()
	if err != writerErr {
		t.Errorf("expected writer error, got %v", err)
	}
}

func TestSplitConnCloseReaderErrorReturned(t *testing.T) {
	readerErr := errors.New("reader close failed")
	conn := &splitConn{
		reader: &errCloser{readerErr},
		writer: &errWriter{nil},
	}

	err := conn.Close()
	if err != readerErr {
		t.Errorf("expected reader error, got %v", err)
	}
}

func TestSplitConnCloseBothErrorsWriterPriority(t *testing.T) {
	writerErr := errors.New("writer close failed")
	readerErr := errors.New("reader close failed")
	conn := &splitConn{
		reader: &errCloser{readerErr},
		writer: &errWriter{writerErr},
	}

	err := conn.Close()
	if err != writerErr {
		t.Errorf("expected writer error (priority), got %v", err)
	}
}

func TestSplitConnCloseSuccess(t *testing.T) {
	conn := &splitConn{
		reader: &errCloser{nil},
		writer: &errWriter{nil},
	}

	if err := conn.Close(); err != nil {
		t.Errorf("Close returned error: %v", err)
	}
}

func TestSplitConnCloseNilReader(t *testing.T) {
	r, w := io.Pipe()
	_ = r
	called := false
	conn := &splitConn{
		writer: w,
		// reader intentionally nil (OpenStream failed before assignment)
		onClose: func() { called = true },
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !called {
		t.Fatal("onClose not called")
	}
}

func TestSplitConnCloseNilWriterAndReader(t *testing.T) {
	called := false
	conn := &splitConn{onClose: func() { called = true }}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !called {
		t.Fatal("onClose not called")
	}
}

func TestSplitConnWriteDeadlineTimesOut(t *testing.T) {
	// Unbuffered pipe: write with short deadline should time out while peer
	// is not reading, then succeed after clearing deadline and draining.
	r, w := io.Pipe()
	defer w.Close()
	defer r.Close()

	conn := &splitConn{reader: r, writer: w}
	if err := conn.SetWriteDeadline(time.Now().Add(40 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	// Drain only after the deadline path has been exercised.
	startDrain := make(chan struct{})
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		<-startDrain
		buf := make([]byte, 64*1024)
		for {
			_, err := r.Read(buf)
			if err != nil {
				return
			}
		}
	}()

	start := time.Now()
	_, err := conn.Write(make([]byte, 64*1024))
	elapsed := time.Since(start)
	if err == nil {
		t.Log("write completed before deadline; skip timeout assert")
	} else {
		ne, ok := err.(net.Error)
		if !ok || !ne.Timeout() {
			t.Fatalf("err=%v type=%T want net.Error Timeout", err, err)
		}
		if elapsed > 500*time.Millisecond {
			t.Fatalf("timeout took %v", elapsed)
		}
	}

	if err := conn.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatalf("clear deadline: %v", err)
	}
	close(startDrain)

	// Completes timed-out in-flight write (if any), then accepts a new one.
	if _, err := conn.Write([]byte("ok")); err != nil {
		t.Fatalf("write after timeout path: %v", err)
	}
	_ = w.Close()
	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		t.Fatal("drain goroutine stuck")
	}
}
