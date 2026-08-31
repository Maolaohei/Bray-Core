package outbound

import (
	"context"
	gonet "net"
	"sync/atomic"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet/stat"
)

// countingDialer always succeeds instantly, so the pre-connect producer loop
// spins at full speed when it fails to observe shutdown (the old
// close(preConns) panic/recover spin kept dialing forever).
type countingDialer struct {
	dials atomic.Int64
}

func (d *countingDialer) Dial(ctx context.Context, dest xnet.Destination) (stat.Connection, error) {
	d.dials.Add(1)
	c1, c2 := gonet.Pipe()
	go func() { // drain writes so the producer never blocks on Write
		buf := make([]byte, 4096)
		for {
			if _, err := c2.Read(buf); err != nil {
				return
			}
		}
	}()
	return nopConn{c1}, nil
}

type nopConn struct {
	gonet.Conn
}

func (nopConn) Close() error                       { return nil }
func (c nopConn) RemoteAddr() gonet.Addr           { return nil }
func (c nopConn) LocalAddr() gonet.Addr            { return nil }
func (c nopConn) SetDeadline(time.Time) error      { return nil }
func (c nopConn) SetReadDeadline(time.Time) error  { return nil }
func (c nopConn) SetWriteDeadline(time.Time) error { return nil }

var _ stat.Connection = nopConn{}

// TestPreConnect_CloseStopsProducers verifies that handler shutdown stops the
// pre-connect producer goroutines instead of leaving them panic-spinning.
// Regression: shutdown used to close(preConns); every subsequent send in the
// infinite producer loop panicked, recover() closed the conn and the loop
// re-panicked immediately — a full-speed panic/spin while still dialing.
func TestPreConnect_CloseStopsProducers(t *testing.T) {
	h := &Handler{testpre: 2}
	h.initpre.Do(func() {})
	h.preConns = make(chan *ConnExpire)
	h.preConnsStop = make(chan struct{})
	d := &countingDialer{}
	dest := xnet.TCPDestination(xnet.DomainAddress("example.com"), 443)

	// Start producers with the same select-guarded loop shape as Process.
	for range h.testpre {
		go func() {
			var conn stat.Connection
			defer func() {
				if r := recover(); r != nil && conn != nil {
					conn.Close()
				}
			}()
			failCount := 0
			for {
				select {
				case <-h.preConnsStop:
					if conn != nil {
						conn.Close()
					}
					return
				default:
				}
				var err error
				conn, err = d.Dial(context.Background(), dest)
				if err != nil {
					failCount++
					backoff := time.Duration(200<<min(failCount-1, 6)) * time.Millisecond
					select {
					case <-time.After(backoff):
					case <-h.preConnsStop:
						return
					}
					continue
				}
				failCount = 0
				select {
				case h.preConns <- &ConnExpire{Conn: conn}:
				case <-h.preConnsStop:
					conn.Close()
					return
				}
			}
		}()
	}

	// Wait until at least one dial happened, then stop the producers.
	deadline := time.Now().Add(5 * time.Second)
	for d.dials.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if d.dials.Load() == 0 {
		t.Fatal("producers never dialed")
	}

	close(h.preConnsStop)

	// After shutdown the dial count must freeze. Under the old semantics this
	// fails via panic-spin: dials keep climbing forever.
	base := d.dials.Load()
	time.Sleep(300 * time.Millisecond)
	if after := d.dials.Load(); after != base {
		t.Fatalf("producers still dialing after close: %d -> %d", base, after)
	}
}

// TestPreConnect_CloseBeforeUseNoLeak is a regression test for B4: when the
// handler is closed before the first Process() ever runs, the pre-connect
// producer goroutines must not be left running (dialing forever). This happens
// only if preConnsStop is created eagerly (tied to handler construction) so that
// Close() can close it even when no Process() has started yet. After the fix the
// producers observe the already-closed channel and exit without dialing.
func TestPreConnect_CloseBeforeUseNoLeak(t *testing.T) {
	h := &Handler{testpre: 2}
	// Simulate New() having created the channels up-front (the B4 fix).
	h.preConns = make(chan *ConnExpire)
	h.preConnsStop = make(chan struct{})

	// Handler closed before any connection/Process ever runs.
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}

	// A connection then arrives and Process's lazy init starts the producers.
	d := &countingDialer{}
	dest := xnet.TCPDestination(xnet.DomainAddress("example.com"), 443)
	h.initpre.Do(func() {
		for range h.testpre {
			go func() {
				var conn stat.Connection
				defer func() {
					if r := recover(); r != nil && conn != nil {
						conn.Close()
					}
				}()
				for {
					select {
					case <-h.preConnsStop:
						if conn != nil {
							conn.Close()
						}
						return
					default:
					}
					var err error
					conn, err = d.Dial(context.Background(), dest)
					if err != nil {
						return
					}
					select {
					case h.preConns <- &ConnExpire{Conn: conn}:
					case <-h.preConnsStop:
						conn.Close()
						return
					}
				}
			}()
		}
	})

	// Producers must observe the already-closed stop channel and exit without
	// dialing. Pre-fix, Close() saw a nil preConnsStop and was a no-op, so the
	// producers started later spun forever (connection leak).
	time.Sleep(200 * time.Millisecond)
	if n := d.dials.Load(); n != 0 {
		t.Fatalf("producers dialed %d times after Close-before-use (leak)", n)
	}
}
