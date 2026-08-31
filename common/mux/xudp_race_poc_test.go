package mux

// POC for XMUX 专项 M1 (HIGH data race): in xudpEstablish(), x.Mux and
// x.Status are written OUTSIDE XUDPManager lock (server.go:306/312/320), while
// the init() ticker (session.go:244-245) and Session.Close (session.go:193-195)
// read the SAME fields WHILE HOLDING XUDPManager lock. Those are two different
// synchronization domains, so concurrent access is a genuine data race.
//
// This test drives the REAL xudpEstablish(): several mux server workers share
// the global XUDPManager and each receive an XUDP datagram with the SAME
// GlobalID, so their xudpEstablish() calls race on the same *XUDP (writing
// x.Mux/x.Status without the lock). Reader goroutines simulate the ticker /
// Session.Close by reading x.Mux/x.Status under XUDPManager lock. Run with
// `go test -race`. Before the fix this reports DATA RACE; after wrapping the
// writes in XUDPManager.Lock() the race is gone.

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

func TestXUDP_M1_EstablishRace(t *testing.T) {
	gid := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}

	var wg sync.WaitGroup

	// Writer side: many mux server workers, each receiving an XUDP datagram
	// with the SAME GlobalID (encodeXUDPFrame hardcodes {1..8}). They share
	// the global XUDPManager, so their xudpEstablish() calls race on the same
	// *XUDP, writing x.Mux/x.Status WITHOUT XUDPManager lock (pre-fix M1).
	const workers = 12
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, serverWriter := pipe.New()
			serverReader, clientWriter := pipe.New()
			link := &transport.Link{Reader: serverReader, Writer: serverWriter}
			worker, err := NewServerWorker(ctx, okayDispatcher{}, link)
			if err != nil {
				t.Error(err)
				return
			}
			defer worker.Close()
			target := net.UDPDestination(net.LocalHostIP, 9999)
			if err := clientWriter.WriteMultiBuffer(buf.MultiBuffer{encodeXUDPFrame(t, target)}); err != nil {
				t.Error(err)
				return
			}
			time.Sleep(300 * time.Millisecond)
		}()
	}

	// Reader side: simulate the init() ticker / Session.Close, which read
	// x.Mux/x.Status WHILE HOLDING XUDPManager lock.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				XUDPManager.Lock()
				if x := XUDPManager.Map[gid]; x != nil {
					if x.Mux != nil {
						_ = x.Mux.input
					}
					_ = x.Status
				}
				XUDPManager.Unlock()
				runtime.Gosched()
			}
		}()
	}

	wg.Wait()
}
