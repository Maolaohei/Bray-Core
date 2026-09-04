package scenarios

// POC: XHTTP + packet-up + dseg 断流 (stalled mid-download) — REPRODUCED.
//
// CONFIRMED root cause chain (each step backed by code + POC logs; see
// DSEG_STALL_ROOT_CAUSE.md):
//
//  1. Accounting hole: the dseg production-leg GET and all 6 segment-pull
//     GETs are issued directly via httpClient2/dc (dialer.go OpenStreamAsync
//     / PullSegment) WITHOUT going through XMUX Borrow bookkeeping and
//     WITHOUT touching the reuse budget (remaining) — the XMUX pool cannot
//     see them. The only XMUX slot a logical conn holds is the upload side
//     (ownedUploadXmux, dialer.go).
//  2. Bidirectional traffic burns the upload POST budget (CMaxReuseTimes /
//     remaining). Once exhausted, GetXmuxClient no longer reuses connection
//     A and returns a fresh client B; the rescue path swaps ownership and
//     calls prev.Release() (dialer.go rescueClient -> ownedUploadXmux swap)
//     — dropping A's activeStreams to 0 while the session's production leg
//     and pullers still live on A.
//  3. Within <=5s, XmuxManager.healthCheckTick sees remaining==0 &&
//     activeStreams==0 and drains A (mux.go retireLocked "budget/lifetime
//     exhausted"); in Draining state tryClose() immediately
//     hard-closes the underlying TLS/H2 transport — the transport carrying
//     the STILL-ACTIVE session's production leg and all segment pulls.
//  4. Server side: the production-leg request context is cancelled
//     (DBGPROD "prodLeg exit: ctx done"), the session defer deletes the
//     session (dllegs=0 -> deleteSession); subsequent GET ?seq=N pulls 404
//     forever while the client puller keeps retrying -> download makes no
//     more progress. User-visible: mid-stream stall — "断流".
//
// Short sessions finish before the upload budget/lifetime is exhausted, which
// is why the bug only appears after LONG usage ("短时间正常，长时间必现").
// Pure-download flows never re-enter the upload decision loop, so they do not
// rotate and do not stall (verified: 100s pure download PASS) — the stall
// requires upload-active flows (video call / SSH / web traffic).
//
// Trigger: the harness keeps production XMUX budgets except HMaxReusableSecs
// (via dsegEndpoints.xmuxOverride), shrunk to 2s so the lifetime-expiry
// rotation lands inside the observation window — the same deadlineUnix
// (ex-UnreusableAt) renewal production users hit after 600-1200s of browsing.
//
// Run: go test ./testing/scenarios -run TestXHTTPDsegStall_POC -v -count 1

import (
	stdnet "net"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport/internet/splithttp"
)

func TestXHTTPDsegStall_POC(t *testing.T) {
	// Production-faithful trigger: the XMUX connection lifetime
	// (HMaxReusableSecs; production default 600-1200s) is shrunk to 2s so
	// the lifetime-expiry rotation lands INSIDE the observation window.
	// Once the lifetime deadline (deadlineUnix, ex-UnreusableAt) passes, the
	// upload chunk loop renews the upload onto
	// a fresh XMUX client at the next written chunk (dialer.go renew path),
	// releasing the Borrow that — before the pin fix — was the only thing
	// keeping the old client (which carries this session's dseg production
	// leg + all segment pulls) out of the health check's drain. All other
	// XMUX knobs keep their production defaults.
	hiRate := int64(4 << 20) // 4 MiB/s paced push
	push := startPushTarget(t, hiRate)
	defer push.Close()

	ep := &dsegEndpoints{
		serverPort:   tcp.PickPort(),
		destOverride: push.dest,
		xmuxOverride: &splithttp.XmuxConfig{
			HMaxReusableSecs: &splithttp.RangeConfig{From: 2, To: 2},
		},
	}
	startServerOnly(t, ep)
	startClientOnly(t, ep, "127.0.0.1", ep.serverPort)

	// The long-lived BIDIRECTIONAL connection: the push target streams
	// downstream at 4 MiB/s while the app keeps uploading (models a video
	// call / SSH / any upload-active flow). The upload chunk loop is what
	// drives the XMUX rotation decision: once the configured Xmux lifetime
	// expires, the next written chunk rotates the upload onto a fresh XMUX
	// client and RELEASES the Borrow that was keeping the old client (which
	// carries this session's dseg production leg + all segment pulls) alive.
	conn1, err := dialClient(ep.clientPort)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()

	// Sustained upload: 16KiB every 50ms (~320KB/s) keeps the packet-up
	// chunk loop active so the rotation point is reached.
	upStop := make(chan struct{})
	defer close(upStop)
	upErrCh := make(chan error, 1)
	go func() {
		chunk := make([]byte, 16<<10)
		for i := range chunk {
			chunk[i] = byte(i)
		}
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-upStop:
				return
			case <-ticker.C:
			}
			if _, err := conn1.Write(chunk); err != nil {
				upErrCh <- err
				return
			}
		}
	}()

	var got1 int64
	lastProgress := time.Now()
	done := make(chan struct{})
	go func() {
		defer close(done)
		var buf [64 << 10]byte
		for {
			n, rerr := conn1.Read(buf[:])
			if n > 0 {
				got1 += int64(n)
				lastProgress = time.Now()
			}
			if rerr != nil {
				return
			}
		}
	}()

	// Interleave short connections so the pool stays warm and the health
	// check's pool-rotation paths run exactly as under real browsing.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			conn2, err := dialClient(ep.clientPort)
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			_ = echoOnce(conn2, 8<<10)
			_ = conn2.Close()
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// Observe for 100s. A healthy 4 MiB/s flow makes continuous progress; a
	// mid-download XMUX rotation kill shows up as a >=20s no-progress stall
	// (the puller's 30s prodErr grace is longer, so a true 断流 shows first).
	const window = 100 * time.Second
	const stallLimit = 20 * time.Second
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		select {
		case <-done:
			t.Fatalf("XHTTP dseg stream closed by peer after %d bytes — mid-download disconnect (断流)", got1)
		case <-time.After(500 * time.Millisecond):
		}
		if stalled := time.Since(lastProgress); stalled > stallLimit {
			t.Fatalf("XHTTP dseg stream STALLED: no progress for %v (received %d bytes so far) — 断流 reproduced", stalled.Round(time.Second), got1)
		}
	}
	t.Logf("POC: download flowed continuously for %v, %d bytes total (%.1f MiB/s), no stall",
		window, got1, float64(got1)/(1<<20)/window.Seconds())
}

// startPushTarget is a paced endless variant of startPushServer: every
// accepted connection gets a continuous rate-limited push (models a live
// high-bitrate stream) until the peer goes away or the test ends.
func startPushTarget(tb testing.TB, bytesPerSecond int64) (st struct {
	dest  net.Destination
	Close func()
}) {
	tb.Helper()
	ln, err := stdnet.Listen("tcp", "127.0.0.1:0")
	common.Must(err)
	dest := net.TCPDestination(net.LocalHostIP, net.Port(ln.Addr().(*stdnet.TCPAddr).Port))

	var wg sync.WaitGroup
	done := make(chan struct{})
	block := 256 << 10 // 256 KiB
	blockDelay := time.Duration(int64(time.Second) * int64(block) / bytesPerSecond)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				close(done)
				return
			}
			wg.Add(1)
			go func(conn stdnet.Conn) {
				defer wg.Done()
				defer conn.Close()
				payload := make([]byte, block)
				for i := range payload {
					payload[i] = 0x5A
				}
				// Drain the peer's uplink (real bidirectional apps read what
				// the other side sends). Without this the server-side XHTTP
				// upload queue fills up ("packet queue full") and the upload
				// chain eventually tears the whole connection down — a POC
				// artifact, not the 断流 under investigation.
				go func() {
					var sink [16 << 10]byte
					for {
						if _, err := conn.Read(sink[:]); err != nil {
							return
						}
					}
				}()
				ticker := time.NewTicker(blockDelay)
				defer ticker.Stop()
				for {
					select {
					case <-done:
						return
					case <-ticker.C:
					}
					if _, err := conn.Write(payload); err != nil {
						return
					}
				}
			}(c)
		}
	}()
	st.dest = dest
	st.Close = func() {
		_ = ln.Close()
		wg.Wait()
	}
	return st
}
