package scenarios

// POC/regression gate for the dseg download self-teardown (user report:
// 64MB download dies after 2-4s, curl(18), downseg_puller EOF -> fatal).
//
// Two workloads:
//
//  1. TestDsegDownlinkStallBeyondDownlinkOnly — origin stalls 2s mid-download;
//     a healthy implementation must deliver all bytes.
//
//  2. TestDsegLargeFileEcho64MiB — the exact user shape: ONE connection,
//     16 x 4MiB echo rounds (64MiB total). The original scenario harness
//     passes despite the teardown cascade because each echo round tolerates
//     a reconnect; this variant fails on ANY mid-stream truncation, so the
//     cascade (prodLeg exit -> session close -> sibling pull EOF) is visible
//     as a hard failure instead of being masked.

import (
	"context"
	"crypto/rand"
	"io"
	stdnet "net"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/testing/servers/tcp"
)

// startStallingPushServer writes headBytes, stalls for stallFor, then writes
// the remaining totalBytes-headBytes and closes — modelling a slow upstream
// (slow disk, cold cache, throttled origin) behind a healthy proxy path.
func startStallingPushServer(totalBytes, headBytes int64, stallFor time.Duration) (net.Destination, func()) {
	ln, err := stdnet.Listen("tcp", "127.0.0.1:0")
	common.Must(err)
	dest := net.TCPDestination(net.LocalHostIP, net.Port(ln.Addr().(*stdnet.TCPAddr).Port))

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func(conn stdnet.Conn) {
				defer wg.Done()
				defer conn.Close()
				payload := make([]byte, 256<<10)
				for i := range payload {
					payload[i] = 0x5A
				}
				written := int64(0)
				for written < totalBytes {
					select {
					case <-ctx.Done():
						return
					default:
					}
					if written == headBytes {
						time.Sleep(stallFor)
					}
					n := int64(len(payload))
					if written+n > totalBytes {
						n = totalBytes - written
					}
					if _, err := conn.Write(payload[:n]); err != nil {
						return
					}
					written += n
				}
			}(c)
		}
	}()

	cleanup := func() {
		cancel()
		_ = ln.Close()
		wg.Wait()
	}
	return dest, cleanup
}

// TestDsegDownlinkStallBeyondDownlinkOnly drives an 8MiB dseg download whose
// origin stalls 2s after the first 4MiB. The whole transfer must complete.
func TestDsegDownlinkStallBeyondDownlinkOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scenario in short mode")
	}
	const totalBytes = int64(8 << 20)
	const headBytes = int64(4 << 20)

	pushDest, cleanup := startStallingPushServer(totalBytes, headBytes, 2*time.Second)
	defer cleanup()

	ep := &dsegEndpoints{serverPort: tcp.PickPort(), destOverride: pushDest}
	startServerOnly(t, ep)
	startClientOnly(t, ep, "127.0.0.1", ep.serverPort)

	conn, err := stdnet.Dial("tcp", "127.0.0.1:"+strconvItoa(int(ep.clientPort)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}

	got, err := io.Copy(io.Discard, conn)
	if err != nil {
		t.Fatalf("download failed mid-stream (DownlinkOnly self-teardown?): %v (got %d/%d bytes)", err, got, totalBytes)
	}
	if got != totalBytes {
		t.Fatalf("truncated download: got %d bytes, want %d", got, totalBytes)
	}
}

// TestDsegLargeFileEcho64MiB replays the user's failing workload: one
// connection, 16 sequential 4MiB request->response rounds (echo semantics),
// byte-verified per round. Any mid-stream teardown surfaces as an error.
func TestDsegLargeFileEcho64MiB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scenario in short mode")
	}
	ep := &dsegEndpoints{serverPort: tcp.PickPort()}
	startServerOnly(t, ep)
	startClientOnly(t, ep, "127.0.0.1", ep.serverPort)

	conn, err := stdnet.Dial("tcp", "127.0.0.1:"+strconvItoa(int(ep.clientPort)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(120 * time.Second)); err != nil {
		t.Fatal(err)
	}

	chunk := make([]byte, 4<<20)
	for j := 0; j < 16; j++ {
		if _, err := rand.Read(chunk); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write(chunk); err != nil {
			t.Fatalf("round %d: write failed: %v", j, err)
		}
		back := make([]byte, len(chunk))
		if _, err := io.ReadFull(conn, back); err != nil {
			t.Fatalf("round %d: response truncated after %d rounds (teardown?): %v", j, j, err)
		}
		for i := range chunk {
			if back[i] != chunk[i]^'c' {
				t.Fatalf("round %d: byte mismatch at %d (stream corruption)", j, i)
			}
		}
	}
}

func strconvItoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}
