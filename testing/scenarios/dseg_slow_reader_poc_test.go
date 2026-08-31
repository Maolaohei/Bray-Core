package scenarios

import (
	"io"
	stdnet "net"
	"testing"
	"time"

	"github.com/xtls/xray-core/testing/servers/tcp"
)

// POC: dseg end-of-transfer truncation under a slow consumer.
//
// DownSegPuller.Read() checks p.fatal BEFORE it looks at p.buf (see
// downseg_puller.go). That ordering is deliberate: a dead production GET means
// the server already discarded the session, so failing fast stops the upload
// side from POSTing on a stale sessionId.
//
// The side effect: when the production leg closes, any segment the puller
// already prefetched but the application has not read yet is discarded.
// monitorProductionLeg sets fatal on ANY prod-leg EOF, including the benign
// one that happens when the transfer completes normally.
//
// So the dangerous window is: the server finishes producing before the
// application drains the prefetch buffer (prefetchAheadSegs = 24 segments,
// ~24 MiB at the 1 MiB default segment size). A fast origin plus a slow
// reader is exactly that shape. Every existing dseg scenario reads with
// io.Copy(io.Discard, conn) or a byte-exact echo, both of which drain the
// buffer as fast as it fills, so none of them cover it.
//
// This test reads deliberately slower than the origin produces, then demands
// the full byte count anyway.
func TestDsegSlowReaderLargeDownload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scenario in short mode")
	}
	const totalBytes = int64(64 << 20)
	// 256 KiB every 5 ms ~= 51 MB/s, well under the ~134 MB/s this link
	// sustains, so the prefetch buffer must build up.
	const chunk = 256 << 10
	const chunkDelay = 5 * time.Millisecond

	pushDest, cleanup := startPushServer(totalBytes)
	defer cleanup()

	ep := &dsegEndpoints{serverPort: tcp.PickPort(), destOverride: pushDest}
	startServerOnly(t, ep)
	startClientOnly(t, ep, "127.0.0.1", ep.serverPort)

	conn, err := stdnet.Dial("tcp", "127.0.0.1:"+strconvItoa(int(ep.clientPort)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Generous absolute deadline: this is a correctness gate, not a speed one.
	if err := conn.SetDeadline(time.Now().Add(180 * time.Second)); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, chunk)
	var got int64
	for got < totalBytes {
		want := chunk
		if remaining := totalBytes - got; int64(want) > remaining {
			want = int(remaining)
		}
		n, err := io.ReadFull(conn, buf[:want])
		got += int64(n)
		if err != nil {
			t.Fatalf("download truncated at %d/%d bytes (prod-leg EOF discarded %d prefetched bytes?): %v",
				got, totalBytes, totalBytes-got, err)
		}
		time.Sleep(chunkDelay)
	}
}
