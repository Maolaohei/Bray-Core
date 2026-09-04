package splithttp

// packet_upload helpers: reliable PostPacket with bounded retry and a limited
// client-side in-flight window. Owned by dialer packet-up path; kept separate
// for unit testing without Dial.

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/bytespool"
	"github.com/xtls/xray-core/common/errors"
)

// packetUploadMaxAttempts is the max tries for a single packet-up POST
// (1 initial + retries). 4 covers one outer-path blip (25/50/100ms backoff
// stays inside the server five-second gap timeout) while bounding HoL delay.
const packetUploadMaxAttempts = 4

// packetUploadRetryBase is the first backoff step after a failed POST.
const packetUploadRetryBase = 25 * time.Millisecond

// packetUploadDefaultWindow is the default number of concurrent POSTs per
// logical connection. Server reorder buffer (scMaxBufferedPosts, default 64)
// absorbs reordering; 12 fills BDP better on mid-RTT paths without flooding.
const packetUploadDefaultWindow = 12

// packetUploadMaxWindow hard-caps client in-flight posts regardless of server
// buffer size (H1 pool, memory, and CDN concurrency friendliness).
const packetUploadMaxWindow = 24

// durableLocal reuses modest durable snapshots for postPacketReliable.
// Store *durableLocal so Put never captures a stack-local slice header.
type durableLocal struct {
	b []byte
}

var durableBytePool = sync.Pool{
	New: func() any {
		return &durableLocal{b: make([]byte, 0, 4096)}
	},
}

// durableKind tags the allocator so freeDurable never crosses pool boundaries.
const (
	durableNone      = 0
	durableLocalPool = 1
	durableBytesPool = 2
)

func allocDurable(n int) ([]byte, int, *durableLocal) {
	if n <= 0 {
		return nil, durableNone, nil
	}
	if n >= 2048 {
		// Prefer size-class pool for multi-KB posts (common full chunk).
		b := bytespool.Alloc(int32(n))
		return b[:n], durableBytesPool, nil
	}
	dl := durableBytePool.Get().(*durableLocal)
	if cap(dl.b) < n {
		dl.b = make([]byte, n)
	} else {
		dl.b = dl.b[:n]
	}
	return dl.b, durableLocalPool, dl
}

func freeDurable(b []byte, kind int, dl *durableLocal) {
	if kind == durableNone {
		return
	}
	switch kind {
	case durableBytesPool:
		if b != nil {
			bytespool.Free(b[:cap(b)])
		}
	case durableLocalPool:
		if dl == nil {
			return
		}
		// Only return modest caps to avoid retaining huge slices forever.
		if cap(dl.b) <= 16*1024 {
			dl.b = dl.b[:0]
			durableBytePool.Put(dl)
		}
	}
}

// formatSeqInt64 formats a non-negative sequence without strconv heap allocs
// on the packet-up hot path (seq is monotonic and almost always small).
func formatSeqInt64(seq int64) string {
	if seq < 0 {
		// Defensive: packet-up never uses negative seq; keep a rare fallback.
		return strconv.FormatInt(seq, 10)
	}
	if seq < int64(len(seqSmallCache)) {
		return seqSmallCache[seq]
	}
	var buf [20]byte
	i := len(buf)
	n := uint64(seq)
	for {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
		if n == 0 {
			break
		}
	}
	return string(buf[i:])
}

// seqSmallCache covers the common first N posts per stream without alloc.
var seqSmallCache = func() [4096]string {
	var a [4096]string
	for i := 0; i < len(a); i++ {
		a[i] = strconv.FormatInt(int64(i), 10)
	}
	return a
}()

// packetUploadWindow returns the client POST in-flight window.
// It never exceeds half of the server's reorder capacity so misordered
// bursts still fit in upload_queue without "packet queue is too large".
// Optional rtt raises the window on high-latency paths (BDP-ish).
func packetUploadWindow(scMaxBufferedPosts int, rtt time.Duration) int {
	maxW := packetUploadDefaultWindow
	if scMaxBufferedPosts > 0 {
		maxW = scMaxBufferedPosts / 2
		if maxW < 1 {
			maxW = 1
		}
		if maxW > packetUploadMaxWindow {
			maxW = packetUploadMaxWindow
		}
	}

	w := packetUploadDefaultWindow
	switch {
	case rtt >= 200*time.Millisecond:
		// Was 24 (packetUploadMaxWindow): on high-RTT/jittery links a lost
		// seq's retry backoff can exceed the server five-second gap timeout and abort
		// the whole session. Cap lower to keep retries inside the gap window.
		// 8 (ToT L2): fewer in-flight POSTs also shrink amplification when
		// the outer path reorders heavily.
		w = 8
	case rtt >= 80*time.Millisecond:
		w = 18
	case rtt >= 20*time.Millisecond:
		w = 12
	case rtt > 0:
		// Very low RTT: less concurrency needed; still pipeline a little.
		w = 8
	}
	if w > maxW {
		return maxW
	}
	if w < 1 {
		return 1
	}
	return w
}

// packet-up chunk floors/caps. Always clamped by configured scMaxEachPostBytes.
// Low RTT: smaller chunks cut TTFB and memory per in-flight POST.
// High RTT: larger chunks fill BDP with the existing window.
const (
	packetUploadChunkMin int32 = 32 * 1024
	packetUploadChunkLow int32 = 256 * 1024
	packetUploadChunkMid int32 = 512 * 1024
)

// packetUploadBulkPaceBytes is the post body size at/above which configured
// scMinPostsIntervalMs is skipped. Idle/tiny posts keep camouflage pacing;
// bulk app writes (bench 16-32KiB, tunnels, large copies) must not be capped
// at ~30ms/post when scMaxEachPostBytes is much larger than the write size.
const packetUploadBulkPaceBytes int32 = 8 * 1024

// packetUploadSmallPacketAvg is the average per-buffer payload below which a
// flow is classified as small-packet (game heartbeats, DNS) and its chunk
// ceiling drops to packetUploadChunkMin so each POST flushes promptly.
const packetUploadSmallPacketAvg int32 = 256

// packetUploadRecentFlowWindow is how long after a launch a sub-bulk post is
// still treated as part of an active flow (pacing skipped). Active small
// flows (game heartbeats, tunnel bursts) already carry natural app timing —
// layering our cadence on top would only add latency with no camouflage
// gain. Reachability invariant: the default pacing band To (60ms) must stay
// ABOVE this window, or paced sleeps become unreachable again (the
// pre-2026.09.05 bug where a fixed 30ms band sat entirely inside the window).
// Pinned in TestDefaultMinPostIntervalJittered.
const packetUploadRecentFlowWindow = 50 * time.Millisecond

// PacketUploadChunkJitterFn returns the jittered effective chunk ceiling for
// one POST. Sustained bulk flows otherwise repeat one exact Content-Length
// on the wire (the chunker fills the ceiling every time) — a fixed body size
// is the same class of statistical fingerprint as a fixed pacing interval
// (pacing got the {20,60}ms band; bodies get this). Draw is right-skewed:
// most POSTs stay at/near the ceiling with an occasional deeper shrink,
// mirroring downsegSizeJitterFn on the download side. Shrink-only by
// contract: the operator ceiling (scMaxEachPostBytes) is never exceeded.
// Replaceable for A/B benchmarks and tests; nil disables jitter.
var PacketUploadChunkJitterFn = func(ceil int32) int32 {
	return ceil - biasedRangeRand(0, ceil/10)
}

// chunkAvgPacketSize returns the average per-buffer payload size of a
// MultiBuffer (0 when empty), used by the L3a small-packet flow detector.
func chunkAvgPacketSize(mb buf.MultiBuffer) int32 {
	if len(mb) == 0 {
		return 0
	}
	var total int32
	for _, b := range mb {
		total += b.Len()
	}
	return total / int32(len(mb))
}

// packetUploadChunkSize chooses an effective max POST body size.
// configuredMax is the operator/config ceiling (must never be exceeded).
// rtt==0 keeps configuredMax so cold starts stay compatible.
func packetUploadChunkSize(configuredMax int32, rtt time.Duration) int32 {
	if configuredMax <= 0 {
		return configuredMax
	}
	target := configuredMax
	switch {
	case rtt <= 0:
		// Unknown RTT: honor config ceiling (stable default).
		return configuredMax
	case rtt >= 200*time.Millisecond:
		// ToT L2: cap chunk size on high-RTT paths — a single lost POST
		// at 1MB amplifies badly through the retry + reorder window.
		if configuredMax > packetUploadChunkMid {
			target = packetUploadChunkMid
		}
	case rtt >= 80*time.Millisecond:
		if configuredMax > packetUploadChunkMid {
			target = packetUploadChunkMid
		}
	case rtt >= 20*time.Millisecond:
		if configuredMax > packetUploadChunkLow {
			target = packetUploadChunkLow
		}
	default:
		// Very low RTT: keep medium posts for fewer round-trips than tiny chunks.
		if configuredMax > packetUploadChunkLow {
			target = packetUploadChunkLow
		}
	}
	if target < packetUploadChunkMin && configuredMax >= packetUploadChunkMin {
		target = packetUploadChunkMin
	}
	if target > configuredMax {
		target = configuredMax
	}
	if target < 1 {
		target = 1
	}
	return target
}

// packetUploadLaunchIntervalMs returns how long to wait before launching the
// next POST. Configured pacing is kept for small/idle writes (camouflage);
// when more data is already queued (backlog / full-size chunk), skip pacing
// so the in-flight window can fill and hide RTT.
// bulkChunk covers continuous bulk traffic that never reaches full scMaxEachPostBytes
// (e.g. 32KiB app writes against a 1MB post ceiling) so request/response benches
// are not hard-capped at scMinPostsIntervalMs.
// recentFlow covers back-to-back small posts while the app is still writing
// (time since previous launch within a short window); first idle tiny post still paces.
func packetUploadLaunchIntervalMs(configuredMs int32, hasBacklog bool, fullChunk bool, bulkChunk bool, recentFlow bool) int32 {
	if configuredMs <= 0 {
		return 0
	}
	if hasBacklog || fullChunk || bulkChunk || recentFlow {
		return 0
	}
	return configuredMs
}

// multiFromDurable wraps a durable byte snapshot as an unmanaged MultiBuffer.
// Fallback path for DialerClient implementations without PostPacketBytes.
// FromBytes uses a pooled Buffer shell so Release does not leak one alloc/post.
func multiFromDurable(durable []byte) buf.MultiBuffer {
	if len(durable) == 0 {
		return nil
	}
	return buf.MultiBuffer{buf.FromBytes(durable)}
}

// packetBytesPoster is optional: DefaultDialerClient implements PostPacketBytes
// to skip MultiBuffer shells on the packet-up retry hot path.
type packetBytesPoster interface {
	PostPacketBytes(ctx context.Context, url string, sessionId string, seqStr string, data []byte) error
}

// postPacketReliable sends one sequenced packet with bounded retries.
// Same seqStr is reused across attempts so the server never sees a hole
// from a failed mid-flight POST.
//
// L4 failure-granularity: when the retry budget on `client` is exhausted,
// `rescue` (if non-nil) is called ONCE to obtain a fresh outer connection
// (new TCP/TLS/H2 stream, same XMUX session id + seqStr — protocol-legal
// replay) and the full attempt budget runs again there. Only if the
// rescued attempt also fails is the error returned and the whole upload
// session torn down. This keeps a single bad outer connection from
// resetting every inner stream multiplexed on the session.
//
// Ownership: takes payload. On entry a single durable byte snapshot is made
// (retry source); payload is released immediately after the snapshot.
// Prefer PostPacketBytes when available (no MultiBuffer/FromBytes per attempt).
// MultiBuffer fallback still uses pooled shells and no second content copy.
func postPacketReliable(
	ctx context.Context,
	client DialerClient,
	url string,
	sessionId string,
	seqStr string,
	payload buf.MultiBuffer,
	rescue func(context.Context) (DialerClient, error),
) error {
	if client == nil {
		if !payload.IsEmpty() {
			buf.ReleaseMulti(payload)
		}
		return errors.New("XHTTP: nil dialer client for PostPacket")
	}

	var durable []byte
	var durableKind int
	var durableLocalRef *durableLocal
	if !payload.IsEmpty() {
		n := int(payload.Len())
		durable, durableKind, durableLocalRef = allocDurable(n)
		payload.Copy(durable)
		buf.ReleaseMulti(payload)
	}
	// free durable after all attempts (success or failure)
	defer freeDurable(durable, durableKind, durableLocalRef)

	lastErr := postPacketReliableOnce(ctx, client, url, sessionId, seqStr, durable)
	if lastErr == nil {
		return nil
	}
	if rescue != nil && ctx.Err() == nil {
		if newClient, rerr := rescue(ctx); rerr == nil && newClient != nil {
			// Debug: rescue legs fire per request under persistent conn
			// faults; Info here flooded error logs in info mode (~6 lines/
			// request during the 2026.08.31 x509 storm).
			errors.LogDebugInner(ctx, lastErr, "XHTTP packet-up POST exhausted on outer conn; rescuing seq=", seqStr)
			err := postPacketReliableOnce(ctx, newClient, url, sessionId, seqStr, durable)
			if err == nil {
				return nil
			}
			lastErr = err
		} else if rerr != nil {
			errors.LogDebugInner(ctx, rerr, "XHTTP packet-up rescue dial failed seq=", seqStr)
		}
	}
	return lastErr
}

// postPacketReliableOnce runs the bounded same-client retry budget
// (packetUploadMaxAttempts) against a single dialer client.
func postPacketReliableOnce(
	ctx context.Context,
	client DialerClient,
	url string,
	sessionId string,
	seqStr string,
	durable []byte,
) error {
	bytesPoster, hasBytes := client.(packetBytesPoster)

	var lastErr error
	for attempt := 1; attempt <= packetUploadMaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		if hasBytes {
			err = bytesPoster.PostPacketBytes(ctx, url, sessionId, seqStr, durable)
		} else {
			chunk := multiFromDurable(durable)
			err = client.PostPacket(ctx, url, sessionId, seqStr, chunk)
		}
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt == packetUploadMaxAttempts {
			break
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Backoff: 25ms, 50ms (cancel-aware).
		sleep := packetUploadRetryBase * time.Duration(attempt)
		t := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
		errors.LogDebugInner(ctx, err, "XHTTP packet-up POST retry ", attempt, "/", packetUploadMaxAttempts, " seq=", seqStr)
	}
	return lastErr
}
