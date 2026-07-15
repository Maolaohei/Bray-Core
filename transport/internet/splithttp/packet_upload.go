package splithttp

// packet_upload helpers: reliable PostPacket with bounded retry and a limited
// client-side in-flight window. Owned by dialer packet-up path; kept separate
// for unit testing without Dial.

import (
	"context"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
)

// packetUploadMaxAttempts is the max tries for a single packet-up POST
// (1 initial + retries). Keep small to bound head-of-line delay.
const packetUploadMaxAttempts = 3

// packetUploadRetryBase is the first backoff step after a failed POST.
const packetUploadRetryBase = 25 * time.Millisecond

// packetUploadDefaultWindow is the default number of concurrent POSTs per
// logical connection. Server reorder buffer (scMaxBufferedPosts, default 64)
// absorbs reordering; 8 hides typical RTT without flooding the edge.
const packetUploadDefaultWindow = 8

// packetUploadMaxWindow hard-caps client in-flight posts regardless of server
// buffer size (H1 pool, memory, and CDN concurrency friendliness).
const packetUploadMaxWindow = 16

// packetUploadWindow returns the client POST in-flight window.
// It never exceeds half of the server's reorder capacity so misordered
// bursts still fit in upload_queue without "packet queue is too large".
func packetUploadWindow(scMaxBufferedPosts int) int {
	w := packetUploadDefaultWindow
	if scMaxBufferedPosts <= 0 {
		return w
	}
	maxW := scMaxBufferedPosts / 2
	if maxW < 1 {
		maxW = 1
	}
	if maxW > packetUploadMaxWindow {
		maxW = packetUploadMaxWindow
	}
	if w > maxW {
		return maxW
	}
	return w
}

// cloneMultiBuffer deep-copies payload so PostPacket consumers that take
// ownership (body container / ReleaseMulti on header placement) never free
// the caller's buffer, and retries can resend the same bytes.
func cloneMultiBuffer(src buf.MultiBuffer) buf.MultiBuffer {
	if src.IsEmpty() {
		return nil
	}
	n := int(src.Len())
	raw := make([]byte, n)
	src.Copy(raw)
	return buf.MergeBytes(nil, raw)
}

// postPacketReliable sends one sequenced packet with bounded retries.
// Same seqStr is reused across attempts so the server never sees a hole
// from a failed mid-flight POST. Caller must advance seq only after nil.
func postPacketReliable(
	ctx context.Context,
	client DialerClient,
	url string,
	sessionId string,
	seqStr string,
	payload buf.MultiBuffer,
) error {
	if client == nil {
		return errors.New("XHTTP: nil dialer client for PostPacket")
	}
	var lastErr error
	for attempt := 1; attempt <= packetUploadMaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Fresh clone each attempt: FillPacketRequest may consume/release payload.
		chunk := cloneMultiBuffer(payload)
		err := client.PostPacket(ctx, url, sessionId, seqStr, chunk)
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
		errors.LogInfoInner(ctx, err, "XHTTP packet-up POST retry ", attempt, "/", packetUploadMaxAttempts, " seq=", seqStr)
	}
	return lastErr
}
