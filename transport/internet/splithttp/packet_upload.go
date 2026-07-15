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
		w = packetUploadMaxWindow
	case rtt >= 80*time.Millisecond:
		w = 12
	case rtt >= 20*time.Millisecond:
		w = 8
	case rtt > 0:
		// Very low RTT: less concurrency needed; still pipeline a little.
		w = 6
	}
	if w > maxW {
		return maxW
	}
	if w < 1 {
		return 1
	}
	return w
}

// packetUploadLaunchIntervalMs returns how long to wait before launching the
// next POST. Configured pacing is kept for small/idle writes (camouflage);
// when more data is already queued (backlog / full-size chunk), skip pacing
// so the in-flight window can fill and hide RTT.
func packetUploadLaunchIntervalMs(configuredMs int32, hasBacklog bool, fullChunk bool) int32 {
	if configuredMs <= 0 {
		return 0
	}
	if hasBacklog || fullChunk {
		return 0
	}
	return configuredMs
}

// cloneMultiBuffer deep-copies payload so PostPacket consumers that take
// ownership never free a buffer the caller still needs.
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
// from a failed mid-flight POST.
//
// Ownership: takes payload. On entry a single durable byte snapshot is made
// (lazy retry source); payload is released immediately after the snapshot so
// the success path pays one copy, not one copy per attempt, and MultiBuffer
// pages return to the pool before the HTTP RTT completes.
func postPacketReliable(
	ctx context.Context,
	client DialerClient,
	url string,
	sessionId string,
	seqStr string,
	payload buf.MultiBuffer,
) error {
	if client == nil {
		if !payload.IsEmpty() {
			buf.ReleaseMulti(payload)
		}
		return errors.New("XHTTP: nil dialer client for PostPacket")
	}

	var durable []byte
	if !payload.IsEmpty() {
		durable = make([]byte, int(payload.Len()))
		payload.Copy(durable)
		buf.ReleaseMulti(payload)
	}

	var lastErr error
	for attempt := 1; attempt <= packetUploadMaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		var chunk buf.MultiBuffer
		if durable != nil {
			chunk = buf.MergeBytes(nil, durable)
		}
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
