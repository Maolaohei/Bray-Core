package splithttp

import (
	"context"
	"errors"
	"strings"
)

// MaybeEvictXmuxAfterOpenFailure is Wave-4 adaptive XMUX lite:
// on fatal-ish open errors, mark the XMUX client dead so the next Dial
// rotates instead of reusing a broken H2/H3 session. Non-fatal CDN mode
// mismatches (often HTTP status from a live conn) do not evict.
//
// When more mode-cascade steps remain, callers should prefer
// ShouldEvictXmuxOnOpenFailure + refresh client rather than killing the only
// borrowed slot without re-obtaining (Wave-7 review).
func MaybeEvictXmuxAfterOpenFailure(c *XmuxClient, err error) {
	if c == nil || err == nil {
		return
	}
	if !ShouldEvictXmuxOnOpenFailure(err) {
		return
	}
	c.MarkDead()
	recordXmuxOpenEvict()
}

// ShouldEvictXmuxOnOpenFailure reports whether err is a fatal transport fault
// that warrants dropping the XMUX session (not CDN mode/status rejects).
func ShouldEvictXmuxOnOpenFailure(err error) bool {
	if err == nil {
		return false
	}
	if err == context.Canceled || err == context.DeadlineExceeded {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return isFatalOpenTransportError(err)
}

// ShouldRefreshXmuxBeforeCascade is true when open failed with a fatal transport
// error and further mode cascade steps remain. Callers should MarkDead the old
// client and re-obtain getHTTPClient before the next mode attempt.
func ShouldRefreshXmuxBeforeCascade(err error, hasMoreModes bool) bool {
	return hasMoreModes && ShouldEvictXmuxOnOpenFailure(err)
}

func isFatalOpenTransportError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	// Keep this list tight to avoid thrashing the pool on CDN 403/404 stream rejects.
	needles := []string{
		"eof",
		"broken pipe",
		"connection reset",
		"reset by peer",
		"goaway",
		"connection refused",
		"use of closed network connection",
		"http2: client conn not usable",
		"http2: client connection force closed",
		"stream closed",
		"no recent network activity",
		"i/o timeout",
	}
	for _, n := range needles {
		if strings.Contains(msg, n) {
			return true
		}
	}
	return false
}
