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
func MaybeEvictXmuxAfterOpenFailure(c *XmuxClient, err error) {
	if c == nil || err == nil {
		return
	}
	if err == context.Canceled || err == context.DeadlineExceeded {
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	if !isFatalOpenTransportError(err) {
		return
	}
	c.MarkDead()
	recordXmuxOpenEvict()
}

func isFatalOpenTransportError(err error) bool {
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
