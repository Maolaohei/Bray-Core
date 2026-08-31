package proxy

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/common/buf"
)

// TestPOC_XtlsPadding_NoNegativePaddingLen is a POC for B1: when the content
// buffer handed to XtlsPadding is large enough that no padding can fit within
// buf.Size, the clamp
//
//	paddingLen = buf.Size - 21 - contentLen
//
// becomes negative, and buf.Buffer.Extend(negative) panics with
// "slice bounds out of range [::...]".
//
// A full 8192-byte TLS record during the XRV/vision padding phase
// (contentLen == buf.Size > buf.Size-21 == 8171) triggers this. The fix clamps
// paddingLen to the range [0, buf.Size-21-contentLen]; when there is no room it
// must be 0 instead of negative.
func TestPOC_XtlsPadding_NoNegativePaddingLen(t *testing.T) {
	seed := []uint32{900, 500, 900, 256}

	// Negative control: a small buffer must pad without panic (normal path).
	small := buf.New()
	small.Extend(100)
	if p := XtlsPadding(small, CommandPaddingContinue, nil, true, context.Background(), seed); p == nil {
		t.Fatal("expected padded buffer for small content")
	} else {
		p.Release()
	}

	// The bug: a content buffer at buf.Size must not make Extend panic.
	big := buf.New()
	big.Extend(buf.Size) // contentLen == 8192 > 8171
	defer big.Release()

	p := XtlsPadding(big, CommandPaddingContinue, nil, true, context.Background(), seed)
	if p == nil {
		t.Fatal("expected padded buffer for large content")
	}
	defer p.Release()

	// If we reach here, paddingLen was non-negative (no panic). Sanity: the
	// padded buffer holds at least the 5-byte vision header.
	if p.Len() < 5 {
		t.Fatalf("padded buffer too small: %d", p.Len())
	}
}
