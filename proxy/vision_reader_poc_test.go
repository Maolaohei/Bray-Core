package proxy

import (
	"context"
	"io"
	"testing"

	"github.com/xtls/xray-core/common/buf"
)

// staticReader returns a fixed MultiBuffer once, then io.EOF. It implements
// buf.Reader so we can drive VisionReader.ReadMultiBuffer without a real conn.
type staticReader struct {
	mb   buf.MultiBuffer
	used bool
}

func (r *staticReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	if r.used {
		return buf.MultiBuffer{}, io.EOF
	}
	r.used = true
	return r.mb, nil
}

// TestPOC_VisionReader_NilInputNoPanic is a POC for B2: a VisionReader built with
// nil input/rawInput (e.g. xtls-rprx-vision over a plain TLS conn whose underlying
// conn exposes no XTLS buffers) must not dereference the nil input when the
// traffic state switches to direct-copy (CommandPaddingDirect / 0x02 parsed).
//
// Before the fix, ReadMultiBuffer reaches `buf.ReadFrom(w.input)` with a nil
// *bytes.Reader and panics ("invalid memory address or nil pointer dereference").
func TestPOC_VisionReader_NilInputNoPanic(t *testing.T) {
	userUUID := make([]byte, 16) // all zeros; also the leading bytes of the crafted block

	ts := NewTrafficState(userUUID)
	ts.NumberOfPacketToFilter = 0
	ts.Inbound.WithinPaddingBuffers = true
	ts.Inbound.RemainingCommand = -1
	ts.Inbound.RemainingContent = -1
	ts.Inbound.RemainingPadding = -1
	ts.Inbound.CurrentCommand = 0

	// Craft a vision block whose command byte is CommandPaddingDirect (0x02):
	//   [16B UserUUID][0x02][remainingContent 2B=0][remainingPadding 2B=0]
	// XtlsUnpadding parses this as currentCommand == 2 -> switchToDirectCopy.
	header := buf.New()
	header.Write(userUUID)
	header.Write([]byte{CommandPaddingDirect})
	header.Write([]byte{0x00, 0x00})
	header.Write([]byte{0x00, 0x00})
	mb := buf.MultiBuffer{header}

	vr := NewVisionReader(&staticReader{mb: mb}, ts, true /*isUplink*/, context.Background(), nil, nil /*input*/, nil /*rawInput*/, nil)
	if _, err := vr.ReadMultiBuffer(); err != nil && err != io.EOF {
		t.Fatalf("unexpected error: %v", err)
	}
	// Reaching here without panic means the nil-input path is safe.
}
