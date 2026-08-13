package xudp_test

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	. "github.com/xtls/xray-core/common/xudp"
)

// batchCollector captures frames written by the PacketWriter.
type batchCollector struct {
	frames [][]byte
}

func (c *batchCollector) WriteMultiBuffer(mb buf.MultiBuffer) error {
	for _, b := range mb {
		cp := make([]byte, b.Len())
		copy(cp, b.Bytes())
		c.frames = append(c.frames, cp)
	}
	buf.ReleaseMulti(mb)
	return nil
}

func readFramePayload(t *testing.T, frame []byte, frameStart int) []byte {
	t.Helper()
	// frame layout: [2B metaLen][meta...][payload...]
	metaLen := int(frame[0])<<8 | int(frame[1])
	payload := frame[2+metaLen:]
	_ = frameStart
	return payload
}

// TestXUDPBatchFrameShape verifies the batch frame wire layout:
// count byte + N×[2B len + payload], with the Batch option bit set.
func TestXUDPBatchFrameShape(t *testing.T) {
	if !strings.EqualFold(os.Getenv("XUDPBatch"), "true") {
		os.Setenv("XUDPBatch", "true")
	}
	defer os.Unsetenv("XUDPBatch")

	c := &batchCollector{}
	w := NewPacketWriter(c, net.UDPDestination(net.LocalHostIP, 53), [8]byte{1, 2, 3, 4, 5, 6, 7, 8})

	mb := make(buf.MultiBuffer, 0, 3)
	for i := 0; i < 3; i++ {
		eb := buf.New()
		eb.Write([]byte{byte(i + 1), byte(i + 2), byte(i + 3), byte(i + 4)})
		eb.UDP = &net.Destination{Network: net.Network_UDP, Address: net.LocalHostIP, Port: 53}
		mb = append(mb, eb)
	}
	if err := w.WriteMultiBuffer(mb); err != nil {
		t.Fatal(err)
	}
	if len(c.frames) != 1 {
		t.Fatalf("expected 1 batch frame, got %d", len(c.frames))
	}
	f := c.frames[0]
	// Batch option bit: opt byte at offset 5 must have 0x04 set.
	if f[5]&0x04 == 0 {
		t.Fatalf("batch option bit not set: frame=%x", f)
	}
	// New-frame IPv4 layout: [2B metaLen][2B sessionID][1B status][1B opt]
	// [1B UDP flag][5B addr][8B GlobalID][1B count][2B len1][p1]...
	// count sits at offset 20.
	const countOffset = 7 + 7 + 8 // prefix + port2+type1+ip4 + GlobalID
	if len(f) <= countOffset {
		t.Fatalf("frame too short for count: %d bytes", len(f))
	}
	count := int(f[countOffset])
	if count != 3 {
		t.Fatalf("expected count=3, got %d", count)
	}
	rest := f[countOffset+1:]
	off := 0
	for i := 0; i < 3; i++ {
		if off+2 > len(rest) {
			t.Fatalf("sub-frame %d truncated", i)
		}
		l := int(rest[off])<<8 | int(rest[off+1])
		off += 2
		if off+l > len(rest) {
			t.Fatalf("sub-frame %d payload truncated", i)
		}
		if l != 4 {
			t.Fatalf("sub-frame %d len=%d want 4", i, l)
		}
		off += l
	}
	if off != len(rest) {
		t.Fatalf("trailing bytes: %d", len(rest)-off)
	}
}

// TestXUDPBatchSinglePacketFallsBack verifies N=1 never produces a batch
// frame (zero regression on the single-packet path).
func TestXUDPBatchSinglePacketFallsBack(t *testing.T) {
	if !strings.EqualFold(os.Getenv("XUDPBatch"), "true") {
		os.Setenv("XUDPBatch", "true")
	}
	defer os.Unsetenv("XUDPBatch")

	c := &batchCollector{}
	w := NewPacketWriter(c, net.UDPDestination(net.LocalHostIP, 53), [8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	eb := buf.New()
	eb.Write([]byte{1, 2, 3, 4})
	eb.UDP = &net.Destination{Network: net.Network_UDP, Address: net.LocalHostIP, Port: 53}
	if err := w.WriteMultiBuffer(buf.MultiBuffer{eb}); err != nil {
		t.Fatal(err)
	}
	if len(c.frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(c.frames))
	}
	if c.frames[0][5]&0x04 != 0 {
		t.Fatal("single packet must not set the batch bit")
	}
}

// TestXUDPBatchMixedTargetsFallsBack verifies mixed destinations fall
// back to per-packet frames.
func TestXUDPBatchMixedTargetsFallsBack(t *testing.T) {
	if !strings.EqualFold(os.Getenv("XUDPBatch"), "true") {
		os.Setenv("XUDPBatch", "true")
	}
	defer os.Unsetenv("XUDPBatch")

	c := &batchCollector{}
	w := NewPacketWriter(c, net.UDPDestination(net.LocalHostIP, 53), [8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	// First write establishes the session (New frame); second write is a
	// keep batch with mixed targets → must fall back.
	eb := buf.New()
	eb.Write([]byte{1, 2, 3, 4})
	eb.UDP = &net.Destination{Network: net.Network_UDP, Address: net.LocalHostIP, Port: 53}
	if err := w.WriteMultiBuffer(buf.MultiBuffer{eb}); err != nil {
		t.Fatal(err)
	}
	mb := make(buf.MultiBuffer, 0, 2)
	for i, addr := range []net.Address{net.LocalHostIP, net.ParseAddress("192.168.9.9")} {
		eb := buf.New()
		eb.Write([]byte{byte(i + 1), 2, 3, 4})
		eb.UDP = &net.Destination{Network: net.Network_UDP, Address: addr, Port: 53}
		mb = append(mb, eb)
	}
	if err := w.WriteMultiBuffer(mb); err != nil {
		t.Fatal(err)
	}
	if len(c.frames) != 3 {
		t.Fatalf("expected 3 single frames (1 new + 2 keep), got %d", len(c.frames))
	}
	for _, f := range c.frames {
		if f[5]&0x04 != 0 {
			t.Fatal("mixed targets must not batch")
		}
	}
}

var _ io.Reader // keep io import if unused later
