package xudp_test

import (
	"testing"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	. "github.com/xtls/xray-core/common/xudp"
)

// discardWriter consumes and releases everything handed to it.
type discardWriter struct{}

func (discardWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	buf.ReleaseMulti(mb)
	return nil
}

// BenchmarkXUDPPacketWriterSmall measures the small-datagram hot path
// (game heartbeats / DNS-style 20-64B payloads). The writer must now
// coalesce several frames into one pooled buffer: expect ~1-2 allocs per
// burst instead of one 8KB buffer per packet.
func BenchmarkXUDPPacketWriterSmall(b *testing.B) {
	w := NewPacketWriter(discardWriter{}, net.UDPDestination(net.LocalHostIP, 53), [8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	payload := make([]byte, 40)
	mb := make(buf.MultiBuffer, 0, 8)
	for i := 0; i < 8; i++ {
		eb := buf.New()
		eb.Write(payload)
		mb = append(mb, eb)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.WriteMultiBuffer(mb); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkXUDPPacketWriterLarge measures one large datagram (QUIC-style
// 1200B) to confirm the coalescing path degrades gracefully.
func BenchmarkXUDPPacketWriterLarge(b *testing.B) {
	w := NewPacketWriter(discardWriter{}, net.UDPDestination(net.LocalHostIP, 443), [8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	payload := make([]byte, 1200)
	eb := buf.New()
	eb.Write(payload)
	mb := buf.MultiBuffer{eb}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.WriteMultiBuffer(mb); err != nil {
			b.Fatal(err)
		}
	}
}
