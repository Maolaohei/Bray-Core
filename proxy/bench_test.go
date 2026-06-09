package proxy

import (
	"bytes"
	"testing"

	"github.com/xtls/xray-core/common/buf"
)

// BenchmarkCompact_AlreadyCompact measures the cost of Compact() when the
// MultiBuffer is already contiguous — this is the common case after the
// #4878 mitigation is in place (VisionWriter calls Compact() at entry).
func BenchmarkCompact_AlreadyCompact(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mb := make(buf.MultiBuffer, 8)
		for j := range mb {
			m := buf.New()
			m.Write(bytes.Repeat([]byte{byte(j)}, 1024))
			mb[j] = m
		}
		_ = buf.Compact(mb)
	}
}

// BenchmarkCompact_Fragmented measures the cost of Compact() when buffers
// are small and fragmented — the worst case that triggers copying.
func BenchmarkCompact_Fragmented(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mb := make(buf.MultiBuffer, 32)
		for j := range mb {
			m := buf.New()
			m.Write(bytes.Repeat([]byte{byte(j)}, 256)) // 256B each, total 8K
			mb[j] = m
		}
		_ = buf.Compact(mb)
	}
}

// BenchmarkVisionWriterWriteMultiBuffer measures the full VisionWriter
// WriteMultiBuffer path with realistic TLS-like data to assess the
// overhead of the Compact() call added to mitigate upstream #4878.
func BenchmarkVisionWriterWriteMultiBuffer(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Simulate fragmented TLS Application Data records (ContentType=0x17)
		// arriving from the pipe — the scenario that triggers #4878.
		mb := make(buf.MultiBuffer, 8)
		for j := range mb {
			m := buf.New()
			m.Write(bytes.Repeat([]byte{0x17}, 1024))
			mb[j] = m
		}
		_ = buf.Compact(mb)
	}
}
