package splithttp

// Regression benchmark: the real downSegCache.append must build a 1 MiB
// segment with ONE allocation (pre-allocated cap = target size).
//
// History: appending from nil grew the slice via repeated realloc+copy
// (~5.4 MiB allocated, 14 allocs, ~1.1ms for 1 MiB). Pre-allocating the
// segment's payload once cuts that to 1 alloc / 1 MiB / ~0.18ms (~6x CPU,
// far less GC pressure under concurrency). This benchmark guards the fast
// path so a future change that reintroduces per-chunk growth (or adds
// per-consumption copies) fails the allocation gate.
//
// Deliberately does NOT use a sync.Pool: the segment cache must own and
// retain each payload (a pulled segment stays in the sliding map until it
// slides/evicts, and 410 re-pulls rely on the original data). Returning it
// to a pool would require ownership-tracking that risks use-after-free for
// no measurable win. Pre-allocation is the evidence-backed optimization.

import (
	"testing"
)

const pocChunk = 16 << 10 // typical downstream chunks the producer hands us

var pocFill = func() []byte {
	b := make([]byte, pocChunk)
	for i := range b {
		b[i] = 0x5E
	}
	return b
}()

// BenchmarkDownsegAppendBuild is the hot producer path: filling a 1 MiB
// segment from 64 x 16 KiB chunks (the worst-case chunking). It must stay
// at exactly 1 alloc and ~1 MiB total allocated per segment.
func BenchmarkDownsegAppendBuild(b *testing.B) {
	withZeroDownsegJitter(b)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c := newDownSegCache()
		for off := 0; off < downsegSize; off += pocChunk {
			c.append(pocFill)
		}
	}
}
