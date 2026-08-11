package splithttp

import "sync"

// post_body_pool recycles server-side POST body payloads on the packet-up
// hot path. Without it every upload POST allocates a fresh make([]byte) of
// chunk size (32K-1M) and the GC churns linearly with throughput.
//
// Size classes are power-of-two from 2K to 4M so common chunk sizes
// (32K/256K/512K) hit a class within 2x of the request. Buffers are returned
// to the class matching their capacity, never a smaller one, so an oversized
// buffer cannot be handed to a small request.
//
// Ownership contract: allocPostBody hands the caller an exclusively-owned
// slice. The caller (or uploadQueue on the consumer side) must call
// freePostBody exactly once when done. Buffers that are never freed are
// reclaimed by GC like any other allocation — pooling only loses its benefit,
// never leaks.

var postBodyPoolSizes = [...]int{
	1 << 11, // 2K
	1 << 12, // 4K
	1 << 13, // 8K
	1 << 14, // 16K
	1 << 15, // 32K
	1 << 16, // 64K
	1 << 17, // 128K
	1 << 18, // 256K
	1 << 19, // 512K
	1 << 20, // 1M
	1 << 21, // 2M
	1 << 22, // 4M
}

var postBodyPools [len(postBodyPoolSizes)]sync.Pool

func init() {
	for i := range postBodyPools {
		postBodyPools[i].New = newPostBodyAllocFunc(postBodyPoolSizes[i])
	}
}

// postBodyAllocFunc returns a fresh zero-length buffer with the class
// capacity, so a Get + reslice is enough for callers.
func newPostBodyAllocFunc(size int) func() any {
	return func() any {
		b := make([]byte, size)
		return &b
	}
}

// allocPostBody returns a len=n, cap=class byte slice from the smallest
// pool class >= n. Small bodies (< 2K) are plain make: pooling tiny
// allocations adds pool traffic without meaningful GC relief.
func allocPostBody(n int) []byte {
	if n <= 0 {
		return nil
	}
	if n < postBodyPoolSizes[0] {
		return make([]byte, n)
	}
	for i, size := range postBodyPoolSizes {
		if n <= size {
			b := *postBodyPools[i].Get().(*[]byte)
			return b[:n]
		}
	}
	// Beyond the largest class: plain allocation (rare: scMaxEachPostBytes
	// overrides above 4M).
	return make([]byte, n)
}

// freePostBody returns b to the pool class matching its capacity. Slice
// content is not cleared; callers must not retain references after free.
func freePostBody(b []byte) {
	if b == nil {
		return
	}
	capb := cap(b)
	// Find the largest class <= cap: that is the class this buffer may have
	// come from (alloc picks the smallest class >= n, so cap is always an
	// exact class size unless the caller resliced a larger borrow).
	for i := len(postBodyPoolSizes) - 1; i >= 0; i-- {
		if capb >= postBodyPoolSizes[i] {
			b = b[:postBodyPoolSizes[i]]
			postBodyPools[i].Put(&b)
			return
		}
	}
}
