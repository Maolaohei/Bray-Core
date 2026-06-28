package quality

import "sync"

// History is a fixed-size ring buffer for recording recent metric samples.
// Used by Debug API to provide temporal context ("why was it slow just now?").
// Memory cost: ~4KB for 64 samples.
//
// Concurrent-safe: Push acquires a write lock, read methods acquire a read lock.
type History struct {
	mu      sync.RWMutex
	rtt     [64]int64
	loss    [64]float64
	quality [64]uint8
	conf    [64]uint8
	head    int
	count   int
}

// Push appends a new sample to the ring buffer.
func (h *History) Push(rtt int64, loss float64, q uint8, conf uint8) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.rtt[h.head] = rtt
	h.loss[h.head] = loss
	h.quality[h.head] = q
	h.conf[h.head] = conf
	h.head = (h.head + 1) % 64
	if h.count < 64 {
		h.count++
	}
}

// Len returns the number of samples in the buffer.
func (h *History) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.count
}

// RTT returns the RTT history in chronological order (oldest first).
func (h *History) RTT() []int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.extract64(h.rtt[:])
}

// Loss returns the loss history in chronological order (oldest first).
func (h *History) Loss() []float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.extractF64(h.loss[:])
}

// Quality returns the quality history in chronological order (oldest first).
func (h *History) Quality() []uint8 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.extractU8(h.quality[:])
}

// Confidence returns the confidence history in chronological order (oldest first).
func (h *History) Confidence() []uint8 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.extractU8(h.conf[:])
}

func (h *History) extract64(src []int64) []int64 {
	if h.count == 0 {
		return nil
	}
	out := make([]int64, h.count)
	start := (h.head - h.count + 64) % 64
	for i := 0; i < h.count; i++ {
		out[i] = src[(start+i)%64]
	}
	return out
}

func (h *History) extractF64(src []float64) []float64 {
	if h.count == 0 {
		return nil
	}
	out := make([]float64, h.count)
	start := (h.head - h.count + 64) % 64
	for i := 0; i < h.count; i++ {
		out[i] = src[(start+i)%64]
	}
	return out
}

func (h *History) extractU8(src []uint8) []uint8 {
	if h.count == 0 {
		return nil
	}
	out := make([]uint8, h.count)
	start := (h.head - h.count + 64) % 64
	for i := 0; i < h.count; i++ {
		out[i] = src[(start+i)%64]
	}
	return out
}
