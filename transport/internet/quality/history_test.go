package quality

import (
	"testing"
)

func TestHistoryPushAndGet(t *testing.T) {
	var h History
	for i := 0; i < 10; i++ {
		h.Push(int64(i*10), float64(i)*0.1, uint8(i*10), uint8(100-i))
	}
	if h.Len() != 10 {
		t.Fatalf("Len() = %d, want 10", h.Len())
	}
	rtts := h.RTT()
	for i, v := range rtts {
		if v != int64(i*10) {
			t.Fatalf("RTT[%d] = %d, want %d", i, v, i*10)
		}
	}
}

func TestHistoryRingOverflow(t *testing.T) {
	var h History
	for i := 0; i < 100; i++ {
		h.Push(int64(i), 0, 0, 0)
	}
	if h.Len() != 64 {
		t.Fatalf("Len() = %d, want 64", h.Len())
	}
	// Oldest should be 36 (100-64)
	rtts := h.RTT()
	if rtts[0] != 36 {
		t.Fatalf("oldest RTT = %d, want 36", rtts[0])
	}
	if rtts[63] != 99 {
		t.Fatalf("newest RTT = %d, want 99", rtts[63])
	}
}

func TestHistoryEmpty(t *testing.T) {
	var h History
	if h.Len() != 0 {
		t.Fatal("empty history should have len 0")
	}
	if h.RTT() != nil {
		t.Fatal("empty history should return nil RTT")
	}
	if h.Loss() != nil {
		t.Fatal("empty history should return nil Loss")
	}
}

