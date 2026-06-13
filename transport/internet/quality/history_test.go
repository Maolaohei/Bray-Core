package quality

import (
	"math"
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

func TestEWMA(t *testing.T) {
	e := NewEWMA(0.0)
	// Start at 0
	if e.Value() != 0.0 {
		t.Fatalf("initial rate = %f, want 0", e.Value())
	}

	// First failure: 0 * 0.95 + 0.05 = 0.05
	e.OnFailure()
	if math.Abs(e.Value()-0.05) > 1e-10 {
		t.Fatalf("after 1 fail: %f, want 0.05", e.Value())
	}

	// Second failure: 0.05 * 0.95 + 0.05 = 0.0975
	e.OnFailure()
	if math.Abs(e.Value()-0.0975) > 1e-10 {
		t.Fatalf("after 2 fails: %f, want 0.0975", e.Value())
	}

	// Success: 0.0975 * 0.95 = 0.092625
	e.OnSuccess()
	if math.Abs(e.Value()-0.092625) > 1e-10 {
		t.Fatalf("after success: %f, want 0.092625", e.Value())
	}
}

func TestEWMADecay(t *testing.T) {
	e := NewEWMA(1.0) // start at 100% failure
	// 14 successes should halve the rate (0.95^14 ≈ 0.4876)
	for i := 0; i < 14; i++ {
		e.OnSuccess()
	}
	if e.Value() > 0.55 || e.Value() < 0.45 {
		t.Fatalf("after 14 successes from 1.0: %f, expected ~0.49", e.Value())
	}
}
