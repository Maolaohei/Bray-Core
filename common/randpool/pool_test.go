package randpool

import (
	"sync"
	"testing"
)

func TestIntN(t *testing.T) {
	p := &Pool{}
	p.buf = make([]byte, bufferSize)
	p.refill()

	for n := 1; n <= 100; n++ {
		for i := 0; i < 1000; i++ {
			v := p.IntN(n)
			if v < 0 || v >= n {
				t.Fatalf("IntN(%d) = %d, want [0, %d)", n, v, n)
			}
		}
	}
}

func TestIntN_Zero(t *testing.T) {
	p := &Pool{}
	p.buf = make([]byte, bufferSize)
	p.refill()

	if v := p.IntN(0); v != 0 {
		t.Fatalf("IntN(0) = %d, want 0", v)
	}
	if v := p.IntN(-1); v != 0 {
		t.Fatalf("IntN(-1) = %d, want 0", v)
	}
}

func TestIntN_Distribution(t *testing.T) {
	p := &Pool{}
	p.buf = make([]byte, bufferSize)
	p.refill()

	n := 10
	counts := make([]int, n)
	total := 100000
	for i := 0; i < total; i++ {
		counts[p.IntN(n)]++
	}

	expected := total / n
	for i, c := range counts {
		ratio := float64(c) / float64(expected)
		if ratio < 0.85 || ratio > 1.15 {
			t.Errorf("IntN(%d) distribution: counts[%d] = %d, expected ~%d (ratio=%.2f)", n, i, c, expected, ratio)
		}
	}
}

func TestIntN_Refill(t *testing.T) {
	p := &Pool{}
	p.buf = make([]byte, bufferSize)
	p.refill()

	// Exhaust the buffer to trigger refill
	for i := 0; i < bufferSize/4+100; i++ {
		p.IntN(100)
	}

	// Should still work after refill
	for i := 0; i < 1000; i++ {
		v := p.IntN(10)
		if v < 0 || v >= 10 {
			t.Fatalf("IntN(10) = %d after refill, want [0, 10)", v)
		}
	}
}

func TestIntN_Concurrent(t *testing.T) {
	p := &Pool{}
	p.buf = make([]byte, bufferSize)
	p.refill()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				v := p.IntN(100)
				if v < 0 || v >= 100 {
					t.Errorf("IntN(100) = %d, want [0, 100)", v)
				}
			}
		}()
	}
	wg.Wait()
}

func TestGlobal(t *testing.T) {
	// Just verify Global is initialized and functional
	for i := 0; i < 100; i++ {
		v := Global.IntN(10)
		if v < 0 || v >= 10 {
			t.Fatalf("Global.IntN(10) = %d, want [0, 10)", v)
		}
	}
}
