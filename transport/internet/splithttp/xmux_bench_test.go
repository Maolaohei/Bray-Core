package splithttp_test

import (
	"context"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/xtls/xray-core/transport/internet/splithttp"
)

type benchFakeConn struct {
	closed bool
}

func (c *benchFakeConn) IsClosed() bool {
	return c.closed
}

func BenchmarkXMUXGetXmuxClient(b *testing.B) {
	// Microbench selection only: pin pool=1, unlimited reuse/concurrency so
	// default CMaxReuseTimes rotation does not dominate ns/op.
	m := NewXmuxManager(&XmuxConfig{
		MaxConnections: &RangeConfig{From: 1, To: 1},
		MaxConcurrency: &RangeConfig{From: 0, To: 0},
		CMaxReuseTimes: &RangeConfig{From: 0, To: 0},
	}, func() XmuxConn {
		return &benchFakeConn{}
	})
	defer m.Close()
	if _, err := m.GetXmuxClient(context.Background()); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := m.GetXmuxClient(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkXMUXGetXmuxClientParallel(b *testing.B) {
	m := NewXmuxManager(&XmuxConfig{
		MaxConnections: &RangeConfig{From: 4, To: 4},
	}, func() XmuxConn {
		return &benchFakeConn{}
	})
	defer m.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c, _ := m.GetXmuxClient(context.Background())
			c.Borrow()
			time.Sleep(time.Microsecond)
			c.Release()
		}
	})
}

func BenchmarkXMUXRTTEWMA(b *testing.B) {
	c := &XmuxClient{}
	c.UpdateRTT(100 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.UpdateRTT(time.Duration(50+i%50) * time.Millisecond)
	}
}

func BenchmarkXMUXPoolScheduling(b *testing.B) {
	for _, poolSize := range []int{1, 4, 8, 16, 32} {
		b.Run("pool_"+itoa(poolSize), func(b *testing.B) {
			// Selection microbench: unlimited reuse/concurrency, force-filled pool.
			// ForceAddClientsForTest bypasses burst soft-cap so pool_32 is real n=32.
			m := NewXmuxManager(&XmuxConfig{
				MaxConnections: &RangeConfig{From: int32(poolSize), To: int32(poolSize)},
				MaxConcurrency: &RangeConfig{From: 0, To: 0},
				CMaxReuseTimes: &RangeConfig{From: 0, To: 0},
			}, func() XmuxConn {
				return &benchFakeConn{}
			})
			defer m.Close()
			m.ForceAddClientsForTest(poolSize)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c, err := m.GetXmuxClient(context.Background())
				if err != nil || c == nil {
					b.Fatal(err)
				}
				if !c.Borrow() {
					b.Fatal("borrow failed")
				}
				c.Release()
			}
		})
	}
}

func BenchmarkXMUXMetrics(b *testing.B) {
	m := NewXmuxManager(&XmuxConfig{}, func() XmuxConn {
		return &benchFakeConn{}
	})
	defer m.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.RecordReuseHit()
		m.RecordTTFB(time.Duration(i%100) * time.Millisecond)
	}
}

// benchXMUXPoolWorkload drives GetXmuxClient + Borrow + simulated work +
// Release under a given parallelism and reports how many underlying
// connections the pool had to create (AIMD expansion pressure).
func benchXMUXPoolWorkload(b *testing.B, cfg *XmuxConfig) {
	var newConns atomic.Int64
	m := NewXmuxManager(cfg, func() XmuxConn {
		newConns.Add(1)
		return &benchFakeConn{}
	})
	defer m.Close()

	// Work per borrow: touch ~1KiB so the stream does non-trivial work
	// without going through real IO (pool behavior under load is what
	// differs between fixed and jittered pool sizes).
	work := make([]byte, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Realistic client behavior: if every connection in the pool
			// is saturated, re-select (the pool may create new
			// connections, counted in new-conns).
			var c *XmuxClient
			for {
				var err error
				c, err = m.GetXmuxClient(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				if c.Borrow() {
					break
				}
				runtime.Gosched()
			}
			work[0] = byte(c.LeftRequests.Load())
			work[512] ^= work[0]
			_ = work
			c.Release()
		}
	})
	b.ReportMetric(float64(newConns.Load()), "new-conns")
}

// BenchmarkXMUXPool compares fixed (6/6, upstream-style) vs jittered
// (nil fields -> process-stable 2-4 ±10%) pool sizes under increasing
// parallelism. Same benchmark name so benchstat can pair them:
//
//	XMUX_BENCH_MODE=fixed   go test -bench BenchmarkXMUXPool ...
//	XMUX_BENCH_MODE=jittered go test -bench BenchmarkXMUXPool ...
func BenchmarkXMUXPool(b *testing.B) {
	mode := os.Getenv("XMUX_BENCH_MODE")
	for _, workers := range []int{1, 8, 32, 64} {
		b.Run("workers_"+itoa(workers), func(b *testing.B) {
			b.SetParallelism(workers)
			cfg := &XmuxConfig{
				MaxConcurrency: &RangeConfig{From: 16, To: 16},
			}
			if mode != "jittered" {
				cfg.MaxConnections = &RangeConfig{From: 6, To: 6}
			}
			benchXMUXPoolWorkload(b, cfg)
		})
	}
}

func BenchmarkXMUXConcurrentReadWrite(b *testing.B) {
	for _, workers := range []int{1, 4, 8, 16} {
		b.Run("workers_"+itoa(workers), func(b *testing.B) {
			m := NewXmuxManager(&XmuxConfig{
				MaxConcurrency: &RangeConfig{From: 2, To: 2},
				MaxConnections: &RangeConfig{From: 4, To: 4},
			}, func() XmuxConn {
				return &benchFakeConn{}
			})
			defer m.Close()

			b.ResetTimer()
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < b.N/workers+1; i++ {
						c, _ := m.GetXmuxClient(context.Background())
						c.Borrow()
						c.UpdateRTT(time.Duration(i%100) * time.Millisecond)
						time.Sleep(time.Microsecond)
						c.Release()
					}
				}()
			}
			wg.Wait()
		})
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := [20]byte{}
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// TestCachedScoreStaleness verifies that cached scores properly rotate
// connections when inflight counts change under concurrent load.
func TestCachedScoreStaleness(t *testing.T) {
	m := NewXmuxManager(&XmuxConfig{
		MaxConnections: &RangeConfig{From: 3, To: 3},
		MaxConcurrency: &RangeConfig{From: 2, To: 2},
	}, func() XmuxConn {
		return &benchFakeConn{}
	})
	defer m.Close()

	time.Sleep(50 * time.Millisecond)

	// Get 3 clients and set their RTT
	clients := make([]*XmuxClient, 3)
	for i := 0; i < 3; i++ {
		c, _ := m.GetXmuxClient(context.Background())
		clients[i] = c
	}

	// Set very different RTT: 10ms vs 200ms
	clients[0].UpdateRTT(10 * time.Millisecond)
	clients[1].UpdateRTT(200 * time.Millisecond)
	clients[2].UpdateRTT(200 * time.Millisecond)

	time.Sleep(10 * time.Millisecond)

	// Track ALL clients returned by GetXmuxClient (including new ones)
	allSelectionCount := make(map[*XmuxClient]int)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				c, _ := m.GetXmuxClient(context.Background())
				c.Borrow()
				mu.Lock()
				allSelectionCount[c]++
				mu.Unlock()
				time.Sleep(500 * time.Microsecond)
				c.Release()
			}
		}()
	}
	wg.Wait()

	// Check that the client with lowest RTT (clients[0]) was preferred
	// when it was available (had capacity)
	lowRTTCount := allSelectionCount[clients[0]]
	highRTTCount := allSelectionCount[clients[1]] + allSelectionCount[clients[2]]

	t.Logf("LowRTT (10ms): %d selections, HighRTT (200ms): %d selections",
		lowRTTCount, highRTTCount)

	total := 0
	for _, count := range allSelectionCount {
		total += count
	}
	if total != 1200 {
		t.Errorf("total selections = %d, want 1200", total)
	}

	// With MaxConcurrency=2, the low-RTT client should get more selections
	// than any single high-RTT client, but not 100% due to concurrency limits
	if lowRTTCount == 1200 {
		t.Error("client[0] got 100% - scheduling not rotating under load")
	}
	if lowRTTCount == 0 {
		t.Error("client[0] got 0% - lowest RTT not preferred")
	}
}
