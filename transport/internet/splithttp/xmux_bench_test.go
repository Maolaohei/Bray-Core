package splithttp_test

import (
	"context"
	"sync"
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
	m := NewXmuxManager(XmuxConfig{}, func() XmuxConn {
		return &benchFakeConn{}
	})
	defer m.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.GetXmuxClient(context.Background())
	}
}

func BenchmarkXMUXGetXmuxClientParallel(b *testing.B) {
	m := NewXmuxManager(XmuxConfig{
		MaxConnections: &RangeConfig{From: 4, To: 4},
	}, func() XmuxConn {
		return &benchFakeConn{}
	})
	defer m.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c := m.GetXmuxClient(context.Background())
			c.Running.Add(1)
			time.Sleep(time.Microsecond)
			c.Running.Add(-1)
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
			m := NewXmuxManager(XmuxConfig{
				MaxConnections: &RangeConfig{From: int32(poolSize), To: int32(poolSize)},
			}, func() XmuxConn {
				return &benchFakeConn{}
			})
			defer m.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c := m.GetXmuxClient(context.Background())
				c.Running.Add(1)
				c.Running.Add(-1)
			}
		})
	}
}

func BenchmarkXMUXWarmupEnqueue(b *testing.B) {
	m := NewXmuxManager(XmuxConfig{}, func() XmuxConn {
		return &benchFakeConn{}
	})
	defer m.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.EnqueueWarmup("example.com", 1)
	}
}

func BenchmarkXMUXMetrics(b *testing.B) {
	m := NewXmuxManager(XmuxConfig{}, func() XmuxConn {
		return &benchFakeConn{}
	})
	defer m.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.RecordReuseHit()
		m.RecordTTFB(time.Duration(i%100) * time.Millisecond)
	}
}

func BenchmarkXMUXConcurrentReadWrite(b *testing.B) {
	for _, workers := range []int{1, 4, 8, 16} {
		b.Run("workers_"+itoa(workers), func(b *testing.B) {
			m := NewXmuxManager(XmuxConfig{
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
						c := m.GetXmuxClient(context.Background())
						c.Running.Add(1)
						c.UpdateRTT(time.Duration(i%100) * time.Millisecond)
						time.Sleep(time.Microsecond)
						c.Running.Add(-1)
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
	m := NewXmuxManager(XmuxConfig{
		MaxConnections: &RangeConfig{From: 3, To: 3},
		MaxConcurrency: &RangeConfig{From: 2, To: 2},
	}, func() XmuxConn {
		return &benchFakeConn{}
	})
	defer m.Close()

	time.Sleep(50 * time.Millisecond)

	clients := make([]*XmuxClient, 3)
	for i := 0; i < 3; i++ {
		c := m.GetXmuxClient(context.Background())
		c.Running.Add(-1)
		clients[i] = c
	}

	// Set very different RTT: 10ms vs 200ms
	clients[0].UpdateRTT(10 * time.Millisecond)
	clients[1].UpdateRTT(200 * time.Millisecond)
	clients[2].UpdateRTT(200 * time.Millisecond)

	time.Sleep(10 * time.Millisecond)

	// Concurrent workers simulate real load: each holds Running for a while
	selectionCount := make(map[*XmuxClient]int)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				c := m.GetXmuxClient(context.Background())
				c.AddRunning() // simulate real usage
				mu.Lock()
				selectionCount[c]++
				mu.Unlock()
				// Hold Running for longer to force rotation
				time.Sleep(500 * time.Microsecond)
				c.DoneRunning()
			}
		}()
	}
	wg.Wait()

	t.Logf("Selection distribution (concurrent, 200 iterations x 6 workers):")
	for i, c := range clients {
		rtt := []int{10, 200, 200}[i]
		t.Logf("  client[%d] RTT=%dms: selected %d times (%.1f%%)",
			i, rtt, selectionCount[c], float64(selectionCount[c])/12.0)
	}

	total := 0
	for _, count := range selectionCount {
		total += count
	}
	if total != 1200 {
		t.Errorf("total selections = %d, want 1200", total)
	}

	// With concurrent load and MaxConcurrency=2, client[0] should get
	// more selections (lower RTT) but not 100% because it can only
	// handle 2 concurrent requests at a time
	if selectionCount[clients[0]] == 1200 {
		t.Error("client[0] got 100% - scheduling not rotating under load")
	}
	if selectionCount[clients[0]] == 0 {
		t.Error("client[0] got 0% - lowest RTT not preferred")
	}
}
