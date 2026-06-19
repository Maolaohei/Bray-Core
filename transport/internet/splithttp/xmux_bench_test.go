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
