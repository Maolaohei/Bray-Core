package splithttp

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestMetricsOutput(t *testing.T) {
	// Create a mock XMUX manager
	xmuxConfig := XmuxConfig{
		MaxConcurrency: &RangeConfig{From: 2, To: 4},
		MaxConnections: &RangeConfig{From: 1, To: 2},
	}

	var callCount atomic.Int32
	m := NewXmuxManager(&xmuxConfig, func() XmuxConn {
		id := callCount.Add(1)
		return &mockConn{id: int(id)}
	})
	defer m.Close()

	// Simulate some operations
	fmt.Println("=== Simulating XMUX operations ===")

	// Get some clients (creates new connections)
	for i := 0; i < 5; i++ {
		client, _ := m.GetXmuxClient(context.Background())
		if client != nil {
			fmt.Printf("Got client %d\n", i+1)
		}
	}

	// Simulate reuse (get same clients again)
	for i := 0; i < 10; i++ {
		client, _ := m.GetXmuxClient(context.Background())
		if client != nil {
			client.UpdateRTT(time.Duration(10+i*5) * time.Millisecond)
		}
	}

	// Simulate TTFB recording
	for i := 0; i < 20; i++ {
		m.RecordTTFB(time.Duration(10+i*3) * time.Millisecond)
	}

	// Simulate network change
	m.metrics.netRecoveryCount.Add(1)

	// Print metrics
	fmt.Println("\n=== Metrics Output ===")
	metrics := m.GetMetrics()
	fmt.Println(metrics.String())

	fmt.Printf("\n=== Raw Values ===\n")
	fmt.Printf("ReuseHit: %d\n", metrics.ReuseHit)
	fmt.Printf("NewConn: %d\n", metrics.NewConn)
	fmt.Printf("TTFBSamples: %d\n", metrics.TTFBSamples)
	fmt.Printf("AvgTTFB: %v\n", metrics.AvgTTFB)
	fmt.Printf("MaxTTFB: %v\n", metrics.MaxTTFB)
	fmt.Printf("NetRecovery: %d\n", metrics.NetRecovery)
}

type mockConn struct {
	id     int
	closed bool
}

func (c *mockConn) IsClosed() bool {
	return c.closed
}
