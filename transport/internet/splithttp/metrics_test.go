package splithttp

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestMetricsOutput(t *testing.T) {
	// Create a mock XMUX manager
	xmuxConfig := XmuxConfig{
		MaxConcurrency: &RangeConfig{From: 2, To: 4},
		MaxConnections: &RangeConfig{From: 1, To: 2},
	}

	callCount := 0
	m := NewXmuxManager(xmuxConfig, func() XmuxConn {
		callCount++
		return &mockConn{id: callCount}
	})
	defer m.Close()

	// Simulate some operations
	fmt.Println("=== Simulating XMUX operations ===")

	// Get some clients (creates new connections)
	for i := 0; i < 5; i++ {
		client := m.GetXmuxClient(context.Background())
		if client != nil {
			fmt.Printf("Got client %d\n", i+1)
		}
	}

	// Simulate reuse (get same clients again)
	for i := 0; i < 10; i++ {
		client := m.GetXmuxClient(context.Background())
		if client != nil {
			client.UpdateRTT(time.Duration(10+i*5) * time.Millisecond)
		}
	}

	// Simulate warmup
	m.EnqueueWarmup("node1.example.com", 1)
	m.EnqueueWarmup("node2.example.com", 2)
	m.processWarmupQueue()

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
	fmt.Printf("WarmupHit: %d\n", metrics.WarmupHit)
	fmt.Printf("WarmupEnqueue: %d\n", metrics.WarmupEnqueue)
	fmt.Printf("WarmupSuccess: %d\n", metrics.WarmupSuccess)
	fmt.Printf("WarmupFailed: %d\n", metrics.WarmupFailed)
	fmt.Printf("TTFBSamples: %d\n", metrics.TTFBSamples)
	fmt.Printf("AvgTTFB: %v\n", metrics.AvgTTFB)
	fmt.Printf("MaxTTFB: %v\n", metrics.MaxTTFB)
	fmt.Printf("NetRecovery: %d\n", metrics.NetRecovery)
}

type mockConn struct {
	id int
}

func (c *mockConn) IsClosed() bool {
	return false
}
