package splithttp_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/xtls/xray-core/transport/internet/splithttp"
)

type fakeRoundTripper struct{}

func (f *fakeRoundTripper) IsClosed() bool {
	return false
}

func TestMaxConnections(t *testing.T) {
	xmuxConfig := XmuxConfig{
		MaxConnections: &RangeConfig{From: 4, To: 4},
	}

	xmuxManager := NewXmuxManager(xmuxConfig, func() XmuxConn {
		return &fakeRoundTripper{}
	})
	defer xmuxManager.Close()

	// preConnectLoop creates 1 initial connection. Drain it first.
	xmuxManager.GetXmuxClient(context.Background())

	xmuxClients := make(map[interface{}]struct{})
	for i := 0; i < 8; i++ {
		c, _ := xmuxManager.GetXmuxClient(context.Background())
		xmuxClients[c] = struct{}{}
	}

	// background goroutines (preConnectLoop, healthCheckTick) may create
	// additional connections concurrently, so the pool can reach the limit
	// before all loop iterations run. We expect at least 2 distinct clients
	// (proving connections are created) and at most 4 (proving the limit works).
	n := len(xmuxClients)
	if n < 2 || n > 4 {
		t.Errorf("expected 2-4 distinct clients, got %d", n)
	}
}

func TestCMaxReuseTimes(t *testing.T) {
	// Pin connection/concurrency to unlimited (0) so only CMaxReuseTimes is under test.
	xmuxConfig := XmuxConfig{
		MaxConcurrency: &RangeConfig{From: 0, To: 0},
		MaxConnections: &RangeConfig{From: 0, To: 0},
		CMaxReuseTimes: &RangeConfig{From: 2, To: 2},
	}

	xmuxManager := NewXmuxManager(xmuxConfig, func() XmuxConn {
		return &fakeRoundTripper{}
	})

	// Let preConnectLoop finish creating the initial connection, then close.
	xmuxManager.Close()
	defer ResetGlobalDialer()

	xmuxClients := make(map[interface{}]struct{})
	for i := 0; i < 64; i++ {
		c, _ := xmuxManager.GetXmuxClient(context.Background())
		xmuxClients[c] = struct{}{}
	}

	// preConnectLoop may create 1 extra client before Close takes effect.
	// With CMaxReuseTimes=2: 64 calls / 2 = 32 clients from the test loop.
	// Plus possibly 1 from preConnectLoop = 32 or 33.
	n := len(xmuxClients)
	if n < 32 || n > 34 {
		t.Error("expected 32-34 distinct clients, got ", n)
	}
}

func TestMaxConcurrency(t *testing.T) {
	xmuxConfig := XmuxConfig{
		MaxConcurrency: &RangeConfig{From: 2, To: 2},
	}

	xmuxManager := NewXmuxManager(xmuxConfig, func() XmuxConn {
		return &fakeRoundTripper{}
	})
	defer xmuxManager.Close()

	xmuxClients := make(map[interface{}]struct{})
	for i := 0; i < 64; i++ {
		xmuxClient, _ := xmuxManager.GetXmuxClient(context.Background())
		xmuxClient.Borrow()
		xmuxClients[xmuxClient] = struct{}{}
	}

	if len(xmuxClients) != 32 {
		t.Error("did not get 32 distinct clients, got ", len(xmuxClients))
	}
}

func TestDefault(t *testing.T) {
	// Bray-V2 browser-like nil defaults: concurrency 8-16, connections 2-4.
	// Holding Borrow() without Release saturates per-conn concurrency and
	// forces additional pool clients (still bounded by connection defaults).
	xmuxConfig := XmuxConfig{}

	xmuxManager := NewXmuxManager(xmuxConfig, func() XmuxConn {
		return &fakeRoundTripper{}
	})
	defer xmuxManager.Close()

	xmuxClients := make(map[interface{}]struct{})
	for i := 0; i < 64; i++ {
		xmuxClient, _ := xmuxManager.GetXmuxClient(context.Background())
		xmuxClient.Borrow()
		xmuxClients[xmuxClient] = struct{}{}
	}

	n := len(xmuxClients)
	// 64 held streams / max concurrency 16 => >=4 clients; pool soft-expands past maxConnections when saturated.
	if n < 4 || n > 32 {
		t.Errorf("expected 4-32 distinct clients under browser defaults with held streams, got %d", n)
	}
}

func TestConcurrentPoolAccess(t *testing.T) {
	xmuxConfig := XmuxConfig{
		MaxConnections: &RangeConfig{From: 4, To: 4},
	}

	var connCount atomic.Int32
	xmuxManager := NewXmuxManager(xmuxConfig, func() XmuxConn {
		connCount.Add(1)
		return &fakeRoundTripper{}
	})
	defer xmuxManager.Close()

	var wg sync.WaitGroup
	errCount := atomic.Int32{}

	// Simulate 50 concurrent goroutines calling GetXmuxClient
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				client, err := xmuxManager.GetXmuxClient(context.Background())
				if err != nil || client == nil {
					errCount.Add(1)
					return
				}
				client.Borrow()
				time.Sleep(time.Microsecond) // brief hold
				client.Release()
			}
		}()
	}

	wg.Wait()

	if errCount.Load() > 0 {
		t.Errorf("got %d nil clients from GetXmuxClient", errCount.Load())
	}

	t.Logf("Total connections created: %d", connCount.Load())
}
