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
	// Unlimited connections (0) so only MaxConcurrency drives expansion.
	// With a finite MaxConnections, burst+over-admit caps pool growth.
	xmuxConfig := XmuxConfig{
		MaxConcurrency: &RangeConfig{From: 2, To: 2},
		MaxConnections: &RangeConfig{From: 0, To: 0},
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
	// Holding Borrow() without Release saturates per-conn concurrency.
	// Soft-expand is now capped at burst (min(16, max(steady*2, steady+2))).
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
	// steady 2-4 => burst 4-8; absolute burst max 16. Beyond that, over-admit reuses.
	if n < 4 || n > 16 {
		t.Errorf("expected 4-16 distinct clients under burst-capped defaults, got %d", n)
	}
}

func TestBurstCapOverAdmit(t *testing.T) {
	// steady maxConnections=2, concurrency=1 => burst=min(16,max(4,4))=4.
	// 20 held Borrow slots must not create more than burst clients.
	xmuxConfig := XmuxConfig{
		MaxConcurrency: &RangeConfig{From: 1, To: 1},
		MaxConnections: &RangeConfig{From: 2, To: 2},
	}
	var created atomic.Int32
	xmuxManager := NewXmuxManager(xmuxConfig, func() XmuxConn {
		created.Add(1)
		return &fakeRoundTripper{}
	})
	defer xmuxManager.Close()

	clients := make(map[interface{}]struct{})
	for i := 0; i < 20; i++ {
		c, err := xmuxManager.GetXmuxClient(context.Background())
		if err != nil || c == nil {
			t.Fatalf("GetXmuxClient: %v", err)
		}
		if !c.Borrow() {
			t.Fatal("Borrow failed on returned client")
		}
		clients[c] = struct{}{}
	}
	n := len(clients)
	if n > 4 {
		t.Fatalf("burst cap broken: got %d distinct clients, want <=4", n)
	}
	if n < 2 {
		t.Fatalf("expected soft-expand at least to steady/burst, got %d", n)
	}
	// All 20 requests succeeded (over-admit after burst).
	if created.Load() > 8 {
		// preConnect/health may add a few; still must stay near burst.
		t.Fatalf("too many connections created: %d", created.Load())
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
