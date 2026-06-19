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
		xmuxClients[xmuxManager.GetXmuxClient(context.Background())] = struct{}{}
	}

	if len(xmuxClients) != 4 {
		t.Error("did not get 4 distinct clients, got ", len(xmuxClients))
	}
}

func TestCMaxReuseTimes(t *testing.T) {
	xmuxConfig := XmuxConfig{
		CMaxReuseTimes: &RangeConfig{From: 2, To: 2},
	}

	xmuxManager := NewXmuxManager(xmuxConfig, func() XmuxConn {
		return &fakeRoundTripper{}
	})

	// Let preConnectLoop finish creating the initial connection, then close.
	xmuxManager.Close()

	xmuxClients := make(map[interface{}]struct{})
	for i := 0; i < 64; i++ {
		xmuxClients[xmuxManager.GetXmuxClient(context.Background())] = struct{}{}
	}

	// preConnectLoop may create 1 extra client before Close takes effect.
	// With CMaxReuseTimes=2: 64 calls / 2 = 32 clients from the test loop.
	// Plus possibly 1 from preConnectLoop = 32 or 33.
	n := len(xmuxClients)
	if n != 32 && n != 33 {
		t.Error("expected 32 or 33 distinct clients, got ", n)
	}
}

func TestMaxConcurrency(t *testing.T) {
	xmuxConfig := XmuxConfig{
		MaxConcurrency: &RangeConfig{From: 2, To: 2},
	}

	xmuxManager := NewXmuxManager(xmuxConfig, func() XmuxConn {
		return &fakeRoundTripper{}
	})

	xmuxClients := make(map[interface{}]struct{})
	for i := 0; i < 64; i++ {
		xmuxClient := xmuxManager.GetXmuxClient(context.Background())
		xmuxClient.AddRunning()
		xmuxClients[xmuxClient] = struct{}{}
	}

	if len(xmuxClients) != 32 {
		t.Error("did not get 32 distinct clients, got ", len(xmuxClients))
	}
}

func TestDefault(t *testing.T) {
	xmuxConfig := XmuxConfig{}

	xmuxManager := NewXmuxManager(xmuxConfig, func() XmuxConn {
		return &fakeRoundTripper{}
	})
	defer xmuxManager.Close()

	xmuxClients := make(map[interface{}]struct{})
	for i := 0; i < 64; i++ {
		xmuxClient := xmuxManager.GetXmuxClient(context.Background())
		xmuxClient.AddRunning()
		xmuxClients[xmuxClient] = struct{}{}
	}

	if len(xmuxClients) != 1 {
		t.Error("did not get 1 distinct clients, got ", len(xmuxClients))
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
				client := xmuxManager.GetXmuxClient(context.Background())
				if client == nil {
					errCount.Add(1)
					return
				}
				client.OpenUsage.Add(1)
				time.Sleep(time.Microsecond) // brief hold
				client.OpenUsage.Add(-1)
			}
		}()
	}

	wg.Wait()

	if errCount.Load() > 0 {
		t.Errorf("got %d nil clients from GetXmuxClient", errCount.Load())
	}

	t.Logf("Total connections created: %d", connCount.Load())
}
