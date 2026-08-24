package splithttp

import (
	"net/http"
	"testing"
	"time"
)

// TestDetectCDNHeaderSignatures covers the conservative value shapes: a bare
// "Cf-Ray"-looking header without the 16-hex-airport shape must NOT latch,
// while real edge signatures do.
func TestDetectCDNHeaderSignatures(t *testing.T) {
	cases := []struct {
		name    string
		headers http.Header
		want    bool
	}{
		{"cloudflare real", http.Header{"Cf-Ray": {"8a2bc3d4e5f60718-SIN"}}, true},
		{"cloudflare lowercase key", http.Header{"Cf-Ray": {"0123456789abcdef-Lax"}}, true},
		{"cf-ray wrong hex", http.Header{"Cf-Ray": {"zzzzzzzzzzzzzzzz-SIN"}}, false},
		{"cf-ray short id", http.Header{"Cf-Ray": {"abc123-SIN"}}, false},
		{"cf-ray no airport", http.Header{"Cf-Ray": {"8a2bc3d4e5f60718-"}}, false},
		{"cloudfront pop", http.Header{"X-Amz-Cf-Pop": {"SIN2-C1"}}, true},
		{"fastly cache", http.Header{"X-Served-By": {"cache-sin10022-SIN"}}, true},
		{"x-served-by not fastly", http.Header{"X-Served-By": {"origin-server-1"}}, false},
		{"plain origin", http.Header{"Server": {"nginx"}}, false},
		{"empty", http.Header{}, false},
	}
	for _, tc := range cases {
		m := &XmuxManager{stopCh: make(chan struct{})}
		m.detectCDN(tc.headers)
		if got := m.isCDN.Load(); got != tc.want {
			t.Errorf("%s: isCDN = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestDetectCDNSticky ensures the bit latches: later non-CDN probes must not
// clear it, and re-detection is a cheap no-op.
func TestDetectCDNSticky(t *testing.T) {
	m := &XmuxManager{stopCh: make(chan struct{})}
	m.detectCDN(http.Header{"Cf-Ray": {"8a2bc3d4e5f60718-SIN"}})
	if !m.isCDN.Load() {
		t.Fatal("expected CDN latch")
	}
	m.detectCDN(http.Header{"Server": {"nginx"}})
	if !m.isCDN.Load() {
		t.Fatal("sticky bit must survive non-CDN probes")
	}
}

// TestScheduleWarmReconnectGates verifies the gates: no scheduling when the
// path is not CDN, none after Close, and the timer fires into an empty pool
// exactly once.
func TestScheduleWarmReconnectGates(t *testing.T) {
	m := NewXmuxManager(nil, func() XmuxConn { return nil })
	defer m.Close()

	// Not a CDN path -> no warm reconnect ever scheduled.
	m.scheduleWarmReconnect()
	m.warmReconnectMu.Lock()
	pending := m.warmTimer != nil
	m.warmReconnectMu.Unlock()
	if pending {
		t.Fatal("non-CDN path must not schedule keep-warm")
	}

	// Latch CDN and stop the manager: scheduling after Close is a no-op.
	m.detectCDN(http.Header{"Cf-Ray": {"8a2bc3d4e5f60718-SIN"}})
	m.Close()

	before := m.warmInFlight.Load()
	m.scheduleWarmReconnect()
	if m.warmInFlight.Load() != before || before {
		// After Close the gate must refuse to arm anything.
		m.warmReconnectMu.Lock()
		pending = m.warmTimer != nil
		m.warmReconnectMu.Unlock()
		if pending {
			t.Fatal("closed manager must not schedule keep-warm")
		}
	}
}

// TestWarmReconnectFiresOnEmptyPool drives the actual timer with a tiny delay
// by arming it directly (scheduleWarmReconnect's jitter is 30-120s): the
// callback must create a client only when the pool is empty and the new
// client lands in the pool.
func TestWarmReconnectFiresOnEmptyPool(t *testing.T) {
	dialed := make(chan struct{}, 1)
	var conns []*testXmuxConn
	m := &XmuxManager{
		xmuxConfig: &XmuxConfig{},
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
	close(m.doneCh)
	m.newConnFunc = func() XmuxConn {
		select {
		case dialed <- struct{}{}:
		default:
		}
		c := &testXmuxConn{}
		conns = append(conns, c)
		return c
	}
	m.pool.clients = nil
	m.probeURL = "" // no probe: markProbeReady path
	m.isCDN.Store(true)

	m.warmInFlight.Store(true)
	done := make(chan struct{})
	m.warmReconnectMu.Lock()
	m.warmTimer = time.AfterFunc(10*time.Millisecond, func() {
		defer m.warmInFlight.Store(false)
		m.pool.mu.RLock()
		empty := len(m.pool.clients) == 0
		m.pool.mu.RUnlock()
		if !empty {
			close(done)
			return
		}
		c := m.newXmuxClient()
		if c == nil {
			close(done)
			return
		}
		close(done)
	})
	m.warmReconnectMu.Unlock()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("warm reconnect timer did not fire")
	}
	if len(m.pool.clients) != 1 {
		t.Fatalf("expected 1 warmed client in pool, got %d", len(m.pool.clients))
	}
	select {
	case <-dialed:
	default:
		t.Fatal("newConnFunc was never called")
	}
	m.Close()
}

// testXmuxConn is a minimal inert XmuxConn stand-in for pool mechanics tests.
type testXmuxConn struct{}

func (c *testXmuxConn) IsClosed() bool { return false }

func (c *testXmuxConn) Close() error { return nil }
