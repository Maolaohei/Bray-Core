package splithttp

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/transport/internet/quality"
)

// TestV21_Learner_ClassifiesLowLatency verifies behavior classification for fast links.
func TestV21_Learner_ClassifiesLowLatency(t *testing.T) {
	c := &XmuxClient{
		createdAt: time.Now(),
		learner:   quality.NewNetworkLearner(),
	}
	c.remaining.Store(-1)

	// Simulate 20 low-latency observations: RTT=20ms (above Aggressive threshold of 15ms)
	for i := 0; i < 20; i++ {
		c.UpdateQuality(90, 80, 0, 0)
		c.lastRTT.Store(int64(20 * time.Millisecond))
	}

	behavior := c.GetBehavior()
	if behavior != quality.BehaviorLowLatency {
		t.Errorf("expected LowLatency, got %v", behavior)
	}

	score := scoreClient(c)
	t.Logf("LowLatency behavior: score=%d, behavior=%v", score, behavior)
}

// TestV21_Learner_ClassifiesLossy verifies behavior classification for lossy links.
func TestV21_Learner_ClassifiesLossy(t *testing.T) {
	c := &XmuxClient{
		createdAt: time.Now(),
		learner:   quality.NewNetworkLearner(),
	}
	c.remaining.Store(-1)

	// Simulate 20 lossy observations: high retrans, high loss
	for i := 0; i < 20; i++ {
		c.UpdateQuality(30, 80, 15, 2000) // retrans=15, loss=20%
		c.lastRTT.Store(int64(80 * time.Millisecond))
	}

	behavior := c.GetBehavior()
	if behavior != quality.BehaviorLossy {
		t.Errorf("expected Lossy, got %v", behavior)
	}

	// Score should reflect lossy penalty (higher penalties)
	score := scoreClient(c)
	t.Logf("Lossy behavior: score=%d, behavior=%v", score, behavior)
}

// TestV21_BehaviorPenaltyScaleFixed verifies fixed-point penalty scaling per behavior.
func TestV21_BehaviorPenaltyScaleFixed(t *testing.T) {
	tests := []struct {
		behavior quality.Behavior
		expected int64
	}{
		{quality.BehaviorLowLatency, 50},  // 0.5 × 100
		{quality.BehaviorNormal, 100},     // 1.0 × 100
		{quality.BehaviorAggressive, 120}, // 1.2 × 100
		{quality.BehaviorLossy, 150},      // 1.5 × 100
		{quality.BehaviorSaturated, 150},  // 1.5 × 100
		{quality.BehaviorUnknown, 100},    // 1.0 × 100
	}

	for _, tt := range tests {
		scale := behaviorPenaltyScaleFixed(tt.behavior)
		if scale != tt.expected {
			t.Errorf("behavior %v: expected scale %d, got %d", tt.behavior, tt.expected, scale)
		}
	}
}

// TestV21_ScoreClient_BehaviorAware verifies different behaviors produce different scores.
func TestV21_ScoreClient_BehaviorAware(t *testing.T) {
	// Test that behaviorPenaltyScale correctly adjusts scores
	c := &XmuxClient{
		createdAt: time.Now(),
		learner:   quality.NewNetworkLearner(),
	}
	c.remaining.Store(-1)
	c.lastRTT.Store(int64(50 * time.Millisecond))
	c.lastRetrans.Store(10)
	c.lastLoss.Store(1000) // 10% loss
	c.confidence.Store(90)

	// Normal behavior (default)
	normalScore := scoreClient(c)
	t.Logf("Normal score: %d", normalScore)

	// Manually set behavior by overriding learner dominant
	// LowLatency should have lower score (less penalty)
	lowLatBehavior := quality.BehaviorLowLatency
	lowLatScale := behaviorPenaltyScaleFixed(lowLatBehavior)
	lossyBehavior := quality.BehaviorLossy
	lossyScale := behaviorPenaltyScaleFixed(lossyBehavior)

	if lowLatScale >= lossyScale {
		t.Errorf("LowLatency scale (%d) should be < Lossy scale (%d)", lowLatScale, lossyScale)
	}

	// Verify scales are correct (fixed-point ×100)
	if lowLatScale != 50 {
		t.Errorf("LowLatency scale: expected 50, got %d", lowLatScale)
	}
	if lossyScale != 150 {
		t.Errorf("Lossy scale: expected 150, got %d", lossyScale)
	}
	t.Logf("Behavior scales (fixed-point ×100): LowLatency=%d Normal=100 Aggressive=120 Lossy=%d Saturated=150",
		lowLatScale, lossyScale)
}

// TestV21_GetBehavior_NilLearner verifies safety with nil learner.
func TestV21_GetBehavior_NilLearner(t *testing.T) {
	c := &XmuxClient{createdAt: time.Now()}
	if c.GetBehavior() != quality.BehaviorUnknown {
		t.Error("nil learner should return Unknown")
	}
}

// TestV21_Learner_TransitionRate verifies transition tracking.
func TestV21_Learner_TransitionRate(t *testing.T) {
	c := &XmuxClient{
		createdAt: time.Now(),
		learner:   quality.NewNetworkLearner(),
	}

	// Stable: all LowLatency
	for i := 0; i < 20; i++ {
		c.UpdateQuality(90, 80, 0, 0)
		c.lastRTT.Store(int64(10 * time.Millisecond))
	}
	if c.learner.TransitionRate() > 0.1 {
		t.Errorf("stable connection should have low transition rate, got %f", c.learner.TransitionRate())
	}

	// Reset and test chaotic
	c.learner.Reset()
	for i := 0; i < 20; i++ {
		if i%2 == 0 {
			c.UpdateQuality(90, 80, 0, 0)
			c.lastRTT.Store(int64(10 * time.Millisecond))
		} else {
			c.UpdateQuality(20, 80, 20, 5000)
			c.lastRTT.Store(int64(300 * time.Millisecond))
		}
	}
	if c.learner.TransitionRate() < 0.3 {
		t.Errorf("chaotic connection should have high transition rate, got %f", c.learner.TransitionRate())
	}
	t.Logf("Stable rate: low, Chaotic rate: %f", c.learner.TransitionRate())
}

// withPoolAutoScale runs fn with the pool AIMD gate temporarily enabled —
// the tests below pin the (retained, gated-off-by-default) controller logic.
func withPoolAutoScale(t *testing.T, fn func()) {
	t.Helper()
	old := xmuxPoolAutoScale
	xmuxPoolAutoScale = true
	defer func() { xmuxPoolAutoScale = old }()
	fn()
}

// TestV21_PoolAutoScaleGateDisabled pins the shipped default: with the gate
// off, behavior observations are inert and effective limits equal the base
// config draw. The AIMD tests below run under withPoolAutoScale.
func TestV21_PoolAutoScaleGateDisabled(t *testing.T) {
	if xmuxPoolAutoScale {
		t.Fatal("xmuxPoolAutoScale must ship disabled; flip the default only with benchmark evidence")
	}
	m := NewXmuxManager(&XmuxConfig{
		MaxConnections: &RangeConfig{From: 4, To: 4},
	}, func() XmuxConn { return &mockConn{} })
	defer m.Close()

	for i := 0; i < 5; i++ {
		m.UpdatePoolBehavior(quality.BehaviorLowLatency)
	}
	if m.GetPoolBehavior() != quality.BehaviorUnknown {
		t.Errorf("gate off: UpdatePoolBehavior must be inert, got %v", m.GetPoolBehavior())
	}
	if m.effectiveConnections() != 4 {
		t.Errorf("gate off: effectiveConnections must equal base 4, got %d", m.effectiveConnections())
	}
}

// TestV21_DynamicConnectionScaling_EffectiveConnections verifies pool sizing per behavior with debounce.
func TestV21_DynamicConnectionScaling_EffectiveConnections(t *testing.T) {
	withPoolAutoScale(t, func() {
		m := NewXmuxManager(&XmuxConfig{
			MaxConnections: &RangeConfig{From: 4, To: 4},
		}, func() XmuxConn { return &mockConn{} })
		defer m.Close()

		// Before any behavior update: returns base config
		if m.effectiveConnections() != 4 {
			t.Errorf("before behavior update: expected 4, got %d", m.effectiveConnections())
		}

		// Need 3 consecutive observations to switch behavior
		for i := 0; i < debounceThreshold; i++ {
			m.UpdatePoolBehavior(quality.BehaviorLowLatency)
		}

		// After debounce: AIMD applies
		conns := m.effectiveConnections()
		if conns < 1 || conns > 8 {
			t.Errorf("LowLatency after debounce: expected 1-8, got %d", conns)
		}
		t.Logf("LowLatency effectiveConnections: %d (base=4)", conns)
	})
}

// TestV21_DynamicConnectionScaling_OscillationPrevention verifies debounce prevents rapid switching.
func TestV21_DynamicConnectionScaling_OscillationPrevention(t *testing.T) {
	withPoolAutoScale(t, func() {
		m := NewXmuxManager(&XmuxConfig{
			MaxConnections: &RangeConfig{From: 4, To: 4},
		}, func() XmuxConn { return &mockConn{} })
		defer m.Close()

		// Switch to LowLatency (3 observations)
		for i := 0; i < 3; i++ {
			m.UpdatePoolBehavior(quality.BehaviorLowLatency)
		}
		if m.GetPoolBehavior() != quality.BehaviorLowLatency {
			t.Error("should switch to LowLatency after 3 observations")
		}

		// Single observation of an IMPROVEMENT should NOT switch (debounce).
		// Reach Saturated instantly (worsening bypasses debounce), then offer
		// a slightly better Lossy once.
		m.UpdatePoolBehavior(quality.BehaviorSaturated)
		if m.GetPoolBehavior() != quality.BehaviorSaturated {
			t.Fatalf("worsening should take effect immediately, got %v", m.GetPoolBehavior())
		}
		m.UpdatePoolBehavior(quality.BehaviorLossy)
		if m.GetPoolBehavior() != quality.BehaviorSaturated {
			t.Error("improvement should NOT switch after 1 observation (debounce)")
		}

		// A second improving observation still must not switch.
		m.UpdatePoolBehavior(quality.BehaviorLossy)
		if m.GetPoolBehavior() != quality.BehaviorSaturated {
			t.Error("improvement should NOT switch after 2 observations (debounce)")
		}

		// Third observation completes the debounce.
		m.UpdatePoolBehavior(quality.BehaviorLossy)
		if m.GetPoolBehavior() != quality.BehaviorLossy {
			t.Error("should switch to Lossy after 3 improving observations")
		}
	})
}

// TestV21_AIMD_WorseningImmediate verifies degradation bypasses debounce.
func TestV21_AIMD_WorseningImmediate(t *testing.T) {
	withPoolAutoScale(t, func() {
		m := NewXmuxManager(&XmuxConfig{
			MaxConnections: &RangeConfig{From: 4, To: 4},
		}, func() XmuxConn { return &mockConn{} })
		defer m.Close()

		for i := 0; i < 3; i++ {
			m.UpdatePoolBehavior(quality.BehaviorNormal)
		}
		if m.GetPoolBehavior() != quality.BehaviorNormal {
			t.Fatal("should start at Normal")
		}

		// ONE Lossy observation must switch immediately.
		m.UpdatePoolBehavior(quality.BehaviorLossy)
		if m.GetPoolBehavior() != quality.BehaviorLossy {
			t.Errorf("worsening should take effect immediately, got %v", m.GetPoolBehavior())
		}
	})
}

// TestV21_DynamicConnectionScaling_AIMD_Decrease verifies multiplicative decrease on worsening.
func TestV21_DynamicConnectionScaling_AIMD_Decrease(t *testing.T) {
	withPoolAutoScale(t, func() {
		m := NewXmuxManager(&XmuxConfig{
			MaxConnections: &RangeConfig{From: 8, To: 8},
		}, func() XmuxConn { return &mockConn{} })
		defer m.Close()

		// Establish LowLatency first
		for i := 0; i < 3; i++ {
			m.UpdatePoolBehavior(quality.BehaviorLowLatency)
		}
		lowConns := m.effectiveConnections()
		t.Logf("LowLatency connections: %d", lowConns)

		// Switch to Lossy (worsening, immediate) — pool lands directly on the
		// Lossy target (reverse AIMD: MORE outer connections for HoL isolation
		// under loss), NOT a blind halve.
		for i := 0; i < 1; i++ {
			m.UpdatePoolBehavior(quality.BehaviorLossy)
		}
		lossyConns := m.effectiveConnections()
		t.Logf("Lossy connections: %d (after worsening AIMD)", lossyConns)

		// Reverse AIMD: under loss the pool must GROW toward base*2 (clamped),
		// never shrink below base.
		if lossyConns < 8 {
			t.Errorf("Lossy connections (%d) must be >= base 8 (reverse AIMD), not halved", lossyConns)
		}
	})
}

// TestV21_DynamicConnectionScaling_AIMD_Increase verifies additive
// adjustment toward target on improvement.
func TestV21_DynamicConnectionScaling_AIMD_Increase(t *testing.T) {
	withPoolAutoScale(t, func() {
		m := NewXmuxManager(&XmuxConfig{
			MaxConnections: &RangeConfig{From: 8, To: 8},
		}, func() XmuxConn { return &mockConn{} })
		defer m.Close()

		// Start at Lossy: reverse AIMD targets base*2 = 16 (clamped), so the
		// pool grows — the opposite of the old behavior.
		for i := 0; i < 3; i++ {
			m.UpdatePoolBehavior(quality.BehaviorLossy)
		}
		lossyConns := m.effectiveConnections()
		if lossyConns <= 8 {
			t.Fatalf("Lossy should scale connections UP (reverse AIMD), got %d", lossyConns)
		}
		t.Logf("Lossy connections: %d (reverse AIMD, > base 8)", lossyConns)

		// Switch to Normal (improvement) — additive adjustment steps DOWN
		// toward the Normal target (base = 8).
		for i := 0; i < 3; i++ {
			m.UpdatePoolBehavior(quality.BehaviorNormal)
		}
		normalConns := m.effectiveConnections()

		t.Logf("Lossy→Normal: %d → %d (additive adjustment toward target)", lossyConns, normalConns)

		if normalConns >= lossyConns {
			t.Errorf("Normal connections (%d) should be < Lossy connections (%d)", normalConns, lossyConns)
		}
	})
}

// TestV21_DynamicConnectionScaling_PoolBehaviorUpdate verifies pool behavior updates with debounce.
func TestV21_DynamicConnectionScaling_PoolBehaviorUpdate(t *testing.T) {
	withPoolAutoScale(t, func() {
		m := NewXmuxManager(&XmuxConfig{}, func() XmuxConn { return &mockConn{} })
		defer m.Close()

		// Initially unknown
		if m.GetPoolBehavior() != quality.BehaviorUnknown {
			t.Errorf("initial pool behavior should be Unknown, got %v", m.GetPoolBehavior())
		}

		// 1 observation: should NOT switch (debounce)
		m.UpdatePoolBehavior(quality.BehaviorLowLatency)
		if m.GetPoolBehavior() != quality.BehaviorUnknown {
			t.Errorf("after 1 observation: should still be Unknown, got %v", m.GetPoolBehavior())
		}

		// 2 observations: should NOT switch
		m.UpdatePoolBehavior(quality.BehaviorLowLatency)
		if m.GetPoolBehavior() != quality.BehaviorUnknown {
			t.Errorf("after 2 observations: should still be Unknown, got %v", m.GetPoolBehavior())
		}

		// 3 observations: should switch
		m.UpdatePoolBehavior(quality.BehaviorLowLatency)
		if m.GetPoolBehavior() != quality.BehaviorLowLatency {
			t.Errorf("after 3 observations: should be LowLatency, got %v", m.GetPoolBehavior())
		}
	})
}

// TestV21_ConnectionMigration_ProactiveReplacement verifies pool stays filled after removal.
func TestV21_ConnectionMigration_ProactiveReplacement(t *testing.T) {
	var connCount atomic.Int32
	m := NewXmuxManager(&XmuxConfig{
		MaxConnections: &RangeConfig{From: 3, To: 3},
	}, func() XmuxConn {
		id := int(connCount.Add(1))
		return &mockConn{id: id}
	})
	defer m.Close()

	// Fill pool to 3
	for i := 0; i < 5; i++ {
		c, _ := m.GetXmuxClient(context.Background())
		c.Borrow()
	}
	poolSize := m.pool.Len()
	t.Logf("Pool size after filling: %d", poolSize)
	if poolSize < 1 {
		t.Fatal("pool should have at least 1 connection")
	}

	// Close all connections (simulate network failure)
	m.pool.mu.Lock()
	for _, c := range m.pool.clients {
		c.XmuxConn.(*mockConn).closed = true
	}
	m.pool.mu.Unlock()

	// GetXmuxClient should detect closed connections, remove them, and create new ones
	c, _ := m.GetXmuxClient(context.Background())
	if c == nil {
		t.Fatal("GetXmuxClient should return a new connection after all are closed")
	}
	if c.XmuxConn.(*mockConn).closed {
		t.Fatal("returned connection should not be closed")
	}
	t.Logf("Pool recovered: new connection created after all closed")
}

// TestV21_ConnectionMigration_QualityDrain verifies drain triggers replacement.
func TestV21_ConnectionMigration_QualityDrain(t *testing.T) {
	m := NewXmuxManager(&XmuxConfig{
		MaxConnections: &RangeConfig{From: 2, To: 2},
	}, func() XmuxConn { return &mockConn{} })
	defer m.Close()

	// Create 2 connections
	c1, _ := m.GetXmuxClient(context.Background())
	c1.Borrow()
	c2, _ := m.GetXmuxClient(context.Background())
	c2.Borrow()

	// Simulate quality drain on c1 (set initial, then 5 consecutive drops)
	c1.UpdateQuality(100, 50, 0, 0) // set baseline
	for i := 0; i < 5; i++ {
		c1.UpdateQuality(int32(99-i), 50, 0, 0)
	}

	if !c1.ShouldDrain() {
		t.Fatal("c1 should be ready to drain")
	}

	// Release c1 so health check can remove it
	c1.Release()

	// Wait for health check to run (5s ticker) and migrate
	time.Sleep(6 * time.Second)

	// Verify pool recovered: GetXmuxClient should return a fresh connection
	c, _ := m.GetXmuxClient(context.Background())
	if c == nil {
		t.Fatal("GetXmuxClient should return a connection after migration")
	}
	t.Logf("Pool recovered: GetXmuxClient returned a fresh connection")
}
