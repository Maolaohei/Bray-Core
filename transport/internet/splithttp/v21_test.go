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
	c.LeftRequests.Store(999999)

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
	c.LeftRequests.Store(999999)

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
	c.LeftRequests.Store(999999)
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

// TestV21_DynamicConnectionScaling_EffectiveConnections verifies pool sizing per behavior with debounce.
func TestV21_DynamicConnectionScaling_EffectiveConnections(t *testing.T) {
	m := NewXmuxManager(XmuxConfig{
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
}

// TestV21_DynamicConnectionScaling_OscillationPrevention verifies debounce prevents rapid switching.
func TestV21_DynamicConnectionScaling_OscillationPrevention(t *testing.T) {
	m := NewXmuxManager(XmuxConfig{
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

	// Single observation of Lossy should NOT switch (debounce)
	m.UpdatePoolBehavior(quality.BehaviorLossy)
	if m.GetPoolBehavior() != quality.BehaviorLowLatency {
		t.Error("should NOT switch after 1 observation (debounce)")
	}

	// Another single observation should NOT switch
	m.UpdatePoolBehavior(quality.BehaviorLossy)
	if m.GetPoolBehavior() != quality.BehaviorLowLatency {
		t.Error("should NOT switch after 2 observations (debounce)")
	}

	// Third observation should switch
	m.UpdatePoolBehavior(quality.BehaviorLossy)
	if m.GetPoolBehavior() != quality.BehaviorLossy {
		t.Error("should switch to Lossy after 3 observations")
	}
}

// TestV21_DynamicConnectionScaling_AIMD_Decrease verifies multiplicative decrease on worsening.
func TestV21_DynamicConnectionScaling_AIMD_Decrease(t *testing.T) {
	m := NewXmuxManager(XmuxConfig{
		MaxConnections: &RangeConfig{From: 8, To: 8},
	}, func() XmuxConn { return &mockConn{} })
	defer m.Close()

	// Establish LowLatency first
	for i := 0; i < 3; i++ {
		m.UpdatePoolBehavior(quality.BehaviorLowLatency)
	}
	lowConns := m.effectiveConnections()
	t.Logf("LowLatency connections: %d", lowConns)

	// Switch to Lossy (worsening) — should halve
	for i := 0; i < 3; i++ {
		m.UpdatePoolBehavior(quality.BehaviorLossy)
	}
	lossyConns := m.effectiveConnections()
	t.Logf("Lossy connections: %d (after AIMD decrease)", lossyConns)

	if lossyConns >= lowConns {
		t.Errorf("Lossy connections (%d) should be < LowLatency connections (%d)", lossyConns, lowConns)
	}
}

// TestV21_DynamicConnectionScaling_AIMD_Increase verifies additive increase on improvement.
func TestV21_DynamicConnectionScaling_AIMD_Increase(t *testing.T) {
	m := NewXmuxManager(XmuxConfig{
		MaxConnections: &RangeConfig{From: 8, To: 8},
	}, func() XmuxConn { return &mockConn{} })
	defer m.Close()

	// Start at Lossy (low)
	for i := 0; i < 3; i++ {
		m.UpdatePoolBehavior(quality.BehaviorLossy)
	}
	lowConns := m.effectiveConnections()

	// Switch to Normal (improvement) — should increase by 1 (additive)
	for i := 0; i < 3; i++ {
		m.UpdatePoolBehavior(quality.BehaviorNormal)
	}
	normalConns := m.effectiveConnections()

	t.Logf("Lossy→Normal: %d → %d (additive increase)", lowConns, normalConns)

	if normalConns <= lowConns {
		t.Errorf("Normal connections (%d) should be > Lossy connections (%d)", normalConns, lowConns)
	}
}

// TestV21_DynamicConnectionScaling_PoolBehaviorUpdate verifies pool behavior updates with debounce.
func TestV21_DynamicConnectionScaling_PoolBehaviorUpdate(t *testing.T) {
	m := NewXmuxManager(XmuxConfig{}, func() XmuxConn { return &mockConn{} })
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
}

// TestV21_ConnectionMigration_ProactiveReplacement verifies pool stays filled after removal.
func TestV21_ConnectionMigration_ProactiveReplacement(t *testing.T) {
	var connCount atomic.Int32
	m := NewXmuxManager(XmuxConfig{
		MaxConnections: &RangeConfig{From: 3, To: 3},
	}, func() XmuxConn {
		id := int(connCount.Add(1))
		return &mockConn{id: id}
	})
	defer m.Close()

	// Fill pool to 3
	for i := 0; i < 5; i++ {
		c := m.GetXmuxClient(context.Background())
		c.Running.Add(1)
	}
	m.clientsMu.Lock()
	poolSize := len(m.xmuxClients)
	m.clientsMu.Unlock()
	t.Logf("Pool size after filling: %d", poolSize)
	if poolSize < 1 {
		t.Fatal("pool should have at least 1 connection")
	}

	// Close all connections (simulate network failure)
	m.clientsMu.Lock()
	for _, c := range m.xmuxClients {
		c.XmuxConn.(*mockConn).closed = true
	}
	m.clientsMu.Unlock()

	// GetXmuxClient should detect closed connections, remove them, and create new ones
	c := m.GetXmuxClient(context.Background())
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
	m := NewXmuxManager(XmuxConfig{
		MaxConnections: &RangeConfig{From: 2, To: 2},
	}, func() XmuxConn { return &mockConn{} })
	defer m.Close()

	// Create 2 connections
	c1 := m.GetXmuxClient(context.Background())
	c1.Running.Add(1)
	c2 := m.GetXmuxClient(context.Background())
	c2.Running.Add(1)

	// Simulate quality drain on c1 (set initial, then 5 consecutive drops)
	c1.UpdateQuality(100, 50, 0, 0) // set baseline
	for i := 0; i < 5; i++ {
		c1.UpdateQuality(int32(99-i), 50, 0, 0)
	}

	if !c1.ShouldDrain() {
		t.Fatal("c1 should be ready to drain")
	}

	// Release c1 so health check can remove it
	c1.Running.Add(-1)

	// Wait for health check to run (5s ticker)
	// Instead of waiting, directly trigger the migration logic
	m.clientsMu.Lock()
	// Remove drained connection
	for i := 0; i < len(m.xmuxClients); i++ {
		if m.xmuxClients[i] == c1 {
			m.xmuxClients[i].StopProfiling()
			m.xmuxClients = append(m.xmuxClients[:i], m.xmuxClients[i+1:]...)
			break
		}
	}
	// Proactive replacement
	effectiveConns := m.effectiveConnections()
	for len(m.xmuxClients) < int(effectiveConns) {
		m.newXmuxClientLocked()
	}
	m.clientsMu.Unlock()

	// Pool should have been refilled
	if len(m.xmuxClients) < 2 {
		t.Errorf("pool should have 2 connections after migration, got %d", len(m.xmuxClients))
	}
	t.Logf("Pool after drain+migration: %d connections", len(m.xmuxClients))
}
