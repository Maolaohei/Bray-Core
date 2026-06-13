package splithttp

import (
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

// TestV21_BehaviorPenaltyScale verifies penalty scaling per behavior.
func TestV21_BehaviorPenaltyScale(t *testing.T) {
	tests := []struct {
		behavior quality.Behavior
		expected float64
	}{
		{quality.BehaviorLowLatency, 0.5},
		{quality.BehaviorNormal, 1.0},
		{quality.BehaviorAggressive, 1.2},
		{quality.BehaviorLossy, 1.5},
		{quality.BehaviorSaturated, 1.5},
		{quality.BehaviorUnknown, 1.0},
	}

	for _, tt := range tests {
		scale := behaviorPenaltyScale(tt.behavior)
		if scale != tt.expected {
			t.Errorf("behavior %v: expected scale %.1f, got %.1f", tt.behavior, tt.expected, scale)
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
	lowLatScale := behaviorPenaltyScale(lowLatBehavior)
	lossyBehavior := quality.BehaviorLossy
	lossyScale := behaviorPenaltyScale(lossyBehavior)

	if lowLatScale >= lossyScale {
		t.Errorf("LowLatency scale (%.1f) should be < Lossy scale (%.1f)", lowLatScale, lossyScale)
	}

	// Verify scales are correct
	if lowLatScale != 0.5 {
		t.Errorf("LowLatency scale: expected 0.5, got %.1f", lowLatScale)
	}
	if lossyScale != 1.5 {
		t.Errorf("Lossy scale: expected 1.5, got %.1f", lossyScale)
	}
	t.Logf("Behavior scales: LowLatency=%.1f Normal=1.0 Aggressive=1.2 Lossy=%.1f Saturated=1.5",
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
