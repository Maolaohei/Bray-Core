package splithttp

import (
	"testing"
	"time"
)

// TestPipeline_UpdateQuality_ReflectsInScoreClient verifies the full pipeline:
// UpdateQuality → scoreClient reads lastRetrans/lastLoss → score changes.
func TestPipeline_UpdateQuality_ReflectsInScoreClient(t *testing.T) {
	c := &XmuxClient{
		createdAt: time.Now(),
	}
	c.LeftRequests.Store(999999)

	// Baseline: no quality data, score should be minimal
	baseline := scoreClient(c)
	if baseline == 0 {
		t.Fatal("baseline score should not be 0")
	}
	t.Logf("baseline score (no quality): %d", baseline)

	// Inject retrans=10, loss=500 (5%)
	c.UpdateQuality(80, 90, 10, 500)

	withQuality := scoreClient(c)
	t.Logf("score with retrans=10 loss=5%%: %d", withQuality)

	// Score should be higher (worse) due to retrans and loss penalties
	// retrans penalty: 10 * 50 = 500
	// loss penalty: 500 / 20 = 25
	// total extra: 525
	expectedDelta := int64(10*50 + 500/20) // 525
	actualDelta := withQuality - baseline
	if actualDelta != expectedDelta {
		t.Errorf("expected delta %d, got %d (baseline=%d, withQuality=%d)",
			expectedDelta, actualDelta, baseline, withQuality)
	}
}

// TestPipeline_UpdateQuality_HighRetransPenalizes verifies high retrans counts.
func TestPipeline_UpdateQuality_HighRetransPenalizes(t *testing.T) {
	c := &XmuxClient{createdAt: time.Now()}
	c.LeftRequests.Store(999999)

	// Low retrans
	c.UpdateQuality(90, 95, 5, 0)
	scoreLow := scoreClient(c)

	// High retrans
	c.UpdateQuality(90, 95, 50, 0)
	scoreHigh := scoreClient(c)

	delta := scoreHigh - scoreLow
	expected := int64(45 * 50) // (50-5) * 50
	if delta != expected {
		t.Errorf("retrans penalty: expected %d, got %d", expected, delta)
	}
	t.Logf("retrans 5→50 adds %d to score", delta)
}

// TestPipeline_UpdateQuality_HighLossPenalizes verifies loss rate penalty.
func TestPipeline_UpdateQuality_HighLossPenalizes(t *testing.T) {
	c := &XmuxClient{createdAt: time.Now()}
	c.LeftRequests.Store(999999)

	// 0% loss
	c.UpdateQuality(90, 95, 0, 0)
	scoreNoLoss := scoreClient(c)

	// 10% loss (lossRate = 1000)
	c.UpdateQuality(90, 95, 0, 1000)
	scoreHighLoss := scoreClient(c)

	delta := scoreHighLoss - scoreNoLoss
	expected := int64(1000 / 20) // 50
	if delta != expected {
		t.Errorf("loss penalty: expected %d, got %d", expected, delta)
	}
	t.Logf("loss 0%%→10%% adds %d to score", delta)
}

// TestPipeline_UpdateQuality_QualityScoreUpdate verifies quality and confidence are stored.
func TestPipeline_UpdateQuality_QualityScoreUpdate(t *testing.T) {
	c := &XmuxClient{createdAt: time.Now()}

	c.UpdateQuality(85, 70, 3, 200)

	if c.qualityScore.Load() != 85 {
		t.Errorf("qualityScore: expected 85, got %d", c.qualityScore.Load())
	}
	if c.confidence.Load() != 70 {
		t.Errorf("confidence: expected 70, got %d", c.confidence.Load())
	}
	if c.lastRetrans.Load() != 3 {
		t.Errorf("lastRetrans: expected 3, got %d", c.lastRetrans.Load())
	}
	if c.lastLoss.Load() != 200 {
		t.Errorf("lastLoss: expected 200, got %d", c.lastLoss.Load())
	}
}

// TestPipeline_ShouldDrain_TracksConsecutiveDrops verifies drain triggers after 5 consecutive drops.
func TestPipeline_ShouldDrain_TracksConsecutiveDrops(t *testing.T) {
	c := &XmuxClient{createdAt: time.Now()}
	c.LeftRequests.Store(999999)

	// Set initial quality score
	c.UpdateQuality(100, 50, 0, 0)

	// 4 drops — should not drain
	for i := 0; i < 4; i++ {
		c.UpdateQuality(int32(99-i), 50, 0, 0)
	}
	if c.ShouldDrain() {
		t.Fatal("should not drain after 4 consecutive drops")
	}

	// 5th drop — should drain
	c.UpdateQuality(94, 50, 0, 0)
	if !c.ShouldDrain() {
		t.Fatal("should drain after 5 consecutive drops")
	}
}

// TestPipeline_ShouldDrain_ResetsOnImprovement verifies consecutive drops reset when quality improves.
func TestPipeline_ShouldDrain_ResetsOnImprovement(t *testing.T) {
	c := &XmuxClient{createdAt: time.Now()}
	c.LeftRequests.Store(999999)

	// 3 drops
	for i := 0; i < 3; i++ {
		c.UpdateQuality(int32(80-i), 50, 0, 0)
	}
	// Improvement
	c.UpdateQuality(90, 50, 0, 0)
	// 2 more drops (total 2, not 5)
	for i := 0; i < 2; i++ {
		c.UpdateQuality(int32(80-i), 50, 0, 0)
	}
	if c.ShouldDrain() {
		t.Fatal("should not drain — consecutive drops were reset by improvement")
	}
}
