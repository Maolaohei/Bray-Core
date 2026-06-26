package splithttp

import (
	"testing"
	"time"
)

func TestPipeline_UpdateQuality_ReflectsInScoreClient(t *testing.T) {
	c := &XmuxClient{createdAt: time.Now()}
	c.LeftRequests.Store(999999)

	baseline := scoreClient(c)
	if baseline == 0 {
		t.Fatal("baseline score should not be 0")
	}
	t.Logf("baseline score: %d", baseline)

	// retrans=10, loss=500(5%), confidence=90, behavior=Unknown
	c.UpdateQuality(80, 90, 10, 500)

	withQuality := scoreClient(c)
	delta := withQuality - baseline
	if delta <= 0 {
		t.Errorf("score should increase after penalty, got delta %d", delta)
	}
	t.Logf("penalty delta: %d (retrans=10, loss=5%%, conf=90)", delta)
}

func TestPipeline_UpdateQuality_HighRetransPenalizes(t *testing.T) {
	c := &XmuxClient{createdAt: time.Now()}
	c.LeftRequests.Store(999999)

	c.UpdateQuality(90, 95, 5, 0)
	scoreLow := scoreClient(c)

	c.UpdateQuality(90, 95, 50, 0)
	scoreHigh := scoreClient(c)

	delta := scoreHigh - scoreLow
	if delta <= 0 {
		t.Errorf("higher retrans should increase score, got delta %d", delta)
	}
	t.Logf("retrans 5->50 adds %d to score", delta)
}

func TestPipeline_UpdateQuality_HighLossPenalizes(t *testing.T) {
	c := &XmuxClient{createdAt: time.Now()}
	c.LeftRequests.Store(999999)

	c.UpdateQuality(90, 95, 0, 0)
	scoreNoLoss := scoreClient(c)

	c.UpdateQuality(90, 95, 0, 1000)
	scoreHighLoss := scoreClient(c)

	delta := scoreHighLoss - scoreNoLoss
	if delta <= 0 {
		t.Errorf("higher loss should increase score, got delta %d", delta)
	}
	t.Logf("loss 0%%->10%% adds %d to score", delta)
}

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

func TestPipeline_ScoreClient_ConfidenceWeighting(t *testing.T) {
	cHi := &XmuxClient{createdAt: time.Now()}
	cHi.LeftRequests.Store(999999)
	cHi.UpdateQuality(80, 90, 10, 500)
	scoreHi := scoreClient(cHi)

	cLo := &XmuxClient{createdAt: time.Now()}
	cLo.LeftRequests.Store(999999)
	cLo.UpdateQuality(80, 10, 10, 500)
	scoreLo := scoreClient(cLo)

	if scoreLo >= scoreHi {
		t.Errorf("low confidence score (%d) should be < high confidence score (%d)", scoreLo, scoreHi)
	}

	delta := scoreHi - scoreLo
	if delta <= 0 {
		t.Errorf("confidence difference should be positive, got %d", delta)
	}
	t.Logf("confidence 10->90 increases penalty by %d", delta)
}

func TestPipeline_ShouldDrain_TracksConsecutiveDrops(t *testing.T) {
	c := &XmuxClient{createdAt: time.Now()}
	c.LeftRequests.Store(999999)

	c.UpdateQuality(100, 50, 0, 0)

	for i := 0; i < 4; i++ {
		c.UpdateQuality(int32(99-i), 50, 0, 0)
	}
	if c.ShouldDrain() {
		t.Fatal("should not drain after 4 consecutive drops")
	}

	c.UpdateQuality(94, 50, 0, 0)
	if !c.ShouldDrain() {
		t.Fatal("should drain after 5 consecutive drops")
	}
}

func TestPipeline_ShouldDrain_ResetsOnImprovement(t *testing.T) {
	c := &XmuxClient{createdAt: time.Now()}
	c.LeftRequests.Store(999999)

	for i := 0; i < 3; i++ {
		c.UpdateQuality(int32(80-i), 50, 0, 0)
	}
	c.UpdateQuality(90, 50, 0, 0)
	for i := 0; i < 2; i++ {
		c.UpdateQuality(int32(80-i), 50, 0, 0)
	}
	if c.ShouldDrain() {
		t.Fatal("should not drain - consecutive drops were reset")
	}
}
