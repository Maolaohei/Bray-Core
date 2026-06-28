package quality

import (
	"testing"
	"time"
)

func TestMetricValid(t *testing.T) {
	m := NewMetric(42)
	if !m.Valid {
		t.Fatal("expected valid")
	}
	if m.Value != 42 {
		t.Fatalf("got %d, want 42", m.Value)
	}
}

func TestMetricUnknown(t *testing.T) {
	m := Unknown[int]()
	if m.Valid {
		t.Fatal("expected invalid")
	}
	if m.Or(99) != 99 {
		t.Fatal("Or should return fallback for unknown")
	}
}

func TestMetricOr(t *testing.T) {
	valid := NewMetric(10)
	if valid.Or(99) != 10 {
		t.Fatal("Or should return value for valid metric")
	}
	unknown := Unknown[int]()
	if unknown.Or(99) != 99 {
		t.Fatal("Or should return fallback for unknown metric")
	}
}

func TestQualityUnknown(t *testing.T) {
	q := UnknownQuality()
	if q.Overall != 0 || q.Latency != 0 || q.Loss != 0 || q.Stability != 0 {
		t.Fatal("unknown quality should be all zeros")
	}
}

func TestQualityWeightsXMUX(t *testing.T) {
	w := DefaultXMUXWeights()
	q := Quality{Latency: 90, Loss: 20, Stability: 60}
	overall := w.ComputeOverall(q)
	// 90*0.3 + 20*0.4 + 60*0.3 = 27 + 8 + 18 = 53
	if overall != 53 {
		t.Fatalf("got %d, want 53", overall)
	}
}

func TestQualityWeightsHEv3(t *testing.T) {
	w := DefaultHEv3Weights()
	q := Quality{Latency: 90, Loss: 20, Stability: 60}
	overall := w.ComputeOverall(q)
	// (90*0.7 + 20*0.3) / 1.0 = 63 + 6 = 69
	if overall != 69 {
		t.Fatalf("got %d, want 69", overall)
	}
}

func TestQualityWeightsNormalize(t *testing.T) {
	w := QualityWeights{LatencyWeight: 3, LossWeight: 4, StabilityWeight: 3}
	q := Quality{Latency: 100, Loss: 100, Stability: 100}
	if w.ComputeOverall(q) != 100 {
		t.Fatal("all 100 should produce 100")
	}
}

func TestQualityWeightsZero(t *testing.T) {
	w := QualityWeights{}
	q := Quality{Overall: 50, Latency: 50, Loss: 50, Stability: 50}
	// zero weights → falls through to q.Overall
	if w.ComputeOverall(q) != 50 {
		t.Fatal("zero weights should return q.Overall")
	}
}

func TestSnapshotUnknown(t *testing.T) {
	snap := NewUnknownSnapshot()
	if snap.Confidence != 0 {
		t.Fatal("unknown snapshot should have 0 confidence")
	}
	if snap.Source != SourceUnknown {
		t.Fatal("unknown snapshot should have SourceUnknown")
	}
}

func TestSnapshotStale(t *testing.T) {
	snap := &Snapshot{
		Timestamp: time.Now().Add(-10 * time.Second),
	}
	if !snap.IsStale(5 * time.Second) {
		t.Fatal("10s-old snapshot should be stale with 5s threshold")
	}
	if snap.Age() < 9*time.Second {
		t.Fatal("age should be ~10s")
	}
}
