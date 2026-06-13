package quality

import (
	"testing"
	"time"
)

func TestNetworkLearnerRecord(t *testing.T) {
	learner := NewNetworkLearner()

	// Feed 10 normal snapshots
	for i := 0; i < 10; i++ {
		snap := &Snapshot{
			Confidence: 80,
			RTT:        NewMetric(50 * time.Millisecond),
			RTTVar:     NewMetric(10 * time.Millisecond),
			Loss:       NewMetric(0.002),
			Retrans:    NewMetric[uint32](1),
			Unacked:    NewMetric[uint32](30),
		}
		dominant := learner.Record(snap)
		if dominant != BehaviorNormal {
			t.Fatalf("iteration %d: dominant = %v, want BehaviorNormal", i, dominant)
		}
	}

	stats := learner.Stats()
	if stats.TotalSamples != 10 {
		t.Errorf("TotalSamples = %d, want 10", stats.TotalSamples)
	}
	if stats.Dominant != BehaviorNormal {
		t.Errorf("Dominant = %v, want BehaviorNormal", stats.Dominant)
	}
	if stats.Transitions != 0 {
		t.Errorf("Transitions = %d, want 0", stats.Transitions)
	}
}

func TestNetworkLearnerTransition(t *testing.T) {
	learner := NewNetworkLearner()

	// 3 normal, then 7 lossy (so lossy is dominant)
	for i := 0; i < 3; i++ {
		snap := &Snapshot{
			Confidence: 80,
			RTT:        NewMetric(50 * time.Millisecond),
			RTTVar:     NewMetric(10 * time.Millisecond),
			Loss:       NewMetric(0.002),
			Retrans:    NewMetric[uint32](1),
			Unacked:    NewMetric[uint32](30),
		}
		learner.Record(snap)
	}
	for i := 0; i < 7; i++ {
		snap := &Snapshot{
			Confidence: 80,
			RTT:        NewMetric(80 * time.Millisecond),
			RTTVar:     NewMetric(20 * time.Millisecond),
			Loss:       NewMetric(0.05),
			Retrans:    NewMetric[uint32](20),
			Unacked:    NewMetric[uint32](10),
		}
		learner.Record(snap)
	}

	stats := learner.Stats()
	if stats.Transitions != 1 {
		t.Errorf("Transitions = %d, want 1 (normal→lossy)", stats.Transitions)
	}
	// After 5 lossy samples, dominant should be lossy
	if stats.Dominant != BehaviorLossy {
		t.Errorf("Dominant = %v, want BehaviorLossy", stats.Dominant)
	}
}

func TestNetworkLearnerReset(t *testing.T) {
	learner := NewNetworkLearner()
	snap := &Snapshot{
		Confidence: 80,
		RTT:        NewMetric(50 * time.Millisecond),
	}
	learner.Record(snap)

	learner.Reset()
	stats := learner.Stats()
	if stats.TotalSamples != 0 {
		t.Errorf("after reset TotalSamples = %d, want 0", stats.TotalSamples)
	}
	if stats.Dominant != BehaviorUnknown {
		t.Errorf("after reset Dominant = %v, want BehaviorUnknown", stats.Dominant)
	}
}

func TestNetworkLearnerDebugInfo(t *testing.T) {
	learner := NewNetworkLearner()
	snap := &Snapshot{
		Confidence: 80,
		RTT:        NewMetric(50 * time.Millisecond),
		RTTVar:     NewMetric(10 * time.Millisecond),
		Loss:       NewMetric(0.002),
		Retrans:    NewMetric[uint32](1),
		Unacked:    NewMetric[uint32](30),
	}
	learner.Record(snap)
	learner.Record(snap)
	learner.Record(snap)

	info := learner.DebugInfo()
	if info.Dominant != "normal" {
		t.Errorf("DebugInfo.Dominant = %q, want 'normal'", info.Dominant)
	}
	if info.TotalSamples != 3 {
		t.Errorf("DebugInfo.TotalSamples = %d, want 3", info.TotalSamples)
	}
	if _, ok := info.Distribution["normal"]; !ok {
		t.Error("DebugInfo.Distribution should contain 'normal'")
	}
}
