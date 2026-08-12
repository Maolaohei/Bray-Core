package quality

import (
	"testing"
	"time"
)

func TestClassifyBehavior_LowLatency(t *testing.T) {
	snap := &Snapshot{
		Confidence: 80,
		RTT:        NewMetric(10 * time.Millisecond),
		RTTVar:     NewMetric(500 * time.Microsecond), // jitter ratio = 0.05 < 0.10
		Loss:       NewMetric(0.001),
		Retrans:    NewMetric[uint32](0),
		Unacked:    NewMetric[uint32](5),
	}
	if got := ClassifyBehavior(snap); got != BehaviorLowLatency {
		t.Errorf("ClassifyBehavior = %v, want BehaviorLowLatency", got)
	}
}

func TestClassifyBehavior_Aggressive(t *testing.T) {
	snap := &Snapshot{
		Confidence: 80,
		RTT:        NewMetric(5 * time.Millisecond),
		RTTVar:     NewMetric(100 * time.Microsecond),
		Loss:       NewMetric(0.001),
		Retrans:    NewMetric[uint32](0),
		Unacked:    NewMetric[uint32](10),
	}
	if got := ClassifyBehavior(snap); got != BehaviorAggressive {
		t.Errorf("ClassifyBehavior = %v, want BehaviorAggressive", got)
	}
}

func TestClassifyBehavior_Normal(t *testing.T) {
	snap := &Snapshot{
		Confidence: 80,
		RTT:        NewMetric(50 * time.Millisecond),
		RTTVar:     NewMetric(10 * time.Millisecond),
		Loss:       NewMetric(0.002),
		Retrans:    NewMetric[uint32](2),
		Unacked:    NewMetric[uint32](30),
	}
	if got := ClassifyBehavior(snap); got != BehaviorNormal {
		t.Errorf("ClassifyBehavior = %v, want BehaviorNormal", got)
	}
}

func TestClassifyBehavior_Lossy(t *testing.T) {
	snap := &Snapshot{
		Confidence: 80,
		RTT:        NewMetric(80 * time.Millisecond),
		RTTVar:     NewMetric(20 * time.Millisecond),
		Loss:       NewMetric(0.05), // 5% loss
		Retrans:    NewMetric[uint32](20),
		Unacked:    NewMetric[uint32](10),
	}
	if got := ClassifyBehavior(snap); got != BehaviorLossy {
		t.Errorf("ClassifyBehavior = %v, want BehaviorLossy", got)
	}
}

func TestClassifyBehavior_Saturated(t *testing.T) {
	snap := &Snapshot{
		Confidence: 80,
		RTT:        NewMetric(300 * time.Millisecond),
		RTTVar:     NewMetric(50 * time.Millisecond),
		Loss:       NewMetric(0.002),
		Retrans:    NewMetric[uint32](1),
		Unacked:    NewMetric[uint32](100),
	}
	if got := ClassifyBehavior(snap); got != BehaviorSaturated {
		t.Errorf("ClassifyBehavior = %v, want BehaviorSaturated", got)
	}
}

func TestClassifyBehavior_Unknown_LowConfidence(t *testing.T) {
	snap := &Snapshot{
		Confidence: 5, // below minimum threshold (10)
		RTT:        NewMetric(10 * time.Millisecond),
	}
	if got := ClassifyBehavior(snap); got != BehaviorUnknown {
		t.Errorf("ClassifyBehavior = %v, want BehaviorUnknown (low confidence)", got)
	}
}

func TestClassifyBehavior_Unknown_NilSnapshot(t *testing.T) {
	if got := ClassifyBehavior(nil); got != BehaviorUnknown {
		t.Errorf("ClassifyBehavior(nil) = %v, want BehaviorUnknown", got)
	}
}

func TestClassifyBehavior_Unknown_NoRTT(t *testing.T) {
	snap := &Snapshot{
		Confidence: 80,
		RTT:        Unknown[time.Duration](),
	}
	if got := ClassifyBehavior(snap); got != BehaviorUnknown {
		t.Errorf("ClassifyBehavior = %v, want BehaviorUnknown (no RTT)", got)
	}
}

func TestBehaviorString(t *testing.T) {
	tests := []struct {
		b    Behavior
		want string
	}{
		{BehaviorUnknown, "unknown"},
		{BehaviorLowLatency, "low_latency"},
		{BehaviorNormal, "normal"},
		{BehaviorLossy, "lossy"},
		{BehaviorSaturated, "saturated"},
		{BehaviorAggressive, "aggressive"},
	}
	for _, tt := range tests {
		if got := tt.b.String(); got != tt.want {
			t.Errorf("Behavior(%d).String() = %q, want %q", tt.b, got, tt.want)
		}
	}
}
