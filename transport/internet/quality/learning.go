package quality

import (
	"sync"
	"time"
)

// NetworkLearner tracks historical behavior patterns for a connection path.
// Aggregates short-term observations into long-term behavior profile.
//
// Usage:
//
//	learner := NewNetworkLearner()
//	// On each snapshot:
//	behavior := learner.Record(snap)
//	// behavior is the inferred type based on recent history.
type NetworkLearner struct {
	mu sync.RWMutex

	// Recent observations (ring buffer of last 32 classifications)
	recent [32]Behavior
	head   int
	count  int

	// Aggregated statistics
	behaviorCounts map[Behavior]int
	totalSamples   int

	// Dominant behavior (most frequent in recent window)
	dominant Behavior

	// Transition tracking: how often behavior changes
	transitions  int
	lastBehavior Behavior
}

// NewNetworkLearner creates a new learner.
func NewNetworkLearner() *NetworkLearner {
	return &NetworkLearner{
		behaviorCounts: make(map[Behavior]int),
	}
}

// Record classifies the given snapshot and records the observation.
// Returns the current dominant behavior (based on recent window).
func (l *NetworkLearner) Record(snap *Snapshot) Behavior {
	b := ClassifyBehavior(snap)

	l.mu.Lock()
	defer l.mu.Unlock()

	// Push to recent ring buffer
	l.recent[l.head] = b
	l.head = (l.head + 1) % 32
	if l.count < 32 {
		l.count++
	}

	// Update aggregate counts
	l.behaviorCounts[b]++
	l.totalSamples++

	// Track transitions
	if l.totalSamples > 1 && b != l.lastBehavior {
		l.transitions++
	}
	l.lastBehavior = b

	// Recompute dominant behavior from recent window
	l.dominant = l.computeDominant()

	return l.dominant
}

// Dominant returns the most frequent behavior in the recent observation window.
func (l *NetworkLearner) Dominant() Behavior {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.dominant
}

// TransitionRate returns how often behavior changes (0.0 = stable, 1.0 = chaotic).
func (l *NetworkLearner) TransitionRate() float64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.totalSamples < 2 {
		return 0
	}
	return float64(l.transitions) / float64(l.totalSamples-1)
}

// BehaviorDistribution returns the relative frequency of each behavior.
func (l *NetworkLearner) BehaviorDistribution() map[Behavior]float64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	dist := make(map[Behavior]float64, len(l.behaviorCounts))
	if l.totalSamples == 0 {
		return dist
	}
	for b, count := range l.behaviorCounts {
		dist[b] = float64(count) / float64(l.totalSamples)
	}
	return dist
}

// Stats returns a summary of the learner state.
type LearnerStats struct {
	Dominant       Behavior
	TransitionRate float64
	TotalSamples   int
	Transitions    int
	Distribution   map[Behavior]float64
}

func (l *NetworkLearner) Stats() LearnerStats {
	l.mu.RLock()
	defer l.mu.RUnlock()
	dist := make(map[Behavior]float64, len(l.behaviorCounts))
	if l.totalSamples > 0 {
		for b, count := range l.behaviorCounts {
			dist[b] = float64(count) / float64(l.totalSamples)
		}
	}
	return LearnerStats{
		Dominant:       l.dominant,
		TransitionRate: l.TransitionRate(),
		TotalSamples:   l.totalSamples,
		Transitions:    l.transitions,
		Distribution:   dist,
	}
}

func (l *NetworkLearner) computeDominant() Behavior {
	var counts [6]int // BehaviorUnknown(0) through BehaviorAggressive(5)
	start := (l.head - l.count + 32) % 32
	for i := 0; i < l.count; i++ {
		b := l.recent[(start+i)%32]
		counts[b]++
	}
	best := BehaviorUnknown
	bestCount := 0
	for i := Behavior(0); i <= BehaviorAggressive; i++ {
		if counts[i] > bestCount {
			best = i
			bestCount = counts[i]
		}
	}
	return best
}

// Reset clears all learning data.
func (l *NetworkLearner) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.recent = [32]Behavior{}
	l.head = 0
	l.count = 0
	l.behaviorCounts = make(map[Behavior]int)
	l.totalSamples = 0
	l.dominant = BehaviorUnknown
	l.transitions = 0
	l.lastBehavior = BehaviorUnknown
}

// LearnerDebugInfo is a JSON-serializable view of the learner for Debug API.
type LearnerDebugInfo struct {
	Dominant       string             `json:"dominant"`
	TransitionRate float64            `json:"transitionRate"`
	TotalSamples   int                `json:"totalSamples"`
	Transitions    int                `json:"transitions"`
	Distribution   map[string]float64 `json:"distribution"`
}

// DebugInfo returns a serializable debug view.
func (l *NetworkLearner) DebugInfo() LearnerDebugInfo {
	stats := l.Stats()
	dist := make(map[string]float64, len(stats.Distribution))
	for b, v := range stats.Distribution {
		dist[b.String()] = v
	}
	return LearnerDebugInfo{
		Dominant:       stats.Dominant.String(),
		TransitionRate: stats.TransitionRate,
		TotalSamples:   stats.TotalSamples,
		Transitions:    stats.Transitions,
		Distribution:   dist,
	}
}

// Ensure time import is used (for potential future timestamping)
var _ = time.Now
