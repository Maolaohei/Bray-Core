package quality

import "testing"

func BenchmarkEWMAOnSuccess(b *testing.B) {
	e := NewEWMA(0.5)
	for i := 0; i < b.N; i++ {
		e.OnSuccess()
	}
}

func BenchmarkEWMAOnFailure(b *testing.B) {
	e := NewEWMA(0.5)
	for i := 0; i < b.N; i++ {
		e.OnFailure()
	}
}

func BenchmarkQualityComputeOverall(b *testing.B) {
	w := DefaultXMUXWeights()
	q := Quality{Latency: 85, Loss: 90, Stability: 70}
	for i := 0; i < b.N; i++ {
		w.ComputeOverall(q)
	}
}

func BenchmarkHistoryPush(b *testing.B) {
	var h History
	for i := 0; i < b.N; i++ {
		h.Push(int64(i), 0.01, 80, 90)
	}
}

func BenchmarkMetricOr(b *testing.B) {
	m := NewMetric(42)
	for i := 0; i < b.N; i++ {
		m.Or(0)
	}
}

func BenchmarkMetricOrUnknown(b *testing.B) {
	m := Unknown[int]()
	for i := 0; i < b.N; i++ {
		m.Or(99)
	}
}

func BenchmarkSnapshotIsStale(b *testing.B) {
	snap := &Snapshot{}
	for i := 0; i < b.N; i++ {
		snap.IsStale(30_000_000_000) // 30s
	}
}
