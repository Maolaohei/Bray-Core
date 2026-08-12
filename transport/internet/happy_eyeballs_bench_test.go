package internet

import (
	"net"
	"testing"
	"time"
)

// BenchmarkScoreIPs measures IP scoring throughput.
func BenchmarkScoreIPs(b *testing.B) {
	ips := make([]net.IP, 20)
	for i := range ips {
		ips[i] = net.IPv4(byte(10), byte(i/256), byte(i%256), 1)
	}

	var buf []HappyIPScore
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = scoreIPsInto(buf, ips, false, nil)
	}
}

// BenchmarkScoreIPs_WithSVCB measures IP scoring with SVCB priorities.
func BenchmarkScoreIPs_WithSVCB(b *testing.B) {
	ips := make([]net.IP, 20)
	svcb := make(map[ipKey]int64, 20)
	for i := range ips {
		ips[i] = net.IPv4(byte(10), byte(i/256), byte(i%256), 1)
		if k, ok := ipToKey(ips[i]); ok {
			svcb[k] = int64(i)
		}
	}

	var buf []HappyIPScore
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = scoreIPsInto(buf, ips, false, svcb)
	}
}

// BenchmarkScoreIPs_V6Prioritized measures IPv6-prioritized scoring.
func BenchmarkScoreIPs_V6Prioritized(b *testing.B) {
	ips := []net.IP{
		net.ParseIP("192.168.1.1"),
		net.ParseIP("192.168.1.2"),
		net.ParseIP("2001:db8::1"),
		net.ParseIP("2001:db8::2"),
		net.ParseIP("2001:db8::3"),
	}

	var buf []HappyIPScore
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = scoreIPsInto(buf, ips, true, nil)
	}
}

// BenchmarkSortIPScores measures the sort operation on scored IPs.
func BenchmarkSortIPScores(b *testing.B) {
	scores := make([]HappyIPScore, 50)
	for i := range scores {
		scores[i] = HappyIPScore{
			IP:       net.IPv4(byte(10), byte(i/256), byte(i%256), 1),
			Priority: int64(i),
			RTT:      int64(time.Duration(10+i*3) * time.Millisecond),
			FailRate: float64(i%3) / 10.0,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sortIPScores(scores)
	}
}

// BenchmarkHappyIPRecord_RecordSuccess measures EWMA RTT update throughput.
func BenchmarkHappyIPRecord_RecordSuccess(b *testing.B) {
	record := &HappyIPRecord{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record.recordSuccess(time.Duration(50+i%100) * time.Millisecond)
	}
}

// BenchmarkHappyIPRecord_ConcurrentRecordSuccess measures concurrent EWMA updates.
func BenchmarkHappyIPRecord_ConcurrentRecordSuccess(b *testing.B) {
	record := &HappyIPRecord{}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			record.recordSuccess(time.Duration(50+i%100) * time.Millisecond)
			i++
		}
	})
}

// BenchmarkHappyIPDB_Get measures IP record lookup throughput.
func BenchmarkHappyIPDB_Get(b *testing.B) {
	db := &HappyIPDB{records: make(map[ipKey]*HappyIPRecord)}
	// Pre-populate
	for i := 0; i < 100; i++ {
		ip := net.IPv4(10, 0, byte(i/256), byte(i%256))
		db.getByIP(ip).recordSuccess(50 * time.Millisecond)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.getByIP(net.ParseIP("10.0.0.1"))
	}
}

// BenchmarkHappyIPDB_GetParallel measures concurrent IP record lookup.
func BenchmarkHappyIPDB_GetParallel(b *testing.B) {
	db := &HappyIPDB{records: make(map[ipKey]*HappyIPRecord)}
	for i := 0; i < 100; i++ {
		ip := net.IPv4(10, 0, byte(i/256), byte(i%256))
		db.getByIP(ip).recordSuccess(50 * time.Millisecond)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			ip := net.IPv4(10, 0, 0, byte(i%100))
			r := db.getByIP(ip)
			r.recordSuccess(time.Duration(i%200) * time.Millisecond)
			i++
		}
	})
}

// BenchmarkTryController_CanTry measures concurrency control throughput.
func BenchmarkTryController_CanTry(b *testing.B) {
	tc := NewTryController(4, 100*time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tc.CanTry()
	}
}

// BenchmarkTryController_AdaptiveDelay measures adaptive delay adjustment.
func BenchmarkTryController_AdaptiveDelay(b *testing.B) {
	tc := NewTryController(4, 200*time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%3 == 0 {
			tc.OnSuccess(20 * time.Millisecond)
		} else if i%3 == 1 {
			tc.OnFail()
		} else {
			tc.OnSuccess(600 * time.Millisecond)
		}
	}
}

// BenchmarkTryController_ConcurrentCanTry measures concurrent CanTry throughput.
func BenchmarkTryController_ConcurrentCanTry(b *testing.B) {
	tc := NewTryController(8, 100*time.Millisecond)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tc.CanTry()
		}
	})
}

// BenchmarkSortIPs measures the RFC 8305 IP sorting.
func BenchmarkSortIPs(b *testing.B) {
	ips := []net.IP{
		net.ParseIP("192.168.1.1"),
		net.ParseIP("192.168.1.2"),
		net.ParseIP("2001:db8::1"),
		net.ParseIP("2001:db8::2"),
		net.ParseIP("10.0.0.1"),
		net.ParseIP("2001:db8::3"),
	}

	b.ResetTimer()
	var buf []net.IP
	for i := 0; i < b.N; i++ {
		buf = sortIPsInto(buf, ips, false, 1)
	}
}

// BenchmarkSortIPs_LargeList measures sorting with many IPs.
func BenchmarkSortIPs_LargeList(b *testing.B) {
	ips := make([]net.IP, 50)
	for i := range ips {
		if i%2 == 0 {
			ips[i] = net.IPv4(byte(10), byte(i/256), byte(i%256), 1)
		} else {
			ips[i] = net.ParseIP("2001:db8::" + string(rune('0'+i%10)))
		}
	}

	b.ResetTimer()
	var buf []net.IP
	for i := 0; i < b.N; i++ {
		buf = sortIPsInto(buf, ips, false, 1)
	}
}

// BenchmarkClampRTT measures RTT clamping throughput.
func BenchmarkClampRTT(b *testing.B) {
	rtts := []int64{0, 50e6, 100e6, 500e6, 1000e6, 5000e6}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clampRTT(rtts[i%len(rtts)])
	}
}

// BenchmarkHappyIPScore_Score measures score computation throughput.
func BenchmarkHappyIPScore_Score(b *testing.B) {
	s := HappyIPScore{
		IP:       net.ParseIP("1.1.1.1"),
		Priority: 5,
		RTT:      50e6,
		FailRate: 0.05,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.score()
	}
}

// BenchmarkHappyIPScore_ScoreWithHighFailRate measures score with high failure rate.
func BenchmarkHappyIPScore_ScoreWithHighFailRate(b *testing.B) {
	s := HappyIPScore{
		IP:       net.ParseIP("1.1.1.1"),
		Priority: 0,
		RTT:      50e6,
		FailRate: 0.9,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.score()
	}
}

// BenchmarkHappyIPRecord_Cleanup measures IP record cleanup throughput.
func BenchmarkHappyIPRecord_Cleanup(b *testing.B) {
	db := &HappyIPDB{records: make(map[ipKey]*HappyIPRecord)}

	// Pre-populate with old records
	for i := 0; i < 1000; i++ {
		ip := net.IPv4(byte(10), byte(i/65536), byte(i/256), byte(i%256))
		r := db.getByIP(ip)
		r.recordSuccess(50 * time.Millisecond)
		r.lastSeen.Store(time.Now().Unix() - 20*60) // 20 minutes ago
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.cleanup()
	}
}
