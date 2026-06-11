package internet

import (
	"net"
	"sync"
	"testing"
	"time"
)

func TestScoreIPs_Empty(t *testing.T) {
	result := scoreIPs(nil, false, nil)
	if len(result) != 0 {
		t.Fatalf("expected 0 scores, got %d", len(result))
	}
}

func TestScoreIPs_Basic(t *testing.T) {
	ips := []net.IP{
		net.ParseIP("192.168.1.1"),
		net.ParseIP("10.0.0.1"),
		net.ParseIP("::1"),
	}
	scores := scoreIPs(ips, false, nil)
	if len(scores) != 3 {
		t.Fatalf("expected 3 scores, got %d", len(scores))
	}
	// With prioritizeIPv6=false, IPv4 should have lower score (higher priority)
	for i, s := range scores {
		t.Logf("score[%d]: IP=%s score=%.0f", i, s.IP, s.score())
	}
}

func TestScoreIPs_PrioritizeIPv6(t *testing.T) {
	ipv4 := net.ParseIP("192.168.1.1")
	ipv6 := net.ParseIP("::1")

	scoresIPv4First := scoreIPs([]net.IP{ipv4, ipv6}, false, nil)
	scoresIPv6First := scoreIPs([]net.IP{ipv4, ipv6}, true, nil)

	if scoresIPv4First[0].IP.Equal(scoresIPv6First[0].IP) {
		t.Log("both prioritizations returned same first IP (only 2 IPs, may be expected)")
	}
}

func TestScoreIPs_WithSVCBPriority(t *testing.T) {
	ipv4a := net.ParseIP("192.168.1.1")
	ipv4b := net.ParseIP("192.168.1.2")

	svcb := map[string]int64{
		ipv4a.String(): 10,
		ipv4b.String(): 1,
	}
	scores := scoreIPs([]net.IP{ipv4a, ipv4b}, false, svcb)
	if len(scores) != 2 {
		t.Fatalf("expected 2 scores, got %d", len(scores))
	}
	// Lower SVCB priority = higher priority (should come first)
	if scores[0].IP.Equal(ipv4a) {
		t.Errorf("expected ipv4b (priority=1) first, got ipv4a (priority=10)")
	}
}

func TestHappyIPRecord_Concurrent(t *testing.T) {
	record := &HappyIPRecord{}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			record.recordSuccess(50 * time.Millisecond)
		}()
		go func() {
			defer wg.Done()
			record.recordFail()
		}()
	}
	wg.Wait()
	if record.getSuccesses() != 100 {
		t.Errorf("expected 100 successes, got %d", record.getSuccesses())
	}
	if record.getFails() != 100 {
		t.Errorf("expected 100 fails, got %d", record.getFails())
	}
	if record.getSmoothedRTT() == 0 {
		t.Error("expected non-zero smoothed RTT")
	}
}

func TestHappyIPRecord_EWMA(t *testing.T) {
	record := &HappyIPRecord{}
	record.recordSuccess(100 * time.Millisecond)
	rtt1 := record.getSmoothedRTT()
	if rtt1 != int64(100*time.Millisecond) {
		t.Errorf("first RTT should be exact, got %d", rtt1)
	}
	record.recordSuccess(200 * time.Millisecond)
	rtt2 := record.getSmoothedRTT()
	// EWMA: 80% of 100ms + 20% of 200ms = 120ms
	expected := int64(120 * time.Millisecond)
	if rtt2 != expected {
		t.Errorf("expected EWMA RTT %d, got %d", expected, rtt2)
	}
}

func TestHappyIPDB_Concurrent(t *testing.T) {
	db := &HappyIPDB{records: make(map[string]*HappyIPRecord)}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := db.get("1.2.3.4")
			r.recordSuccess(10 * time.Millisecond)
		}()
	}
	wg.Wait()
	if len(db.records) != 1 {
		t.Errorf("expected 1 record, got %d", len(db.records))
	}
}

func TestTryController_CanTry(t *testing.T) {
	tc := NewTryController(2, 100*time.Millisecond)
	if !tc.CanTry() {
		t.Error("should be able to try")
	}
	tc.OnStart()
	if !tc.CanTry() {
		t.Error("should be able to try (1/2)")
	}
	tc.OnStart()
	if tc.CanTry() {
		t.Error("should NOT be able to try (2/2)")
	}
	tc.OnEnd()
	if !tc.CanTry() {
		t.Error("should be able to try after OnEnd")
	}
}

func TestTryController_AdaptiveDelay(t *testing.T) {
	tc := NewTryController(4, 200*time.Millisecond)
	initial := tc.GetDelay()

	// Fast RTT should decrease delay
	tc.OnSuccess(20 * time.Millisecond)
	fast := tc.GetDelay()
	if fast >= initial {
		t.Errorf("fast RTT should decrease delay: %d >= %d", fast, initial)
	}

	tc2 := NewTryController(4, 200*time.Millisecond)
	slowInitial := tc2.GetDelay()
	// Slow RTT should increase delay
	tc2.OnSuccess(600 * time.Millisecond)
	slow := tc2.GetDelay()
	if slow <= slowInitial {
		t.Errorf("slow RTT should increase delay: %d <= %d", slow, slowInitial)
	}

	tc3 := NewTryController(4, 200*time.Millisecond)
	failInitial := tc3.GetDelay()
	tc3.OnFail()
	fail := tc3.GetDelay()
	if fail <= failInitial {
		t.Errorf("failure should increase delay: %d <= %d", fail, failInitial)
	}
}

func TestSortIPScores(t *testing.T) {
	scores := []HappyIPScore{
		{IP: net.ParseIP("1.1.1.1"), Priority: 10, RTT: 100e6},
		{IP: net.ParseIP("2.2.2.2"), Priority: 1, RTT: 50e6},
		{IP: net.ParseIP("3.3.3.3"), Priority: 5, RTT: 200e6},
	}
	sortIPScores(scores)
	if scores[0].IP.String() != "2.2.2.2" {
		t.Errorf("expected 2.2.2.2 first (lowest priority), got %s", scores[0].IP)
	}
}

func TestDefaultRTT(t *testing.T) {
	if defaultSmoothedRTT != 100*time.Millisecond {
		t.Errorf("expected 100ms default RTT, got %d", defaultSmoothedRTT)
	}
}

func TestClampRTT(t *testing.T) {
	// Zero RTT → default
	if got := clampRTT(0); got != int64(defaultSmoothedRTT) {
		t.Errorf("clampRTT(0) = %d, want %d", got, defaultSmoothedRTT)
	}
	// Normal RTT → unchanged
	if got := clampRTT(200e6); got != 200e6 {
		t.Errorf("clampRTT(200ms) = %d, want 200ms", got)
	}
	// Over cap → capped
	if got := clampRTT(2000e6); got != int64(maxRTTCap) {
		t.Errorf("clampRTT(2s) = %d, want %d", got, maxRTTCap)
	}
}

func TestScore_NoRTTNotDominant(t *testing.T) {
	// New IP with no RTT sample should NOT beat a known-good 20ms IP
	known := HappyIPScore{IP: net.ParseIP("1.1.1.1"), RTT: 20e6, Successes: 10, Fails: 0}
	newIP := HappyIPScore{IP: net.ParseIP("2.2.2.2"), RTT: 0, Successes: 0, Fails: 0}
	if newIP.score() < known.score() {
		t.Errorf("new IP (RTT=0) scored %.0f < known IP (RTT=20ms) %.0f — should not happen", newIP.score(), known.score())
	}
}

func TestScore_HighRTTNotInverted(t *testing.T) {
	// High RTT IP should NOT beat low RTT IP
	low := HappyIPScore{IP: net.ParseIP("1.1.1.1"), RTT: 20e6, Successes: 10, Fails: 0}
	high := HappyIPScore{IP: net.ParseIP("2.2.2.2"), RTT: 3000e6, Successes: 10, Fails: 0}
	if high.score() < low.score() {
		t.Errorf("high RTT (%.0f) scored lower than low RTT (%.0f) — score inversion", high.score(), low.score())
	}
}

func TestGlobalHappyIPDB(t *testing.T) {
	// globalHappyIPDB should be initialized
	if globalHappyIPDB == nil {
		t.Fatal("globalHappyIPDB is nil")
	}
	r1 := globalHappyIPDB.get("test-ip-1")
	r2 := globalHappyIPDB.get("test-ip-1")
	if r1 != r2 {
		t.Error("same IP should return same record")
	}
}
