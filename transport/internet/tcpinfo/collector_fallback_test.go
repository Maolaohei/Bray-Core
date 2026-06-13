//go:build !linux

package tcpinfo

import (
	"testing"
	"time"

	"github.com/xtls/xray-core/transport/internet/quality"
)

func TestFallbackCollector_FeedRTT(t *testing.T) {
	c := &fallbackCollector{}

	// No data yet
	snap, err := c.Collect(nil)
	if err != nil {
		t.Fatal(err)
	}
	if snap.RTT.Valid {
		t.Error("RTT should be unknown before any FeedRTT")
	}
	if snap.Confidence != 10 {
		t.Errorf("confidence before any data: expected 10, got %d", snap.Confidence)
	}

	// Feed some RTT samples
	for i := 0; i < 10; i++ {
		c.FeedRTT(50 * time.Millisecond)
	}

	snap, err = c.Collect(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !snap.RTT.Valid {
		t.Error("RTT should be valid after FeedRTT")
	}
	t.Logf("RTT after 10 samples: %v (confidence=%d)", snap.RTT.Value, snap.Confidence)

	if snap.RTT.Value < 40*time.Millisecond || snap.RTT.Value > 60*time.Millisecond {
		t.Errorf("RTT should be ~50ms, got %v", snap.RTT.Value)
	}
}

func TestFallbackCollector_StableRTT(t *testing.T) {
	c := &fallbackCollector{}

	// Stable RTT: 30ms
	for i := 0; i < 20; i++ {
		c.FeedRTT(30 * time.Millisecond)
	}

	snap, _ := c.Collect(nil)

	if !snap.RTT.Valid {
		t.Fatal("RTT should be valid")
	}
	if snap.Loss.Valid && snap.Loss.Value > 0 {
		t.Errorf("stable RTT should have 0 loss, got %f", snap.Loss.Value)
	}
	if snap.Quality.Latency < 80 {
		t.Errorf("30ms RTT should have high latency score, got %d", snap.Quality.Latency)
	}
	t.Logf("Stable 30ms: RTT=%v Loss=%v Quality=%+v Confidence=%d",
		snap.RTT.Value, snap.Loss.Value, snap.Quality, snap.Confidence)
}

func TestFallbackCollector_VolatileRTT(t *testing.T) {
	c := &fallbackCollector{}

	// Volatile RTT: alternating 30ms and 100ms
	for i := 0; i < 20; i++ {
		if i%2 == 0 {
			c.FeedRTT(30 * time.Millisecond)
		} else {
			c.FeedRTT(100 * time.Millisecond)
		}
	}

	snap, _ := c.Collect(nil)

	if !snap.RTT.Valid {
		t.Fatal("RTT should be valid")
	}
	if !snap.Loss.Valid || snap.Loss.Value == 0 {
		t.Error("volatile RTT should estimate some loss")
	}
	if snap.Quality.Stability > 50 {
		t.Errorf("volatile RTT should have low stability, got %d", snap.Quality.Stability)
	}
	t.Logf("Volatile 30-100ms: RTT=%v Loss=%v Stability=%d Confidence=%d",
		snap.RTT.Value, snap.Loss.Value, snap.Quality.Stability, snap.Confidence)
}

func TestFallbackCollector_ScoreClientIntegration(t *testing.T) {
	c := &fallbackCollector{}

	// Stable connection: 20ms RTT
	for i := 0; i < 30; i++ {
		c.FeedRTT(20 * time.Millisecond)
	}

	snap, _ := c.Collect(nil)

	// Simulate what UpdateQuality would do
	q := int32(snap.Quality.Overall)
	conf := int32(snap.Confidence)
	var retrans int32 // not available on Windows, 0
	var lossRate int64
	if snap.Loss.Valid {
		lossRate = int64(snap.Loss.Value * 10000)
	}

	t.Logf("Stable connection: quality=%d confidence=%d retrans=%d lossRate=%d",
		q, conf, retrans, lossRate)

	if conf < 50 {
		t.Errorf("30 samples should give confidence >= 50, got %d", conf)
	}
	if lossRate != 0 {
		t.Errorf("stable RTT should have 0 loss rate, got %d", lossRate)
	}
}

func TestFallbackCollector_Source(t *testing.T) {
	c := &fallbackCollector{}
	if c.Source() != quality.SourceEstimated {
		t.Errorf("source should be 'estimated', got %q", c.Source())
	}
}
