package quality

import (
	"testing"
	"time"
)

func TestCalcWarmupDelay(t *testing.T) {
	tests := []struct {
		name     string
		rtt      time.Duration
		loss     float64
		minDelay time.Duration
		maxDelay time.Duration
	}{
		{"baseline 20ms no loss", 20 * time.Millisecond, 0, 50 * time.Millisecond, 70 * time.Millisecond},
		{"20ms 5% loss", 20 * time.Millisecond, 0.05, 70 * time.Millisecond, 80 * time.Millisecond},
		{"20ms 10% loss", 20 * time.Millisecond, 0.10, 85 * time.Millisecond, 95 * time.Millisecond},
		{"90ms no loss (Japan)", 90 * time.Millisecond, 0, 250 * time.Millisecond, 290 * time.Millisecond},
		{"15ms no loss (LotSpeed)", 15 * time.Millisecond, 0, 40 * time.Millisecond, 55 * time.Millisecond},
		{"unknown RTT", 0, 0, 280 * time.Millisecond, 320 * time.Millisecond},
		{"negative RTT", -1 * time.Millisecond, 0, 280 * time.Millisecond, 320 * time.Millisecond},
		{"extreme loss 50%", 20 * time.Millisecond, 0.50, 190 * time.Millisecond, 210 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delay := CalcWarmupDelay(tt.rtt, tt.loss)
			if delay < tt.minDelay || delay > tt.maxDelay {
				t.Errorf("CalcWarmupDelay(%v, %f) = %v, want [%v, %v]",
					tt.rtt, tt.loss, delay, tt.minDelay, tt.maxDelay)
			}
		})
	}
}

func TestCalcWarmupDelayClamp(t *testing.T) {
	// Very high RTT should clamp to 2s max
	delay := CalcWarmupDelay(5*time.Second, 0)
	if delay != 2*time.Second {
		t.Errorf("extreme RTT: got %v, want 2s", delay)
	}

	// Very low RTT should clamp to 20ms min
	delay = CalcWarmupDelay(time.Millisecond, 0)
	if delay != 20*time.Millisecond {
		t.Errorf("extreme low RTT: got %v, want 20ms", delay)
	}
}
