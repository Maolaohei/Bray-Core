package splithttp

import (
	"errors"
	"testing"
	"time"
)

func TestIsProbeDialDead(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errors.New("context canceled"), false},
		{errors.New("dial tcp 127.0.0.1:1: connectex: No connection could be made"), true},
		{errors.New("connection refused"), true},
		{errors.New("i/o timeout"), true},
		{errors.New("stream canceled"), false},
	}
	for _, tc := range cases {
		if got := isProbeDialDead(tc.err); got != tc.want {
			t.Errorf("isProbeDialDead(%v)=%v want %v", tc.err, got, tc.want)
		}
	}
}

func TestProbeCooldownEngagesAfterStreak(t *testing.T) {
	m := NewXmuxManager(&XmuxConfig{}, func() XmuxConn {
		return &fakeProbeConn{}
	})
	defer m.Close()

	// First two dial-dead notes should not yet cool.
	_ = m.noteProbeFailure()
	if m.probeInCooldown() {
		t.Fatal("cooldown should not engage after 1 fail")
	}
	_ = m.noteProbeFailure()
	if m.probeInCooldown() {
		t.Fatal("cooldown should not engage after 2 fails")
	}
	_ = m.noteProbeFailure()
	if !m.probeInCooldown() {
		t.Fatal("cooldown should engage after 3 dial-dead fails")
	}

	// Success clears cooldown.
	m.noteProbeSuccess()
	if m.probeInCooldown() {
		t.Fatal("success should clear cooldown")
	}
}

func TestProbeCooldownLogRateLimit(t *testing.T) {
	m := NewXmuxManager(&XmuxConfig{}, func() XmuxConn {
		return &fakeProbeConn{}
	})
	defer m.Close()

	// Burst of failures: only first should log within the window.
	logs := 0
	for i := 0; i < 10; i++ {
		if m.noteProbeFailure() {
			logs++
		}
	}
	if logs != 1 {
		t.Fatalf("expected 1 log in rate window, got %d", logs)
	}
}

func TestProbeCooldownExpires(t *testing.T) {
	m := NewXmuxManager(&XmuxConfig{}, func() XmuxConn {
		return &fakeProbeConn{}
	})
	defer m.Close()

	m.probeFailMu.Lock()
	m.probeFailStreak = 3
	m.probeCoolUntil = time.Now().Add(-time.Millisecond)
	m.probeFailMu.Unlock()
	if m.probeInCooldown() {
		t.Fatal("expired coolUntil should not block probes")
	}
}

type fakeProbeConn struct{}

func (f *fakeProbeConn) IsClosed() bool { return false }
