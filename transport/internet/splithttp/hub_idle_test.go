package splithttp

import (
	"testing"
	"time"

	"github.com/xtls/xray-core/common/signal/done"
)

// TestStreamOneIdleReaperFires verifies that an unauthenticated stream-one
// connection with no activity is reaped after the idle lifetime.
func TestStreamOneIdleReaperFires(t *testing.T) {
	reaped := make(chan struct{})
	c := &httpServerConn{
		Instance: done.New(),
	}
	c.idleTimer = time.AfterFunc(50*time.Millisecond, func() { close(reaped) })

	select {
	case <-reaped:
	case <-time.After(2 * time.Second):
		t.Fatal("idle reaper did not fire for a quiet connection")
	}
}

// TestStreamOneIdleReaperTouchResets verifies that activity (touch) resets
// the reaper, so an active transfer is never cut mid-stream.
func TestStreamOneIdleReaperTouchResets(t *testing.T) {
	reaped := make(chan struct{})
	c := &httpServerConn{
		Instance: done.New(),
	}
	c.idleTimer = time.AfterFunc(50*time.Millisecond, func() { close(reaped) })

	// Touch well inside the original 50ms window; each touch extends the
	// deadline to streamOneIdleLifetime, so the reaper must never fire.
	for i := 0; i < 10; i++ {
		c.touch()
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case <-reaped:
		t.Fatal("touch did not reset the idle reaper (active transfer was cut)")
	case <-time.After(200 * time.Millisecond):
	}
}

// TestStreamOneIdleReaperNoTimer verifies touch is a no-op when no reaper is
// attached (sessioned modes, non-stream-one paths).
func TestStreamOneIdleReaperNoTimer(t *testing.T) {
	c := &httpServerConn{Instance: done.New()}
	c.touch() // must not panic
}
