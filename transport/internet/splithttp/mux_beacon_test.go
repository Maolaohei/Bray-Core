package splithttp

import (
	"testing"
)

// TestNoteBeaconFailureEvictsAfterTwo covers the CDN silent-idle-kill path:
// a beacon failure is the only signal that the flow is gone (no RST reaches
// the client), so consecutive failures must rotate the pooled client off
// before business traffic hits it.
func TestNoteBeaconFailureEvictsAfterTwo(t *testing.T) {
	c := &XmuxClient{ready: make(chan struct{})}
	close(c.ready)
	c.state.Store(StateActive)

	// First failure: tolerated (transient jitter must not evict).
	c.NoteBeaconFailure()
	if c.state.Load() != StateActive {
		t.Fatal("single beacon failure must not mark the client dead")
	}

	// Second consecutive failure: path presumed gone -> MarkDead.
	c.NoteBeaconFailure()
	if c.state.Load() != StateClosed {
		t.Fatal("two consecutive beacon failures should MarkDead the client")
	}

	// Further failures on a dead client are no-ops (no panic, no counter churn).
	c.NoteBeaconFailure()
}

// TestNoteBeaconSuccessResetsCounter ensures transient failures followed by
// a healthy beacon keep the connection in the pool.
func TestNoteBeaconSuccessResetsCounter(t *testing.T) {
	c := &XmuxClient{ready: make(chan struct{})}
	close(c.ready)
	c.state.Store(StateActive)

	c.NoteBeaconFailure()
	c.NoteBeaconSuccess()
	if c.beaconFailures.Load() != 0 {
		t.Fatal("success must reset the consecutive-failure counter")
	}
	c.NoteBeaconFailure()
	if c.state.Load() != StateActive {
		t.Fatal("isolated failure after success must not evict")
	}
}

// TestNoteBeaconNilSafe guards the nil-receiver convenience used by callers
// that may race pool teardown.
func TestNoteBeaconNilSafe(t *testing.T) {
	var c *XmuxClient
	c.NoteBeaconFailure() // must not panic
	c.NoteBeaconSuccess() // must not panic
}
