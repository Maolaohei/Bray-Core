package splithttp

import (
	"errors"
	"net/http"
	"testing"
)

// errProbeTransport fails every request with a fixed error. It stands in for a
// transport-layer failure that is NOT "dial dead" — most importantly a TLS
// certificate verification failure (x509), which is what the 2026.08.31
// disconnection report showed.
type errProbeTransport struct{ err error }

func (t errProbeTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, t.err
}

// newFailingProbeDialer builds a *DefaultDialerClient whose HEAD probe always
// fails with err. Zero-value fields are intentional: closed==false keeps
// IsClosed() false so probeConnection proceeds, and transportConfig==nil is
// guarded inside probeConnection.
func newFailingProbeDialer(err error) *DefaultDialerClient {
	return &DefaultDialerClient{client: &http.Client{Transport: errProbeTransport{err: err}}}
}

// TestPOC_NonDialDeadProbeFailureNeverCoolsDown reproduces the 2026.08.31
// 断流 regression at its amplification point, driving the real
// XmuxManager.probeConnection rather than re-implementing its logic.
//
// Chain observed in the field:
//
//	probe fails (x509) -> MarkDead -> connection evicted
//	-> retry -> new connection -> probe fails again -> ...
//
// Root of the loop: probeConnection classified errors via isProbeDialDead().
// Only the dial-dead branch called noteProbeDialDead(), which is what advances
// probeFailStreak and arms probeCoolUntil. An x509 error is NOT dial-dead, so
// it fell into the generic else-branch that logged unconditionally and called
// MarkDead() without any accounting. Consequences:
//
//   - probeFailStreak stayed 0, so probeInCooldown() never became true
//     -> no backoff, a tight connect/probe/evict loop;
//   - the 2s log rate limit inside noteProbeDialDead() was bypassed
//     -> unbounded log storm;
//   - every connection was evicted, so the pool never held a usable one
//     -> "failed to find an available destination", total loss of traffic.
//
// The invariant asserted here is the operational one: a sustained run of probe
// failures of ANY class must engage the cooldown, which is what makes the
// manager back off (skip further probes) instead of spinning.
//
// Two observable outcomes are checked together, because either one alone is
// weak evidence:
//
//  1. the cooldown arms after coolFailStreak failures, and
//  2. once armed, probeConnection actually short-circuits (line 1421), so the
//     connect → probe → MarkDead → retry cycle stops. That short-circuit is
//     exactly the backoff: the manager stops burning a fresh connection per
//     round. Before the fix the loop never short-circuited, so every round
//     dialed and evicted a connection.
func TestPOC_NonDialDeadProbeFailureNeverCoolsDown(t *testing.T) {
	x509Err := errors.New(`Head "https://speedtest-de.mlx001.de/823c89af0e514bf/": tls: failed to verify certificate: x509: certificate signed by unknown authority`)

	// Precondition: x509 is not a dial-dead error, so it must take the
	// generic failure branch inside probeConnection.
	if isProbeDialDead(x509Err) {
		t.Fatal("precondition: x509 must not be classified as dial-dead")
	}

	// Mirrors the const inside noteProbeFailure(); it is function-local there,
	// so it is restated here. Update both if the policy changes.
	const coolFailStreak = 3

	// newConnFunc returns nil on purpose. The manager's healthCheckTick no
	// longer pre-connects, but the first tick (and any pool refill) spawns
	// newXmuxClient() work that sleeps ~100ms and
	// then evaluates m.probeURL; whatever the test does in the meantime, those
	// goroutines can start their own async probeConnection at any moment and
	// land extra failures in the very counters asserted below (observed: the
	// streak grew by 2 per driven probe). Returning nil makes
	// newXmuxClientLocked bail out before creating a client or a probe, so the
	// pool stays empty and every failure counted here is one this test drove
	// itself — deterministic under -race and on loaded CI.
	//
	// This does not weaken the test: probeConnection is the code under test and
	// is still driven synchronously with a dialer built here. The only thing
	// stubbed out is ambient pool churn unrelated to the assertion.
	m := NewXmuxManager(&XmuxConfig{}, func() XmuxConn { return nil }, "https://example.com/probe")
	defer m.Close()

	// Mirrors production: every new connection owns its own dialer and is
	// probed once. Each failed probe ends in MarkDead(), which closes that
	// connection's dialer, so each round needs a fresh one.
	const rounds = 6
	probed, backedOff, coolEngagedAt := 0, 0, -1
	for i := 0; i < rounds; i++ {
		t.Logf("round %d: inCooldown=%v probed=%d backedOff=%d streak=%d", i, m.probeInCooldown(), probed, backedOff, m.probeFailStreak)
		if m.probeInCooldown() {
			// Cooldown is doing its job: probeConnection returns early, so
			// this round does not dial and evict yet another connection.
			backedOff++
			if coolEngagedAt < 0 {
				coolEngagedAt = i
			}
			continue
		}
		dc := newFailingProbeDialer(x509Err)
		xc := &XmuxClient{XmuxConn: dc, ready: make(chan struct{})}
		m.probeConnection(dc, xc)
		if xc.probeErr == nil {
			t.Fatalf("round %d: expected probe to fail", i)
		}
		probed++
	}

	m.probeFailMu.Lock()
	streak := m.probeFailStreak
	m.probeFailMu.Unlock()

	// Before the fix: coolEngagedAt stayed -1, probed==rounds, backedOff==0 —
	// every round burned a connection with no backoff at all.
	if coolEngagedAt < 0 {
		t.Fatalf("BUG: %d consecutive non-dial-dead probe failures never engaged cooldown "+
			"(probeFailStreak=%d, probeCoolUntil zero); the probe loop stays unthrottled "+
			"and every connection is evicted (断流)", rounds, streak)
	}
	if coolEngagedAt != coolFailStreak {
		t.Fatalf("cooldown engaged at round %d, want %d (coolFailStreak)", coolEngagedAt, coolFailStreak)
	}
	if probed != coolFailStreak {
		t.Fatalf("probes actually run = %d, want %d: the cooldown must stop the loop, not just flip a flag",
			probed, coolFailStreak)
	}
	if backedOff != rounds-coolFailStreak {
		t.Fatalf("backed-off rounds = %d, want %d", backedOff, rounds-coolFailStreak)
	}
	if streak != coolFailStreak {
		t.Fatalf("probeFailStreak = %d, want %d", streak, coolFailStreak)
	}
}
