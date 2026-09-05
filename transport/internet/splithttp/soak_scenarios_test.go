//go:build soak

package splithttp_test

// Soak scenarios (expert blueprint §2 personas, §3 oracles):
//   - TestSoak_ChatIdle: chat persona — mostly idle, small messages after
//     idle gaps; the direct analogue of "挂一晚上第二天发不出消息". Asserts
//     every post-idle first-message latency AND per-fault recovery liveness.
//   - TestSoak_MobileUser: phase-switching persona (web burst → video →
//     blackhole) with RST/stall/truncate/dialfail pounding; asserts integrity,
//     recovery SLA, and the resource/leak oracle.
//   - TestSoak_ShapeStatistics: behavior-shape oracle over the wire — burst
//     size variance (no fixed body size) and inter-burst gap autocorrelation
//     (no fixed cadence) must both stay in bounds.
//
// Runtime ≈ 2.5 min for the trio. Reproduce any failure with
// BRAY_SOAK_SEED=<printed value> go test -tags soak -run <name>.

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"runtime"
	"strconv"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
)

// soakDial dials the product THROUGH the fault proxy.
func soakDial(t *testing.T, proxy *faultProxy, settings *internet.MemoryStreamConfig) (net.Conn, error) {
	t.Helper()
	host, portStr, _ := net.SplitHostPort(proxy.addr())
	port, _ := strconv.Atoi(portStr)
	dest := xnet.TCPDestination(xnet.ParseAddress(host), xnet.Port(port))
	return Dial(context.Background(), dest, settings)
}

// runIntegritySession: one product conn; writer executes schedule(); a
// reader goroutine verifies `frames` frames and stamps arrivals. Returns the
// reader error and the liveness tracker.
func runIntegritySession(t *testing.T, proxy *faultProxy, settings *internet.MemoryStreamConfig, frames int, schedule func(w *integrityWriter) error) (*livenessTracker, error) {
	t.Helper()
	conn, err := soakDial(t, proxy, settings)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	w := newIntegrityWriter(conn, 256*1024)
	r := newIntegrityReader(conn, 256*1024)
	live := &livenessTracker{}

	readErr := make(chan error, 1)
	go func() {
		for i := 0; i < frames; i++ {
			n, err := r.readFrame()
			if err != nil {
				readErr <- fmt.Errorf("frame %d: %w", i, err)
				return
			}
			live.stamp(n)
		}
		readErr <- nil
	}()

	if err := schedule(w); err != nil {
		return live, fmt.Errorf("writer: %w", err)
	}
	if err := <-readErr; err != nil {
		return live, err
	}
	return live, nil
}

// assertRecoverySLA: every fault event must be followed by data flowing
// again within the SLA — the product-level "no progress but no error" trap.
func assertRecoverySLA(t *testing.T, live *livenessTracker, proxy *faultProxy, sla time.Duration) {
	t.Helper()
	counts := map[string]int{}
	lastRead := live.lastStampTime()
	var worst time.Duration
	var worstKind string
	for _, ev := range proxy.eventsSnapshot() {
		counts[ev.Kind]++
		switch ev.Kind {
		case "rst", "stall", "blackhole", "truncate":
			// Events after the last data arrival have no in-flight data to
			// recover (the session finished before the fault landed) — skip.
			if !lastRead.IsZero() && proxy.start.Add(ev.At).After(lastRead) {
				counts["post-completion"]++
				continue
			}
			end := proxy.start.Add(ev.At + ev.Dur)
			rec := live.recoveryFor(end)
			if rec < 0 {
				t.Errorf("liveness: no data ever flowed after %s event at %v (session died silently)", ev.Kind, ev.At)
				continue
			}
			if rec > worst {
				worst, worstKind = rec, ev.Kind
			}
		}
	}
	if worst > sla {
		t.Errorf("liveness: worst recovery %v (%s) > SLA %v", worst, worstKind, sla)
	}
	t.Logf("recovery oracle: events=%v worst=%v (%s) SLA=%v", counts, worst, worstKind, sla)
}

// TestSoak_ChatIdle: 16 chat cycles — small message, then idle 1-3s. Faults
// recycle connections mid-idle (RST lifetime), stall mid-write, blackhole
// once, half-close and dial-fail sprinkled in. Every post-idle first-message
// latency must stay under the SLA; integrity must hold across all of it.
func TestSoak_ChatIdle(t *testing.T) {
	seed := soakSeed()
	t.Logf("SEED=%d", seed)
	bindIP := testBindIP(t)
	ln, hubAddr := soakServer(t, bindIP, "soak-chat")
	defer ln.Close()

	plan := FaultPlan{
		GracePeriod:     3 * time.Second,
		RTTMean:         30 * time.Millisecond,
		RTTJitter:       10 * time.Millisecond,
		RSTLifetime:     [2]time.Duration{4 * time.Second, 9 * time.Second},
		Stall:           [2]time.Duration{1 * time.Second, 2500 * time.Millisecond},
		StallChance:     0.5,
		Blackhole:       []Blackhole{{Start: 20 * time.Second, End: 22 * time.Second}},
		TruncateChance:  0.2,
		HalfCloseChance: 0.25,
		DialFailRate:    0.2,
	}
	proxy := newFaultProxy(bindIP, hubAddr, plan, seed)
	defer proxy.close()

	const cycles = 16
	const framesPerCycle = 2
	rng := rand.New(rand.NewSource(seed + 1))
	type cycleStamp struct {
		start time.Time
	}
	cycleStarts := make([]cycleStamp, 0, cycles)

	base := runtime.NumGoroutine()
	live, err := runIntegritySession(t, proxy, soakSettings("soak-chat"), cycles*framesPerCycle, func(w *integrityWriter) error {
		for c := 0; c < cycles; c++ {
			cycleStarts = append(cycleStarts, cycleStamp{time.Now()})
			for k := 0; k < framesPerCycle; k++ {
				size := 200 + rng.Intn(800)
				if err := w.writeFrame(size); err != nil {
					return err
				}
			}
			idle := time.Duration(1000+rng.Intn(2000)) * time.Millisecond
			time.Sleep(idle)
		}
		return nil
	})
	if err != nil {
		for _, ev := range proxy.eventsSnapshot() {
			t.Logf("FAULT-TIMELINE: %-9s conn=%d at=%v dur=%v", ev.Kind, ev.Conn, ev.At.Round(time.Millisecond), ev.Dur.Round(time.Millisecond))
		}
		t.Fatalf("chat session failed: %v", err)
	}

	// Post-idle first-message latency: first arrival after each cycle START
	// that came after the PREVIOUS cycle's data — approximated by first stamp
	// after cycle start minus cycle start. Idle hygiene SLA 10s (catches the
	// "hung all night" class); normal cycles land ≈ RTT + pacing.
	sla := 10 * time.Second
	var worst time.Duration
	live.mu.Lock()
	stamps := append([]readStamp(nil), live.stamps...)
	live.mu.Unlock()
	for _, cs := range cycleStarts {
		for _, s := range stamps {
			if s.at.After(cs.start) {
				if d := s.at.Sub(cs.start); d > worst {
					worst = d
				}
				break
			}
		}
	}
	if worst > sla {
		t.Errorf("chat first-message latency: worst %v > SLA %v (idle kills the session?)", worst, sla)
	}
	assertRecoverySLA(t, live, proxy, 15*time.Second)
	time.Sleep(2 * time.Second) // settle: let server pools finish reaping before sampling
	final := runtime.NumGoroutine()
	assertLeakGuard(t, base, final, 30)
	t.Logf("chat soak: %d cycles ok, worst first-msg %v, goroutines %d (base %d)", cycles, worst, final, base)
}

// TestSoak_MobileUser: 3 rounds of [web burst → video steady] with a
// blackhole (elevator/Wi-Fi switch) per round and the full fault mix. The
// session must hold integrity and recover within SLA; resources must stay
// bounded.
func TestSoak_MobileUser(t *testing.T) {
	seed := soakSeed()
	t.Logf("SEED=%d", seed)
	bindIP := testBindIP(t)
	ln, hubAddr := soakServer(t, bindIP, "soak-mobile")
	defer ln.Close()

	plan := FaultPlan{
		GracePeriod:     3 * time.Second,
		RTTMean:         15 * time.Millisecond,
		RTTJitter:       5 * time.Millisecond,
		RSTLifetime:     [2]time.Duration{6 * time.Second, 12 * time.Second},
		Stall:           [2]time.Duration{1 * time.Second, 2 * time.Second},
		StallChance:     0.6,
		TruncateChance:  0.5,
		HalfCloseChance: 0.3,
		DialFailRate:    0.25,
	}
	// Pre-planned elevator windows (proxy-relative clock): they land mid-run
	// while the writer KEEPS WRITING — writes block on the dead path (real
	// backpressure) and must resume flowing after the window ends. Windows
	// stay <3.4s, under the product's seq-gap teardown budget (maxSeqGapWait
	// 5s), so the session is expected to survive by design; the SLA asserts
	// how fast it resumes.
	plan.Blackhole = []Blackhole{
		{Start: 10 * time.Second, End: 13 * time.Second},
		{Start: 17 * time.Second, End: 20 * time.Second},
		{Start: 24 * time.Second, End: 27 * time.Second},
	}
	proxy := newFaultProxy(bindIP, hubAddr, plan, seed)
	defer proxy.close()

	const rounds = 5
	const frames = rounds * (20 + 8) // web(20)+video(8) per round
	base := runtime.NumGoroutine()
	sampler := startResourceSampler(2 * time.Second)
	doneReading := make(chan struct{}, 1)
	go func() {
		t0 := time.Now()
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-doneReading:
				return
			case <-tick.C:
				t.Logf("progress: %v elapsed, proxy events=%d, active conns=%d",
					time.Since(t0).Round(time.Second), len(proxy.eventsSnapshot()), proxy.active.Load())
			}
		}
	}()

	live, err := runIntegritySession(t, proxy, soakSettings("soak-mobile"), frames, func(w *integrityWriter) error {
		rng := rand.New(rand.NewSource(seed + 2))
		for round := 0; round < rounds; round++ {
			// Web burst: 20 small frames, brisk. The writer does NOT pause
			// for blackholes — in-flight data piling against a dead path is
			// exactly the "no error, no progress" trap under test.
			for i := 0; i < 20; i++ {
				if err := w.writeFrame(4*1024 + rng.Intn(28*1024)); err != nil {
					return err
				}
				time.Sleep(30 * time.Millisecond)
			}
			// Video: 8 steady 64-128KB frames.
			for i := 0; i < 8; i++ {
				if err := w.writeFrame(64*1024 + rng.Intn(64*1024)); err != nil {
					return err
				}
				time.Sleep(150 * time.Millisecond)
			}
			time.Sleep(2 * time.Second) // user reading between phases
		}
		return nil
	})
	doneReading <- struct{}{}
	samples := sampler.stopAndCollect()
	if err != nil {
		for _, ev := range proxy.eventsSnapshot() {
			t.Logf("FAULT-TIMELINE: %-9s conn=%d at=%v dur=%v", ev.Kind, ev.Conn, ev.At.Round(time.Millisecond), ev.Dur.Round(time.Millisecond))
		}
		t.Fatalf("mobile session failed: %v", err)
	}

	assertRecoverySLA(t, live, proxy, 15*time.Second)
	// Resource oracle: goroutines during the run must stay bounded, and the
	// leak guard must hold after settle.
	maxGoros := 0
	for _, s := range samples {
		if s.goros > maxGoros {
			maxGoros = s.goros
		}
	}
	time.Sleep(2 * time.Second) // settle: let server pools finish reaping before sampling
	final := runtime.NumGoroutine()
	assertLeakGuard(t, base, final, 30)
	if maxGoros > base+120 {
		t.Errorf("resource bound: peak goroutines %d > base %d + 120", maxGoros, base)
	}
	t.Logf("mobile soak: %d frames ok, peak goros %d (base %d), final %d", frames, maxGoros, base, final)
}

// TestSoak_ShapeStatistics: the behavior-shape oracle. With a chat-like
// stream engaging the pacing band, wire bursts must show (a) size variance —
// no repeated fixed body size, and (b) low autocorrelation of inter-burst
// gaps — no fixed cadence at any small lag. Degeneration back to a fixed
// pacing interval or fixed chunk size trips this.
func TestSoak_ShapeStatistics(t *testing.T) {
	seed := soakSeed()
	t.Logf("SEED=%d", seed)
	bindIP := testBindIP(t)
	ln, hubAddr := soakServer(t, bindIP, "soak-shape")
	defer ln.Close()

	plan := FaultPlan{GracePeriod: 2 * time.Second, RTTMean: 10 * time.Millisecond, RTTJitter: 3 * time.Millisecond}
	proxy := newFaultProxy(bindIP, hubAddr, plan, seed)
	defer proxy.close()

	const msgs = 300
	_, err := runIntegritySession(t, proxy, soakSettings("soak-shape"), msgs, func(w *integrityWriter) error {
		rng := rand.New(rand.NewSource(seed + 3))
		for i := 0; i < msgs; i++ {
			if err := w.writeFrame(300 + rng.Intn(600)); err != nil {
				return err
			}
			time.Sleep(time.Duration(30+rng.Intn(40)) * time.Millisecond)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("shape session failed: %v", err)
	}

	bursts := proxy.takeBursts()
	if len(bursts) < 100 {
		t.Fatalf("shape stats need ≥100 bursts, got %d", len(bursts))
	}
	// (a) Size variance: coefficient of variation across bursts.
	var sum, sumSq float64
	for _, b := range bursts {
		sum += float64(b.Size)
		sumSq += float64(b.Size) * float64(b.Size)
	}
	n := float64(len(bursts))
	mean := sum / n
	variance := sumSq/n - mean*mean
	cv := math.Sqrt(variance) / mean
	if cv < 0.10 {
		t.Errorf("shape: burst size CV %.3f < 0.10 — sizes too uniform (quantization fingerprint?)", cv)
	}
	// (b) Inter-burst gap autocorrelation at lags 1..5.
	gaps := make([]float64, 0, len(bursts)-1)
	for i := 1; i < len(bursts); i++ {
		gaps = append(gaps, float64((bursts[i].At - bursts[i-1].At).Microseconds()))
	}
	var gm, gsq float64
	for _, g := range gaps {
		gm += g
		gsq += g * g
	}
	gm /= float64(len(gaps))
	gsq /= float64(len(gaps))
	sd := math.Sqrt(gsq - gm*gm)
	worstAC := 0.0
	for lag := 1; lag <= 5 && lag < len(gaps); lag++ {
		var cov float64
		for i := 0; i+lag < len(gaps); i++ {
			cov += (gaps[i] - gm) * (gaps[i+lag] - gm)
		}
		cov /= float64(len(gaps) - lag)
		ac := cov / (sd * sd)
		if math.Abs(ac) > worstAC {
			worstAC = math.Abs(ac)
		}
	}
	if worstAC > 0.6 {
		t.Errorf("shape: gap autocorrelation |%.3f| > 0.6 at some lag ≤5 — fixed cadence fingerprint?", worstAC)
	}
	t.Logf("shape oracle: bursts=%d size CV=%.3f worst |AC(1..5)|=%.3f", len(bursts), cv, worstAC)
}
