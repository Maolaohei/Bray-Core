//go:build soak

package splithttp_test

// Soak harness (expert blueprint §1+§3+§4): a programmable fault-injection
// TCP proxy driven by a seeded FaultPlan, plus the three oracles —
//   data integrity  : seq-framed position-derived stream, byte-exact echo
//                     check (dup/loss/reorder/truncation all localized),
//   liveness        : per-fault-event recovery latency (fault end → bytes
//                     flowing again), asserted against an SLA, not just
//                     "didn't crash",
//   resource bounds : periodic goroutine/heap sampling + leak guard.
//
// Every random draw derives from ONE seed (BRAY_SOAK_SEED, logged per run):
// a failure prints SEED=N and reruns with the identical plan. Fault types
// chosen to hit distinct product paths: RST (error paths), STALL (timeout
// logic — no error ever surfaces), BLACKHOLE windows (mass reconnect/backoff),
// truncate (downlink rebuild), half-close (directional EOF), slow/failed dial
// (dialer pools). Loss/reorder are netem-layer faults (TCP retransmits below
// us) — see SOAK_BLUEPRINT.md.

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport/internet"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/stat"
)

// soakSeed returns the single reproducibility seed for the whole run.
func soakSeed() int64 {
	if s := os.Getenv("BRAY_SOAK_SEED"); s != "" {
		var v int64
		fmt.Sscanf(s, "%d", &v)
		if v != 0 {
			return v
		}
	}
	return time.Now().UnixNano()
}

// FaultPlan drives one proxy instance. Zero-value fields disable the fault.
type FaultPlan struct {
	// GracePeriod: faults start only after this much proxy uptime — the
	// session must be established first (a real user's session exists
	// before they enter the elevator; a killed handshake is the dialer's
	// fail-closed design, not a soak finding).
	GracePeriod     time.Duration
	RTTMean         time.Duration   // per-chunk forward delay mean
	RTTJitter       time.Duration   // ±jitter around mean
	RSTLifetime     [2]time.Duration // per-conn lifetime draw → RST both ends
	Stall           [2]time.Duration // hold bytes mid-conn (no error surfaces)
	StallChance     float64
	Blackhole       []Blackhole // global windows (proxy-relative clock)
	TruncateChance  float64     // server→client RST mid-response
	HalfCloseChance float64     // client→server CloseWrite after first chunk
	BWLimitBytes    int         // per-direction bytes/sec token bucket, 0=off
	DialFailRate    float64     // proxy→server dial failures
}

type Blackhole struct{ Start, End time.Duration }

type FaultEvent struct {
	Kind string // rst|stall|blackhole|truncate|halfclose|dialfail
	Conn int64
	At   time.Duration
	Dur  time.Duration
}

type faultProxy struct {
	plan   FaultPlan
	rng    *rand.Rand
	target string
	start  time.Time
	mu     sync.Mutex
	events []FaultEvent
	opened atomic.Int64
	active atomic.Int64
	ln     net.Listener
	// c2s burst observation (>=5ms silence separates bursts) for shape stats.
	burstMu     sync.Mutex
	bursts      []burst
	curBurst    int64
	curBurstAt  time.Duration
	lastReadAt  time.Duration
	bw          *tokenBucket
	truncatedC2S atomic.Bool // guard: only one s2c truncate per proxy
}

type burst struct {
	At   time.Duration
	Size int64
}

func newFaultProxy(bindIP, target string, plan FaultPlan, seed int64) *faultProxy {
	p := &faultProxy{plan: plan, rng: rand.New(rand.NewSource(seed)), target: target, start: time.Now()}
	p.bw = newTokenBucket(plan.BWLimitBytes)
	ln, err := net.Listen("tcp", net.JoinHostPort(bindIP, fmt.Sprint(int(tcp.PickPort()))))
	common.Must(err)
	p.ln = ln
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.handle(c)
		}
	}()
	return p
}

func (p *faultProxy) addr() string { return p.ln.Addr().String() }
func (p *faultProxy) close()       { p.ln.Close() }

func (p *faultProxy) logEvent(kind string, conn int64, at, dur time.Duration) {
	p.mu.Lock()
	p.events = append(p.events, FaultEvent{kind, conn, at, dur})
	p.mu.Unlock()
}

func (p *faultProxy) inBlackhole(now time.Duration) (bool, time.Duration) {
	for _, bh := range p.plan.Blackhole {
		if now >= bh.Start && now < bh.End {
			return true, bh.End - now
		}
	}
	return false, 0
}

func (p *faultProxy) handle(client net.Conn) {
	defer client.Close()
	p.active.Add(1)
	defer p.active.Add(-1)
	id := p.opened.Add(1)

	// Grace window: forward transparently until faults are allowed. The
	// c2s direction still records bursts — shape statistics must observe
	// connections established during the grace window too.
	if p.plan.GracePeriod > 0 && time.Since(p.start) < p.plan.GracePeriod {
		upstream, err := net.Dial("tcp", p.target)
		if err != nil {
			return
		}
		defer upstream.Close()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			chunk := make([]byte, 16*1024)
			for {
				n, rerr := client.Read(chunk)
				if n > 0 {
					p.recordBurst(time.Since(p.start), int64(n))
					if _, werr := upstream.Write(chunk[:n]); werr != nil {
						return
					}
				}
				if rerr != nil {
					return
				}
			}
		}()
		go func() { defer wg.Done(); io.Copy(client, upstream) }()
		wg.Wait()
		return
	}
	if p.plan.DialFailRate > 0 && p.rng.Float64() < p.plan.DialFailRate {
		p.logEvent("dialfail", id, time.Since(p.start), 0)
		return
	}
	upstream, err := net.Dial("tcp", p.target)
	if err != nil {
		p.logEvent("dialfail", id, time.Since(p.start), 0)
		return
	}
	defer upstream.Close()

	rng := rand.New(rand.NewSource(p.rng.Int63()))
	tc := func(c net.Conn) *net.TCPConn {
		if t, ok := c.(*net.TCPConn); ok {
			return t
		}
		return nil
	}

	// Per-conn fault draws.
	var rstAt time.Time
	if p.plan.RSTLifetime[0] > 0 {
		span := int64(p.plan.RSTLifetime[1] - p.plan.RSTLifetime[0])
		lt := p.plan.RSTLifetime[0] + time.Duration(rng.Int63n(span))
		rstAt = p.start.Add(lt)
		if t := tc(client); t != nil {
			t.SetLinger(0) // close => RST, not FIN
		}
		if t := tc(upstream); t != nil {
			t.SetLinger(0)
		}
	}
	stallArmed := p.plan.StallChance > 0 && rng.Float64() < p.plan.StallChance
	stallDur := time.Duration(0)
	if stallArmed {
		span := int64(p.plan.Stall[1] - p.plan.Stall[0])
		stallDur = p.plan.Stall[0] + time.Duration(rng.Int63n(span))
	}
	truncate := p.plan.TruncateChance > 0 && rng.Float64() < p.plan.TruncateChance
	halfClose := p.plan.HalfCloseChance > 0 && rng.Float64() < p.plan.HalfCloseChance
	if halfClose {
		p.logEvent("halfclose", id, time.Since(p.start), 0)
	}

	forward := func(dst, src net.Conn, isC2S bool) {
		chunk := make([]byte, 16*1024)
		var stallFired bool
		var s2cSent int64
		firstChunk := true
		for {
			n, rerr := src.Read(chunk)
			now := time.Since(p.start)
			if isC2S {
				p.recordBurst(now, int64(n))
			}
			if n > 0 {
				// Global blackhole: hold silently until the window ends.
				if bh, remain := p.inBlackhole(now); bh {
					p.logEvent("blackhole", id, now, remain)
					time.Sleep(remain + 5*time.Millisecond)
				}
				// Per-conn stall: hold once mid-conn, then resume.
				if stallArmed && !stallFired {
					stallFired = true
					p.logEvent("stall", id, time.Since(p.start), stallDur)
					time.Sleep(stallDur)
				}
				// Path RTT per chunk.
				if p.plan.RTTMean > 0 {
					j := time.Duration(0)
					if p.plan.RTTJitter > 0 {
						j = time.Duration(rng.Int63n(int64(2*p.plan.RTTJitter))) - p.plan.RTTJitter
					}
					time.Sleep(p.plan.RTTMean + j)
				}
				p.bw.wait(n)
				if isC2S && halfClose && firstChunk {
					firstChunk = false
					if t := tc(upstream); t != nil {
						t.CloseWrite() // server sees EOF, still writes back
					}
				}
				if !isC2S && truncate && s2cSent > 32*1024 {
					if p.truncatedC2S.CompareAndSwap(false, true) {
						p.logEvent("truncate", id, time.Since(p.start), 0)
						if t := tc(src); t != nil {
							t.SetLinger(0)
						}
						src.Close()
						return
					}
				}
				if _, werr := dst.Write(chunk[:n]); werr != nil {
					return
				}
				if !isC2S {
					s2cSent += int64(n)
				}
			}
			if rerr != nil {
				return
			}
			if !rstAt.IsZero() && time.Now().After(rstAt) {
				p.logEvent("rst", id, time.Since(p.start), 0)
				client.Close()
				upstream.Close()
				return
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); forward(upstream, client, true) }()
	go func() { defer wg.Done(); forward(client, upstream, false) }()
	wg.Wait()
}

func (p *faultProxy) recordBurst(at time.Duration, n int64) {
	const burstGap = 5 * time.Millisecond
	p.burstMu.Lock()
	defer p.burstMu.Unlock()
	if n == 0 {
		return
	}
	if p.curBurst > 0 && at-p.lastReadAt <= burstGap {
		p.curBurst += n
	} else {
		if p.curBurst > 0 {
			p.bursts = append(p.bursts, burst{At: p.curBurstAt, Size: p.curBurst})
		}
		p.curBurst = n
		p.curBurstAt = at
	}
	p.lastReadAt = at
}

func (p *faultProxy) takeBursts() []burst {
	p.burstMu.Lock()
	defer p.burstMu.Unlock()
	if p.curBurst > 0 {
		p.bursts = append(p.bursts, burst{At: p.curBurstAt, Size: p.curBurst})
		p.curBurst = 0
	}
	out := make([]burst, len(p.bursts))
	copy(out, p.bursts)
	return out
}

func (p *faultProxy) eventsSnapshot() []FaultEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]FaultEvent, len(p.events))
	copy(out, p.events)
	return out
}

type tokenBucket struct {
	mu     sync.Mutex
	rate   int
	tokens float64
	last   time.Time
}

func newTokenBucket(rate int) *tokenBucket {
	if rate <= 0 {
		return &tokenBucket{}
	}
	return &tokenBucket{rate: rate, tokens: 32 * 1024, last: time.Now()}
}

func (tb *tokenBucket) wait(n int) {
	if tb.rate <= 0 {
		return
	}
	for {
		tb.mu.Lock()
		now := time.Now()
		tb.tokens += now.Sub(tb.last).Seconds() * float64(tb.rate)
		if tb.tokens > 32*1024 {
			tb.tokens = 32 * 1024
		}
		tb.last = now
		if tb.tokens >= float64(n) {
			tb.tokens -= float64(n)
			tb.mu.Unlock()
			return
		}
		need := float64(n) - tb.tokens
		tb.mu.Unlock()
		time.Sleep(time.Duration(need / float64(tb.rate) * float64(time.Second)))
	}
}

// --- integrity stream: seq-framed, position-derived payload --------------

const frameHdr = 8 // u32 seq + u32 len

func framePayload(seq uint32, payload []byte) {
	for i := range payload {
		payload[i] = byte(seq*131 + uint32(i)*7 + 0x5A)
	}
}

type integrityWriter struct {
	w       io.Writer
	seq     uint32
	payload []byte
}

func newIntegrityWriter(w io.Writer, maxFrame int) *integrityWriter {
	return &integrityWriter{w: w, payload: make([]byte, maxFrame)}
}

func (f *integrityWriter) writeFrame(size int) error {
	framePayload(f.seq, f.payload[:size])
	hdr := make([]byte, frameHdr)
	binary.BigEndian.PutUint32(hdr[0:], f.seq)
	binary.BigEndian.PutUint32(hdr[4:], uint32(size))
	if _, err := f.w.Write(hdr); err != nil {
		return err
	}
	if _, err := f.w.Write(f.payload[:size]); err != nil {
		return err
	}
	f.seq++
	return nil
}

type integrityReader struct {
	r       io.Reader
	expect  uint32
	payload []byte
	hdr     []byte
}

func newIntegrityReader(r io.Reader, maxFrame int) *integrityReader {
	return &integrityReader{r: r, payload: make([]byte, maxFrame), hdr: make([]byte, frameHdr)}
}

// readFrame verifies one frame: continuity (no dup/loss/reorder) + content.
func (f *integrityReader) readFrame() (int, error) {
	if _, err := io.ReadFull(f.r, f.hdr); err != nil {
		return 0, err
	}
	seq := binary.BigEndian.Uint32(f.hdr[0:])
	size := int(binary.BigEndian.Uint32(f.hdr[4:]))
	if seq != f.expect {
		return 0, fmt.Errorf("stream discontinuity: got seq %d want %d (dup/loss/reorder)", seq, f.expect)
	}
	if size <= 0 || size > len(f.payload) {
		return 0, fmt.Errorf("frame %d: bad size %d", seq, size)
	}
	if _, err := io.ReadFull(f.r, f.payload[:size]); err != nil {
		return 0, fmt.Errorf("frame %d: short body: %w", seq, err)
	}
	for i := 0; i < size; i++ {
		if want := byte(seq*131 + uint32(i)*7 + 0x5A); f.payload[i] != want {
			return 0, fmt.Errorf("frame %d: content mismatch at +%d", seq, i)
		}
	}
	f.expect++
	return size, nil
}

// --- liveness oracle ------------------------------------------------------

type readStamp struct {
	at    time.Time
	bytes int
}

type livenessTracker struct {
	mu     sync.Mutex
	stamps []readStamp
}

func (l *livenessTracker) stamp(n int) {
	l.mu.Lock()
	l.stamps = append(l.stamps, readStamp{time.Now(), n})
	l.mu.Unlock()
}

// recoveryFor returns the time from event end until bytes flowed again
// (-1 when nothing ever flowed after the event ended).
func (l *livenessTracker) recoveryFor(evEnd time.Time) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, s := range l.stamps {
		if s.at.After(evEnd) {
			return s.at.Sub(evEnd)
		}
	}
	return -1
}

// lastStampTime returns the arrival time of the last observed byte batch
// (zero when nothing was ever read).
func (l *livenessTracker) lastStampTime() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.stamps) == 0 {
		return time.Time{}
	}
	return l.stamps[len(l.stamps)-1].at
}

// --- resource oracle ------------------------------------------------------

type resourceSample struct {
	goros int
	heap  uint64
}

type resourceSampler struct {
	start   time.Time
	stop    chan struct{}
	done    chan struct{}
	samples []resourceSample
	mu      sync.Mutex
}

func startResourceSampler(period time.Duration) *resourceSampler {
	s := &resourceSampler{start: time.Now(), stop: make(chan struct{}), done: make(chan struct{})}
	var ms runtime.MemStats
	go func() {
		defer close(s.done)
		t := time.NewTicker(period)
		defer t.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-t.C:
				runtime.ReadMemStats(&ms)
				s.mu.Lock()
				s.samples = append(s.samples, resourceSample{runtime.NumGoroutine(), ms.HeapAlloc})
				s.mu.Unlock()
			}
		}
	}()
	return s
}

func (s *resourceSampler) stopAndCollect() []resourceSample {
	close(s.stop)
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.samples
}

// assertLeakGuard: goroutines must return within tolerance of the pre-soak
// baseline after the run (server pools may retain a bounded number).
func assertLeakGuard(t *testing.T, baseline, final, tol int) {
	t.Helper()
	if final > baseline+tol {
		t.Errorf("leak guard: goroutines %d > baseline %d + %d", final, baseline, tol)
	}
}

// --- shared scenario scaffolding ------------------------------------------

func soakSettings(secret string) *internet.MemoryStreamConfig {
	return &internet.MemoryStreamConfig{
		ProtocolName: "splithttp",
		ProtocolSettings: &Config{
			Path:    "/sh",
			Mode:    "packet-up",
			Headers: map[string]string{BraySessionSecretHeader: secret},
		},
	}
}

func soakServer(t *testing.T, bindIP, secret string) (internet.Listener, string) {
	t.Helper()
	port := tcp.PickPort()
	ln, err := ListenXH(context.Background(), xnet.ParseAddress(bindIP), port, soakSettings(secret), func(conn stat.Connection) {
		go func(c stat.Connection) {
			defer c.Close()
			buf.Copy(buf.NewReader(c), buf.NewWriter(c))
		}(conn)
	})
	common.Must(err)
	return ln, fmt.Sprintf("%s:%d", bindIP, int(port))
}
