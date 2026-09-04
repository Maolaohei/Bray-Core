package splithttp

import (
	"context"
	"errors"
	"io"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
)

type stubDialerClient struct {
	posts       atomic.Int32
	failN       atomic.Int32 // fail first N posts
	lastSeq     string
	payloads    []string
	mu          sync.Mutex
	block       chan struct{}
	started     chan struct{}
	maxInflight atomic.Int32
	inflight    atomic.Int32
}

func (s *stubDialerClient) IsClosed() bool { return false }

func (s *stubDialerClient) OpenStream(context.Context, *url.URL, string, io.Reader, bool) (io.ReadCloser, net.Addr, net.Addr, error) {
	return nil, nil, nil, errors.New("stub OpenStream")
}

func (s *stubDialerClient) OpenStreamAsync(context.Context, *url.URL, string, io.Reader, bool, func(remote, local net.Addr)) (io.ReadCloser, error) {
	return nil, errors.New("stub OpenStreamAsync")
}

func (s *stubDialerClient) PostPacket(ctx context.Context, url string, sessionId string, seqStr string, payload buf.MultiBuffer) error {
	cur := s.inflight.Add(1)
	for {
		max := s.maxInflight.Load()
		if cur <= max || s.maxInflight.CompareAndSwap(max, cur) {
			break
		}
	}
	defer s.inflight.Add(-1)

	if s.started != nil {
		select {
		case s.started <- struct{}{}:
		default:
		}
	}
	if s.block != nil {
		select {
		case <-s.block:
		case <-ctx.Done():
			if payload != nil {
				buf.ReleaseMulti(payload)
			}
			return ctx.Err()
		}
	}

	n := s.posts.Add(1)
	s.mu.Lock()
	s.lastSeq = seqStr
	if payload != nil {
		s.payloads = append(s.payloads, payload.String())
	}
	s.mu.Unlock()
	// consume like production path may release
	buf.ReleaseMulti(payload)
	if int(n) <= int(s.failN.Load()) {
		return errors.New("transient post failure")
	}
	return nil
}

func TestPostPacketReliable_RetriesSameSeq(t *testing.T) {
	s := &stubDialerClient{}
	s.failN.Store(2) // first two fail, third succeeds
	mb := buf.MergeBytes(nil, []byte("hello-packet"))
	err := postPacketReliable(context.Background(), s, "https://x/p", "sid", "7", mb, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.posts.Load() != 3 {
		t.Fatalf("posts=%d want 3", s.posts.Load())
	}
	if s.lastSeq != "7" {
		t.Fatalf("seq=%s", s.lastSeq)
	}
	for _, p := range s.payloads {
		if p != "hello-packet" {
			t.Fatalf("payload mutated: %q", p)
		}
	}
	// ownership transferred: caller must not use mb after call
}

func TestPostPacketReliable_Exhausts(t *testing.T) {
	s := &stubDialerClient{}
	s.failN.Store(100)
	mb := buf.MergeBytes(nil, []byte("x"))
	start := time.Now()
	err := postPacketReliable(context.Background(), s, "u", "s", "0", mb, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if s.posts.Load() != int32(packetUploadMaxAttempts) {
		t.Fatalf("posts=%d", s.posts.Load())
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("retries took too long")
	}
}

// TestPostPacketReliable_RescueOnFreshConn covers L4 failure granularity:
// after the retry budget is exhausted on the original client, the rescue
// closure supplies a FRESH outer connection and the same seq replay
// succeeds there — no whole-session teardown.
func TestPostPacketReliable_RescueOnFreshConn(t *testing.T) {
	bad := &stubDialerClient{}
	bad.failN.Store(100) // original outer connection: permanently failing
	good := &stubDialerClient{}
	rescued := false
	rescue := func(ctx context.Context) (DialerClient, error) {
		rescued = true
		return good, nil
	}
	mb := buf.MergeBytes(nil, []byte("rescue-me"))
	err := postPacketReliable(context.Background(), bad, "u", "s", "42", mb, rescue)
	if err != nil {
		t.Fatalf("rescue should succeed: %v", err)
	}
	if !rescued {
		t.Fatal("rescue closure was not called")
	}
	if good.posts.Load() != 1 {
		t.Fatalf("fresh conn posts=%d want 1", good.posts.Load())
	}
	if bad.posts.Load() != int32(packetUploadMaxAttempts) {
		t.Fatalf("original conn posts=%d want %d", bad.posts.Load(), packetUploadMaxAttempts)
	}
	good.mu.Lock()
	seq := good.lastSeq
	good.mu.Unlock()
	if seq != "42" {
		t.Fatalf("rescued seq=%s want 42 (same seq replay)", seq)
	}
}

// TestPostPacketReliable_RescueFailsStillErrors verifies that when the
// rescue leg also fails, the error surfaces and the session tears down.
func TestPostPacketReliable_RescueFailsStillErrors(t *testing.T) {
	bad := &stubDialerClient{}
	bad.failN.Store(100)
	worse := &stubDialerClient{}
	worse.failN.Store(100)
	calls := 0
	rescue := func(ctx context.Context) (DialerClient, error) {
		calls++
		return worse, nil
	}
	mb := buf.MergeBytes(nil, []byte("x"))
	err := postPacketReliable(context.Background(), bad, "u", "s", "0", mb, rescue)
	if err == nil {
		t.Fatal("expected error when both legs fail")
	}
	if calls != 1 {
		t.Fatalf("rescue called %d times, want exactly 1", calls)
	}
}

func TestPostPacketReliable_RespectsCancel(t *testing.T) {
	s := &stubDialerClient{}
	s.failN.Store(100)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mb := buf.MergeBytes(nil, []byte("x"))
	err := postPacketReliable(ctx, s, "u", "s", "0", mb, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}

func TestPacketUploadWindow(t *testing.T) {
	if got := packetUploadWindow(0, 0); got != packetUploadDefaultWindow {
		t.Fatalf("zero buffered=%d", got)
	}
	if got := packetUploadWindow(64, 0); got != packetUploadDefaultWindow {
		t.Fatalf("default buffered=%d", got)
	}
	if got := packetUploadWindow(4, 0); got != 2 {
		t.Fatalf("half of 4 => %d", got)
	}
	if got := packetUploadWindow(1, 0); got != 1 {
		t.Fatalf("tiny buffer=%d", got)
	}
	// half of 1000 = 500, still capped by default window when rtt unknown
	if got := packetUploadWindow(1000, 0); got != packetUploadDefaultWindow {
		t.Fatalf("large buffered default=%d", got)
	}
	if got := packetUploadWindow(64, 10*time.Millisecond); got != 8 {
		t.Fatalf("low rtt=%d", got)
	}
	if got := packetUploadWindow(64, 100*time.Millisecond); got != 18 {
		t.Fatalf("mid rtt=%d", got)
	}
	if got := packetUploadWindow(64, 250*time.Millisecond); got != 8 {
		t.Fatalf("high rtt=%d", got)
	}
	// server buffer still caps high-RTT growth (half of 8 = 4)
	if got := packetUploadWindow(8, 250*time.Millisecond); got != 4 {
		t.Fatalf("high rtt capped by buffer=%d", got)
	}
}

func TestFormatSeqInt64(t *testing.T) {
	for _, seq := range []int64{0, 1, 9, 10, 99, 100, 999, 1000, 4095, 4096, 123456789} {
		want := strconv.FormatInt(seq, 10)
		if got := formatSeqInt64(seq); got != want {
			t.Fatalf("seq=%d got=%q want=%q", seq, got, want)
		}
	}
}

func TestPacketUploadChunkSize(t *testing.T) {
	const cfg int32 = 1000000
	if got := packetUploadChunkSize(cfg, 0); got != cfg {
		t.Fatalf("unknown rtt must keep configured: %d", got)
	}
	if got := packetUploadChunkSize(cfg, 10*time.Millisecond); got != packetUploadChunkLow {
		t.Fatalf("low rtt=%d", got)
	}
	if got := packetUploadChunkSize(cfg, 50*time.Millisecond); got != packetUploadChunkLow {
		t.Fatalf("mid-low rtt=%d", got)
	}
	if got := packetUploadChunkSize(cfg, 100*time.Millisecond); got != packetUploadChunkMid {
		t.Fatalf("mid rtt=%d", got)
	}
	if got := packetUploadChunkSize(cfg, 250*time.Millisecond); got != packetUploadChunkMid {
		t.Fatalf("high rtt must cap at mid chunk (ToT L2): %d", got)
	}
	// Never exceed configured, even when floors are higher.
	if got := packetUploadChunkSize(16*1024, 10*time.Millisecond); got != 16*1024 {
		t.Fatalf("tiny config must win: %d", got)
	}
	// Server-side safety: negative/zero passthrough.
	if got := packetUploadChunkSize(0, 100*time.Millisecond); got != 0 {
		t.Fatalf("zero=%d", got)
	}
}

func TestPacketUploadLaunchInterval(t *testing.T) {
	if got := packetUploadLaunchIntervalMs(30, false, false, false, false); got != 30 {
		t.Fatalf("idle=%d", got)
	}
	if got := packetUploadLaunchIntervalMs(30, true, false, false, false); got != 0 {
		t.Fatalf("backlog=%d", got)
	}
	if got := packetUploadLaunchIntervalMs(30, false, true, false, false); got != 0 {
		t.Fatalf("full chunk=%d", got)
	}
	if got := packetUploadLaunchIntervalMs(30, false, false, true, false); got != 0 {
		t.Fatalf("bulk chunk=%d", got)
	}
	if got := packetUploadLaunchIntervalMs(30, false, false, false, true); got != 0 {
		t.Fatalf("recent flow=%d", got)
	}
	if got := packetUploadLaunchIntervalMs(0, false, false, false, false); got != 0 {
		t.Fatalf("disabled=%d", got)
	}
}

// TestDefaultMinPostIntervalJittered pins the Bray-only anti-fingerprint
// default: idle-post pacing must come from a jittered band, never a fixed
// cadence, and the band must extend past 50ms so the paced sleep is actually
// reachable (recentFlow skips sub-50ms gaps).
func TestDefaultMinPostIntervalJittered(t *testing.T) {
	cfg := &Config{}
	r := cfg.GetNormalizedScMinPostsIntervalMs()
	if r.From >= r.To {
		t.Fatalf("default interval must be a jitter band, got [%d,%d]", r.From, r.To)
	}
	if r.To <= 50 {
		t.Fatalf("band To must exceed the 50ms recentFlow window so paced sleeps can occur, To=%d", r.To)
	}
	seen := map[int32]bool{}
	for i := 0; i < 64; i++ {
		v := r.rand()
		if v < r.From || v > r.To {
			t.Fatalf("draw %d out of band [%d,%d]: %d", i, r.From, r.To, v)
		}
		seen[v] = true
	}
	if len(seen) < 4 {
		t.Fatalf("band shows no jitter diversity after 64 draws: %v", seen)
	}
}

func TestPostPacketReliable_ConcurrentSlots(t *testing.T) {
	// Simulates limited-window launch: N concurrent postPacketReliable calls.
	const n = 8
	s := &stubDialerClient{
		block:   make(chan struct{}),
		started: make(chan struct{}, n),
	}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()
			mb := buf.MergeBytes(nil, []byte("p"))
			_ = postPacketReliable(context.Background(), s, "u", "s", strconv.Itoa(seq), mb, nil)
		}(i)
	}
	// Wait until all N have entered PostPacket.
	for i := 0; i < n; i++ {
		select {
		case <-s.started:
		case <-time.After(2 * time.Second):
			t.Fatalf("only started %d/%d", i, n)
		}
	}
	if s.maxInflight.Load() < int32(n) {
		t.Fatalf("maxInflight=%d want >= %d", s.maxInflight.Load(), n)
	}
	close(s.block)
	wg.Wait()
}

func BenchmarkGetRequestHeader(b *testing.B) {
	c := &Config{
		Headers: map[string]string{
			"User-Agent": "BrayBench/1.0",
			"Accept":     "*/*",
		},
	}
	// Warm cache
	_ = c.GetRequestHeader()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := c.GetRequestHeader()
		h.Set("X-Padding", "x")
	}
}

func BenchmarkFormatSeqInt64(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = formatSeqInt64(int64(i % 10000))
	}
}
