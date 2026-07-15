package splithttp

import (
	"context"
	"errors"
	"io"
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

func (s *stubDialerClient) OpenStream(context.Context, string, string, io.Reader, bool) (io.ReadCloser, net.Addr, net.Addr, error) {
	return nil, nil, nil, errors.New("not implemented")
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
	err := postPacketReliable(context.Background(), s, "https://x/p", "sid", "7", mb)
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
	// original still intact for caller release
	if mb.String() != "hello-packet" {
		t.Fatalf("source buffer corrupted: %q", mb.String())
	}
	buf.ReleaseMulti(mb)
}

func TestPostPacketReliable_Exhausts(t *testing.T) {
	s := &stubDialerClient{}
	s.failN.Store(100)
	mb := buf.MergeBytes(nil, []byte("x"))
	start := time.Now()
	err := postPacketReliable(context.Background(), s, "u", "s", "0", mb)
	if err == nil {
		t.Fatal("expected error")
	}
	if s.posts.Load() != int32(packetUploadMaxAttempts) {
		t.Fatalf("posts=%d", s.posts.Load())
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("retries took too long")
	}
	buf.ReleaseMulti(mb)
}

func TestPostPacketReliable_RespectsCancel(t *testing.T) {
	s := &stubDialerClient{}
	s.failN.Store(100)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mb := buf.MergeBytes(nil, []byte("x"))
	err := postPacketReliable(ctx, s, "u", "s", "0", mb)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	buf.ReleaseMulti(mb)
}

func TestCloneMultiBuffer(t *testing.T) {
	src := buf.MergeBytes(nil, []byte("abc"))
	cp := cloneMultiBuffer(src)
	if cp.String() != "abc" {
		t.Fatal(cp.String())
	}
	buf.ReleaseMulti(src)
	// clone independent
	if cp.String() != "abc" {
		t.Fatal("clone tied to source")
	}
	buf.ReleaseMulti(cp)
}

func TestPacketUploadWindow(t *testing.T) {
	if got := packetUploadWindow(0); got != packetUploadDefaultWindow {
		t.Fatalf("zero buffered=%d", got)
	}
	if got := packetUploadWindow(64); got != packetUploadDefaultWindow {
		t.Fatalf("default buffered=%d", got)
	}
	if got := packetUploadWindow(4); got != 2 {
		t.Fatalf("half of 4 => %d", got)
	}
	if got := packetUploadWindow(1); got != 1 {
		t.Fatalf("tiny buffer=%d", got)
	}
	if got := packetUploadWindow(1000); got != packetUploadDefaultWindow {
		t.Fatalf("large buffered default=%d", got)
	}
	if got := packetUploadWindow(2); got != 1 {
		t.Fatalf("half of 2 => %d", got)
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
			_ = postPacketReliable(context.Background(), s, "u", "s", strconv.Itoa(seq), mb)
			buf.ReleaseMulti(mb)
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
