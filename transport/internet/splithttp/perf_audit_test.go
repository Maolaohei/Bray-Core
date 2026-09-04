package splithttp

// Performance checklist item (性能★): window × chunk sizing must track the
// bandwidth-delay product across RTT tiers without exploding memory.
//
// These are the audited invariants, pinned as code so a future constant edit
// that silently breaks one of them fails here instead of in the field:
//
//   - BDP coverage: in-flight bytes (window × chunk) must stay ≥ ~half of a
//     100 Mbit/s BDP at the tier's RTT (below that, throughput collapses to
//     window×chunk/RTT regardless of the path's real capacity);
//   - memory bound: in-flight bytes must stay ≤ 12 MiB per logical
//     connection (packet-up POSTs are fully buffered client-side);
//   - retry chain fits the gap: worst-case postPacketReliable backoff chain
//     (packetUploadMaxAttempts steps at 25/50/100ms) must stay well under
//     maxSeqGapWait, or a single flaky outer path would tear the session
//     down through the server-side seq-gap timeout instead of retrying.

import (
	"testing"
	"time"
)

func TestPerfAudit_WindowChunkTracksBDP(t *testing.T) {
	// window under the default server buffer (scMaxBufferedPosts=64 → maxW=32,
	// i.e. the RTT tier decides), plus the per-tier chunk ceiling.
	const mbit = 1_000_000.0 // bits per second, wire rate under test
	cases := []struct {
		rtt       time.Duration
		wantWin   int
		chunkCeil int32
	}{
		{0, packetUploadDefaultWindow, 1 << 20},  // unknown RTT: defaults
		{5 * time.Millisecond, 8, packetUploadChunkLow},
		{20 * time.Millisecond, 12, packetUploadChunkLow},
		{80 * time.Millisecond, 18, packetUploadChunkMid},
		{300 * time.Millisecond, 8, packetUploadChunkMid},
	}
	for _, c := range cases {
		w := packetUploadWindow(64, c.rtt)
		if w != c.wantWin {
			t.Fatalf("rtt=%v: window = %d, want %d", c.rtt, w, c.wantWin)
		}
		chunk := packetUploadChunkSize(1<<20, c.rtt)
		if chunk > c.chunkCeil {
			t.Fatalf("rtt=%v: chunk %d exceeds tier ceiling %d", c.rtt, chunk, c.chunkCeil)
		}
		inflight := int64(w) * int64(chunk)
		if inflight > 12<<20 {
			t.Fatalf("rtt=%v: in-flight %d B exceeds 12 MiB memory bound", c.rtt, inflight)
		}
		if c.rtt >= 20*time.Millisecond {
			bdpBytes := 100 * mbit * c.rtt.Seconds() / 8
			if float64(inflight) < bdpBytes/2 {
				t.Fatalf("rtt=%v: in-flight %d B covers only %.0f%% of BDP %.0f B — throughput would collapse to window×chunk/RTT",
					c.rtt, inflight, 100*float64(inflight)/bdpBytes, bdpBytes)
			}
		}
	}
}

func TestPerfAudit_RetryChainFitsGapTimeout(t *testing.T) {
	// Backoff steps are 25ms doubled per attempt, minus the final attempt
	// which gives up instead of waiting; the sum of all waits plus one
	// worst-case attempt RTT must stay inside half the gap timeout so a
	// flaky outer path retries instead of tripping the server's seq-gap
	// teardown.
	backoff := time.Duration(0)
	step := packetUploadRetryBase
	for i := 1; i < packetUploadMaxAttempts; i++ {
		backoff += step
		step *= 2
	}
	worstAttempt := 300*time.Millisecond // slowest audited RTT
	total := backoff + worstAttempt
	if total > maxSeqGapWait/2 {
		t.Fatalf("retry chain (%v + one attempt RTT %v = %v) exceeds half of maxSeqGapWait (%v): a single blip would tear the session down",
			backoff, worstAttempt, total, maxSeqGapWait/2)
	}
}
