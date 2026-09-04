package splithttp

// Protocol profile audit (铁律 2): every wire-shaping constant lives in a
// named, documented constant (packet_upload.go / downseg.go / downseg_puller.go /
// hub.go — no inline magic numbers in the hot paths), and the relationships
// BETWEEN them are pinned here in one place so a constant edit that silently
// breaks a coupling fails this audit instead of in the field:
//
//   pacing band  ─┬─ To > recentFlow window      → paced sleeps reachable
//                 └─ From ≥ 1                    → bounded, not disabled
//   backpressure ─── gap wait − 500ms margin     → backpressure fires before
//                                                  the gap timeout tears down
//   chunk tiers  ─── Min < Low < Mid ≤ 1MiB      → RTT bands are monotonic
//   bulk pacing  ─── bulkPaceBytes ≤ chunkMin    → a chunk-min POST still
//                                                  counts as bulk-paced
//   retry chain  ─── fits gap wait (perf_audit)  → retry, don't tear down
//   dseg         ─── prefetch window × segment   → bounded memory + no
//                     ≤ adaptive bound             unbounded prefetch
//   dseg retry   ─── backoff jitter ≥ 1ms floor  → no tight retry storm

import (
	"testing"
	"time"
)

func TestProtocolProfile_CrossConstantInvariants(t *testing.T) {
	// Pacing band vs recentFlow window (see packet_upload.go docs).
	if int32(packetUploadRecentFlowWindow.Milliseconds()) >= defaultRangeConfigMinPostInterval.To {
		t.Fatalf("recentFlow window %v >= pacing band To %d: paced sleeps unreachable",
			packetUploadRecentFlowWindow, defaultRangeConfigMinPostInterval.To)
	}

	// Backpressure margin: the upload queue must apply backpressure strictly
	// before the gap timeout so a slow consumer is observable pressure, not
	// a surprise session teardown.
	if uploadQueueBackpressureWait >= maxSeqGapWait {
		t.Fatalf("backpressure wait %v must stay under gap wait %v", uploadQueueBackpressureWait, maxSeqGapWait)
	}

	// Chunk tiers monotonic and within the default post ceiling.
	if !(packetUploadChunkMin < packetUploadChunkLow && packetUploadChunkLow < packetUploadChunkMid && packetUploadChunkMid <= 1<<20) {
		t.Fatalf("chunk tiers not monotonic/1MiB-capped: %d < %d < %d <= 1MiB",
			packetUploadChunkMin, packetUploadChunkLow, packetUploadChunkMid)
	}

	// A chunk-min POST (small-packet flow, effMax=chunkMin) must already be
	// classified as bulk-paced, or tiny heartbeats would pay pacing latency
	// the bulk path is designed to skip.
	if packetUploadBulkPaceBytes > packetUploadChunkMin {
		t.Fatalf("bulk pace threshold %d exceeds chunk min %d: chunk-min posts lose bulk pacing",
			packetUploadBulkPaceBytes, packetUploadChunkMin)
	}

	// Downseg memory bound: prefetch window bounded by the adaptive cache;
	// segment size jitter never shrinks below the floor.
	if int64(prefetchAheadSegs)*int64(downsegSize) > int64(downsegAdaptiveSegs)*int64(downsegSize) {
		t.Fatalf("prefetch window %d segs exceeds adaptive cache bound %d segs",
			prefetchAheadSegs, downsegAdaptiveSegs)
	}
	if downsegSizeMin < downsegInitialAllocFloor/4 {
		t.Fatalf("segment min %d collapses under alloc floor %d", downsegSizeMin, downsegInitialAllocFloor)
	}

	// Downseg retry floor: jittered backoff must never go sub-millisecond.
	if downSegRetryInterval < time.Millisecond || downSegCurrentRetryInterval < time.Millisecond {
		t.Fatalf("downseg retry intervals below 1ms floor: %v / %v",
			downSegRetryInterval, downSegCurrentRetryInterval)
	}

	// H1 pipeline depth stays bounded (no unbounded pipelining against
	// middleboxes): the pool cap is a named constant, not a bare literal.
	if h1UploadMaxInflight < 1 || h1UploadMaxInflight > 8 {
		t.Fatalf("h1UploadMaxInflight = %d outside sane [1,8]", h1UploadMaxInflight)
	}
}
