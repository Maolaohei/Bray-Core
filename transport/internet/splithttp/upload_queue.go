package splithttp

// upload_queue is a specialized typed priority queue + channel to reorder
// packets by a sequence number. Uses a concrete-typed heap to avoid
// interface{} boxing/unboxing overhead from container/heap.

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

type Packet struct {
	Reader  io.ReadCloser
	Payload []byte
	Seq     uint64
	// Pooled marks Payload as owned by the postBodyPool allocator. The queue
	// (Push on failure, Read after consumption / on duplicate / on abort)
	// returns it via freePostBody; non-pooled payloads are GC'd as before.
	Pooled bool
}

// discardQueuedPacket releases a packet abandoned during queue/session
// teardown. Unlike freePacketPayload it also closes a queued stream-up reader
// so a half-open HTTP body cannot outlive the session.
func discardQueuedPacket(p *Packet) {
	if p == nil {
		return
	}
	freePacketPayload(p)
	if p.Reader != nil {
		_ = p.Reader.Close()
		p.Reader = nil
	}
}

// freePacketPayload returns a consumed/aborted packet's pooled payload.
func freePacketPayload(p *Packet) {
	if p != nil && p.Pooled && p.Payload != nil {
		freePostBody(p.Payload)
		p.Payload = nil
		p.Pooled = false
	}
}

// maxSeqGapWait is how long Read will wait for a missing nextSeq before
// aborting the session stream. Prevents a lost packet from stalling forever,
// but wide enough to survive real weak links: on high-RTT/lossy paths a
// retransmitted seq can arrive late (client backoff + outer network). 2s was
// too tight and aborted uploads under high-RTT/limited-bandwidth links
// (V2rayN weak-net drops: "packet sequence gap timeout waiting for seq=N").
const maxSeqGapWait = 5 * time.Second

type uploadQueue struct {
	reader        atomic.Pointer[io.ReadCloser]
	nomore        bool
	pushedPackets chan Packet
	// room is a coalesced notification sent by Read after it removes a
	// packet from pushedPackets. Full Push waiters listen on it instead of
	// dropping their packet as a 404 immediately.
	room chan struct{}
	// done closes exactly once in Close, waking a full Push that is waiting
	// for room while the logical session is torn down. It is deliberately
	// separate from pushedPackets: Push sends only while writeCloseMutex is
	// held, but waits after releasing it; done makes that wait close-safe.
	done            chan struct{}
	writeCloseMutex sync.Mutex
	// waiters counts full-queue Push calls waiting on room. It is guarded by
	// writeCloseMutex and caps extra retained POST bodies/handler goroutines
	// under a malicious or terminally-stuck consumer.
	waiters    int
	heap       packetHeap
	nextSeq    uint64
	closed     atomic.Bool
	maxPackets int
	// gapSince is wall time when we first observed a hole at nextSeq.
	// Zero means no outstanding gap.
	gapSince time.Time
}

func NewUploadQueue(maxPackets int) *uploadQueue {
	return &uploadQueue{
		// L1: channels and heap backing are allocated lazily on first
		// Push — pure download / stream-only sessions never pay the
		// channel/heap or backpressure-notification overhead.
		maxPackets: maxPackets,
	}
}

// uploadQueueBackpressureWait is the bounded grace period for an upload POST
// that arrives exactly while the queue is full. It deliberately matches
// maxSeqGapWait: a real high-RTT/slow-target burst can need hundreds of ms
// (or several RTTs) for the VLESS consumer to drain, and returning a 404
// sooner causes the client to retry the SAME packet, compounding load and
// creating the observed 20Mbps -> 1Mbps oscillation. Leave a 500ms margin
// before maxSeqGapWait so the newly admitted missing sequence can reach
// uploadQueue.Read before its strict gap timer tears the session down.
const uploadQueueBackpressureWait = maxSeqGapWait - 500*time.Millisecond

// uploadQueueMaxWaiters bounds full-queue HTTP handlers per session. Each
// waiter can retain one decoded POST body (up to ScMaxEachPostBytes), so this
// is a memory/goroutine security bound as well as a fairness guard. Twelve
// matches packetUploadDefaultWindow: a healthy client may legally have that
// many POSTs in flight, so accepting fewer creates an artificial 404 burst
// exactly when server-side backpressure is needed.
const uploadQueueMaxWaiters = packetUploadDefaultWindow

// ensureQueue lazily allocates the channel, backpressure notification and
// heap. Caller must hold writeCloseMutex. Assign pushedPackets LAST so a
// Read that observes it non-nil is guaranteed room/done have been initialized.
func (h *uploadQueue) ensureQueue() {
	if h.pushedPackets == nil {
		// L2: heap backing capped at 16 (deep enough for reordering:
		// packet-up window maxes at 24 in-flight, heap rarely exceeds a
		// handful of misordered packets); channel keeps maxPackets.
		h.room = make(chan struct{}, 1)
		h.done = make(chan struct{})
		h.heap = make(packetHeap, 0, min(h.maxPackets, 16))
		h.pushedPackets = make(chan Packet, h.maxPackets)
	}
}

// notifyRoom coalesces a freed channel slot notification. room is never
// closed, so this remains safe while Close races with a reader draining the
// already-buffered channel.
func (h *uploadQueue) notifyRoom() {
	select {
	case h.room <- struct{}{}:
	default:
	}
}

// queuePollInterval is how often Read re-checks for a lazily-created queue
// while waiting for the first Push. Bounded by the five-second gap timeout.
const queuePollInterval = 200 * time.Microsecond

// Push retains the historical context-free API for unit tests and internal
// callers. HTTP handlers must use PushContext(request.Context(), ...) so a
// client disconnect cancels a full-queue backpressure wait immediately.
func (h *uploadQueue) Push(p Packet) error {
	return h.PushContext(context.Background(), p)
}

// PushContext inserts an upload packet in sequence order. On a momentarily
// full queue it applies bounded, cancel-aware backpressure instead of
// immediately returning "packet queue full" (which hub maps to HTTP 404 and
// causes the packet-up client to retry/thrash). The per-session waiter cap
// prevents waiting handlers/body buffers from becoming an unbounded DoS sink.
func (h *uploadQueue) PushContext(ctx context.Context, p Packet) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var deadline time.Time // lazy: uncontended hot path has no clock read
	for {
		h.writeCloseMutex.Lock()
		if h.closed.Load() {
			h.writeCloseMutex.Unlock()
			freePacketPayload(&p)
			return errors.New("packet queue closed")
		}
		if h.nomore {
			h.writeCloseMutex.Unlock()
			freePacketPayload(&p)
			return errors.New("h.reader already exists")
		}
		h.ensureQueue()
		select {
		case h.pushedPackets <- p:
			// A stream-up reader is exclusive, but do not mark it until
			// the packet actually enters the channel: a full-queue retry
			// must not reject its own still-unenqueued reader as duplicate.
			if p.Reader != nil {
				h.nomore = true
			}
			h.writeCloseMutex.Unlock()
			return nil // queue owns p (and any pooled payload)
		default:
		}

		// Queue is full. Admit only a small, bounded number of waiting
		// handlers; every waiter retains one already-decoded POST body.
		// Do NOT cap this by h.maxPackets: a deliberately small server queue
		// must still absorb the client's normal 12-post launch window instead
		// of converting its own backpressure into a synthetic 404 storm.
		if h.waiters >= uploadQueueMaxWaiters {
			h.writeCloseMutex.Unlock()
			freePacketPayload(&p)
			return errors.New("packet queue full")
		}
		h.waiters++
		room, done := h.room, h.done
		h.writeCloseMutex.Unlock()

		if deadline.IsZero() {
			deadline = time.Now().Add(uploadQueueBackpressureWait)
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			h.leaveWaiter()
			freePacketPayload(&p)
			return errors.New("packet queue full")
		}
		timer := time.NewTimer(remaining)
		var waitErr error
		select {
		case <-room:
			// A reader freed a slot. Re-check state and safely send under
			// the mutex in the next loop iteration.
		case <-done:
			waitErr = errors.New("packet queue closed")
		case <-ctx.Done():
			waitErr = ctx.Err()
		case <-timer.C:
			waitErr = errors.New("packet queue full")
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		h.leaveWaiter()
		if waitErr != nil {
			freePacketPayload(&p)
			return waitErr
		}
	}
}

// leaveWaiter decrements the per-session pending full-queue waiter count.
func (h *uploadQueue) leaveWaiter() {
	h.writeCloseMutex.Lock()
	if h.waiters > 0 {
		h.waiters--
	}
	h.writeCloseMutex.Unlock()
}

func (h *uploadQueue) Close() error {
	h.writeCloseMutex.Lock()
	defer h.writeCloseMutex.Unlock()

	if !h.closed.Swap(true) {
		// Wake a Push backpressured on a full queue before closing the
		// packet channel. Sends always hold this same mutex, so no sender can
		// race the close; waiters select on done after the mutex is released.
		if h.done != nil {
			close(h.done)
		}
		if h.pushedPackets != nil {
			close(h.pushedPackets)
			// Read returns EOF immediately once closed, so it may never
			// consume packets still buffered in the channel. Drain them here
			// while sends are excluded by writeCloseMutex; otherwise pooled
			// bodies fall back to GC and queued stream-up readers leak until
			// their transport times out.
			for p := range h.pushedPackets {
				discardQueuedPacket(&p)
			}
		}
	}
	if r := h.reader.Load(); r != nil {
		return (*r).Close()
	}
	return nil
}

func (h *uploadQueue) Read(b []byte) (int, error) {
	if r := h.reader.Load(); r != nil {
		return (*r).Read(b)
	}

	for {
		if h.closed.Load() {
			return 0, io.EOF
		}

		// Read the lazily-created queue pointer under the same mutex Push
		// uses for ensureQueue, so the nil check cannot race with the
		// first Push. Once non-nil the channel field never changes again.
		h.writeCloseMutex.Lock()
		q := h.pushedPackets
		h.writeCloseMutex.Unlock()
		if q == nil {
			// Queue not created yet (L1 lazy): wait for the first Push
			// or a close. Poll at a bounded interval — the 2s gap
			// timeout is the outer bound on any stall here.
			time.Sleep(queuePollInterval)
			continue
		}

		if h.heap.Len() == 0 {
			packet, more := <-q
			if !more {
				return 0, io.EOF
			}
			// A channel slot is free now; wake one or more bounded Push
			// waiters so transient full-queue stalls become flow control,
			// not HTTP 404 upload drops.
			h.notifyRoom()
			if packet.Reader != nil {
				r := packet.Reader
				h.reader.Store(&r)
				return (*h.reader.Load()).Read(b)
			}
			h.heap.push(packet)
		}

		for h.heap.Len() > 0 {
			packet := h.heap.pop()
			n := 0

			if packet.Seq == h.nextSeq {
				h.gapSince = time.Time{}
				copy(b, packet.Payload)
				n = min(len(b), len(packet.Payload))

				if n < len(packet.Payload) {
					// partial read
					packet.Payload = packet.Payload[n:]
					h.heap.push(packet)
				} else {
					h.nextSeq = packet.Seq + 1
					freePacketPayload(&packet)
				}

				return n, nil
			}

			// misordered packet
			if packet.Seq > h.nextSeq {
				if h.heap.Len() > h.maxPackets {
					h.drainAndFree()
					return 0, errors.New("packet queue is too large")
				}
				// Start / extend gap timer while waiting for nextSeq.
				now := time.Now()
				if h.gapSince.IsZero() {
					h.gapSince = now
				} else if now.Sub(h.gapSince) > maxSeqGapWait {
					// Lost packet: close the stream rather than corrupt order
					// by skipping (security + correctness over silent gap fill).
					h.drainAndFree()
					return 0, errors.New("packet sequence gap timeout waiting for seq=", h.nextSeq)
				}
				h.heap.push(packet)
				// Bounded wait so a permanent hole cannot stall forever.
				waitLeft := maxSeqGapWait - now.Sub(h.gapSince)
				if waitLeft < 0 {
					waitLeft = 0
				}
				timer := time.NewTimer(waitLeft)
				select {
				case packet2, more := <-h.pushedPackets:
					timer.Stop()
					if !more {
						h.drainAndFree()
						return 0, io.EOF
					}
					h.notifyRoom()
					h.heap.push(packet2)
				case <-timer.C:
					h.drainAndFree()
					return 0, errors.New("packet sequence gap timeout waiting for seq=", h.nextSeq)
				}
			} else {
				// packet.Seq < h.nextSeq: duplicate — skip and return its payload.
				freePacketPayload(&packet)
			}
		}
		// all packets in heap were duplicates; loop back to wait for more
	}
}

// drainAndFree discards every queued packet and returns pooled payloads to
// the allocator. Called on abort paths (gap timeout, queue overflow, EOF)
// where the stream dies with packets still buffered.
func (h *uploadQueue) drainAndFree() {
	for h.heap.Len() > 0 {
		p := h.heap.pop()
		discardQueuedPacket(&p)
	}
}

// packetHeap is a min-heap of Packets ordered by Seq.
// Unlike container/heap, this uses concrete types to avoid interface{} boxing.
type packetHeap []Packet

func (h packetHeap) Len() int { return len(h) }

func (h *packetHeap) push(p Packet) {
	*h = append(*h, p)
	i := len(*h) - 1
	// sift up
	for i > 0 {
		parent := (i - 1) / 2
		if (*h)[i].Seq >= (*h)[parent].Seq {
			break
		}
		(*h)[i], (*h)[parent] = (*h)[parent], (*h)[i]
		i = parent
	}
}

func (h *packetHeap) pop() Packet {
	top := (*h)[0]
	n := len(*h) - 1
	(*h)[0] = (*h)[n]
	*h = (*h)[:n]
	// sift down
	if len(*h) > 0 {
		h.siftDown(0)
	}
	return top
}

func (h *packetHeap) siftDown(i int) {
	n := len(*h)
	for {
		smallest := i
		left := 2*i + 1
		right := 2*i + 2

		if left < n && (*h)[left].Seq < (*h)[smallest].Seq {
			smallest = left
		}
		if right < n && (*h)[right].Seq < (*h)[smallest].Seq {
			smallest = right
		}
		if smallest == i {
			break
		}
		(*h)[i], (*h)[smallest] = (*h)[smallest], (*h)[i]
		i = smallest
	}
}
