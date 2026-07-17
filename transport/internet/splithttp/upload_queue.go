package splithttp

// upload_queue is a specialized typed priority queue + channel to reorder
// packets by a sequence number. Uses a concrete-typed heap to avoid
// interface{} boxing/unboxing overhead from container/heap.

import (
	"io"
	"sync"
	"sync/atomic"

	"github.com/xtls/xray-core/common/errors"
)

type Packet struct {
	Reader  io.ReadCloser
	Payload []byte
	Seq     uint64
}

// PacketPool reuses Packet structs to reduce GC pressure.
var PacketPool = sync.Pool{
	New: func() any {
		return &Packet{}
	},
}

func NewPacket(reader io.ReadCloser, payload []byte, seq uint64) *Packet {
	p := PacketPool.Get().(*Packet)
	p.Reader = reader
	p.Payload = payload
	p.Seq = seq
	return p
}

func ReleasePacket(p *Packet) {
	p.Reader = nil
	p.Payload = nil
	p.Seq = 0
	PacketPool.Put(p)
}

type uploadQueue struct {
	reader          io.ReadCloser
	nomore          bool
	pushedPackets   chan Packet
	writeCloseMutex sync.Mutex
	heap            packetHeap
	nextSeq         uint64
	closed          atomic.Bool
	maxPackets      int
}

func NewUploadQueue(maxPackets int) *uploadQueue {
	return &uploadQueue{
		pushedPackets: make(chan Packet, maxPackets),
		heap:          make(packetHeap, 0, min(maxPackets, 64)),
		nextSeq:       0,
		maxPackets:    maxPackets,
	}
}

func (h *uploadQueue) Push(p Packet) error {
	// Serialize all channel sends with Close(). An unlocked try-send races
	// with close(pushedPackets) and trips the race detector under concurrent
	// upload/session teardown, even when a recover would swallow the panic.
	h.writeCloseMutex.Lock()
	defer h.writeCloseMutex.Unlock()

	if h.closed.Load() {
		return errors.New("packet queue closed")
	}
	if h.nomore {
		return errors.New("h.reader already exists")
	}
	if p.Reader != nil {
		h.nomore = true
	}
	// Prefer non-blocking when the queue still has capacity; otherwise block
	// under the same mutex so Close cannot interleave with the send.
	// Read() does not take writeCloseMutex, so a full queue still drains.
	select {
	case h.pushedPackets <- p:
		return nil
	default:
	}
	h.pushedPackets <- p
	return nil
}

func (h *uploadQueue) Close() error {
	h.writeCloseMutex.Lock()
	defer h.writeCloseMutex.Unlock()

	if !h.closed.Swap(true) {
		close(h.pushedPackets)
	}
	if h.reader != nil {
		return h.reader.Close()
	}
	return nil
}

func (h *uploadQueue) Read(b []byte) (int, error) {
	if h.reader != nil {
		return h.reader.Read(b)
	}

	for {
		if h.closed.Load() {
			return 0, io.EOF
		}

		if h.heap.Len() == 0 {
			packet, more := <-h.pushedPackets
			if !more {
				return 0, io.EOF
			}
			if packet.Reader != nil {
				h.reader = packet.Reader
				return h.reader.Read(b)
			}
			h.heap.push(packet)
		}

		for h.heap.Len() > 0 {
			packet := h.heap.pop()
			n := 0

			if packet.Seq == h.nextSeq {
				copy(b, packet.Payload)
				n = min(len(b), len(packet.Payload))

				if n < len(packet.Payload) {
					// partial read
					packet.Payload = packet.Payload[n:]
					h.heap.push(packet)
				} else {
					h.nextSeq = packet.Seq + 1
				}

				return n, nil
			}

			// misordered packet
			if packet.Seq > h.nextSeq {
				if h.heap.Len() > h.maxPackets {
					return 0, errors.New("packet queue is too large")
				}
				h.heap.push(packet)
				packet2, more := <-h.pushedPackets
				if !more {
					return 0, io.EOF
				}
				h.heap.push(packet2)
			}
			// packet.Seq < h.nextSeq: duplicate, skip and continue
		}
		// all packets in heap were duplicates; loop back to wait for more
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
