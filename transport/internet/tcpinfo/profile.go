package tcpinfo

import (
	"net"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/xtls/xray-core/transport/internet/quality"
)

// Profile is a TransportProfile that observes network path quality.
//
// Architecture principle: Profile only observes (Observe).
// XMUX/HEv3/Warmup decide (Decide).
//
// Usage:
//
//	prof := tcpinfo.NewProfile(conn)
//	prof.Start(context.Background())
//	defer prof.Stop()
//
//	// Read (lock-free, zero-alloc):
//	snap := prof.Snapshot()
type Profile struct {
	conn      net.Conn
	collector Collector
	interval  time.Duration
	maxStale  time.Duration

	// Immutable snapshot — atomic swap, never modified after creation.
	snapshot unsafe.Pointer // *quality.Snapshot

	// History ring buffer for Debug API.
	history quality.History

	// Callback when snapshot is updated. Called from background goroutine.
	onUpdate func(*quality.Snapshot)

	// Lifecycle
	stopCh  chan struct{}
	stopped atomic.Bool
}

// NewProfile creates a new TransportProfile for the given connection.
// If collector is nil, a platform-appropriate default is used.
func NewProfile(conn net.Conn, collector Collector) *Profile {
	if collector == nil {
		collector = newDefaultCollector()
	}
	p := &Profile{
		conn:      conn,
		collector: collector,
		interval:  DefaultInterval,
		maxStale:  DefaultMaxStale,
		stopCh:    make(chan struct{}),
	}
	// Initialize with unknown snapshot
	unknown := quality.NewUnknownSnapshot()
	atomic.StorePointer(&p.snapshot, unsafe.Pointer(unknown))
	return p
}

// SetInterval sets the sampling interval. Must be called before Start.
func (p *Profile) SetInterval(d time.Duration) {
	p.interval = d
}

// SetMaxStale sets the maximum snapshot age before it's considered stale.
func (p *Profile) SetMaxStale(d time.Duration) {
	p.maxStale = d
}

// OnUpdate sets a callback that is called each time a new snapshot is collected.
// The callback runs in the background goroutine — keep it fast and non-blocking.
func (p *Profile) OnUpdate(fn func(*quality.Snapshot)) {
	p.onUpdate = fn
}

// Snapshot returns the current immutable snapshot. Lock-free read.
func (p *Profile) Snapshot() *quality.Snapshot {
	return (*quality.Snapshot)(atomic.LoadPointer(&p.snapshot))
}

// History returns the debug history ring buffer.
func (p *Profile) History() *quality.History {
	return &p.history
}

// Start begins background sampling. Safe to call multiple times.
func (p *Profile) Start() {
	if p.stopped.Load() {
		return
	}
	go p.loop()
}

// Stop halts background sampling.
func (p *Profile) Stop() {
	if p.stopped.CompareAndSwap(false, true) {
		close(p.stopCh)
	}
}

func (p *Profile) loop() {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	// Collect immediately on start
	p.collect()

	for {
		select {
		case <-ticker.C:
			p.collect()
		case <-p.stopCh:
			return
		}
	}
}

func (p *Profile) collect() {
	snap, err := p.collector.Collect(p.conn)
	if err != nil || snap == nil {
		return
	}
	atomic.StorePointer(&p.snapshot, unsafe.Pointer(snap))

	// Push to history for Debug API
	q := snap.Quality
	var rtt int64
	if snap.RTT.Valid {
		rtt = snap.RTT.Value.Microseconds()
	}
	var loss float64
	if snap.Loss.Valid {
		loss = snap.Loss.Value
	}
	p.history.Push(rtt, loss, q.Overall, snap.Confidence)

	// Notify listener (XMUX scoreClient, etc.)
	if p.onUpdate != nil {
		p.onUpdate(snap)
	}
}
