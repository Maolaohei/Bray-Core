package quality

import "time"

// Source identifies where the snapshot data came from.
type Source string

const (
	SourceTCPInfo   Source = "tcp_info"
	SourceQUICStats Source = "quic_stats"
	SourceEstimated Source = "estimated"
	SourceUnknown   Source = "unknown"
)

// Snapshot is an immutable, point-in-time observation of network path quality.
//
// Invariant: once created, a Snapshot is never modified.
// Updates use atomic swap:
//
//	new := NewSnapshot(...)
//	atomic.StorePointer(&m.snapshot, unsafe.Pointer(new))
//
// Consumers read via:
//
//	snap := (*Snapshot)(atomic.LoadPointer(&m.snapshot))
type Snapshot struct {
	Timestamp time.Time
	Source    Source

	RTT     Duration
	RTTVar  Duration
	Loss    Float64
	Retrans Uint32
	Unacked Uint32

	Quality    Quality
	Confidence uint8 // 0-100, how much we trust this data
}

// NewUnknownSnapshot creates a snapshot with no valid data and zero confidence.
func NewUnknownSnapshot() *Snapshot {
	return &Snapshot{
		Timestamp:  time.Now(),
		Source:     SourceUnknown,
		Quality:    UnknownQuality(),
		Confidence: 0,
	}
}

// IsStale returns true if the snapshot is older than maxAge.
func (s *Snapshot) IsStale(maxAge time.Duration) bool {
	return time.Since(s.Timestamp) > maxAge
}

// Age returns how old the snapshot is.
func (s *Snapshot) Age() time.Duration {
	return time.Since(s.Timestamp)
}
