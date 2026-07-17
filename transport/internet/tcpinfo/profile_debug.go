package tcpinfo

import (
	"fmt"
	"time"

	"github.com/xtls/xray-core/transport/internet/quality"
)

// DebugInfo is a JSON-serializable view of the TransportProfile state.
// Used by Debug API for troubleshooting.
type DebugInfo struct {
	Network NetworkDebug   `json:"networkProfile"`
	History []HistoryEntry `json:"history,omitempty"`
}

// NetworkDebug contains the current snapshot in a human-readable format.
type NetworkDebug struct {
	RTT        string          `json:"rtt,omitempty"`
	RTTVar     string          `json:"rttVar,omitempty"`
	Loss       float64         `json:"loss,omitempty"`
	Retrans    uint32          `json:"retrans,omitempty"`
	Unacked    uint32          `json:"unacked,omitempty"`
	Quality    quality.Quality `json:"quality"`
	Confidence uint8           `json:"confidence"`
	Source     string          `json:"source"`
	Age        string          `json:"age"`
	IsStale    bool            `json:"isStale"`
	Reason     []string        `json:"reason"`
}

// HistoryEntry is a single point in the debug history ring buffer.
type HistoryEntry struct {
	RTT        int64   `json:"rtt"`
	Loss       float64 `json:"loss"`
	Quality    uint8   `json:"quality"`
	Confidence uint8   `json:"confidence"`
}

// GetDebugInfo builds a DebugInfo from the current profile state.
func (p *Profile) GetDebugInfo() DebugInfo {
	snap := p.Snapshot()
	info := DebugInfo{
		Network: networkDebugFromSnapshot(snap),
	}

	// Build history entries — call all extractors; each is atomic under its own RLock.
	// RTT/Loss/Quality/Confidence each return a snapshot that may differ slightly
	// if a Push happens between calls, but this is acceptable for debug output.
	h := p.History()
	rtts := h.RTT()
	if len(rtts) > 0 {
		losses := h.Loss()
		qualities := h.Quality()
		confs := h.Confidence()
		info.History = make([]HistoryEntry, len(rtts))
		for i := range rtts {
			info.History[i] = HistoryEntry{
				RTT:        rtts[i],
				Loss:       losses[i],
				Quality:    qualities[i],
				Confidence: confs[i],
			}
		}
	}

	return info
}

func networkDebugFromSnapshot(snap *quality.Snapshot) NetworkDebug {
	nd := NetworkDebug{
		Quality:    snap.Quality,
		Confidence: snap.Confidence,
		Source:     string(snap.Source),
		Age:        snap.Age().Truncate(time.Millisecond).String(),
		IsStale:    snap.IsStale(DefaultMaxStale),
	}

	if snap.RTT.Valid {
		nd.RTT = snap.RTT.Value.Truncate(time.Microsecond).String()
	}
	if snap.RTTVar.Valid {
		nd.RTTVar = snap.RTTVar.Value.Truncate(time.Microsecond).String()
	}
	if snap.Loss.Valid {
		nd.Loss = snap.Loss.Value
	}
	if snap.Retrans.Valid {
		nd.Retrans = snap.Retrans.Value
	}
	if snap.Unacked.Valid {
		nd.Unacked = snap.Unacked.Value
	}

	nd.Reason = buildReasons(snap)
	return nd
}

// buildReasons produces human-readable reasons for the quality score.
func buildReasons(snap *quality.Snapshot) []string {
	var reasons []string

	if snap.RTT.Valid {
		rttMs := snap.RTT.Value.Milliseconds()
		reasons = append(reasons, fmt.Sprintf("rtt=%dms", rttMs))
	}
	if snap.Loss.Valid {
		lossPct := snap.Loss.Value * 100
		if lossPct > 0.1 {
			reasons = append(reasons, fmt.Sprintf("loss_rate=%.1f%%", lossPct))
		}
	}
	if snap.Retrans.Valid && snap.Retrans.Value > 0 {
		reasons = append(reasons, fmt.Sprintf("retrans=%d", snap.Retrans.Value))
	}
	if snap.Unacked.Valid && snap.Unacked.Value > 0 {
		reasons = append(reasons, fmt.Sprintf("unacked=%d", snap.Unacked.Value))
	}
	if snap.Confidence < 50 {
		reasons = append(reasons, fmt.Sprintf("confidence=%d (low)", snap.Confidence))
	}
	if snap.IsStale(DefaultMaxStale) {
		reasons = append(reasons, fmt.Sprintf("stale=%s", snap.Age().Truncate(time.Second)))
	}

	return reasons
}
