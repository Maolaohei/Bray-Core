package internet

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/net"
)

// HappyIPScore holds scoring data for a single IP address used in v3 sorting.
type HappyIPScore struct {
	IP        net.IP
	Priority  int64 // from DNS SVCB/HTTPS record, lower = higher priority
	RTT       int64 // smoothed RTT in nanoseconds
	Successes int
	Fails     int
}

// score computes a composite priority score. Lower is better.
func (s *HappyIPScore) score() float64 {
	if s.Successes+s.Fails == 0 {
		return float64(s.Priority)*1e9 + float64(s.RTT)
	}
	failRate := float64(s.Fails) / float64(s.Successes+s.Fails)
	rttScore := float64(s.RTT)
	if s.RTT == 0 {
		rttScore = 500e6 // default 500ms for unknown RTT
	}
	return float64(s.Priority)*1e9 + rttScore*(1+failRate*10)
}

// HappyIPRecord tracks historical connection metrics for an IP.
type HappyIPRecord struct {
	fails       atomic.Int64
	successes   atomic.Int64
	smoothedRTT atomic.Int64 // nanoseconds, EWMA
}

func (r *HappyIPRecord) getFails() int        { return int(r.fails.Load()) }
func (r *HappyIPRecord) getSuccesses() int     { return int(r.successes.Load()) }
func (r *HappyIPRecord) getSmoothedRTT() int64 { return r.smoothedRTT.Load() }

func (r *HappyIPRecord) recordSuccess(rtt time.Duration) {
	r.successes.Add(1)
	newRTT := int64(rtt)
	for {
		old := r.smoothedRTT.Load()
		var smoothed int64
		if old == 0 {
			smoothed = newRTT
		} else {
			smoothed = (old*8 + newRTT*2) / 10
		}
		if r.smoothedRTT.CompareAndSwap(old, smoothed) {
			return
		}
	}
}

func (r *HappyIPRecord) recordFail() {
	r.fails.Add(1)
}

// HappyIPDB is the global database for per-IP historical metrics.
type HappyIPDB struct {
	mu      sync.RWMutex
	records map[string]*HappyIPRecord
}

var globalHappyIPDB = &HappyIPDB{
	records: make(map[string]*HappyIPRecord),
}

func (db *HappyIPDB) get(ip string) *HappyIPRecord {
	db.mu.RLock()
	r, ok := db.records[ip]
	db.mu.RUnlock()
	if ok {
		return r
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	// double check
	if r, ok = db.records[ip]; ok {
		return r
	}
	r = &HappyIPRecord{}
	db.records[ip] = r
	return r
}

// scoreIPs scores and sorts IPs by v3 priority. Lower score = higher priority.
func scoreIPs(ips []net.IP, prioritizeIPv6 bool, svcbPriorities map[string]int64) []HappyIPScore {
	scores := make([]HappyIPScore, 0, len(ips))
	for _, ip := range ips {
		ipStr := ip.String()
		record := globalHappyIPDB.get(ipStr)

		priority := int64(0)
		if p, ok := svcbPriorities[ipStr]; ok {
			priority = p
		}

		ipv6Boost := int64(0)
		if prioritizeIPv6 && ip.To4() == nil {
			ipv6Boost = -1e9
		} else if !prioritizeIPv6 && ip.To4() != nil {
			ipv6Boost = -1e9
		}

		scores = append(scores, HappyIPScore{
			IP:        ip,
			Priority:  priority + ipv6Boost,
			RTT:       record.getSmoothedRTT(),
			Successes: record.getSuccesses(),
			Fails:     record.getFails(),
		})
	}

	sortIPScores(scores)
	return scores
}

func sortIPScores(scores []HappyIPScore) {
	sort.SliceStable(scores, func(i, j int) bool {
		return scores[i].score() < scores[j].score()
	})
}

// TryController manages dynamic concurrency for connection attempts.
type TryController struct {
	maxConcurrent int
	currentActive int32
	baseDelay     time.Duration
	currentDelay  atomic.Int64 // nanoseconds
}

func NewTryController(maxConcurrent int, baseDelay time.Duration) *TryController {
	tc := &TryController{
		maxConcurrent: maxConcurrent,
		baseDelay:     baseDelay,
	}
	tc.currentDelay.Store(int64(baseDelay))
	return tc
}

func (tc *TryController) CanTry() bool {
	return int(atomic.LoadInt32(&tc.currentActive)) < tc.maxConcurrent
}

func (tc *TryController) OnStart() {
	atomic.AddInt32(&tc.currentActive, 1)
}

func (tc *TryController) OnEnd() {
	atomic.AddInt32(&tc.currentActive, -1)
}

func (tc *TryController) GetDelay() time.Duration {
	return time.Duration(tc.currentDelay.Load())
}

func (tc *TryController) OnSuccess(rtt time.Duration) {
	current := time.Duration(tc.currentDelay.Load())
	if rtt < 100*time.Millisecond {
		newDelay := current * 8 / 10
		if newDelay < 10*time.Millisecond {
			newDelay = 10 * time.Millisecond
		}
		tc.currentDelay.Store(int64(newDelay))
	} else if rtt > 500*time.Millisecond {
		newDelay := current * 12 / 10
		if newDelay > 1*time.Second {
			newDelay = 1 * time.Second
		}
		tc.currentDelay.Store(int64(newDelay))
	}
}

func (tc *TryController) OnFail() {
	current := time.Duration(tc.currentDelay.Load())
	newDelay := current * 11 / 10
	if newDelay > 1*time.Second {
		newDelay = 1 * time.Second
	}
	tc.currentDelay.Store(int64(newDelay))
}

func defaultRTT() time.Duration {
	return 500 * time.Millisecond
}
