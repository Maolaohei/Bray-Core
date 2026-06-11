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

const (
	// defaultSmoothedRTT is the assumed RTT (in ns) for IPs with no sample.
	// 100ms is conservative enough to not dominate scoring but high enough
	// to not let unsampled IPs always win.
	defaultSmoothedRTT = 100 * time.Millisecond

	// maxRTTCap is the upper bound for RTT in scoring (in ns).
	// Prevents pathological RTTs from inverting the score.
	maxRTTCap = 999 * time.Millisecond
)

// clampRTT caps RTT at maxRTTCap. If rtt is 0 (no sample), returns defaultSmoothedRTT.
func clampRTT(rtt int64) int64 {
	if rtt == 0 {
		return int64(defaultSmoothedRTT)
	}
	if rtt > int64(maxRTTCap) {
		return int64(maxRTTCap)
	}
	return rtt
}

// score computes a composite priority score. Lower is better.
func (s *HappyIPScore) score() float64 {
	failRate := float64(0)
	if s.Successes+s.Fails > 0 {
		failRate = float64(s.Fails) / float64(s.Successes+s.Fails)
	}
	rttScore := float64(clampRTT(s.RTT))
	return float64(s.Priority)*1e9 + rttScore*(1+failRate*10)
}

// HappyIPRecord tracks historical connection metrics for an IP.
type HappyIPRecord struct {
	fails       atomic.Int64
	successes   atomic.Int64
	smoothedRTT atomic.Int64 // nanoseconds, EWMA
	lastSeen    atomic.Int64 // Unix timestamp of last activity
}

func (r *HappyIPRecord) getFails() int         { return int(r.fails.Load()) }
func (r *HappyIPRecord) getSuccesses() int     { return int(r.successes.Load()) }
func (r *HappyIPRecord) getSmoothedRTT() int64 { return r.smoothedRTT.Load() }

func (r *HappyIPRecord) recordSuccess(rtt time.Duration) {
	r.lastSeen.Store(time.Now().Unix())
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
	r.lastSeen.Store(time.Now().Unix())
	r.fails.Add(1)
}

// HappyIPDB is the global database for per-IP historical metrics.
type HappyIPDB struct {
	mu      sync.RWMutex
	records map[string]*HappyIPRecord
}

const (
	// ipRecordTTL is how long an IP record lives without activity.
	ipRecordTTL = 10 * time.Minute
	// ipRecordCleanupInterval is how often to run cleanup.
	ipRecordCleanupInterval = 5 * time.Minute
)

var globalHappyIPDB = &HappyIPDB{
	records: make(map[string]*HappyIPRecord),
}

func init() {
	go globalHappyIPDB.cleanupLoop()
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
	r = &HappyIPRecord{lastSeen: atomic.Int64{}}
	r.lastSeen.Store(time.Now().Unix())
	db.records[ip] = r
	return r
}

// cleanupLoop periodically removes expired IP records.
func (db *HappyIPDB) cleanupLoop() {
	ticker := time.NewTicker(ipRecordCleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		db.cleanup()
	}
}

// cleanup removes IP records that haven't been seen within TTL.
func (db *HappyIPDB) cleanup() {
	now := time.Now().Unix()
	cutoff := now - int64(ipRecordTTL.Seconds())

	db.mu.Lock()
	defer db.mu.Unlock()

	for ip, record := range db.records {
		if record.lastSeen.Load() < cutoff {
			delete(db.records, ip)
		}
	}
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
