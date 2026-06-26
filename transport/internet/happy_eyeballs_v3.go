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
	IP       net.IP
	Priority int64   // from DNS SVCB/HTTPS record, lower = higher priority
	RTT      int64   // smoothed RTT in nanoseconds
	FailRate float64 // V2.0: EWMA failure rate 0.0-1.0 (replaces Successes/Fails)

	// V2.0: loss rate from TransportProfile (0-10000 fixed-point, 0=none, 10000=100%)
	LastLoss int64
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
//
// V2.0 formula:
//
//	base = priority * 1e9
//	rttTerm = rttNs * (1 + failRate*10 + lossPenalty)
//
// failRate comes from EWMA (0.0-1.0), no counters needed.
// lossPenalty comes from external quality data (TCP_INFO).
func (s *HappyIPScore) score() float64 {
	rttScore := float64(clampRTT(s.RTT))

	// V2.0: loss penalty (0-1.0 scale, increases RTT effective cost)
	var lossPenalty float64
	if s.LastLoss > 0 {
		lossPenalty = float64(s.LastLoss) / 10000.0
	}

	rttTerm := rttScore * (1 + s.FailRate*10 + lossPenalty)
	return float64(s.Priority)*1e9 + rttTerm
}

// HappyIPRecord tracks historical connection metrics for an IP.
//
// V2.0: Uses EWMA for failure rate (no dual counters, no cleanup goroutine).
// Decay factor 0.95 → ~14 successes to halve failure rate.
type HappyIPRecord struct {
	smoothedRTT atomic.Int64 // nanoseconds, EWMA
	lastSeen    atomic.Int64 // Unix timestamp of last activity
	lastLoss    atomic.Int64 // V2.0: loss rate 0-10000 fixed-point, from TransportProfile

	// V2.0 EWMA failure rate (0-10000 fixed-point, replaces fails/successes counters).
	// On success: rate = rate * 9500 / 10000
	// On failure: rate = rate * 9500 / 10000 + 500
	failureRate atomic.Int64 // fixed-point × 10000 to avoid float64 atomic
}

const ewmaDecayNumerator = 9500 // 0.95 × 10000
const ewmaDecayDenominator = 10000
const ewmaFailWeight = 500 // (1-0.95) × 10000

func (r *HappyIPRecord) getFailureRate() float64 {
	return float64(r.failureRate.Load()) / 10000.0
}

// getFails/getSuccesses kept for backward compatibility with score().
// Returns approximated values from EWMA.
func (r *HappyIPRecord) getFails() int {
	rate := r.failureRate.Load()
	if rate == 0 {
		return 0
	}
	// Approximate: if rate=0.1 (1000/10000), and we've had some observations
	// We return a pseudo-count that makes score() behave similarly
	return int(rate / 100) // scale to reasonable range
}

func (r *HappyIPRecord) getSuccesses() int {
	rate := r.failureRate.Load()
	if rate == 0 {
		return 10 // new connection with no failures → high success
	}
	return int((10000 - rate) / 100)
}

func (r *HappyIPRecord) getSmoothedRTT() int64 { return r.smoothedRTT.Load() }
func (r *HappyIPRecord) getLoss() int64        { return r.lastLoss.Load() }

// UpdateLoss sets the loss rate from TransportProfile (0-10000 fixed-point).
func (r *HappyIPRecord) UpdateLoss(lossRate int64) {
	r.lastLoss.Store(lossRate)
}

func (r *HappyIPRecord) recordSuccess(rtt time.Duration) {
	r.lastSeen.Store(time.Now().Unix())
	// Update EWMA failure rate: rate *= 0.95 (decays toward 0)
	for {
		old := r.failureRate.Load()
		newRate := old * ewmaDecayNumerator / ewmaDecayDenominator
		if r.failureRate.CompareAndSwap(old, newRate) {
			break
		}
	}
	// Update RTT EWMA
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
	// Update EWMA failure rate: rate = rate*0.95 + 0.05 (decays toward 1.0)
	for {
		old := r.failureRate.Load()
		newRate := old*ewmaDecayNumerator/ewmaDecayDenominator + ewmaFailWeight
		if newRate > 10000 {
			newRate = 10000
		}
		if r.failureRate.CompareAndSwap(old, newRate) {
			return
		}
	}
}

// HappyIPDB is the global database for per-IP historical metrics.
type HappyIPDB struct {
	mu      sync.RWMutex
	records map[string]*HappyIPRecord
	quit    chan struct{}
}

const (
	// ipRecordTTL is how long an IP record lives without activity.
	ipRecordTTL = 10 * time.Minute
	// ipRecordCleanupInterval is how often to run cleanup.
	ipRecordCleanupInterval = 5 * time.Minute
)

var globalHappyIPDB = &HappyIPDB{
	records: make(map[string]*HappyIPRecord),
	quit:    make(chan struct{}),
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

	for {
		select {
		case <-ticker.C:
			db.cleanup()
		case <-db.quit:
			return
		}
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
			IP:       ip,
			Priority: priority + ipv6Boost,
			RTT:      record.getSmoothedRTT(),
			FailRate: record.getFailureRate(), // V2.0: EWMA failure rate
			LastLoss: record.getLoss(),
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
