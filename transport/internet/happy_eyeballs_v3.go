package internet

import (
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
)

// scoreIPsInto fills dst with scored IPs (growing if needed) and sorts in place.
// Prefer this on hot dial paths with a reusable buffer for 0 steady-state allocs
// beyond the first growth. scoreIPs remains for tests/simple callers.
func scoreIPsInto(dst []HappyIPScore, ips []xnet.IP, prioritizeIPv6 bool, svcbPriorities map[ipKey]int64) []HappyIPScore {
	n := len(ips)
	if n == 0 {
		if dst == nil {
			return nil
		}
		return dst[:0]
	}
	if cap(dst) < n {
		dst = make([]HappyIPScore, n)
	} else {
		dst = dst[:n]
	}
	db := globalHappyIPDB
	haveSVCB := len(svcbPriorities) > 0
	db.mu.RLock()
	for i, ip := range ips {
		var (
			rtt      int64
			failRate float64
			lastLoss int64
			priority int64
		)
		k, ok := ipToKey(ip)
		if ok {
			if record := db.records[k]; record != nil {
				rtt = record.getSmoothedRTT()
				failRate = record.getFailureRate()
				lastLoss = record.getLoss()
			}
			if haveSVCB {
				priority = svcbPriorities[k]
			}
		}

		ipv6Boost := int64(0)
		v4 := ip.To4()
		if prioritizeIPv6 {
			if v4 == nil {
				ipv6Boost = -1e9
			}
		} else if v4 != nil {
			ipv6Boost = -1e9
		}

		dst[i] = HappyIPScore{
			IP:       ip,
			Priority: priority + ipv6Boost,
			RTT:      rtt,
			FailRate: failRate,
			LastLoss: lastLoss,
		}
	}
	db.mu.RUnlock()
	sortIPScores(dst)
	return dst
}

// HappyIPScore holds scoring data for a single IP address used in v3 sorting.
type HappyIPScore struct {
	IP       xnet.IP
	Priority int64   // from DNS SVCB/HTTPS record, lower = higher priority
	RTT      int64   // smoothed RTT in nanoseconds
	FailRate float64 // V2.0: EWMA failure rate 0.0-1.0 (replaces Successes/Fails)

	// V2.0: loss rate from TransportProfile (0-10000 fixed-point, 0=none, 10000=100%)
	LastLoss int64

	// cachedScore is filled by sortIPScores before sorting so the comparator
	// does not recompute float score O(n log n) times.
	cachedScore float64
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
//	rttTerm = rttNs * (1 + failRate*10 + lossPenalty*5)
//
// failRate comes from EWMA (0.0-1.0), no counters needed.
// lossPenalty comes from external quality data (TCP_INFO), weighted 5x.
func (s *HappyIPScore) score() float64 {
	rttScore := float64(clampRTT(s.RTT))
	// Common path: unsampled / healthy IP has no fail or loss samples.
	if s.FailRate == 0 && s.LastLoss == 0 {
		return float64(s.Priority)*1e9 + rttScore
	}
	// V2.0: loss penalty (0-1.0 scale, increases RTT effective cost)
	lossPenalty := float64(s.LastLoss) / 10000.0
	rttTerm := rttScore * (1 + s.FailRate*10 + lossPenalty*5)
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

func (r *HappyIPRecord) getSmoothedRTT() int64 { return r.smoothedRTT.Load() }
func (r *HappyIPRecord) getLoss() int64        { return r.lastLoss.Load() }

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

// ipKey is a fixed-size map key for IPv4/IPv6 without heap string conversion.
// IPv4 is stored in the first 4 bytes with family=4; IPv6 uses all 16 bytes with family=6.
type ipKey struct {
	addr   [16]byte
	family uint8 // 4 or 6
}

func ipToKey(ip net.IP) (ipKey, bool) {
	var k ipKey
	if v4 := ip.To4(); v4 != nil {
		k.family = 4
		copy(k.addr[:4], v4)
		return k, true
	}
	v6 := ip.To16()
	if v6 == nil {
		return k, false
	}
	k.family = 6
	copy(k.addr[:], v6)
	return k, true
}

// HappyIPDB is the global database for per-IP historical metrics.
type HappyIPDB struct {
	mu      sync.RWMutex
	records map[ipKey]*HappyIPRecord
	quit    chan struct{}
}

const (
	// ipRecordTTL is how long an IP record lives without activity.
	ipRecordTTL = 10 * time.Minute
	// ipRecordCleanupInterval is how often to run cleanup.
	ipRecordCleanupInterval = 5 * time.Minute
)

var globalHappyIPDB = &HappyIPDB{
	records: make(map[ipKey]*HappyIPRecord),
	quit:    make(chan struct{}),
}

func init() {
	go globalHappyIPDB.cleanupLoop()
}

// getByIP returns (and creates if needed) the record for ip.
func (db *HappyIPDB) getByIP(ip net.IP) *HappyIPRecord {
	k, ok := ipToKey(ip)
	if !ok {
		return nil
	}
	db.mu.RLock()
	r, exists := db.records[k]
	db.mu.RUnlock()
	if exists {
		return r
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if r, exists = db.records[k]; exists {
		return r
	}
	r = &HappyIPRecord{}
	r.lastSeen.Store(time.Now().Unix())
	db.records[k] = r
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
// Lookup is read-only: unknown IPs use default RTT/fail without writing the DB.
// Success/fail paths still create records via getByIP.
// svcbPriorities uses ipKey so scoring never allocates via ip.String().
func scoreIPs(ips []xnet.IP, prioritizeIPv6 bool, svcbPriorities map[ipKey]int64) []HappyIPScore {
	return scoreIPsInto(nil, ips, prioritizeIPv6, svcbPriorities)
}

func sortIPScores(scores []HappyIPScore) {
	// Precompute once: comparator would otherwise re-run score()
	// O(n log n) times with float/clamp work on every comparison.
	for i := range scores {
		scores[i].cachedScore = scores[i].score()
	}
	// slices.SortFunc avoids sort.Slice reflection/interface allocs.
	// Equal scores are rare; dial order among ties is not a correctness contract.
	slices.SortFunc(scores, func(a, b HappyIPScore) int {
		switch {
		case a.cachedScore < b.cachedScore:
			return -1
		case a.cachedScore > b.cachedScore:
			return 1
		default:
			return 0
		}
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
	for {
		current := time.Duration(tc.currentDelay.Load())
		var newDelay time.Duration
		if rtt < 100*time.Millisecond {
			newDelay = current * 8 / 10
			if newDelay < 10*time.Millisecond {
				newDelay = 10 * time.Millisecond
			}
		} else if rtt > 500*time.Millisecond {
			newDelay = current * 12 / 10
			if newDelay > 1*time.Second {
				newDelay = 1 * time.Second
			}
		} else {
			return // RTT in 100-500ms range, no adjustment needed
		}
		if tc.currentDelay.CompareAndSwap(int64(current), int64(newDelay)) {
			return
		}
	}
}

func (tc *TryController) OnFail() {
	for {
		current := time.Duration(tc.currentDelay.Load())
		newDelay := current * 11 / 10
		if newDelay > 1*time.Second {
			newDelay = 1 * time.Second
		}
		if tc.currentDelay.CompareAndSwap(int64(current), int64(newDelay)) {
			return
		}
	}
}
