package splithttp

import (
	"strings"
	"sync"
	"time"
)

// Sticky last-good multi-endpoint winner per destination pool (Wave-5).
// When multi-endpoint race is enabled, prefer the last successful endpoint
// first (head-start in RaceDialEndpoints) so recovered edges win without
// waiting the race stagger on every dial.
//
// TTL-bounded; default ON when multi-endpoint is active.
// Opt-out: headers["x-bray-sticky-endpoint"]=false|0|off|no.
// Per-entry TTL is stored at remember time (no process-global header mutation).

// StickyEndpointTTL is the default TTL when an entry has no per-entry TTL.
var StickyEndpointTTL = 10 * time.Minute

// StickyEndpointMaxEntries bounds the process-local sticky endpoint map.
const StickyEndpointMaxEntries = 256

type stickyEndpointEntry struct {
	endpoint string
	at       time.Time
	ttl      time.Duration // 0 => use StickyEndpointTTL at lookup
}

var (
	stickyEPMu sync.Mutex
	stickyEP   = make(map[string]stickyEndpointEntry, 64)
)

// StickyEndpointEnabled reports whether endpoint sticky is on (default true).
// Opt-out: x-bray-sticky-endpoint = false|0|off|no.
func StickyEndpointEnabled(headers map[string]string) bool {
	if headers == nil {
		return true
	}
	for k, v := range headers {
		if strings.EqualFold(k, "x-bray-sticky-endpoint") {
			switch strings.ToLower(strings.TrimSpace(v)) {
			case "0", "false", "off", "no":
				return false
			case "1", "true", "on", "yes":
				return true
			}
		}
	}
	return true
}

// stickyEndpointKey builds a stable key for a multi-endpoint dial pool.
// Uses primary dest + optional host/SNI so different frontnames do not share affinity.
func stickyEndpointKey(primary, host string) string {
	return stickyDestKey(primary, host)
}

func stickyEndpointExpired(e stickyEndpointEntry, now time.Time) bool {
	ttl := e.ttl
	if ttl <= 0 {
		ttl = StickyEndpointTTL
	}
	return ttl > 0 && now.Sub(e.at) > ttl
}

// LookupStickyEndpoint returns a non-expired sticky endpoint for key.
func LookupStickyEndpoint(key string) (string, bool) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", false
	}
	now := time.Now()
	stickyEPMu.Lock()
	defer stickyEPMu.Unlock()
	e, ok := stickyEP[key]
	if !ok {
		return "", false
	}
	if stickyEndpointExpired(e, now) {
		delete(stickyEP, key)
		return "", false
	}
	return e.endpoint, e.endpoint != ""
}

// RememberStickyEndpoint stores last-good endpoint for key using default TTL.
func RememberStickyEndpoint(key, endpoint string) {
	RememberStickyEndpointTTL(key, endpoint, 0)
}

// RememberStickyEndpointTTL stores last-good endpoint with optional per-entry TTL.
func RememberStickyEndpointTTL(key, endpoint string, ttl time.Duration) {
	key = strings.TrimSpace(key)
	endpoint = strings.TrimSpace(endpoint)
	if key == "" || endpoint == "" {
		return
	}
	if ttl < 0 {
		ttl = 0
	}
	stickyEPMu.Lock()
	defer stickyEPMu.Unlock()
	if len(stickyEP) >= StickyEndpointMaxEntries {
		var drop string
		oldest := time.Now()
		for k, e := range stickyEP {
			if e.at.Before(oldest) {
				oldest = e.at
				drop = k
			}
		}
		if drop != "" {
			delete(stickyEP, drop)
		}
	}
	stickyEP[key] = stickyEndpointEntry{endpoint: endpoint, at: time.Now(), ttl: ttl}
	recordEndpointStickyRemember()
}

// ForgetStickyEndpoint removes sticky preference for key (if any).
func ForgetStickyEndpoint(key string) {
	key = strings.TrimSpace(key)
	if key == "" {
		return
	}
	stickyEPMu.Lock()
	delete(stickyEP, key)
	stickyEPMu.Unlock()
}

// ApplyStickyEndpoints reorders the race list so sticky is first when present.
// Other endpoints keep relative order as race backups (staggered head-start).
func ApplyStickyEndpoints(endpoints []string, sticky string) []string {
	sticky = strings.TrimSpace(sticky)
	if sticky == "" || len(endpoints) <= 1 {
		return endpoints
	}
	idx := -1
	for i, ep := range endpoints {
		if strings.EqualFold(strings.TrimSpace(ep), sticky) {
			idx = i
			break
		}
	}
	if idx <= 0 {
		return endpoints
	}
	out := make([]string, 0, len(endpoints))
	out = append(out, endpoints[idx])
	for i, ep := range endpoints {
		if i == idx {
			continue
		}
		out = append(out, ep)
	}
	return out
}

// ClearStickyEndpointForTest clears the sticky endpoint map (tests only).
func ClearStickyEndpointForTest() {
	stickyEPMu.Lock()
	stickyEP = make(map[string]stickyEndpointEntry, 64)
	stickyEPMu.Unlock()
}
