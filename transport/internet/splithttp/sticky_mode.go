package splithttp

import (
	"strings"
	"sync"
	"time"
)

// Sticky last-good XHTTP mode per destination (Wave-4).
// When mode cascade is allowed, prefer the last successful mode to avoid
// repeated stream-one failures on CDN edges that only allow packet-up.
//
// TTL-bounded so the ladder can re-probe after recovery. Opt-out via
// headers["x-bray-sticky-mode"]=false|0|off|no. Default: enabled when cascade is allowed.

// StickyModeTTL is how long a remembered mode is preferred.
var StickyModeTTL = 10 * time.Minute

// StickyModeMaxEntries bounds the process-local sticky map.
const StickyModeMaxEntries = 256

type stickyEntry struct {
	mode string
	at   time.Time
}

var (
	stickyMu   sync.Mutex
	stickyMode = make(map[string]stickyEntry, 64)
)

// StickyModeEnabled reports whether sticky preference is on (default true).
// Opt-out: x-bray-sticky-mode = false|0|off|no.
func StickyModeEnabled(headers map[string]string) bool {
	if headers == nil {
		return true
	}
	for k, v := range headers {
		if strings.EqualFold(k, "x-bray-sticky-mode") {
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

// stickyDestKey builds a stable key from dial target + HTTP host/SNI.
func stickyDestKey(destNetAddr, host string) string {
	destNetAddr = strings.TrimSpace(destNetAddr)
	host = strings.TrimSpace(host)
	if host == "" {
		return destNetAddr
	}
	return destNetAddr + "|" + strings.ToLower(host)
}

// LookupStickyMode returns a non-expired sticky mode for key.
func LookupStickyMode(key string) (string, bool) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", false
	}
	now := time.Now()
	stickyMu.Lock()
	defer stickyMu.Unlock()
	e, ok := stickyMode[key]
	if !ok {
		return "", false
	}
	if StickyModeTTL > 0 && now.Sub(e.at) > StickyModeTTL {
		delete(stickyMode, key)
		return "", false
	}
	return e.mode, e.mode != ""
}

// RememberStickyMode stores last-good mode for key.
func RememberStickyMode(key, mode string) {
	key = strings.TrimSpace(key)
	mode = NormalizeXHTTPMode(mode)
	if key == "" || mode == "" {
		return
	}
	stickyMu.Lock()
	defer stickyMu.Unlock()
	if len(stickyMode) >= StickyModeMaxEntries {
		// Drop one arbitrary expired or oldest-ish entry (first map iteration).
		var drop string
		oldest := time.Now()
		for k, e := range stickyMode {
			if e.at.Before(oldest) {
				oldest = e.at
				drop = k
			}
		}
		if drop != "" {
			delete(stickyMode, drop)
		}
	}
	stickyMode[key] = stickyEntry{mode: mode, at: time.Now()}
	recordStickyRemember()
}

// ApplyStickyMode reorders cascade to start at sticky when sticky is on the ladder.
// Modes "above" sticky (already proven weaker for this dest) are skipped until TTL expiry.
func ApplyStickyMode(cascade []string, sticky string) []string {
	sticky = NormalizeXHTTPMode(sticky)
	if sticky == "" || len(cascade) <= 1 {
		return cascade
	}
	idx := -1
	for i, m := range cascade {
		if NormalizeXHTTPMode(m) == sticky {
			idx = i
			break
		}
	}
	if idx <= 0 {
		// not found or already first
		if idx == 0 {
			return cascade
		}
		return cascade
	}
	return append([]string(nil), cascade[idx:]...)
}

// ClearStickyModeForTest clears the sticky map (tests only).
func ClearStickyModeForTest() {
	stickyMu.Lock()
	stickyMode = make(map[string]stickyEntry, 64)
	stickyMu.Unlock()
}
