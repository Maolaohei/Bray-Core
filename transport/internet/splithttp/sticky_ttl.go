package splithttp

import (
	"strconv"
	"strings"
	"time"
)

// ParseStickyTTLDuration parses a human-friendly duration for sticky TTL.
// Accepts Go durations ("10m", "30s", "1h") or plain integer minutes ("10").
// Returns 0,false on empty/invalid. Max clamp: 24h.
func ParseStickyTTLDuration(raw string) (time.Duration, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	// Plain integer minutes
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if n <= 0 {
			return 0, false
		}
		d := time.Duration(n) * time.Minute
		if d > 24*time.Hour {
			d = 24 * time.Hour
		}
		return d, true
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, false
	}
	if d > 24*time.Hour {
		d = 24 * time.Hour
	}
	return d, true
}

// headerValueCI returns first header value matching name (case-insensitive).
func headerValueCI(headers map[string]string, name string) string {
	if headers == nil {
		return ""
	}
	for k, v := range headers {
		if strings.EqualFold(k, name) {
			return v
		}
	}
	return ""
}

// ApplyStickyTTLFromHeaders optionally overrides process-wide sticky TTLs from
// client-local headers. Safe: only when valid positive duration is present.
//
//	x-bray-sticky-mode-ttl: 10m | 30s | 15 (minutes)
//	x-bray-sticky-endpoint-ttl: same
//
// Call once at dial setup (not per packet). Invalid values leave defaults.
func ApplyStickyTTLFromHeaders(headers map[string]string) {
	if headers == nil {
		return
	}
	if raw := headerValueCI(headers, "x-bray-sticky-mode-ttl"); raw != "" {
		if d, ok := ParseStickyTTLDuration(raw); ok {
			StickyModeTTL = d
		}
	}
	if raw := headerValueCI(headers, "x-bray-sticky-endpoint-ttl"); raw != "" {
		if d, ok := ParseStickyTTLDuration(raw); ok {
			StickyEndpointTTL = d
		}
	}
}
