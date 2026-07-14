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

// StickyTTLFromHeaders returns optional per-dial sticky TTLs from client-local
// headers. Zero means "use package default / entry default". Does NOT mutate
// process-global StickyModeTTL / StickyEndpointTTL (Wave-7 review fix).
//
//	x-bray-sticky-mode-ttl: 10m | 30s | 15 (minutes)
//	x-bray-sticky-endpoint-ttl: same
func StickyTTLFromHeaders(headers map[string]string) (modeTTL, endpointTTL time.Duration) {
	if headers == nil {
		return 0, 0
	}
	if raw := headerValueCI(headers, "x-bray-sticky-mode-ttl"); raw != "" {
		if d, ok := ParseStickyTTLDuration(raw); ok {
			modeTTL = d
		}
	}
	if raw := headerValueCI(headers, "x-bray-sticky-endpoint-ttl"); raw != "" {
		if d, ok := ParseStickyTTLDuration(raw); ok {
			endpointTTL = d
		}
	}
	return modeTTL, endpointTTL
}

// ApplyStickyTTLFromHeaders is retained for compatibility with older call sites
// and tests. It only reads headers and does not change process globals.
// Prefer StickyTTLFromHeaders + Remember*TTL.
//
// Deprecated: no process-wide override; use StickyTTLFromHeaders.
func ApplyStickyTTLFromHeaders(headers map[string]string) {
	// Intentionally a no-op for process globals. Parsing still validated by
	// StickyTTLFromHeaders at dial / remember sites.
	_, _ = StickyTTLFromHeaders(headers)
}
