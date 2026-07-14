package splithttp

import (
	"context"
	"errors"
	"strings"
)

// XHTTP mode cascade for CDN / reverse-proxy degradation (Wave-2).
// Prefer long-lived streams first; fall back toward packet-up when the edge
// rejects bidirectional or half-open stream semantics.
//
//	stream-one -> stream-up -> packet-up
//
// Explicit operator modes still win unless mode is "auto" or degrade is enabled.

// NormalizeXHTTPMode lowercases and trims mode tokens.
func NormalizeXHTTPMode(mode string) string {
	return strings.ToLower(strings.TrimSpace(mode))
}

// ResolveInitialMode picks the first mode for a dial.
// Explicit modes are returned as-is; auto selects REALITY-aware defaults.
func ResolveInitialMode(mode string, hasREALITY, hasDownloadSettings bool) string {
	m := NormalizeXHTTPMode(mode)
	if m != "" && m != "auto" {
		return m
	}
	if !hasREALITY {
		return "packet-up"
	}
	if hasDownloadSettings {
		return "stream-up"
	}
	return "stream-one"
}

// NextDegradedMode returns the next mode in the CDN degradation ladder, or ""
// when no further degradation is available.
func NextDegradedMode(mode string) string {
	switch NormalizeXHTTPMode(mode) {
	case "stream-one":
		return "stream-up"
	case "stream-up":
		return "packet-up"
	default:
		return ""
	}
}

// CanDegradeMode reports whether mode has a next cascade step.
func CanDegradeMode(mode string) bool {
	return NextDegradedMode(mode) != ""
}

// ModeDegradeEnabled is opt-in via xhttpSettings.headers["x-bray-mode-degrade"].
// Accepted truthy values: "1", "true", "yes", "on".
func ModeDegradeEnabled(headers map[string]string) bool {
	if headers == nil {
		return false
	}
	for k, v := range headers {
		if strings.EqualFold(k, "x-bray-mode-degrade") {
			switch strings.ToLower(strings.TrimSpace(v)) {
			case "1", "true", "yes", "on":
				return true
			}
		}
	}
	return false
}

// ShouldAttemptModeDegrade decides whether dial failure may retry with a
// degraded mode. Auto always allows cascade; explicit modes require opt-in.
func ShouldAttemptModeDegrade(configuredMode string, headers map[string]string) bool {
	m := NormalizeXHTTPMode(configuredMode)
	if m == "" || m == "auto" {
		return true
	}
	return ModeDegradeEnabled(headers)
}

// BuildModeCascade returns the ordered mode attempts for a dial.
// When allowDegrade is false, only the initial mode is returned.
func BuildModeCascade(initial string, allowDegrade bool) []string {
	m := NormalizeXHTTPMode(initial)
	if m == "" {
		m = "packet-up"
	}
	out := []string{m}
	if !allowDegrade {
		return out
	}
	for next := NextDegradedMode(m); next != ""; next = NextDegradedMode(next) {
		out = append(out, next)
		m = next
	}
	return out
}

// IsDegradeEligibleError reports whether an open/dial error should trigger
// mode cascade. Context cancellation is never retried with a different mode.
func IsDegradeEligibleError(err error) bool {
	if err == nil {
		return false
	}
	if err == context.Canceled || err == context.DeadlineExceeded {
		return false
	}
	// std errors.Is for wrapped context errors
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return true
}
