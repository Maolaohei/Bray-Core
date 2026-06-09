//go:build !linux && !darwin

package tun

// getSystemMemoryMB returns 0 on unsupported platforms, triggering the
// fallback UDP egress buffer size (256).
func getSystemMemoryMB() int {
	return 0
}
