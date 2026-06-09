//go:build darwin

package tun

import "golang.org/x/sys/unix"

// getSystemMemoryMB returns total system RAM in megabytes via sysctl
// hw.memsize. Returns 0 if detection fails, triggering the fallback.
func getSystemMemoryMB() int {
	memsize, err := unix.SysctlUint64("hw.memsize")
	if err != nil {
		return 0
	}
	return int(memsize / (1024 * 1024))
}
