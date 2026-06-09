//go:build linux

package tun

import "golang.org/x/sys/unix"

// getSystemMemoryMB returns total system RAM in megabytes via sysinfo(2).
// Returns 0 if detection fails, triggering the fallback buffer size.
func getSystemMemoryMB() int {
	var info unix.Sysinfo_t
	if err := unix.Sysinfo(&info); err != nil {
		return 0
	}
	totalMB := uint64(info.Totalram) * uint64(info.Unit) / (1024 * 1024)
	return int(totalMB)
}
