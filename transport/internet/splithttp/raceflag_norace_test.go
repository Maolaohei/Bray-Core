//go:build !race

package splithttp_test

// raceEnabled reports whether the test binary was built with the race detector.
// (runtime/race no longer exposes Enabled in Go 1.26, so we gate it by build tag.)
func raceEnabled() bool { return false }
