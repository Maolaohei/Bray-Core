//go:build !race

package splithttp

// raceEnabled is true when built with -race (see race_enabled_test.go).
const raceEnabled = false
