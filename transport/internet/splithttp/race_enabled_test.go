//go:build race

package splithttp

// raceEnabled is true when built with -race. The detector clears sync.Pool
// between Get/Put, so pool-reuse assertions must be skipped under it.
const raceEnabled = true
