package main

// Scenario (suite) definitions for the generic benchmark comparison harness.
//
// Design principle: the harness ships its OWN copy of every bench source file
// under benches/ and injects them into each target worktree, so the exact same
// benchmark code runs on every target. Scenarios are data, decoupled from any
// target repository — a target simply lacks a scenario (coverage ✗) if the
// benchmark cannot run there (API mismatch, missing file, etc.).

import (
	"fmt"
	"strings"
)

// benchTarget is a repository that benchmarks can be run against.
type benchTarget struct {
	Name    string // "bray" or "upstream"
	Dir     string // worktree root (absolute path)
	Commit  string // resolved commit
	Session string // value for BENCHCMP_XHTTP_SESSION (Bray needs it; upstream ignores it)
}

// benchSuite is one named collection of benchmarks.
type benchSuite struct {
	Name    string // report/suite id, e.g. "xhttp"
	Pkg     string // package path relative to repo root, e.g. "transport/internet/splithttp"
	BenchRE string // -bench regex
	Count   int
	Timeout string // go test -timeout, e.g. "600s"
	// BenchTime overrides go test's default 1s -benchtime for suites whose
	// benchmarks are dominated by long single-shot tasks (e.g. ML-DSA sign
	// ~250µs/op): short benches let per-sample noise drown the median.
	BenchTime string
	Targets   []string
	// bench source files to inject, keyed by target name; paths are relative
	// to tools/benchcompare/benches/<target>/... and get copied into Pkg dir.
	Inject map[string][]string
	// repo files that conflict with injected benches (same func names) and must
	// be removed from the worktree, keyed by target name.
	Remove map[string][]string
	// quickBenchRE replaces BenchRE when --quick is used (lightweight subset).
	QuickRE string
}

func defaultSuites() []*benchSuite {
	return []*benchSuite{
		{
			Name:    "xhttp",
			Pkg:     "transport/internet/splithttp",
			BenchRE: `BenchmarkXHTTP_`,
			QuickRE: `BenchmarkXHTTP_(TTFB|MemoryAllocations)`,
			Count:   3,
			Timeout: "900s",
			Targets: []string{"bray", "upstream"},
			Inject: map[string][]string{
				"bray":     {`benches/common/xhttp_bench_test.go`},
				"upstream": {`benches/common/xhttp_bench_test.go`},
			},
			Remove: map[string][]string{
				// Repo ships its own richer xhttp bench with the same function
				// names; drop it so the harness copy is the single source of truth.
				"bray": {"transport/internet/splithttp/xhttp_bench_test.go"},
			},
		},
		{
			Name:    "vless",
			Pkg:     "proxy/vless/encoding",
			BenchRE: `BenchmarkVless_(Encode|Decode)`,
			Count:   3,
			Timeout: "120s",
			Targets: []string{"bray", "upstream"},
			Inject: map[string][]string{
				"bray":     {`benches/common/vless_bench_test.go`},
				"upstream": {`benches/common/vless_bench_test.go`},
			},
		},
		{
			Name:    "xmux",
			Pkg:     "transport/internet/splithttp",
			BenchRE: `BenchmarkXMUX`,
			Count:   3,
			Timeout: "300s",
			Targets: []string{"bray"}, // upstream XMUX API diverged; coverage ✗ there
			Inject: map[string][]string{
				"bray": {`benches/bray/xmux_bench_test.go`},
			},
			Remove: map[string][]string{
				"bray": {"transport/internet/splithttp/xmux_bench_test.go"},
			},
		},
		{
			// Repo-native common/buf micros: both forks ship the same-named
			// Benchmark funcs in common/buf/*_test.go, so no injection needed —
			// running each side's own bench measures its own implementation.
			Name:    "buf",
			Pkg:     "common/buf",
			BenchRE: `Benchmark(NewBuffer|NewBufferStack|Write2|Write8|Write32|WriteByte2|WriteByte8|Copy|SplitBytes)`,
			Count:   3,
			Timeout: "300s",
			Targets: []string{"bray", "upstream"},
		},
		{
			// REALITY handshake crypto micros. The harness copy uses only
			// stdlib + shared deps (circl, x/crypto/hkdf, common/crypto), so the
			// exact same code compiles and runs on both forks.
			Name:      "reality",
			Pkg:       "transport/internet",
			BenchRE:   `BenchmarkReality`,
			Count:     3,
			Timeout:   "300s",
			BenchTime: "5s", // MLDSA65Sign ~250µs/op: 1s benchtime is too noisy
			Targets:   []string{"bray", "upstream"},
			Inject: map[string][]string{
				"bray":     {`benches/common/reality_bench_test.go`},
				"upstream": {`benches/common/reality_bench_test.go`},
			},
			Remove: map[string][]string{
				"bray": {"transport/internet/reality_bench_test.go"},
			},
		},
		{
			// DNS message parse/build/EDNS0 micros. Both forks share identical
			// helper signatures (genEDNS0Options/buildReqMsgs/parseResponse/
			// Fqdn/record), so the harness copy runs on both; the Bray-only pool
			// release helpers are not invoked (fair baseline).
			Name:    "dns",
			Pkg:     "app/dns",
			BenchRE: `Benchmark(ParseResponse|BuildReqMsgs|GenEDNS0|Fqdn|RecordAlloc)`,
			Count:   3,
			Timeout: "120s",
			Targets: []string{"bray", "upstream"},
			Inject: map[string][]string{
				"bray":     {`benches/common/dns_bench_test.go`},
				"upstream": {`benches/common/dns_bench_test.go`},
			},
			Remove: map[string][]string{
				"bray": {"app/dns/bench_test.go"},
			},
		},
	}
}

// selectSuites returns the suites named by the comma-separated flag value
// ("all" or "xhttp,vless" etc.).
func selectSuites(names string) ([]*benchSuite, error) {
	if names == "" || names == "all" {
		return defaultSuites(), nil
	}
	var out []*benchSuite
	for _, n := range strings.Split(names, ",") {
		n = strings.TrimSpace(n)
		found := false
		for _, s := range defaultSuites() {
			if s.Name == n {
				out = append(out, s)
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("unknown suite %q (available: all,xhttp,vless,xmux,buf,reality,dns)", n)
		}
	}
	return out, nil
}

// applyQuick narrows each suite to its lightweight subset.
func applyQuick(suites []*benchSuite) {
	for _, s := range suites {
		if s.QuickRE != "" {
			s.BenchRE = s.QuickRE
		}
		s.Count = 1
	}
}

// applyCount overrides the per-suite count when --count is given.
func applyCount(suites []*benchSuite, count int) {
	for _, s := range suites {
		s.Count = count
	}
}
