package main

// Parser: turns `go test -bench -benchmem` output into structured samples.
//
// Benchmark line format (single line, with -benchmem):
//   BenchmarkXHTTP_TTFB-8   12345   456.7 ns/op   128 B/op   4 allocs/op

import (
	"regexp"
	"sort"
	"strconv"
)

var (
	// benchLine matches a benchmark result line. Groups: name, ns/op,
	// optional MB/s (when SetBytes), optional B/op, optional allocs/op.
	benchLine = regexp.MustCompile(`^(Benchmark\S+)\s+\d+\s+([0-9.]+)\s+ns/op(?:\s+([0-9.]+)\s+MB/s)?(?:\s+([0-9.]+)\s+B/op)?(?:\s+(\d+)\s+allocs/op)?`)
)

type benchSample struct {
	NS     float64 // ns/op
	MBPS   float64 // MB/s (0 when the bench does not SetBytes)
	BPerOp float64 // B/op
	Allocs float64 // allocs/op
}

type benchResult struct {
	Name    string
	Samples []benchSample
}

// parseBench parses raw filtered output into results keyed by benchmark name.
// The `-<GOMAXPROCS>` suffix is stripped so results stay aligned even when
// targets are measured on machines with different parallelism.
func parseBench(raw string) []benchResult {
	byName := map[string][]benchSample{}
	var order []string
	for _, l := range splitLines(raw) {
		m := benchLine.FindStringSubmatch(l)
		if m == nil {
			continue
		}
		name := stripProcSuffix(m[1])
		ns, _ := strconv.ParseFloat(m[2], 64)
		s := benchSample{NS: ns}
		if m[3] != "" {
			s.MBPS, _ = strconv.ParseFloat(m[3], 64)
		}
		if m[4] != "" {
			s.BPerOp, _ = strconv.ParseFloat(m[4], 64)
		}
		if m[5] != "" {
			s.Allocs, _ = strconv.ParseFloat(m[5], 64)
		}
		if _, ok := byName[name]; !ok {
			order = append(order, name)
		}
		byName[name] = append(byName[name], s)
	}
	out := make([]benchResult, 0, len(order))
	for _, name := range order {
		out = append(out, benchResult{Name: name, Samples: byName[name]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

var procSuffix = regexp.MustCompile(`-\d+$`)

func stripProcSuffix(name string) string {
	return procSuffix.ReplaceAllString(name, "")
}

func medianSamples(rs []benchSample) benchSample {
	if len(rs) == 0 {
		return benchSample{}
	}
	sorted := make([]benchSample, len(rs))
	copy(sorted, rs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].NS < sorted[j].NS })
	return sorted[len(sorted)/2]
}
