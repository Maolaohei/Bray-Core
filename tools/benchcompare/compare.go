package main

// Comparison engine: aligns benchmark results across two targets, computes
// deltas and the coverage matrix (which scenarios run on which target).

import (
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// eqThreshold is the |delta%| below which we call a pair "tie".
const eqThreshold = 3.0

type pair struct {
	Name       string
	Bray, Up   *benchResult // nil = missing on that target
	BrayMed    benchSample
	UpMed      benchSample
	DeltaPct   float64 // (bray - upstream)/upstream * 100; negative = bray faster
	Verdict    string  // "bray-faster" | "upstream-faster" | "tie" | "bray-only" | "upstream-only"
	HasMem     bool
	BrayB, UpB float64
	BrayA, UpA float64
}

type comparison struct {
	Suite  string
	Pairs  []pair
	Counts struct {
		Both, BrayOnly, UpOnly int
		BrayFaster, UpFaster   int
		Ties                   int
	}
}

// compare aligns bray and upstream results for a suite.
func compareResults(s *benchSuite, bray, up []benchResult) *comparison {
	idx := func(rs []benchResult) map[string]*benchResult {
		m := map[string]*benchResult{}
		for i := range rs {
			m[rs[i].Name] = &rs[i]
		}
		return m
	}
	bm, um := idx(bray), idx(up)

	names := map[string]bool{}
	for n := range bm {
		names[n] = true
	}
	for n := range um {
		names[n] = true
	}
	sorted := make([]string, 0, len(names))
	for n := range names {
		sorted = append(sorted, n)
	}
	sort.Strings(sorted)

	c := &comparison{Suite: s.Name}
	for _, n := range sorted {
		p := pair{Name: n}
		b, bOk := bm[n]
		u, uOk := um[n]
		switch {
		case bOk && uOk:
			p.Bray, p.Up = b, u
			p.BrayMed = medianSamples(b.Samples)
			p.UpMed = medianSamples(u.Samples)
			p.DeltaPct = (p.BrayMed.NS - p.UpMed.NS) / p.UpMed.NS * 100
			p.HasMem = b.Samples[0].BPerOp > 0 || u.Samples[0].BPerOp > 0
			p.BrayB, p.UpB = p.BrayMed.BPerOp, p.UpMed.BPerOp
			p.BrayA, p.UpA = p.BrayMed.Allocs, p.UpMed.Allocs
			p.Verdict = verdictOf(p.DeltaPct)
			c.Counts.Both++
			switch p.Verdict {
			case "bray-faster":
				c.Counts.BrayFaster++
			case "upstream-faster":
				c.Counts.UpFaster++
			default:
				c.Counts.Ties++
			}
		case bOk:
			p.Bray = b
			p.BrayMed = medianSamples(b.Samples)
			p.Verdict = "bray-only"
			c.Counts.BrayOnly++
		default:
			p.Up = u
			p.UpMed = medianSamples(u.Samples)
			p.Verdict = "upstream-only"
			c.Counts.UpOnly++
		}
		c.Pairs = append(c.Pairs, p)
	}
	return c
}

func verdictOf(deltaPct float64) string {
	if deltaPct < -eqThreshold {
		return "bray-faster"
	}
	if deltaPct > eqThreshold {
		return "upstream-faster"
	}
	return "tie"
}

// benchstatIfAvailable runs benchstat (old=upstream, new=bray) when the binary
// is on PATH and both raws have >=2 samples; embeds its text into the report.
func benchstatIfAvailable(upstream, bray *benchRun, outDir string) string {
	if _, err := exec.LookPath("benchstat"); err != nil {
		return ""
	}
	if !enoughSamples(upstream.Raw) || !enoughSamples(bray.Raw) {
		return ""
	}
	oldF := filepath.Join(outDir, upstream.Suite+"_"+upstream.Target+".txt")
	newF := filepath.Join(outDir, bray.Suite+"_"+bray.Target+".txt")
	// Re-materialize filtered .txt (goos lines excluded; benchstat tolerates them,
	// but keep it minimal for clean diff output).
	if err := writeBenchTxt(oldF, upstream.Raw); err != nil {
		return ""
	}
	if err := writeBenchTxt(newF, bray.Raw); err != nil {
		return ""
	}
	cmd := exec.Command("benchstat", oldF, newF)
	out, err := cmd.CombinedOutput()
	if err != nil {
		// benchstat can exit non-zero when some benches failed; still show output.
		return string(out)
	}
	return string(out)
}

func enoughSamples(raw string) bool {
	counts := map[string]int{}
	for _, l := range splitLines(raw) {
		m := benchLine.FindStringSubmatch(l)
		if m != nil {
			counts[m[1]]++
		}
	}
	for _, n := range counts {
		if n >= 2 {
			return true
		}
	}
	return false
}

func writeBenchTxt(path, raw string) error {
	var lines []string
	for _, l := range splitLines(raw) {
		if strings.HasPrefix(l, "Benchmark") {
			lines = append(lines, l)
		}
	}
	return writeFileSimple(path, joinLines(lines)+"\n")
}

func writeFileSimple(path, data string) error {
	return os.WriteFile(path, []byte(data), 0o644)
}
