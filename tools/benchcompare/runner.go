package main

// Runner: executes `go test -bench` inside a target worktree and captures the
// raw output. Only stdout is kept for parsing (bench lines); stderr is
// captured separately and surfaced on failure.

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// benchRun captures one suite x target execution.
type benchRun struct {
	Target string
	Suite  string
	Raw    string // filtered bench output (Benchmark/goos/goarch/cpu/pkg/ok lines)
}

// runnerOptions carries per-invocation go test knobs.
type runnerOptions struct {
	CPUProfile bool   // add -cpuprofile=<out>/<suite>_<target>.prof
	BenchTime  string // optional -benchtime override (e.g. "5s")
	Repeats    int    // independent go test processes per suite/target (raw merged)
}

func (bt *benchTarget) runBench(s *benchSuite, outDir string, opts runnerOptions) (*benchRun, error) {
	repeats := opts.Repeats
	if repeats < 1 {
		repeats = 1
	}
	if opts.CPUProfile && repeats > 1 {
		// -cpuprofile would overwrite itself across repeats; profile mode is
		// single-pass only.
		repeats = 1
	}
	var merged string
	var lastErr error
	for r := 1; r <= repeats; r++ {
		raw, err := bt.runBenchOnce(s, outDir, opts)
		if err != nil {
			lastErr = err
			continue
		}
		if r > 1 && len(raw) > 0 {
			merged += "\n"
		}
		merged += raw
	}
	if outAbs, err := filepath.Abs(outDir); err == nil {
		if err := os.MkdirAll(outAbs, 0o755); err == nil {
			base := filepath.Join(outAbs, s.Name+"_"+bt.Name)
			_ = os.WriteFile(base+".raw", []byte(merged), 0o644)
			// Drop stale diagnostics from earlier failed runs so a later
			// success is never mistaken for a failure.
			_ = os.Remove(base + ".err")
			_ = os.Remove(base + ".out")
		}
	}
	if lastErr != nil {
		// Partial data still reported; surface the last failure.
		return &benchRun{Target: bt.Name, Suite: s.Name, Raw: merged}, lastErr
	}
	return &benchRun{Target: bt.Name, Suite: s.Name, Raw: merged}, nil
}

func (bt *benchTarget) runBenchOnce(s *benchSuite, outDir string, opts runnerOptions) (string, error) {
	start := time.Now()
	outAbs, err := filepath.Abs(outDir)
	if err != nil {
		return "", err
	}
	outDir = outAbs
	args := []string{
		"test",
		"-run=^$",
		"-bench=" + s.BenchRE,
		"-benchmem",
		fmt.Sprintf("-count=%d", s.Count),
		"-timeout=" + s.Timeout,
	}
	benchtime := opts.BenchTime
	if benchtime == "" {
		// Suite-level default (e.g. long-task suites like MLDSA); the CLI
		// --benchtime flag still wins when given explicitly.
		benchtime = s.BenchTime
	}
	if benchtime != "" {
		args = append(args, "-benchtime="+benchtime)
	}
	if opts.CPUProfile {
		prof := filepath.Join(outDir, s.Name+"_"+bt.Name+".prof")
		args = append(args, "-cpuprofile="+prof)
	}
	args = append(args, "./"+s.Pkg+"/")
	cmd := exec.Command("go", args...)
	cmd.Dir = bt.Dir
	// Hand the session header (Bray needs it for fail-closed wire modes;
	// upstream simply ignores the unknown header).
	cmd.Env = append(os.Environ(), "BENCHCMP_XHTTP_SESSION="+bt.Session)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	raw := filterBenchOutput(stdout.String())

	if runErr != nil {
		// Keep full diagnostics for debugging: unfiltered stdout + stderr.
		_ = os.MkdirAll(outDir, 0o755)
		_ = os.WriteFile(filepath.Join(outDir, s.Name+"_"+bt.Name+".err"), stderr.Bytes(), 0o644)
		_ = os.WriteFile(filepath.Join(outDir, s.Name+"_"+bt.Name+".out"), stdout.Bytes(), 0o644)
		diag := stderr.String()
		if len(diag) > 4000 {
			diag = "…(stderr trimmed)…\n" + diag[len(diag)-4000:]
		}
		return raw, fmt.Errorf("%s/%s: go test failed after %s: %v\n%s",
			bt.Name, s.Name, time.Since(start).Round(time.Millisecond), runErr, diag)
	}
	if !hasBenchmarkLines(raw) {
		return raw, fmt.Errorf("%s/%s: no Benchmark lines matched %q (coverage gap or bench error)",
			bt.Name, s.Name, s.BenchRE)
	}
	fmt.Printf("  [%s/%s] %d bench lines in %s (goos %s)\n", bt.Name, s.Name,
		countBenchmarkLines(raw), time.Since(start).Round(time.Millisecond), goosOf(raw))
	return raw, nil
}

// filterBenchOutput keeps only the lines benchstat/parsers care about.
func filterBenchOutput(out string) string {
	var keep []string
	for _, line := range splitLines(out) {
		if isBenchLine(line) {
			keep = append(keep, line)
		}
	}
	return joinLines(keep)
}

func isBenchLine(l string) bool {
	return hasPrefix(l, "Benchmark") ||
		hasPrefix(l, "goos:") || hasPrefix(l, "goarch:") ||
		hasPrefix(l, "cpu:") || hasPrefix(l, "pkg:") ||
		l == "PASS" || l == "FAIL" || hasPrefix(l, "ok  ")
}

func hasBenchmarkLines(raw string) bool {
	for _, l := range splitLines(raw) {
		if hasPrefix(l, "Benchmark") {
			return true
		}
	}
	return false
}

func countBenchmarkLines(raw string) int {
	n := 0
	for _, l := range splitLines(raw) {
		if hasPrefix(l, "Benchmark") {
			n++
		}
	}
	return n
}

func goosOf(raw string) string {
	for _, l := range splitLines(raw) {
		if hasPrefix(l, "goos:") {
			return l
		}
	}
	return "?"
}

func splitLines(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == '\n' {
			out = append(out, cur)
			cur = ""
		} else if r != '\r' {
			cur += string(r)
		}
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

func joinLines(ls []string) string {
	var b bytes.Buffer
	for i, l := range ls {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(l)
	}
	return b.String()
}

func hasPrefix(s, p string) bool {
	return len(s) >= len(p) && s[:len(p)] == p
}
