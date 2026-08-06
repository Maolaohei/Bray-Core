// benchcompare — generic benchmark comparison harness for Bray-Core vs upstream.
//
// It ships its own bench source files (tools/benchcompare/benches/), materializes
// a detached git worktree per target, injects the same bench code into every
// target, runs `go test -bench`, then aligns results by benchmark name to
// produce a coverage matrix + delta report (Markdown + JSON).
//
// Usage:
//
//	go run ./tools/benchcompare --suite xhttp,vless --targets bray,upstream
//	go run ./tools/benchcompare --suite all --quick            # fast smoke
//	go run ./tools/benchcompare --suite vless --upstream-path D:/UGit/Xray-upstream
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	var (
		suiteFlag    = flag.String("suite", "all", "suites to run: all,xhttp,vless,xmux,buf,reality (comma separated)")
		targetsFlag  = flag.String("targets", "bray,upstream", "targets to run: bray,upstream")
		countFlag    = flag.Int("count", 0, "bench count override (default: per-suite)")
		quick        = flag.Bool("quick", false, "lightweight subset, count=1")
		benchReFlag  = flag.String("bench-re", "", "override the -bench regex for every suite (e.g. 'BenchmarkXHTTP_(ConnectionStorm|Modes)')")
		cpuProfile   = flag.Bool("cpuprofile", false, "collect go test -cpuprofile into <out>/<suite>_<target>.prof")
		benchtime    = flag.String("benchtime", "", "go test -benchtime override (e.g. 5s); recommended with --cpuprofile")
		repeats      = flag.Int("repeats", 3, "run each suite/target N times (independent go test processes; default 3 so conclusions are not drawn from a single pass); raw outputs are merged so medians cover N×count samples")
		upstreamPath = flag.String("upstream-path", "", "use an existing local checkout as the upstream target instead of a fresh worktree")
		outFlag      = flag.String("out", "", "output dir (default: bench_results/compare)")
		keepFlag     = flag.Bool("keep", false, "keep worktrees after run (debug)")
	)
	flag.Parse()

	suites, err := selectSuites(*suiteFlag)
	if err != nil {
		fatal(err)
	}
	if *quick {
		applyQuick(suites)
	}
	if *countFlag > 0 {
		applyCount(suites, *countFlag)
	}
	if *benchReFlag != "" {
		for _, s := range suites {
			s.BenchRE = *benchReFlag
		}
	}
	if *cpuProfile && *countFlag == 0 {
		// -cpuprofile is overwritten on every -count run; default to a single
		// measured pass so the profile file stays meaningful.
		applyCount(suites, 1)
	}
	runnerOpts := runnerOptions{CPUProfile: *cpuProfile, BenchTime: *benchtime, Repeats: *repeats}
	targets := strings.Split(*targetsFlag, ",")
	if len(targets) == 0 {
		fatal(fmt.Errorf("--targets empty"))
	}

	outDir := *outFlag
	if outDir == "" {
		outDir = filepath.Join(repoRoot, "bench_results", "compare")
	}
	_ = os.MkdirAll(outDir, 0o755)

	fmt.Printf("benchcompare: suites=%s targets=%s count=%d quick=%v out=%s\n",
		suiteNames(suites), strings.Join(targets, ","), suites[0].Count, *quick, outDir)

	// Prepare targets (worktrees + bench injection).
	targetsMap, cleanup, err := prepareTargets(suites, targets, *upstreamPath)
	if err != nil {
		fatal(fmt.Errorf("prepare targets: %w", err))
	}
	if *keepFlag {
		cleanup = func() {}
	} else {
		defer cleanup()
	}

	// Run every suite on every target.
	meta := reportMeta{
		GeneratedAt: nowStamp(),
		Suites:      suiteNames(suites),
		Targets:     targets,
		Count:       suites[0].Count,
		Quick:       *quick,
		Commits:     map[string]string{},
	}
	var runs []*benchRun
	bySuiteTarget := map[string]*benchRun{}
	runFail := 0
	for _, s := range suites {
		for _, name := range targets {
			bt, ok := targetsMap[name]
			if !ok {
				continue
			}
			meta.Commits[bt.Name] = bt.Commit
			fmt.Printf("  run %s/%s …\n", name, s.Name)
			r, err := bt.runBench(s, outDir, runnerOpts)
			if err != nil {
				fmt.Fprintf(os.Stderr, "  ⚠ %v\n", err)
				runFail++
			}
			runs = append(runs, r)
			bySuiteTarget[s.Name+"|"+name] = r
		}
	}

	// Compare + report.
	var comps []*comparison
	benchstatText := map[string]string{}
	for _, s := range suites {
		br, bok := bySuiteTarget[s.Name+"|bray"]
		ur, uok := bySuiteTarget[s.Name+"|upstream"]
		switch {
		case bok && uok:
			c := compareResults(s, parseBench(br.Raw), parseBench(ur.Raw))
			comps = append(comps, c)
			meta.Benchstat = true
			benchstatText[s.Name] = benchstatIfAvailable(ur, br, outDir)
		case bok:
			// bray-only suite: report coverage without a delta table.
			c := &comparison{Suite: s.Name}
			for _, res := range parseBench(br.Raw) {
				p := pair{Name: res.Name, Bray: &res, BrayMed: medianSamples(res.Samples), Verdict: "bray-only"}
				c.Pairs = append(c.Pairs, p)
				c.Counts.BrayOnly++
			}
			comps = append(comps, c)
		case uok:
			c := &comparison{Suite: s.Name}
			for _, res := range parseBench(ur.Raw) {
				p := pair{Name: res.Name, Up: &res, UpMed: medianSamples(res.Samples), Verdict: "upstream-only"}
				c.Pairs = append(c.Pairs, p)
				c.Counts.UpOnly++
			}
			comps = append(comps, c)
		}
	}

	mdPath, err := writeReports(outDir, meta, runs, comps, benchstatText)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("✔ report: %s\n", mdPath)
	if runFail > 0 {
		fmt.Fprintf(os.Stderr, "⚠ %d suite/target run(s) failed; see raw .err files in %s\n", runFail, outDir)
	}
}

func suiteNames(suites []*benchSuite) []string {
	var out []string
	for _, s := range suites {
		out = append(out, s.Name)
	}
	return out
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "benchcompare: %v\n", err)
	os.Exit(1)
}
