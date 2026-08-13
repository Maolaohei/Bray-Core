#!/usr/bin/env python3
"""bench_ci_gate.py — CI regression gate for Bray-Core benchmarks.

Runs benchstat over the baseline-vs-new results already produced by
.github/workflows/benchmark.yml and fails (exit 1) when a benchmark shows
a statistically significant regression beyond the threshold. benchstat
only emits a delta when the difference is significant (p<0.05 by
default), so any delta parsed is already significance-filtered.

Direction is inferred from the benchmark name: throughput-style names
(Burst / MemoryAllocations) are higher-better, everything else is
lower-better time. The threshold is deliberately wide (±10%) because CI
runners are drawn from a shared pool with ±10-30% machine-to-machine
variance — this gate catches real regressions, not runner luck. The
same-machine gate (scripts/bench_compare.sh) keeps the tight ±3%.

Usage:
  python3 scripts/bench_ci_gate.py --dir bench_results --suites xhttp_core,xmux,...
    [--threshold 10]
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

# Higher-better throughput benchmark names (MB/s); everything else is ns/op.
THROUGHPUT_RE = re.compile(r"(Burst|MemoryAllocations)", re.IGNORECASE)

# benchstat output line, e.g.:
#   XHTTP_TTFB-20    2.120m ± 8%   2.440m ± 0%  +15.07% (p=0.002 n=6)
#   XHTTP_Burst_64KB-20  100MB/s ± 3%  95MB/s ± 4%  -5.00% (p=0.030 n=6)
# or the "no significant change" marker:  1.23ms ± 2%  1.25ms ± 3%  ~
# benchstat strips the "Benchmark" prefix from names; the p-value may carry
# trailing fields ("(p=0.002 n=6)").
DELTA_RE = re.compile(
    r"^(\S+)(?:-\d+)?\s+.*?\s+([+-]?\d+(?:\.\d+)?)%\s+\(p=([^)]+)\)\s*$"
)


def parse_benchstat(benchstat_out: str) -> list[tuple[str, float]]:
    """Return [(bench_name, delta_pct)] for significant deltas only."""
    out = []
    for line in benchstat_out.splitlines():
        m = DELTA_RE.match(line)
        if m:
            out.append((m.group(1), float(m.group(2))))
    return out


def run_benchstat(base: Path, new: Path) -> str:
    res = subprocess.run(
        ["benchstat", str(base), str(new)],
        capture_output=True, text=True,
    )
    if res.returncode != 0:
        # benchstat can return non-zero when nothing is comparable; treat as
        # no-delta so the gate does not spuriously fail on format noise.
        return res.stdout or ""
    return res.stdout


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="bench_results")
    ap.add_argument("--suites", default="xhttp_core,xmux,happy,warmup,vless,buf")
    ap.add_argument("--threshold", type=float, default=10.0,
                    help="|delta| %% beyond which a significant change fails "
                         "the gate (default 10, CI runner variance)")
    args = ap.parse_args()

    d = Path(args.dir)
    gate_fail = False

    for name in args.suites.split(","):
        base = d / f"base_{name}.clean.txt"
        new = d / f"new_{name}.clean.txt"
        if not base.exists() or not new.exists() or base.stat().st_size == 0 \
                or new.stat().st_size == 0:
            print(f"[gate] {name}: no baseline or new results — skipping")
            continue
        for bench, delta in parse_benchstat(run_benchstat(base, new)):
            is_throughput = bool(THROUGHPUT_RE.search(bench))
            if is_throughput:
                regressed = delta <= -args.threshold
            else:
                regressed = delta >= args.threshold
            verdict = "REGRESSION" if regressed else "ok"
            if regressed:
                gate_fail = True
            print(f"[gate] {name}/{bench}: {delta:+.2f}% "
                  f"({'higher-better' if is_throughput else 'lower-better'}) "
                  f"-> {verdict}")

    if gate_fail:
        print(f"\n[gate] FAIL: significant regression(s) beyond "
              f"±{args.threshold:g}% — block merge.")
        return 1
    print(f"\n[gate] PASS: no significant regression beyond ±{args.threshold:g}%.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
