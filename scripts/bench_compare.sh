#!/usr/bin/env bash
# bench_compare.sh — benchmark regression gate for Bray-Core.
#
# Compares the current working tree against a baseline commit (default HEAD)
# across the hot-path packages, using benchstat. Exits non-zero if any
# benchmark shows a statistically significant regression (benchstat p<0.05
# with delta >= +3%).
#
# Usage:
#   scripts/bench_compare.sh [baseline-commit] [bench-regex]
#
# Examples:
#   scripts/bench_compare.sh                # working tree vs HEAD
#   scripts/bench_compare.sh HEAD~1         # vs one commit back
#   scripts/bench_compare.sh HEAD 'HappyIPDB|XMUX'   # narrow bench set
#
# Requirements: git, go, benchstat (go install golang.org/x/perf/cmd/benchstat@latest)
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

BASELINE="${1:-HEAD}"
BENCH_REGEX="${2:-.}"
PKGS="./common/geodata/strmatcher/ ./common/crypto/ ./transport/internet/ ./transport/internet/quality/ ./transport/internet/reality/ ./transport/internet/splithttp/"
COUNT=6

command -v benchstat >/dev/null 2>&1 || { echo "benchstat not found — run: go install golang.org/x/perf/cmd/benchstat@latest"; exit 1; }

TMP="$(mktemp -d .gotmp/benchcmp.XXXXXX)"
TMP="$(cd "$TMP" && pwd -W 2>/dev/null || pwd)"  # Windows-native path for git
trap 'git worktree remove --force "$TMP/base" 2>/dev/null || true; rm -rf "$TMP"' EXIT

echo "== baseline: $BASELINE (current: $(git rev-parse --short HEAD))"
git worktree add -q "$TMP/base" "$BASELINE"
# REALITY is a submodule; the baseline tree needs it for transport/internet to build.
( cd "$TMP/base" && git submodule update --init REALITY >/dev/null 2>&1 || true )

echo "== running baseline benchmarks (count=$COUNT)..."
( cd "$TMP/base" && go test -bench "$BENCH_REGEX" -benchmem -run='^$' -count=$COUNT -timeout 30m $PKGS ) > "$TMP/old.txt" 2>"$TMP/old.err" || true

echo "== running current benchmarks (count=$COUNT)..."
go test -bench "$BENCH_REGEX" -benchmem -run='^$' -count=$COUNT -timeout 30m $PKGS > "$TMP/new.txt" 2>"$TMP/new.err" || true

echo "== benchstat (old=$BASELINE new=working-tree)"
benchstat "$TMP/old.txt" "$TMP/new.txt" | tee "$TMP/report.txt"

# Fail the gate on significant regressions: benchstat prints a numeric delta
# with (p=...) only when the change is statistically significant. Direction
# depends on the block unit: time/op and B/op shrink = good, throughput
# (MiB/s, MB/s, GB/s) grow = good. Track the block unit from the table
# header line and only flag the bad direction.
REGRESSIONS=$(awk '
  /sec\/op|B\/op|allocs\/op/ { unit="latency"; next }
  /B\/s|MiB\/s|MB\/s|GiB\/s|GB\/s/ { unit="throughput"; next }
  /\(p=/ {
    delta="";
    for (i=1; i<=NF; i++) if ($i ~ /^[+-][0-9]+\.[0-9]+%$/) delta=$i;
    if (delta=="") next;
    d=delta+0;
    if (unit=="throughput") { if (d <= -3) print delta }
    else                    { if (d >= 3)  print delta }
  }' "$TMP/report.txt" || true)

if [ -n "$REGRESSIONS" ]; then
  echo ""
  echo "❌ REGRESSION(S) DETECTED (time/op >= +3%, p<0.05):"
  echo "$REGRESSIONS"
  exit 1
fi
echo ""
echo "✅ no significant regressions (threshold: time/op >= +3%, p<0.05)"
