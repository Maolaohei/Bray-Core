#!/usr/bin/env bash
# Strict ABAB adjudication for a working-tree change vs a baseline commit.
# Designed for noisy Windows benchmarks: alternates baseline/current each
# sample so CPU boost / thermal drift affects both sides equally.
# Usage: bash scripts/bench_abab.sh [baseline=HEAD] [bench=BenchmarkXHTTP_H2C_Throughput] [rounds=10]
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
BASE="${1:-HEAD}"
BENCH="${2:-BenchmarkXHTTP_H2C_Throughput}"
ROUNDS="${3:-10}"
PKG=./transport/internet/splithttp/
TMP="$(mktemp -d .gotmp/abab.XXXXXX)"
TMP="$(cd "$TMP" && pwd -W 2>/dev/null || pwd)"
trap 'git worktree remove --force "$TMP/base" 2>/dev/null || true; rm -rf "$TMP"' EXIT
git worktree add -q "$TMP/base" "$BASE"
( cd "$TMP/base" && git submodule update --init REALITY >/dev/null 2>&1 || true )

for i in $(seq 1 "$ROUNDS"); do
  # AB order flips per pair (A=baseline, B=current; then B,A) to eliminate
  # thermal/boost one-direction drift even inside each pair.
  if (( i % 2 )); then
    ( cd "$TMP/base" && go test "$PKG" -run '^$' -bench "$BENCH" -benchmem -benchtime 1s -count 1 -timeout 90s ) >> "$TMP/base.txt" 2>&1
    go test "$PKG" -run '^$' -bench "$BENCH" -benchmem -benchtime 1s -count 1 -timeout 90s >> "$TMP/current.txt" 2>&1
  else
    go test "$PKG" -run '^$' -bench "$BENCH" -benchmem -benchtime 1s -count 1 -timeout 90s >> "$TMP/current.txt" 2>&1
    ( cd "$TMP/base" && go test "$PKG" -run '^$' -bench "$BENCH" -benchmem -benchtime 1s -count 1 -timeout 90s ) >> "$TMP/base.txt" 2>&1
  fi
  echo "round $i/$ROUNDS"
done

echo "=== ABAB benchstat: base=$BASE current=working tree ==="
benchstat "$TMP/base.txt" "$TMP/current.txt"
echo "OUTDIR=$TMP"
