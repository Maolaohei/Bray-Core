#!/usr/bin/env bash
# ABAB cross-mode downlink adjudication for the real TLS/H2 dual-end harness.
# Alternates packet-up dseg and packet-up legacy (same 512MiB push-server)
# so boost/thermal drift cannot favor the mode that always runs last.
# Usage: bash scripts/bench_modes_abab.sh [rounds=8]
set -euo pipefail
cd /d/UGit/Bray-Core
ROUNDS="${1:-8}"
OUT=$(mktemp -d)
# benchstat is a native Windows binary; normalize the MSYS temp directory.
OUT="$(cd "$OUT" && pwd -W 2>/dev/null || pwd)"
trap 'rm -rf "$OUT"' EXIT

run_mode() {
  local mode="$1" dst="$2"
  go test ./testing/scenarios/ -run '^$' \
    -bench "BenchmarkXHTTPModes_Downlink/${mode}$" \
    -benchtime 1x -count 1 -timeout 120s -v 2>&1 \
    | grep -aE "^BenchmarkXHTTPModes_Downlink/${mode}.*ns/op" \
    | sed -E "s#BenchmarkXHTTPModes_Downlink/${mode}#BenchmarkModeDownlink#" \
    >> "$dst"
}

for i in $(seq 1 "$ROUNDS"); do
  if (( i % 2 )); then
    run_mode packet-up-dseg "$OUT/dseg.txt"
    run_mode packet-up-legacy "$OUT/legacy.txt"
  else
    run_mode packet-up-legacy "$OUT/legacy.txt"
    run_mode packet-up-dseg "$OUT/dseg.txt"
  fi
  echo "round $i/$ROUNDS"
done

echo "=== ABAB TLS/H2 downlink: legacy vs dseg ==="
benchstat "$OUT/legacy.txt" "$OUT/dseg.txt"
echo "samples legacy=$(wc -l < "$OUT/legacy.txt") dseg=$(wc -l < "$OUT/dseg.txt")"
