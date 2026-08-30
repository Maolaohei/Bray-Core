#!/usr/bin/env bash
# Correct A/B sampling for the real dual-end downlink benchmarks.
#
# Solves the measurement traps that made baseline numbers unreliable:
#  1. `go test -bench -count N` (non -v) prints iteration 1 with the
#     "BenchmarkX..." name and iterations 2..N as continuation lines that
#     start with whitespace, no name. A naive `grep '^Benchmark'` drops
#     every continuation sample -> "missing samples".
#  2. child xray processes print a version banner on stdout that would
#     interleave into the stream (routed to stderr by common_regular.go).
#
# We filter BOTH the named row and its continuation rows (lines that start
# with whitespace then a number), then aggregate with benchstat per mode.
#
# Usage: bash scripts/bench_dseg.sh            # both modes, 6 iters
#        bash scripts/bench_dseg.sh dseg       # only dseg
#        bash scripts/bench_dseg.sh both 10    # more samples
cd "$(git rev-parse --show-toplevel)" || exit 1
OUT=$(mktemp -d)
F=testing/scenarios/downlink_bench_test.go
cp "$F" "$OUT/orig.go"
trap 'cp "$OUT/orig.go" "$F" 2>/dev/null' EXIT

# benchstat needs a real binary on PATH; `go tool benchstat` is not shipped
# with all Go versions and silently produces no output when missing.
if command -v benchstat >/dev/null 2>&1; then
  BS=benchstat
else
  BS="go tool benchstat"
fi

# Force every totalBytes const to 512MiB so both modes measure steady-state.
python - "$F" <<'PY'
import re,sys
p=sys.argv[1]; s=open(p,encoding='utf-8').read()
s=re.sub(r'const totalBytes = int64\([0-9]* << 20\)','const totalBytes = int64(512 << 20)',s)
open(p,'w',encoding='utf-8').write(s)
PY

# sample <name> <pattern> <iters>: run and capture named rows + continuation
# rows, then rewrite them into benchstat's input format:
#
#   BenchmarkName-N  <iters-per-sample>  <value> ns/op
#
# Two things matter for a valid comparison:
#  - the iteration column MUST be present (benchstat rejects "name value unit"
#    with "parsing measurement: invalid syntax"); -benchtime 1x => 1.
#  - both modes MUST share one benchmark name. benchstat can only A/B rows
#    with identical names, and the two benchmarks are naturally named
#    differently, so we rename to a common "BenchmarkDownlink". The mode is
#    carried by the file name and printed in the section headers instead.
sample() {
  local name="$1" pat="$2" iters="$3"
  go test ./testing/scenarios/ -run '^$' -bench "$pat" -benchtime 1x -count "$iters" -timeout 900s 2>/dev/null \
    | grep -aE "^Benchmark.*ns/op|^[[:space:]]+[0-9]+[[:space:]]+[0-9]+ ns/op" \
    | sed -E 's/[[:space:]]+/ /g; s/^ //' > "$OUT/$name.raw.txt"
  awk '{
    if ($1 ~ /^Benchmark/) { printf "BenchmarkDownlink\t%s\t%s %s\n", $2, $3, $4; next }
    printf "BenchmarkDownlink\t%s\t%s %s\n", $1, $2, $3
  }' "$OUT/$name.raw.txt" > "$OUT/$name.norm.txt"
  echo "$name: $(wc -l < "$OUT/$name.norm.txt") samples"
}

MODE="${1:-both}"
# benchstat needs >= 6 samples before it will report a confidence interval.
ITERS="${2:-6}"
if [ "$MODE" = "legacy" ] || [ "$MODE" = "both" ]; then
  sample legacy BenchmarkLegacyLongGETDownlink "$ITERS"
fi
if [ "$MODE" = "dseg" ] || [ "$MODE" = "both" ]; then
  sample dseg   BenchmarkDsegRealDownlink      "$ITERS"
fi

echo ""
for m in "$OUT"/legacy.norm.txt "$OUT"/dseg.norm.txt; do
  [ -f "$m" ] || continue
  base=$(basename "$m" .norm.txt)
  echo "=== $base @512MiB ==="
  $BS "$m" 2>&1 || cat "$m"
  echo ""
done
if [ -f "$OUT/legacy.norm.txt" ] && [ -f "$OUT/dseg.norm.txt" ]; then
  echo "=== A/B (baseline=legacy, comparison=dseg) ==="
  echo "    negative delta = dseg faster"
  $BS "$OUT/legacy.norm.txt" "$OUT/dseg.norm.txt" 2>&1 | tail -16
fi
echo "OUTDIR=$OUT"
