#!/usr/bin/env bash
# Correct A/B sampling for the real dual-end downlink benchmarks.
#
# Solves two measurement traps that made baseline numbers unreliable:
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
# Usage: bash scripts/bench_dseg.sh            # both modes, ABAB, 3 iters
#        bash scripts/bench_dseg.sh dseg       # only dseg
cd /d/UGit/Bray-Core || exit 1
OUT=$(mktemp -d)
F=testing/scenarios/downlink_bench_test.go
CP=/tmp/dseg_bench_cp.bak
cp "$F" "$CP"
trap 'cp "$CP" "$F" 2>/dev/null; rm -f "$CP"' EXIT

# Force every totalBytes const to 512MiB so both modes measure steady-state.
python - "$F" <<'PY'
import re,sys
p=sys.argv[1]; s=open(p,encoding='utf-8').read()
s=re.sub(r'const totalBytes = int64\([0-9]* << 20\)','const totalBytes = int64(512 << 20)',s)
open(p,'w',encoding='utf-8').write(s)
PY

# sample <name> <pattern>: run and capture named rows + continuation rows.
sample() {
  local name="$1" pat="$2" iters="$3"
  go test ./testing/scenarios/ -run '^$' -bench "$pat" -benchtime 1x -count "$iters" -timeout 400s 2>/dev/null \
    | grep -aE "^Benchmark.*ns/op|^[[:space:]]+[0-9]+[[:space:]]+[0-9]+ ns/op" \
    | sed -E 's/[[:space:]]+/ /g' > "$OUT/$name.raw.txt"
  # normalize: keep bench name only on first row, drop per-iteration count col
  awk '{
    if ($1 ~ /^Benchmark/) {name=$1; printf "%s %s %s\n",$1,$3,$4; next}
    printf "%s %s %s\n", name,$2,$3
  }' "$OUT/$name.raw.txt" > "$OUT/$name.norm.txt"
  echo "$name: $(wc -l < "$OUT/$name.norm.txt") samples"
}

MODE="${1:-both}"
ITERS="${2:-3}"
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
  go tool benchstat "$m" 2>/dev/null || cat "$m"
  echo ""
done
if [ -f "$OUT/legacy.norm.txt" ] && [ -f "$OUT/dseg.norm.txt" ]; then
  echo "=== A/B (New=legacy vs Old=dseg; +% = legacy faster) ==="
  go tool benchstat "$OUT/legacy.norm.txt" "$OUT/dseg.norm.txt" 2>/dev/null | tail -16
fi
echo "OUTDIR=$OUT"
