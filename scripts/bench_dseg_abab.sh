#!/usr/bin/env bash
# ABAB interleaved A/B: measures the dseg fix against the pre-fix code on the
# SAME machine, alternating so machine drift (thermal / background load) lands
# on both arms instead of correlating with whichever ran second.
#
# Usage: bash scripts/bench_dseg_abab.sh [rounds]   # default 2 (4 bench runs)
set -u
cd "$(git rev-parse --show-toplevel)" || exit 1

ROUNDS=${1:-2}
D=transport/internet/splithttp
FILES=("$D/downseg.go" "$D/downseg_puller.go" "$D/hub.go")
BAK=$(mktemp -d)
trap 'for f in "${FILES[@]}"; do cp "$BAK/$(basename "$f").new" "$f" 2>/dev/null; done' EXIT

# Save the fixed ("new") versions and the HEAD ("old") versions.
for f in "${FILES[@]}"; do
  cp "$f" "$BAK/$(basename "$f").new"
  git show "HEAD:./$f" > "$BAK/$(basename "$f").old"
done

swap() { # swap <old|new>
  for f in "${FILES[@]}"; do cp "$BAK/$(basename "$f").$1" "$f"; done
}

for r in $(seq 1 "$ROUNDS"); do
  for arm in old new; do
    swap "$arm"
    out=/tmp/abab_${arm}_r${r}.log
    bash scripts/bench_dseg.sh both 6 > "$out" 2>&1
    line=$(grep -E '^Downlink' "$out" | tail -1)
    echo "round $r  $arm  $line"
  done
done
swap new
echo "restored=fixed"
