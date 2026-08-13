#!/usr/bin/env bash
# upstream_triage.sh — list upstream commits NOT yet covered/merged, for
# merge evaluation. Commits registered in docs/upstream-coverage.md
# (via scripts/upstream_cover.sh) are filtered out, so already-handled
# upstream work never reappears in the triage view.
#
# Usage:
#   scripts/upstream_triage.sh          # list uncovered commits
#   scripts/upstream_triage.sh --count  # just the count
#
# Requirements: git; upstream remote. Fetch first: git fetch upstream.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

LEDGER="docs/upstream-coverage.md"

# Build an exclude list of registered short shas (and full shas).
EXCL="$(mktemp)"
trap 'rm -f "$EXCL"' EXIT
if [[ -f "$LEDGER" ]]; then
  awk -F'|' '/^\| `[0-9a-f]/{gsub(/[ `]/,"",$2); print $2}' "$LEDGER" > "$EXCL"
fi

list() {
  # Filter by short sha (ledger stores short shas; 7-hex collisions are
  # negligible here) — no per-line subprocess needed.
  git log --oneline HEAD..upstream/main 2>/dev/null \
    | awk -v excl="$EXCL" 'NR==FNR { seen[$1]=1; next } { if (!($1 in seen)) print }' \
      "$EXCL" - \
    || true
}

if [[ "${1:-}" == "--count" ]]; then
  n=$(list | grep -c . || true)
  echo "${n:-0}"
  exit 0
fi

echo "== upstream commits NOT covered/merged (HEAD..upstream/main):"
list
echo
echo "== registered as covered: $(wc -l < "$EXCL")"
