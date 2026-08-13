#!/usr/bin/env bash
# upstream_cover.sh — register an upstream commit as "already covered /
# already merged" so it disappears from the triage view
# (scripts/upstream_triage.sh filters it out via docs/upstream-coverage.md).
#
# (A git-replace based approach was tried and rejected: nested replace
# chains hit git's replace-depth limit on long covered chains, and a
# replaced upstream head would corrupt traversal of FUTURE fetched
# commits whose parents point at the replaced object. Ledger filtering is
# side-effect free.)
#
# Usage:
#   scripts/upstream_cover.sh <upstream-sha> [reason]
#
# Examples:
#   scripts/upstream_cover.sh 0bafca94 "stats atomicity already in 66567499"
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

SHA="${1:-}"
REASON="${2:-}"
if [[ -z "$SHA" ]]; then
  echo "usage: $0 <upstream-sha> [reason]" >&2
  exit 1
fi

FULL="$(git rev-parse "$SHA^{commit}")"
SHORT="$(git rev-parse --short "$FULL")"
SUBJECT="$(git log -1 --format=%s "$FULL" | sed 's/|/\\|/g')"

LEDGER="docs/upstream-coverage.md"
if grep -q "^| \`$SHORT\`" "$LEDGER" 2>/dev/null; then
  echo "already registered: $SHORT"
  exit 0
fi

{
  echo "| \`$SHORT\` | $SUBJECT | ${REASON:-} | $(date +%F) |"
} >> "$LEDGER"
echo "registered: $SHORT ($SUBJECT)"
