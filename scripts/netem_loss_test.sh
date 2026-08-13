#!/usr/bin/env bash
# netem_loss_test.sh — high-loss/high-latency tunnel verification for
# Bray-Core XHTTP (ToT L1/L2 + L2a batch + M1 segment work).
#
# Requires Linux + tc (netem). Run the Bray client and server on two
# hosts (or namespaces) with this script applying the impairment on the
# WAN-side interface of the server (or a router in between).
#
# Usage:
#   scripts/netem_loss_test.sh <iface> [loss%] [delay-ms] [duration-s]
#
# Examples:
#   scripts/netem_loss_test.sh eth0 1 150 120     # 1% loss, 150ms RTT
#   scripts/netem_loss_test.sh eth0 5 150 120     # 5% loss (collapse zone)
#
# Metrics to collect while it runs (on the client):
#   - iperf3 -c <server> -u -b 20M (UDP throughput)
#   - ping -i 0.2 <target>          (latency spikes / tail)
#   - XHTTP TTFB via the benchmark suite with the same impairment
# Compare before/after (baseline run with loss 0) using benchstat.
set -euo pipefail

IFACE="${1:-}"
LOSS="${2:-1}"
DELAY_MS="${3:-150}"
DUR="${4:-120}"

if [[ -z "$IFACE" ]]; then
  echo "usage: $0 <iface> [loss%] [delay-ms] [duration-s]" >&2
  exit 1
fi

if ! command -v tc >/dev/null 2>&1; then
  echo "tc not found — this script must run on Linux with iproute2." >&2
  exit 1
fi

echo "== applying netem on $IFACE: loss ${LOSS}%, delay ${DELAY_MS}ms, ${DUR}s"
sudo tc qdisc replace dev "$IFACE" root netem loss "${LOSS}%" delay "${DELAY_MS}ms" 2>/dev/null \
  || sudo tc qdisc add dev "$IFACE" root netem loss "${LOSS}%" delay "${DELAY_MS}ms"

echo "== impairments active for ${DUR}s — run your measurements now"
sleep "$DUR"

echo "== removing impairments"
sudo tc qdisc del dev "$IFACE" root
echo "== done"
