#!/usr/bin/env bash
# netem_loss_test.sh — high-loss/high-latency tunnel verification for
# Bray-Core XHTTP packet-up+dseg. This is the physical packet-loss validation
# lane; the Windows/CI weak-link proxy tests latency/bandwidth but MUST NOT
# "drop bytes" inside a TCP stream (that would be corruption, not IP loss).
#
# Requires Linux + tc (netem). Run the Bray client and server on two hosts
# (or namespaces) with this script applying impairment on the WAN-facing
# egress of the server / router. The optional fifth argument is a shell
# command that executes the actual tunneled workload while impairment is on.
#
# Usage:
#   scripts/netem_loss_test.sh <iface> [loss%] [delay-ms] [duration-s] [workload-command]
#
# Examples:
#   scripts/netem_loss_test.sh eth0 1 150 120
#   scripts/netem_loss_test.sh eth0 5 150 120 'iperf3 -c <tunnel-target> -t 90'
#
# Metrics to record on the CLIENT during each run:
#   - packet-up POST retry/404 count; dseg 410 count; reconnect count
#   - sustained tunnel download/upload throughput + TTFB
#   - p50/p95/p99 latency and whether the application stream remains intact
#
# Run control first (loss 0), then 1% and 5% loss with the SAME workload.
# Compare before/after builds with benchstat where the workload produces
# benchmark-format output; otherwise retain timestamped logs/graphs.
set -euo pipefail

IFACE="${1:-}"
LOSS="${2:-1}"
DELAY_MS="${3:-150}"
DUR="${4:-120}"
WORKLOAD="${5:-}"
APPLIED=0

if [[ -z "$IFACE" ]]; then
  echo "usage: $0 <iface> [loss%] [delay-ms] [duration-s] [workload-command]" >&2
  exit 1
fi
if ! command -v tc >/dev/null 2>&1; then
  echo "tc not found — this script must run on Linux with iproute2." >&2
  exit 1
fi

cleanup() {
  if [[ "$APPLIED" == 1 ]]; then
    echo "== removing netem impairment from $IFACE"
    sudo tc qdisc del dev "$IFACE" root 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

echo "== applying netem on $IFACE: loss ${LOSS}%, egress delay ${DELAY_MS}ms, ${DUR}s"
sudo tc qdisc replace dev "$IFACE" root netem loss "${LOSS}%" delay "${DELAY_MS}ms"
APPLIED=1

if [[ -n "$WORKLOAD" ]]; then
  echo "== running workload under impairment: $WORKLOAD"
  # The operator owns the deployment/endpoint command. It must finish before
  # DUR or be backgrounded by the caller; no credentials/configs are embedded.
  bash -lc "$WORKLOAD"
else
  echo "== impairment active for ${DUR}s — run the tunneled workload now"
  sleep "$DUR"
fi

echo "== workload window complete"
