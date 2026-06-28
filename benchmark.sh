#!/bin/bash
# ============================================================================
# Bray-Core Benchmark Suite
# Comprehensive performance benchmarks for comparison with upstream Xray-core
# ============================================================================
#
# Usage:
#   ./benchmark.sh                   - Run all benchmarks
#   ./benchmark.sh -short            - Run quick benchmarks only
#   ./benchmark.sh -bench=XHTTP      - Run only XHTTP benchmarks
#   ./benchmark.sh -bench=XMUX       - Run only XMUX benchmarks
#   ./benchmark.sh -bench=Reality    - Run only Reality benchmarks
#   ./benchmark.sh -bench=HappyEyeballs - Run only Happy Eyeballs benchmarks
#   ./benchmark.sh -bench=PProf      - Run only PProf profiling (CPU 20s + Memory + Goroutine)
#   ./benchmark.sh -race             - Run with race detector
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUTPUT_DIR="$SCRIPT_DIR/bench_results"
mkdir -p "$OUTPUT_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "============================================================================"
echo " Bray-Core Benchmark Suite"
echo " $(date)"
echo "============================================================================"
echo

# Determine mode
RACE_FLAG=""
BENCH_FILTER=""
for arg in "$@"; do
    case "$arg" in
        -race)
            RACE_FLAG="-race"
            echo "(Race detector enabled)"
            ;;
        -short)
            SHORT_FLAG="-short"
            echo "(Short mode)"
            ;;
        -bench=*)
            BENCH_FILTER="${arg#-bench=}"
            echo "(Filtering benchmarks: $BENCH_FILTER)"
            ;;
    esac
done

if [ -z "$BENCH_FILTER" ]; then
    BENCH_FILTER="."
fi

run_reality() {
echo
echo "============================================================================"
echo " [1/6] Reality Handshake Benchmarks"
echo "============================================================================"
go test -bench=BenchmarkReality -benchmem -count=3 -timeout=300s \
    $RACE_FLAG $SHORT_FLAG ./transport/internet/... 2>&1 | tee "$OUTPUT_DIR/reality_$TIMESTAMP.log"
}

run_xmux() {
echo
echo "============================================================================"
echo " [2/6] XMUX Connection Pool Benchmarks"
echo "============================================================================"
go test -bench=BenchmarkXMUX -benchmem -count=3 -timeout=300s \
    $RACE_FLAG $SHORT_FLAG ./transport/internet/splithttp/... 2>&1 | tee "$OUTPUT_DIR/xmux_$TIMESTAMP.log"
}

run_xhttp() {
echo
echo "============================================================================"
echo " [3/6] XHTTP Throughput Benchmarks"
echo "============================================================================"
go test -bench=BenchmarkXHTTP -benchmem -count=3 -timeout=600s \
    $RACE_FLAG $SHORT_FLAG ./transport/internet/splithttp/... 2>&1 | tee "$OUTPUT_DIR/xhttp_$TIMESTAMP.log"
}

run_he() {
echo
echo "============================================================================"
echo " [4/6] Happy Eyeballs v3 Benchmarks"
echo "============================================================================"
go test -bench='Benchmark(ScoreIPs|SortIPScores|HappyIPRecord|HappyIPDB|TryController|SortIPs|ClampRTT|HappyIPScore)' \
    -benchmem -count=3 -timeout=300s \
    $RACE_FLAG $SHORT_FLAG ./transport/internet/... 2>&1 | tee "$OUTPUT_DIR/happy_eyeballs_$TIMESTAMP.log"
}

run_warmup() {
echo
echo "============================================================================"
echo " [5/6] Warmup Pipeline Benchmarks"
echo "============================================================================"
go test -bench='BenchmarkWarmup|BenchmarkIsIP|BenchmarkNetworkHash' \
    -benchmem -count=3 -timeout=300s \
    $RACE_FLAG $SHORT_FLAG ./transport/internet/... 2>&1 | tee "$OUTPUT_DIR/warmup_$TIMESTAMP.log"
}

run_pprof() {
echo
echo "============================================================================"
echo " [6/6] PProf Profiling (CPU 20s + Memory + Goroutine)"
echo "============================================================================"
go test -v -tags pprof -run TestPProf_Profiling -timeout=60s ./testing/ 2>&1 | tee "$OUTPUT_DIR/pprof_$TIMESTAMP.log"
echo "  Analyze: go tool pprof -top pprof_cpu_*.prof"
}

echo
case "$BENCH_FILTER" in
    .)
        run_reality
        run_xmux
        run_xhttp
        run_he
        run_warmup
        run_pprof
        ;;
    Reality) run_reality ;;
    XMUX) run_xmux ;;
    XHTTP) run_xhttp ;;
    HappyEyeballs) run_he ;;
    Warmup) run_warmup ;;
    PProf) run_pprof ;;
    *) echo "Unknown filter: $BENCH_FILTER" ;;
esac

echo
echo "============================================================================"
echo " Benchmark Results saved to: $OUTPUT_DIR"
echo "============================================================================"
echo

# Generate comparison report
cat > "$OUTPUT_DIR/README_$TIMESTAMP.md" << 'REPORT_EOF'
# Bray-Core Benchmark Report

## Key Metrics to Compare with Upstream Xray-core

| Metric | Upstream | Bray-Core | Delta |
|--------|----------|-----------|-------|
| Reality Handshake QPS | ? | ? | ? |
| Reality ECDH ops/s | ? | ? | ? |
| Reality AEAD Seal ops/s | ? | ? | ? |
| Reality AEAD Open ops/s | ? | ? | ? |
| Reality ML-DSA-65 Sign ops/s | ? | ? | ? |
| Reality ML-DSA-65 Verify ops/s | ? | ? | ? |
| XHTTP H2C Throughput (Mbps) | ? | ? | ? |
| XHTTP H2 Throughput (Mbps) | ? | ? | ? |
| XHTTP Stream-Up Throughput (Mbps) | ? | ? | ? |
| XHTTP TTFB (ns) | ? | ? | ? |
| XHTTP Connection Storm (conn/s) | ? | ? | ? |
| XMUX GetXmuxClient (ops/s) | ? | ? | ? |
| XMUX ScoreClient (ops/s) | ? | ? | ? |
| XMUX RTT EWMA (ops/s) | ? | ? | ? |
| Happy Eyeballs v3 ScoreIPs (ops/s) | ? | ? | ? |
| Happy Eyeballs v3 TryController (ops/s) | ? | ? | ? |
| Warmup Pipeline DNS (ops/s) | ? | ? | ? |
| Memory: Bytes/Op | ? | ? | ? |
| Memory: Allocs/Op | ? | ? | ? |
| P99 Latency (TTFB) | ? | ? | ? |

## How to Compare

1. Run these benchmarks on Bray-Core (this script)
2. Clone upstream Xray-core and run same benchmarks
3. Use benchstat for statistical comparison:

```bash
go install golang.org/x/perf/cmd/benchstat@latest
benchstat upstream_reality.txt bray_reality.txt
benchstat upstream_xhttp.txt bray_xhttp.txt
benchstat upstream_xmux.txt bray_xmux.txt
benchstat upstream_happy_eyeballs.txt bray_happy_eyeballs.txt
```

## Race Detector

```bash
./race_test.sh
```

Tests with race detector for:
- XMUX concurrent pool access
- Warmup pipeline concurrent execution
- Happy Eyeballs v3 concurrent scoring
REPORT_EOF

echo "Comparison report generated: $OUTPUT_DIR/README_$TIMESTAMP.md"
echo
echo "To compare with upstream:"
echo "  1. Run same benchmarks on upstream Xray-core"
echo "  2. Save results to files"
echo "  3. Run: benchstat upstream.txt bray.txt"
