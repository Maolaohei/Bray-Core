#!/bin/bash
# ============================================================================
# Bray-Core Race Detector Tests
# Run all tests with Go race detector enabled
# ============================================================================
#
# Usage:
#   ./race_test.sh                    - Run all tests with -race
#   ./race_test.sh -short             - Run quick tests only
#   ./race_test.sh XMUX               - Run only XMUX tests
#   ./race_test.sh Warmup             - Run only Warmup tests
#   ./race_test.sh HappyEyeballs      - Run only Happy Eyeballs tests
#   ./race_test.sh Reality            - Run only Reality tests
# ============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUTPUT_DIR="$SCRIPT_DIR/race_results"
mkdir -p "$OUTPUT_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "============================================================================"
echo " Bray-Core Race Detector Tests"
echo " $(date)"
echo "============================================================================"
echo

# Determine what to test
if [ -z "$1" ]; then
    TEST_FILTER="."
else
    case "$1" in
        XMUX|xmux)
            TEST_FILTER="./transport/internet/splithttp/..."
            echo "Running XMUX race tests..."
            ;;
        Warmup|warmup)
            TEST_FILTER="./transport/internet/..."
            echo "Running Warmup race tests..."
            ;;
        HappyEyeballs|happy_eyeballs)
            TEST_FILTER="./transport/internet/..."
            echo "Running Happy Eyeballs race tests..."
            ;;
        Reality|reality)
            TEST_FILTER="./transport/internet/reality/..."
            echo "Running Reality race tests..."
            ;;
        *)
            TEST_FILTER="$1"
            echo "Running race tests for: $TEST_FILTER"
            ;;
    esac
fi

SHORT_FLAG=""
if [ "$2" = "-short" ] || [ "$1" = "-short" ]; then
    SHORT_FLAG="-short"
    echo "(Short mode enabled)"
fi

echo
echo "============================================================================"
echo " [1/4] Running unit tests with -race"
echo "============================================================================"
go test -race -count=1 -timeout=300s $SHORT_FLAG $TEST_FILTER 2>&1 | tee "$OUTPUT_DIR/unit_race_$TIMESTAMP.log"
echo

echo "============================================================================"
echo " [2/4] Running XMUX race tests"
echo "============================================================================"
go test -race -run='Test.*XMUX|TestMaxConnections|TestCMaxReuseTimes|TestMaxConcurrency|TestConcurrentPoolAccess' \
    -count=1 -timeout=120s ./transport/internet/splithttp/... 2>&1 | tee "$OUTPUT_DIR/xmux_race_$TIMESTAMP.log"
echo

echo "============================================================================"
echo " [3/4] Running Warmup pipeline race tests"
echo "============================================================================"
go test -race -run='TestWarmup|TestExtractWarmupDomains' \
    -count=1 -timeout=120s ./transport/internet/... 2>&1 | tee "$OUTPUT_DIR/warmup_race_$TIMESTAMP.log"
echo

echo "============================================================================"
echo " [4/4] Running Happy Eyeballs v3 race tests"
echo "============================================================================"
go test -race -run='TestScore|TestHappyIP|TestTryController|TestSortIPs' \
    -count=1 -timeout=120s ./transport/internet/... 2>&1 | tee "$OUTPUT_DIR/happy_eyeballs_race_$TIMESTAMP.log"
echo

echo "============================================================================"
echo " Race test results saved to: $OUTPUT_DIR"
echo "============================================================================"
echo

# Summary
echo "Race test summary:"
PASS_COUNT=$(grep -r "^ok" "$OUTPUT_DIR"/*_race_$TIMESTAMP.log 2>/dev/null | wc -l)
FAIL_COUNT=$(grep -r "^FAIL" "$OUTPUT_DIR"/*_race_$TIMESTAMP.log 2>/dev/null | wc -l)
echo "  Passed: $PASS_COUNT"
echo "  Failed: $FAIL_COUNT"

if [ "$FAIL_COUNT" -gt 0 ]; then
    echo
    echo "FAILED TESTS:"
    grep -r "^FAIL" "$OUTPUT_DIR"/*_race_$TIMESTAMP.log 2>/dev/null
    exit 1
fi

echo
echo "All race tests passed!"
