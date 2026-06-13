@echo off
REM ============================================================================
REM Bray-Core Benchmark Suite
REM Comprehensive performance benchmarks for comparison with upstream Xray-core
REM ============================================================================
REM
REM Usage:
REM   benchmark.bat                   - Run all benchmarks
REM   benchmark.bat -short            - Run quick benchmarks only
REM   benchmark.bat -bench=XHTTP      - Run only XHTTP benchmarks
REM   benchmark.bat -bench=XMUX       - Run only XMUX benchmarks
REM   benchmark.bat -bench=Reality    - Run only Reality benchmarks
REM   benchmark.bat -bench=HappyEyeballs - Run only Happy Eyeballs benchmarks
REM   benchmark.bat -race             - Run with race detector
REM ============================================================================

setlocal enabledelayedexpansion

set BENCH_DIR=%~dp0
set OUTPUT_DIR=%BENCH_DIR%bench_results
if not exist "%OUTPUT_DIR%" mkdir "%OUTPUT_DIR%"

set TIMESTAMP=%date:~-4%%date:~4,2%%date:~7,2%_%time:~0,2%%time:~3,2%%time:~6,2%
set TIMESTAMP=%TIMESTAMP: =0%

echo ============================================================================
echo  Bray-Core Benchmark Suite
echo  %date% %time%
echo ============================================================================
echo.

REM Check if specific benchmark was requested
if "%1"=="" (
    set BENCH_FILTER=.*
) else (
    set BENCH_FILTER=%~1
)

REM Check for race detector flag
set RACE_FLAG=
if "%2"=="-race" set RACE_FLAG=-race
if "%1"=="-race" set RACE_FLAG=-race

echo [1/5] Running Reality Benchmarks...
echo ------------------------------------------------------------
go test -bench=BenchmarkReality -benchmem -count=3 -timeout=300s ./transport/internet/... 2>&1 | tee "%OUTPUT_DIR%/reality_%TIMESTAMP%.log"
echo.

echo [2/5] Running XMUX Benchmarks...
echo ------------------------------------------------------------
go test -bench=BenchmarkXMUX -benchmem -count=3 -timeout=300s ./transport/internet/splithttp/... 2>&1 | tee "%OUTPUT_DIR%/xmux_%TIMESTAMP%.log"
echo.

echo [3/5] Running XHTTP Throughput Benchmarks...
echo ------------------------------------------------------------
go test -bench=BenchmarkXHTTP -benchmem -count=3 -timeout=600s ./transport/internet/splithttp/... 2>&1 | tee "%OUTPUT_DIR%/xhttp_%TIMESTAMP%.log"
echo.

echo [4/5] Running Happy Eyeballs Benchmarks...
echo ------------------------------------------------------------
go test -bench=BenchmarkHappy -benchmem -bench=BenchmarkSortIPs -bench=BenchmarkScoreIPs -bench=BenchmarkClampRTT -bench=BenchmarkTryController -count=3 -timeout=300s ./transport/internet/... 2>&1 | tee "%OUTPUT_DIR%/happy_eyeballs_%TIMESTAMP%.log"
echo.

echo [5/5] Running Warmup Pipeline Benchmarks...
echo ------------------------------------------------------------
go test -bench=BenchmarkWarmup -benchmem -count=3 -timeout=300s ./transport/internet/... 2>&1 | tee "%OUTPUT_DIR%/warmup_%TIMESTAMP%.log"
echo.

echo ============================================================================
echo  Benchmark Results saved to: %OUTPUT_DIR%
echo ============================================================================
echo.

REM Generate comparison report
echo Generating comparison report...
(
echo ============================================================================
echo  Bray-Core Benchmark Report
echo  Generated: %date% %time%
echo ============================================================================
echo.
echo This report contains benchmark results for Bray-Core modules.
echo Compare these numbers against upstream Xray-core at:
echo   https://github.com/XTLS/Xray-core
echo.
echo Key Metrics to Compare:
echo   - Reality Handshake QPS (crypto operations per second)
echo   - XHTTP Throughput (Mbps for each mode)
echo   - P99 Latency (TTFB measurements)
echo   - CPU Usage (operations per second)
echo   - Memory Usage (allocations per operation)
echo   - GC Time (via -gcflags=-m)
echo.
echo Run 'go tool benchstat' on results for statistical comparison:
echo   go install golang.org/x/perf/cmd/benchstat@latest
echo   benchstat old.txt new.txt
echo ============================================================================
) > "%OUTPUT_DIR%/README_%TIMESTAMP%.md"

echo Done! Results in %OUTPUT_DIR%
echo.
echo To compare with upstream:
echo   1. Run same benchmarks on upstream Xray-core
echo   2. Save results to files
echo   3. Run: benchstat upstream.txt bray.txt

endlocal
