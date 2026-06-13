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
REM   benchmark.bat -race             - Run with race detector (requires gcc)
REM ============================================================================

setlocal enabledelayedexpansion

set BENCH_DIR=%~dp0
set OUTPUT_DIR=%BENCH_DIR%bench_results
if not exist "%OUTPUT_DIR%" mkdir "%OUTPUT_DIR%"

for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set DATETIME=%%I
set TIMESTAMP=%DATETIME:~0,4%%DATETIME:~4,2%%DATETIME:~6,2%_%DATETIME:~8,2%%DATETIME:~10,2%%DATETIME:~12,2%

echo ============================================================================
echo  Bray-Core Benchmark Suite
echo  %date% %time%
echo ============================================================================
echo.

REM Parse arguments
set RACE_FLAG=
set SHORT_FLAG=
set BENCH_FILTER=.
:parse_args
if "%~1"=="" goto :done_args
if "%~1"=="-race" set RACE_FLAG=-race
if "%~1"=="-short" set SHORT_FLAG=-short
if "%~1"=="-bench=XHTTP" set BENCH_FILTER=XHTTP
if "%~1"=="-bench=XMUX" set BENCH_FILTER=XMUX
if "%~1"=="-bench=Reality" set BENCH_FILTER=Reality
if "%~1"=="-bench=HappyEyeballs" set BENCH_FILTER=HappyEyeballs
if "%~1"=="-bench=Warmup" set BENCH_FILTER=Warmup
shift
goto :parse_args
:done_args

if defined RACE_FLAG echo (Race detector enabled)
if defined SHORT_FLAG echo (Short mode)

echo.
echo ============================================================================
echo  [1/5] Reality Handshake Benchmarks
echo ============================================================================
if "%BENCH_FILTER%"=="." (
    go test -bench=BenchmarkReality -benchmem -count=3 -timeout=300s %RACE_FLAG% %SHORT_FLAG% ./transport/internet/... 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%OUTPUT_DIR%\reality_%TIMESTAMP%.log'"
) else if "%BENCH_FILTER%"=="Reality" (
    go test -bench=BenchmarkReality -benchmem -count=3 -timeout=300s %RACE_FLAG% %SHORT_FLAG% ./transport/internet/... 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%OUTPUT_DIR%\reality_%TIMESTAMP%.log'"
)
echo.

echo ============================================================================
echo  [2/5] XMUX Connection Pool Benchmarks
echo ============================================================================
if "%BENCH_FILTER%"=="." (
    go test -bench=BenchmarkXMUX -benchmem -count=3 -timeout=300s %RACE_FLAG% %SHORT_FLAG% ./transport/internet/splithttp/... 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%OUTPUT_DIR%\xmux_%TIMESTAMP%.log'"
) else if "%BENCH_FILTER%"=="XMUX" (
    go test -bench=BenchmarkXMUX -benchmem -count=3 -timeout=300s %RACE_FLAG% %SHORT_FLAG% ./transport/internet/splithttp/... 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%OUTPUT_DIR%\xmux_%TIMESTAMP%.log'"
)
echo.

echo ============================================================================
echo  [3/5] XHTTP Throughput Benchmarks
echo ============================================================================
if "%BENCH_FILTER%"=="." (
    go test -bench=BenchmarkXHTTP -benchmem -count=3 -timeout=600s %RACE_FLAG% %SHORT_FLAG% ./transport/internet/splithttp/... 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%OUTPUT_DIR%\xhttp_%TIMESTAMP%.log'"
) else if "%BENCH_FILTER%"=="XHTTP" (
    go test -bench=BenchmarkXHTTP -benchmem -count=3 -timeout=600s %RACE_FLAG% %SHORT_FLAG% ./transport/internet/splithttp/... 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%OUTPUT_DIR%\xhttp_%TIMESTAMP%.log'"
)
echo.

echo ============================================================================
echo  [4/5] Happy Eyeballs v3 Benchmarks
echo ============================================================================
if "%BENCH_FILTER%"=="." (
    go test -bench='Benchmark(ScoreIPs|SortIPScores|HappyIPRecord|HappyIPDB|TryController|SortIPs|ClampRTT|HappyIPScore)' -benchmem -count=3 -timeout=300s %RACE_FLAG% %SHORT_FLAG% ./transport/internet/... 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%OUTPUT_DIR%\happy_eyeballs_%TIMESTAMP%.log'"
) else if "%BENCH_FILTER%"=="HappyEyeballs" (
    go test -bench='Benchmark(ScoreIPs|SortIPScores|HappyIPRecord|HappyIPDB|TryController|SortIPs|ClampRTT|HappyIPScore)' -benchmem -count=3 -timeout=300s %RACE_FLAG% %SHORT_FLAG% ./transport/internet/... 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%OUTPUT_DIR%\happy_eyeballs_%TIMESTAMP%.log'"
)
echo.

echo ============================================================================
echo  [5/5] Warmup Pipeline Benchmarks
echo ============================================================================
if "%BENCH_FILTER%"=="." (
    go test -bench='BenchmarkWarmup|BenchmarkIsIP|BenchmarkNetworkHash' -benchmem -count=3 -timeout=300s %RACE_FLAG% %SHORT_FLAG% ./transport/internet/... 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%OUTPUT_DIR%\warmup_%TIMESTAMP%.log'"
) else if "%BENCH_FILTER%"=="Warmup" (
    go test -bench='BenchmarkWarmup|BenchmarkIsIP|BenchmarkNetworkHash' -benchmem -count=3 -timeout=300s %RACE_FLAG% %SHORT_FLAG% ./transport/internet/... 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath '%OUTPUT_DIR%\warmup_%TIMESTAMP%.log'"
)
echo.

echo ============================================================================
echo  Benchmark Results saved to: %OUTPUT_DIR%
echo ============================================================================
echo.
echo To compare with upstream:
echo   1. Run same benchmarks on upstream Xray-core
echo   2. Save results to files
echo   3. Run: benchstat upstream.txt bray.txt

endlocal
