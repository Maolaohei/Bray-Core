@echo off
REM ============================================================================
REM Bray-Core Benchmark Suite
REM ============================================================================
REM
REM Usage:
REM   benchmark.bat                   - Run all benchmarks (short mode)
REM   benchmark.bat -full             - Run all benchmarks (include endurance)
REM   benchmark.bat -bench=XHTTP      - Run only XHTTP benchmarks
REM   benchmark.bat -bench=XMUX       - Run only XMUX benchmarks
REM   benchmark.bat -bench=Reality    - Run only Reality benchmarks
REM   benchmark.bat -bench=HappyEyeballs - Run only Happy Eyeballs benchmarks
REM   benchmark.bat -bench=Warmup     - Run only Warmup benchmarks
REM   benchmark.bat -bench=PProf      - Run only PProf profiling (CPU 20s + Memory + Goroutine)
REM   benchmark.bat -race             - Run with race detector
REM ============================================================================

setlocal enabledelayedexpansion

set BENCH_DIR=%~dp0
set OUTPUT_DIR=%BENCH_DIR%bench_results
if not exist "%OUTPUT_DIR%" mkdir "%OUTPUT_DIR%"

for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set TIMESTAMP=%%I

echo Bray-Core Benchmark Suite - %date% %time%

set RACE_FLAG=
set SHORT_FLAG=-short
set BENCH_FILTER=.
:parse_args
if "%~1"=="" goto :done_args
if "%~1"=="-race" set RACE_FLAG=-race
if "%~1"=="-full" set SHORT_FLAG=
if "%~1"=="-bench=XHTTP" set BENCH_FILTER=XHTTP
if "%~1"=="-bench=XMUX" set BENCH_FILTER=XMUX
if "%~1"=="-bench=Reality" set BENCH_FILTER=Reality
if "%~1"=="-bench=HappyEyeballs" set BENCH_FILTER=HappyEyeballs
if "%~1"=="-bench=Warmup" set BENCH_FILTER=Warmup
shift
goto :parse_args
:done_args

if "%SHORT_FLAG%"=="" (echo Mode: FULL) else echo Mode: SHORT (endurance skipped)

if "%BENCH_FILTER%"=="." goto :all
if "%BENCH_FILTER%"=="Reality" goto :run_reality
if "%BENCH_FILTER%"=="XMUX" goto :run_xmux
if "%BENCH_FILTER%"=="XHTTP" goto :run_xhttp
if "%BENCH_FILTER%"=="HappyEyeballs" goto :run_he
if "%BENCH_FILTER%"=="Warmup" goto :run_warmup
if "%BENCH_FILTER%"=="PProf" goto :run_pprof
goto :done

:all
call :run_reality
call :run_xmux
call :run_xhttp
call :run_he
call :run_warmup
call :run_pprof
goto :done

:run_reality
echo [1/6] Reality...
go test -bench=BenchmarkReality -benchmem -count=3 -timeout=300s -run=^$ %RACE_FLAG% %SHORT_FLAG% ./transport/internet/ >"%OUTPUT_DIR%\reality_%TIMESTAMP%.log" 2>&1
echo   Saved: reality_%TIMESTAMP%.log
goto :eof

:run_xmux
echo [2/6] XMUX...
go test -bench=BenchmarkXMUX -benchmem -count=3 -timeout=300s -run=^$ %RACE_FLAG% %SHORT_FLAG% ./transport/internet/splithttp/... >"%OUTPUT_DIR%\xmux_%TIMESTAMP%.log" 2>&1
echo   Saved: xmux_%TIMESTAMP%.log
goto :eof

:run_xhttp
echo [3/6] XHTTP...
go test -bench=BenchmarkXHTTP -benchmem -count=3 -timeout=600s -run=^$ %RACE_FLAG% %SHORT_FLAG% ./transport/internet/splithttp/... >"%OUTPUT_DIR%\xhttp_%TIMESTAMP%.log" 2>&1
echo   Saved: xhttp_%TIMESTAMP%.log
goto :eof

:run_he
echo [4/6] Happy Eyeballs...
go test -bench="Benchmark(ScoreIPs|SortIPScores|HappyIPRecord|HappyIPDB|SortIPs|ClampRTT|HappyIPScore)" -benchmem -count=3 -timeout=300s -run=^$ %RACE_FLAG% %SHORT_FLAG% ./transport/internet/ >"%OUTPUT_DIR%\happy_eyeballs_%TIMESTAMP%.log" 2>&1
echo   Saved: happy_eyeballs_%TIMESTAMP%.log
goto :eof

:run_warmup
echo [5/6] Warmup...
go test -bench="BenchmarkWarmup|BenchmarkIsIP|BenchmarkNetworkHash" -benchmem -count=3 -timeout=300s -run=^$ %RACE_FLAG% %SHORT_FLAG% ./transport/internet/ >"%OUTPUT_DIR%\warmup_%TIMESTAMP%.log" 2>&1
echo   Saved: warmup_%TIMESTAMP%.log
goto :eof

:run_pprof
echo [6/6] PProf Profiling...
go test -v -tags pprof -run TestPProf_Profiling -timeout=60s ./testing/ >"%OUTPUT_DIR%\pprof_%TIMESTAMP%.log" 2>&1
echo   Saved: pprof_%TIMESTAMP%.log
echo   Analyze: go tool pprof -top pprof_cpu_*.prof
goto :eof

:done
echo.
echo Results in: %OUTPUT_DIR%
echo Use: findstr "Benchmark" %OUTPUT_DIR%\*_TIMESTAMP.log
endlocal
