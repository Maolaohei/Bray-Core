# Handoff: Benchmark Dual-Track (Step 1+2)

## Goal
Make Bray-Core bench reports show **product advantage**, not only “no regression”.

## Done in this change
1. **Step 1 – Narrative + Advantage board**
   - `scripts/format_bench_report.py`
     - `advantage_section()` / `ADVANTAGE_SUITES` / name hints
     - Report meta: Regression vs Advantage tracks
     - SVG prefers advantage-suite deltas when present
     - Default suites include `xhttp_core`
   - README + `bench_results/COMPARISON_REPORT.md` dual-track wording
2. **Step 2 – CI suite `xhttp_core`**
   - `.github/workflows/benchmark.yml`
     - New step **Run XHTTP core benchmarks**
     - Regex: `XHTTP_TTFB|XHTTP_Burst_|XHTTP_MemoryAllocations|XHTTP_Modes|GetRequestHeader|FormatSeqInt64`
     - timeout 420s, count=3
     - **Not** included: `ConnectionStorm` (slow/noisy; leave nightly/local)
     - sanitize / format / baseline cache loops include `xhttp_core`

## Key files
| Path | Role |
|------|------|
| `scripts/format_bench_report.py` | MD tables, Advantage Highlights, SVG, history |
| `.github/workflows/benchmark.yml` | CI suites + formatter invocation |
| `bench_results/upstream/xray-core-v26.6.22.json` | Fixed Upstream column (common/*) |
| `bench_results/history/` | Per-run JSON + latest.md |
| `transport/internet/splithttp/xhttp_bench_test.go` | XHTTP benches |
| `transport/internet/splithttp/packet_upload_test.go` | GetRequestHeader / FormatSeqInt64 |

## Readout contract
| Column / board | Meaning |
|----------------|---------|
| Upstream | Fixed Xray-core snapshot; mostly common micro; **not** Bray marketing |
| Self-baseline | Previous main CI `base_*.txt` |
| Current | This run `new_*.txt` |
| Summary | Regression counts |
| Advantage Highlights | Product-facing subset |
| Trend emoji | vs **Self-baseline**, ±3% = ⚪ |

## Local verify
```powershell
cd D:\UGit\Bray-Core
# optional: produce new_xhttp_core.txt
$env:GOCACHE='D:\UGit\Bray-Core\.gocache'
go test -run=^$ -bench='Benchmark(XHTTP_TTFB|XHTTP_Burst_|XHTTP_MemoryAllocations|XHTTP_Modes|GetRequestHeader|FormatSeqInt64)' -benchmem -count=1 -timeout=420s ./transport/internet/splithttp/ | Select-String '^Benchmark' | Set-Content bench_results/new_xhttp_core.txt
python scripts/format_bench_report.py --history --sha dryrun-adv --suites xhttp_core,xmux,happy,warmup,vless,buf
# report.md should contain "## Advantage Highlights"
```

## Not done / next (Step 3+)
- Nightly: ConnectionStorm, multi-conn product thruput, REALITY heavier benches
- Optional live UpstreamCompare job for same-name micro only
- README top-line KPI strip fed from `history/latest.md` after first green CI with xhttp_core baseline
- Do **not** `git add` `.gocache` / `.gotmp*` / `__pycache__`

## User prefs
- Chinese wrap-ups; intentional stage only; if git ACL blocks agent push, give local commands
- Bray-only stack; upstream compatibility not required for protocol work


## Commit status (as of agent session)
- Code/docs for Step 1+2 are **written in the worktree** but this agent environment could not spawn `git`/`python` (Windows CreateProcess EPERM).
- Current `.git/HEAD` was `e96eaa05…` when checked via filesystem (may move if user committed elsewhere).
- Use `.gotmp/commit_step12.ps1` or the commands below to verify + commit + push.

## Required commit set (only these)
```
scripts/format_bench_report.py
.github/workflows/benchmark.yml
README.md
bench_results/COMPARISON_REPORT.md
bench_results/history/README.md
docs/HANDOFF_BENCH_ADVANTAGE.md
```
Do **not** add `.gocache/`, `.gotmp*/`, `scripts/__pycache__/`, synthetic `bench_results/new_*.txt` from local dry-runs.

## Push commands
```powershell
cd D:\UGit\Bray-Core
python -m py_compile scripts/format_bench_report.py
# optional dry-run then delete report artifacts
git add scripts/format_bench_report.py .github/workflows/benchmark.yml README.md bench_results/COMPARISON_REPORT.md bench_results/history/README.md docs/HANDOFF_BENCH_ADVANTAGE.md
git commit -m "docs(bench): dual-track Advantage board + CI xhttp_core suite"
git pull --rebase origin main   # if rejected
git push origin main
git ls-remote origin refs/heads/main
git rev-parse HEAD
```
