# optN10 residual — 2026-07-25

## Goal
Continue P1/P2/P3 under **stability first** after optN9. Measure honestly; do not overwrite product thruput headlines with short soft windows.

## Code (this wave)
- **P1 path** (`transport/internet/splithttp/config.go`)
  - `appendToPath2`: `strings.Builder` → single `[]byte` then `string(buf)` (still 1 path alloc, less builder overhead)
- **P2 XMUX** (`transport/internet/splithttp/mux.go`)
  - `idleTimeoutNs` cached on manager (no per-candidate `int64(clientIdleTimeout)`)
  - multi-pool scan uses indexed loop over local `clients` slice
  - **Rejected** sticky skip-scan (would pin high-RTT client under load)
- **P2 HE** (`transport/internet/happy_eyeballs.go`)
  - `sortIPs`: classify by `len(ip)` (`IPv4len` / `IPv6len`) before `To4`/`To16` conversion work
- **P3 common/buf** (`common/buf/copy.go`)
  - `copyPlain` no-option hot path; `Copy` skips empty `onData` handler setup when no options

## Verify
- Prior unit gates this wave: `go test ./common/buf/`, HE Sort/Happy/Score, splithttp Acquire/Fill/H1/H2/XMUX — **PASS**
- Bench binary: `bench_results/run_20260725_optN10/splithttp_optN10.test.exe`
- Launcher: `run_optn10.ps1` · status all exit **0** · `done.flag` present

## Official micro (median of serial samples)

### common/buf.Copy
| Bench | upstream v26.6.22 | Bray quiet (old) | **optN10 med** | Trend |
|------|------------------:|-----------------:|---------------:|:-----:|
| `BenchmarkCopy` | 98.13 ns · 4 alloc | ~126.5 ns | **~79.9 ns · 4 alloc** (samples 93.8 / 77.5 / 73.5 / 86.0 / 79.9) | 🟢 vs upstream + vs old Bray |

### XMUX (0 B/op)
| Bench | quiet | optN8 clean | **optN10 med** | Trend |
|------|------:|------------:|---------------:|:-----:|
| Get | 129 | ~63 | **~59.4** (59.4 / 71.0 / 58.4) | 🟢 |
| pool_1 | 160 | ~69–82 | **~61.5** | 🟢 |
| pool_4 | 187 | ~142–156 | **~85.3** | 🟢 |
| pool_8 | 243 | ~224–258 | **~152.8** (one soft 224.6) | 🟢 |
| pool_16 | 242 | ~245–249 | **~218.1** | 🟢 |
| pool_32 | 357 | ~404–424 | **~377.2** (289 / 377 / 381) | 🟢 vs optN8; ⚪/⚠️ vs quiet |

### HE
| Bench | optN7d | optN8 | **optN10 med** | Trend |
|------|-------:|------:|---------------:|:-----:|
| ScoreIPs | 905 · 1 | 667 · 0 | **~526.5 · 0** | 🟢 |
| SVCB | 1180 · 1 | 1250 · 0 | **~811 · 0** | 🟢 |
| V6 | 268 · 1 | 232 · 0 | **~153 · 0** | 🟢 |
| SortIPs | 134 · 1 | 191 · 1 | **~165.5 · 1 · 144 B** | 🟢 vs optN8; still ⚠️ vs optN7d peak |
| LargeList | 799 · 1 | 1299 · 1 | **~1007 · 1 · 1281 B** | 🟢 vs optN8; still ⚠️ vs optN7d peak |
| HappyIPScore_Score | — | — | **~0.200 ns · 0** | ⚪ |

### Product short (alloc signal only; thruput **not** headline)
| Bench | optN9 short | **optN10 short med** | Notes |
|------|------------:|---------------------:|-------|
| H2C packet-up alloc | ~110 | **~111** (111 / 111 / 110) | ⚪ path micro only |
| H2+TLS packet-up alloc | ~197 | **~197** (198 / 197 / 197) | ⚪ residual stack |
| H2C B/op | ~88k | **~89k** | ⚪ |
| H2 B/op | ~87k | **~86.8k** | ⚪ |
| Modes packet / stream-up / stream-one alloc | 110 / 18 / 18 | **110–111 / 18 / 18** | ⚪ |
| H2C short MB/s | soft | med **~205.6** (one 235.5) | soft window; do not overwrite |
| H2 short MB/s | soft | med **~66.9** (one 84.2) | soft window; do not overwrite |

**Product thruput headlines remain quiet2/optN3**: H2C ~**224–226** · H2+TLS ~**84** · stream-up ~**262** · stream-one ~**213**.

## Interpretation
1. **Not “200 commits = full regression.”** Pre-P0 30ms cliff is still gone; product H2C class remains ~225 MB/s. This wave’s wins are **micro residual**: `buf.Copy` now **beats** fixed upstream snapshot; HE Score/SVCB/V6 clearly under optN8; XMUX mid-pool scan cheaper without sticky pin.
2. **H2+TLS ~197 vs H2C ~111** is still mostly **http2/TLS stack**, not missing XHTTP shell pools. `appendToPath2` did not move packet-up alloc counts (already 1 path alloc).
3. **SortIPs/LargeList** improved vs optN8 soft window but still not optN7d best; do not claim absolute HE sort win without a quiet reconfirm series.
4. Short thruput windows remain noisy; one H2 sample hit ~84 MB/s (product class) while median soft — **alloc + official micro** are the trustworthy signals this wave.

## Residual next (ROI, stability-first)
1. **P1 H2+TLS ceiling**: header shallow clone / `Request.WithContext` / Client.Do / TLS record — expect only small XHTTP-owned cuts.
2. **P2 XMUX large-n structure** without dropping score/probe/over-admit (pool_32 still O(n)).
3. **P2 stream-one** residual vs stream-up product peak.
4. **P2 HE SortIPs quiet reconfirm** only if still behind optN7d after multi-window.
5. **P3 buf** secondary; Copy is no longer the embarrassing upstream gap.

## Do not
- Claim XMUX should match old 17ns simple pool
- Overwrite product headlines with 500–800ms soft windows
- Frame “200 commits = full regression”
- Sticky-bypass multi-pool score scan
- Cut probe / MarkDead / over-admit / H1 ordered / session IP / P0 pace for ns

## Artifacts
- `buf_copy.txt`, `xmux_get.txt`, `xmux_pool.txt`, `h2c.txt`, `h2.txt`, `modes.txt`, `he.txt`
- `status.txt`, `done.flag`, `run_optn10.ps1`, `splithttp_optN10.test.exe`
