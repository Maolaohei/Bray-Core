# Bray-Core Benchmark Comparison Report

**Last human snapshot**: 2026-06-24 · **Environment**: i5-13600KF, Windows amd64, Go 1.26  
**Baseline label**: Xray-core / Bray-Core **v26.6.22** (pre Bray-only packet-up wave)

> CI 每次 push/PR 会生成新的表格报告（`bench_results/report.md` + `summary.svg`）。  
> 本文件保留**可对比的历史快照**；无法在同配置下复现的旧吞吐数字已标注为 **not comparable**。

**图例**

| Trend | 含义 |
|-------|------|
| 🟢 | improved（延迟更低 / 吞吐更高，≥3%） |
| ⚪ | stable（±3% 噪声带） |
| 🔴 | slower（≥3%） |
| 🆕 | 无 baseline / 首次观测 |

**Delta 约定**：`Delta` **为正 = 更好**。

---

## 1. Common Modules: Bray vs Upstream Xray-core

| Benchmark | Upstream | Run1 | Run2 | Delta vs Upstream | Trend |
|-----------|---------:|-----:|-----:|------------------:|-------|
| **common/buf** | | | | | |
| `NewBuffer` | 47.15 ns/op | 40.27 | 43.13 | **+8.5%** | 🟢 |
| `NewBufferStack` | 30.06 ns/op | 25.78 | 28.16 | **+6.3%** | 🟢 |
| `Write2` | 1.520 ns/op | 1.507 | 1.525 | ~0 | ⚪ |
| `Write8` | 1.814 ns/op | 1.805 | 1.817 | ~0 | ⚪ |
| `Write32` | 1.834 ns/op | 1.826 | 1.813 | ~0 | ⚪ |
| `WriteByte2` | 1.146 ns/op | 1.141 | 1.140 | ~0 | ⚪ |
| `WriteByte8` | 4.180 ns/op | 4.185 | 4.114 | ~0 | ⚪ |
| `Copy` | 98.13 ns/op | 93.59 | 90.71 | **+7.6%** | 🟢 |
| `SplitBytes` | 159.4 ns/op | 169.7 | 156.0 | +2.1% | ⚪ |
| **common/crypto** | | | | | |
| `ChaCha20` | 625 MB/s | 624 | 598 | ~0 | ⚪ |
| `ChaCha20IETF` | 624 MB/s | 619 | 605 | ~0 | ⚪ |
| `AES Encryption` | 1006 MB/s | 1008 | 1004 | ~0 | ⚪ |
| `AES Decryption` | 1148 MB/s | 1151 | 1141 | ~0 | ⚪ |
| **common/dice** | | | | | |
| `Roll1` | 0.102 ns/op | 0.102 | 0.109 | ~0 | ⚪ |
| `Roll20` | 6.28 ns/op | 6.29 | 6.46 | ~0 | ⚪ |
| `Intn1` / `Intn20` / `Int63` / `Int31` | — | — | — | ~0 | ⚪ |
| **common/serial** | | | | | |
| `ReadUint16` | 11.12 ns/op | 11.24 | 11.35 | ~0 | ⚪ |
| `WriteUint64` | 9.19 ns/op | 9.14 | 9.44 | ~0 | ⚪ |
| `Concat` | 59.65 ns/op | 60.16 | 61.03 | ~0 | ⚪ |
| **common/mux** | | | | | |
| `FrameWrite` | 47.93 ns/op | 47.93 | 47.34 | ~0 | ⚪ |

**Verdict**: ⚪/🟢 **No regression vs upstream**。`buf` 路径仍略快。

---

## 2. XMUX Connection Pool（同名指标对比）

| Benchmark | Run1 | Run2 | Delta | Trend |
|-----------|-----:|-----:|------:|-------|
| `GetXmuxClient` | — | 17.06 ns/op | — | 🆕 baseline |
| `GetXmuxClientParallel` | — | 25,821 ns/op | — | 🆕 |
| `RTTEWMA` | — | 8.41 ns/op | — | 🆕 |
| `PoolScheduling/pool_1` | — | 23.80 ns/op | — | 🆕 |
| `PoolScheduling/pool_4` | — | 32.27 ns/op | — | 🆕 |
| `PoolScheduling/pool_8` | — | 44.00 ns/op | — | 🆕 |
| `PoolScheduling/pool_16` | — | 78.03 ns/op | — | 🆕 |
| `PoolScheduling/pool_32` | — | 148.3 ns/op | — | 🆕 |
| `WarmupEnqueue` | 10.9 ns/op | 10.83 | **+0.6%** | ⚪ |
| `Metrics` | 10.9 ns/op | 10.97 | -0.6% | ⚪ |
| `ConcurrentRW/1` | 513,000 | 519,873 | -1.3% | ⚪ |
| `ConcurrentRW/4` | 128,000 | 128,968 | -0.8% | ⚪ |
| `ConcurrentRW/8` | 64,500 | 64,672 | -0.3% | ⚪ |
| `ConcurrentRW/16` | 32,300 | 32,269 | **+0.1%** | ⚪ |

**Verdict**: ⚪ **Zero material regression** on XMUX hot paths.

---

## 3. Happy Eyeballs v3

| Benchmark | Run1 | Run2 | Delta | Trend |
|-----------|-----:|-----:|------:|-------|
| `ScoreIPs` | 1,104 ns/op | 1,124 | -1.8% | ⚪ |
| `ScoreIPs_WithSVCB` | 1,213 | 1,232 | -1.6% | ⚪ |
| `ScoreIPs_V6Prioritized` | 567 | 585 | -3.2% | 🔴 borderline |
| `SortIPScores` | 441 | 422 | **+4.3%** | 🟢 |
| `SortIPs` | 352 | 277 | **+21.3%** | 🟢 |
| `SortIPs_LargeList` | 3,642 | 1,965 | **+46.1%** | 🟢 |
| `ClampRTT` | 0.10 | 0.10 | ~0 | ⚪ |
| `Score` | 0.10 | 0.21 | expected | ⚪* |
| `ScoreWithHighFailRate` | 0.10 | 0.10 | ~0 | ⚪ |

\* `Score` 变慢是 V2.0 增加 retrans/loss 因子的预期成本（绝对量仍是亚纳秒级）。

**Verdict**: sort 路径明显 🟢；score 路径无实质回归。

---

## 4. REALITY Handshake Microbenches

| Benchmark | ns/op | B/op | allocs/op | Trend |
|-----------|------:|-----:|----------:|-------|
| `RealityHandshakeKeyExchange` | 24,820 | 32 | 1 | ⚪ snapshot |
| `RealityAEADSeal` | 64.08 | 32 | 1 | ⚪ |
| `RealityAEADOpen` | 53.11 | 16 | 1 | ⚪ |
| `RealityHKDF` | 26,651 | 17,064 | 217 | ⚪ |
| `RealityECDSA` | 22,308 | 6,064 | 59 | ⚪ |
| `RealityMLDSA65Verify` | 24,065 | 450 | 3 | ⚪ |

**Verdict**: crypto 热路径稳定快照（非与上游逐行 CI 对比）。

---

## 5. XHTTP Throughput — 场景隔离（勿横向比）

不同 mode / 连接数 / TLS 与否会得到完全不同的 MB/s，**不能**把 H2、H2C、packet-up 放在同一「Run1 vs Run2」列里当 delta。

| Scenario (固定配置) | Metric | Value | Comparable? | Notes |
|---------------------|--------|------:|:------------:|-------|
| `BenchmarkXHTTP_H2C_Throughput` | MB/s | 35.36 | yes (self) | cleartext H2C microbench |
| `BenchmarkXHTTP_H2_Throughput` | MB/s | 268 | yes (self) | TLS H2; not H2C |
| Packet-up 16 conns | MB/s | 265 | yes (self) | multi-conn bulk |
| Packet-up 1 conn | MB/s | 200 | yes (self) | single-conn bulk |

后续 CI / 本地请以**同名 Benchmark + 同 `-benchmem -count`** 对比；新 packet-up window/chunk 优化以 CI `buf`/`xmux` 微基准 + 专项吞吐名为准。

---

## 6. Summary

| Category | Status | Notes |
|----------|--------|-------|
| vs Upstream (common) | 🟢/⚪ **NO REGRESSION** | buf/Copy 仍略优 |
| vs Upstream (crypto) | ⚪ **IDENTICAL** | stdlib |
| XMUX hot paths | ⚪ **NO REGRESSION** | WarmupEnqueue ~10.8ns |
| HE v3 sort | 🟢 **IMPROVED** | LargeList +46% |
| HE v3 score | ⚪ expected | +retrans/loss |
| XHTTP throughput | scenario-bound | 见表 5，勿混比 |
| Goroutine lifecycle | fixed | session/padding/dialer |
| Bench report UX | tables + emoji + SVG + history | |

### Bray-only 数据面（相对本快照之后的代码）

- packet-up window / RTT chunk / zero-alloc seq / shared headers / padding shrink / deadline `bytespool`
- 这些属于 **2026-07 Bray-only** 变更；请用 **CI Benchmark Tracking** 产物与 `bench_results/history/` 看演进，而不是改写上表 2026-06-24 数字。

---

## 7. 自动化与历史

| 路径 / 动作 | 说明 |
|-------------|------|
| `.github/workflows/benchmark.yml` | push/PR 跑 XMUX / HE / Warmup / VLESS / buf |
| `scripts/format_bench_report.py` | 输出统一 Markdown 表 + 🟢⚪🔴🆕 + `summary.svg` |
| `bench_results/report.md` | 当次完整报告（CI artifact） |
| `bench_results/summary.json` | 机器可读 summary |
| `bench_results/history/*.json` | 每次 CI 快照（release / 演进曲线用） |
| `bench_results/history/latest.md` | 最近一次短摘要 |
| `bench_results/upstream/xray-core-v26.6.22.json` | 固定上游 Xray-core 对照指标（CI Upstream 列） |
| `./benchmark.sh` | 本地全套（含 XHTTP / REALITY） |

```bash
# 本地快速格式化（在已有 new_*.txt / base_*.txt 时）
python scripts/format_bench_report.py --history \
  --sha "$(git rev-parse --short HEAD)" \
  --runner local --go "$(go env GOVERSION)"
```
