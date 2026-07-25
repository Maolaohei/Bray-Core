# Bray-Core Benchmark Comparison Report

**Last human snapshot**: 2026-07-25 (**optN10 residual** + optN9 packet + optN8 micro) · **Environment**: i5-13600KF, Windows amd64, Go 1.26.x
**Tree**: optN10 residual · **Method**: 产品 thruput headline 仍 quiet2/optN3；XMUX/HE/buf 官方 micro 以 **optN10 串行** 为准；packet-up short 只报 alloc 信号
**Artifacts**: `bench_results/run_20260725_optN10/`（本轮 residual）· `run_20260725_optN9/` · `run_20260725_optN8/` · `run_20260724_optN3/` · `run_20260724_quiet2/`
**Baseline label**: Xray-core / Bray-Core **v26.6.22** 固定外部对照（`bench_results/upstream/xray-core-v26.6.22.json`，2026-06-24 同机）

> CI 每次 push/PR 会生成新的表格报告（`bench_results/report.md` + `summary.svg`）。  
> 本文件是**可复现的本地快照**；无法在同配置下复现的旧吞吐数字标为 **not comparable** 并以下方 07-24 数字为准。
>
> **Regression board ≠ Advantage board**：Summary 计数看回归；**Advantage Highlights** + suite `xhttp_core` 看 Bray XHTTP/XMUX 数据面。common/* Upstream 对照不是产品卖点主表。

**图例**

| Trend | 含义 |
|-------|------|
| 🟢 | improved（延迟更低 / 吞吐更高，≥3%） |
| ⚪ | stable（±3% 噪声带） |
| 🔴 | slower（≥3%） |
| 🆕 | 无 baseline / 首次观测 / 场景隔离 |
| ⚠️ | 数字有效但**不要**当产品卖点（噪声、模式绑定、或已知瓶颈） |

**Delta 约定**：`Delta` **为正 = 更好**（ns/op 更低或 MB/s 更高）。

---

## 0. 一句话结论（2026-07-25 · quiet2 + optN3 产品 + **optN10 micro residual**）

| 维度 | 结论 |
|------|------|
| vs 固定上游 common micro | ⚪ 多数接近；**`buf.Copy` 已 🟢**：optN10 med **~79.9 ns · 4 alloc** 优于上游 **98.13 ns**（旧 quiet ~126.5 的尴尬点已关掉）。其余 write micro 仍可能略慢，**不是** XHTTP 主战场。 |
| 200+ commit 是否“白做” | **否。** 最大可测跃迁仍是 **packet-up 默认 30ms pacing → P0 跳过 bulk/recentFlow**（H2C ~2.17 → ~220+ MB/s）。optN4→optN10 把 **XMUX Get/Pool、HE Score/SVCB/V6、buf.Copy** 相对 quiet/上游明确拉回。 |
| Bray-only packet-up H2C | 🟢 **P0 量级保持**：quiet2 med **~224.5**；**optN3** short med **~226 MB/s**，allocs **114** 类。相对 pre-P0 **~100×** 仍在。optN10 short alloc **~111**（不覆盖 thruput headline）。 |
| Bray-only Modes/stream-* | quiet2 曾软；**optN3** stream-up med **~262**、stream-one **~213**。optN10 modes alloc 仍 **18 / 18**。 |
| H2 packet-up+TLS | ⚠️/**🟢 residual 小幅推进后见顶**：产品 headline 仍 optN3 ~**84 MB/s**；optN9/optN10 short alloc **~197**。TLS 单连接仍是产品天花板，~90 gap 主要在 http2/TLS 栈。 |
| H1 pipeline | 🟢 depth **3** ordered + first-dial 串行化；稳定性回归 PASS。 |
| XMUX Get/Pool | 🟢 **optN10**：Get med **~59 ns**；pool_1 **~61.5** / pool_4 **~85** / pool_8 **~153** / pool_16 **~218** / pool_32 **~377**（均 0 alloc）。相对 quiet Get 129 / pool_1 160 **明确回升**，且中大池相对 optN8 再砍一截；**不能**对标旧 17ns 简单随机选池。 |
| HE v3 | 🟢 **optN10 官方 `he.txt`**：ScoreIPs **~527 · 0 alloc**、SVCB **~811 · 0**、V6 **~153 · 0**、SortIPs **~166 · 1**、LargeList **~1007 · 1**。相对 optN8 Score **667→527**、SVCB **1250→811**、V6 **232→153**；Sort/LargeList 优于 optN8 软窗，仍未声称压过 optN7d 峰值。 |
| 污染跑次 | `run_20260724_h1_pipeline` **作废**；产品 thruput quiet2/optN3；micro 以 **optN10 干净串行** 为准（短 thruput 窗不覆盖 headline）。 |

**给“感觉全回退了 / 200 commit 更慢”的直接回答**：pre-P0 的 30ms 悬崖**没有**回来；产品 H2C headline 仍 ~225 MB/s 类。HE 已**不是**比 Run2 慢——Score **0 alloc** 且 optN10 ns 再优于 optN8。XMUX Get/Pool 相对 quiet **明确回升**，optN10 中大池又比 optN8 更便宜。`buf.Copy` 已从“落后上游 ~29%”翻成 **快于上游**。旧 17ns XMUX 与当前 Bray 不是同一算法。**真还没打穿的是 H2+TLS 单连接 ~197 alloc / ~84 MB/s 产品天花板**（optN9/optN10 卡在 ~197），不是“全面回退”。

## 0.1 optN8 residual（2026-07-25 · 代码 + 串行 micro）

**代码面（稳定性优先，未砍 probe / MarkDead / over-admit / H1 ordered / session IP / P0 pace）**

| 区域 | 改动 |
|------|------|
| XMUX | `cachedScore` 只存 quality/RTT **base**；`selectionScore` = base + inflight×10000；Borrow/Release 热路径不再 `recomputeScore`；`behaviorScale` 在 UpdateQuality 刷新 |
| HE | `scoreIPsInto` 可复用 buffer；dial + Score microbench 0 steady-state alloc |
| packet-up | 默认 session+seq **PlacementPath** 走 `appendToPath2` 单次 path 拼接；H2 PostPacket 无 RTT 回调时不 `time.Now` |

**官方 micro（中位，干净串行）**

| Bench | quiet | optN6/optN7d | **optN8** | Trend |
|-------|------:|-------------:|---------:|:-----:|
| XMUX Get | 129 | ~62 | **~63**（clean2；噪声窗可到 ~100–140） | 🟢 vs quiet |
| XMUX pool_1 | 160 | 102 | **~69–82** | 🟢 |
| XMUX pool_4 | 187 | 137 | **~142–156** | 🟢/⚪ |
| XMUX pool_8 | 243 | 193 | **~224–258** | ⚪ 窗噪 / 仍优于 quiet |
| XMUX pool_16 | 242 | 190 | **~245–249** | ⚪ |
| XMUX pool_32 | 357 | 309 | **~404–424** | ⚠️ 本窗偏软；算法扫描 O(n) 仍在 |
| HE ScoreIPs | — | 905 · 1 | **667 · 0** | 🟢 |
| HE SVCB | — | 1180 · 1 | **1250 · 0** | 🟢 alloc / ⚪ ns |
| HE V6 | — | 268 · 1 | **232 · 0** | 🟢 |
| HE SortIPScores | — | 274 · 0 | **440 · 0** | ⚪ 窗噪（仍 0 alloc） |
| HE SortIPs | — | 134 · 1 | **191 · 1** | ⚠️ 非本轮主改 |
| HE LargeList | — | 799 · 1 | **1299 · 1** | ⚠️ 非本轮主改 / 窗噪 |

**产品短窗（500ms×3，只读 alloc；thruput 不覆盖 headline）**

| Bench | headline | optN8 short med | alloc |
|-------|---------:|----------------:|------:|
| H2C packet-up | ~224–226 MB/s | ~156 MB/s | **113** |
| H2+TLS | ~84 MB/s | ~62 MB/s | **201** |
| stream-up / stream-one | ~262 / ~213 | ~154 / ~159 | **18** |

**剩余能继续优化的点（按 ROI）**

1. **P1 H2+TLS / packet-up alloc gap（~197 vs H2C ~110，optN9）**：header 浅拷贝、path、`http.Request`/`client.Do`、TLS/http2 栈；XHTTP 内继续只剩小数。
2. **P2 XMUX 多池扫描**：大 pool 仍 O(n) + RLock；可考虑分层/堆，但别砍 score/probe/over-admit。
3. **P2 HE SortIPs/LargeList**：非 scoreIPs 主路径；与 Run2/optN7 比有窗噪，需要单独安静窗再定是否值得动。
4. **P2 stream-one residual**：相对 stream-up / P0 峰值仍有空间。
5. **P3 common/buf 其它 write micro**：Copy 已由 optN10 关掉上游缺口；其余次要。


## 0.2 optN9 residual（2026-07-25 · packet-up zero-copy + short alloc）

**代码面（稳定性优先，未砍 probe / MarkDead / over-admit / H1 ordered / session IP / P0 pace）**

| 区域 | 改动 |
|------|------|
| common/buf | `FromBytes` 复用 unmanaged `*Buffer` shell（`bufferShellPool`） |
| packet-up body | `FillPacketRequestBytes` + `PostPacketBytes` + `durableBodyPool`；`postPacketReliable` 优先 bytes 路径 |
| request shell | `urlURLPool` 复用 request-local `*url.URL`；H1 在 `req.Write` 后 Close body |
| 接口 | 不改 `DialerClient`；`packetBytesPoster` 可选 type assert |

**产品短窗（800ms×3 H2C/H2，只读 alloc；thruput 不覆盖 headline）**

| Bench | pre（同目录旧 binary） | **optN9 post med** | Trend |
|-------|----------------------:|-------------------:|:-----:|
| H2C packet-up alloc | 112 | **110**（109–110） | 🟢 |
| H2+TLS packet-up alloc | 201 | **197**（196–197） | 🟢 |
| H2C B/op | 91.2k | **~88.1k** | 🟢 |
| H2 B/op | 90.2k | **~87.0k** | 🟢 |
| Modes packet-up / stream-up / stream-one | — | **110 / 18 / 18** | 🟢/⚪ |

**pprof 结论**：`PostPacketBytes` 已在热路径；`FillPacketRequestBytes` 自身很小。剩余 mass 主要是 `net/http`·http2·TLS/uTLS·header 浅拷贝·`Request.WithContext`。padding `generateTokenish*` 在 profile 顶层多为 **package init 预热污染**，稳态走 cache。

**剩余能继续优化的点（按 ROI）**

1. **P1 H2+TLS / packet-up alloc gap（~197 vs H2C ~110）**：XHTTP 内再砍 header/path/request shell 只能挤小数；**不能**指望单靠 XHTTP 抹平 ~90 的 TLS/http2 差。
2. **P2 XMUX 多池扫描**：大 pool 仍 O(n)+RLock；可分层/堆，但别砍 score/probe/over-admit。
3. **P2 HE SortIPs/LargeList**：非 scoreIPs 主路径；需安静窗再确认。
4. **P2 stream-one residual**：相对 stream-up 峰值仍有空间。
5. **P3 common/buf 其它 write micro**：Copy 已由 optN10 关掉上游缺口；其余次要。

详见 `bench_results/run_20260725_optN9/SUMMARY.md`。

## 0.3 optN10 residual（2026-07-25 · path/XMUX/HE/buf micro）

**代码面（稳定性优先，未砍 probe / MarkDead / over-admit / H1 ordered / session IP / P0 pace）**

| 区域 | 改动 |
|------|------|
| path | `appendToPath2` 单次 `[]byte` 拼接（仍 1 path alloc） |
| XMUX | `idleTimeoutNs` 缓存；多池扫描 indexed local slice；**拒绝** sticky skip-scan |
| HE | `sortIPs` 用 `len(ip)` 先分族，减少 `To4`/`To16` 转换 |
| common/buf | `copyPlain` 无 option 热路径；`Copy` 跳过空 `onData` 组装 |

**官方 micro（中位，干净串行 · `run_20260725_optN10`）**

| Bench | quiet / optN8 / optN9 | **optN10** | Trend |
|-------|----------------------:|-----------:|:-----:|
| `buf.Copy` | upstream 98.13 · quiet ~126.5 | **~79.9 ns · 4 alloc** | 🟢 vs upstream |
| XMUX Get | quiet 129 · optN8 ~63 | **~59.4 ns · 0** | 🟢 |
| XMUX pool_1 | quiet 160 · optN8 ~69–82 | **~61.5** | 🟢 |
| XMUX pool_4 | quiet 187 · optN8 ~142–156 | **~85.3** | 🟢 |
| XMUX pool_8 | quiet 243 · optN8 ~224–258 | **~152.8** | 🟢 |
| XMUX pool_16 | quiet 242 · optN8 ~245–249 | **~218.1** | 🟢 |
| XMUX pool_32 | quiet 357 · optN8 ~404–424 | **~377.2** | 🟢 vs optN8 |
| HE ScoreIPs | optN8 667 · 0 | **~526.5 · 0** | 🟢 |
| HE SVCB | optN8 1250 · 0 | **~811 · 0** | 🟢 |
| HE V6 | optN8 232 · 0 | **~153 · 0** | 🟢 |
| HE SortIPs | optN8 191 · 1 | **~165.5 · 1** | 🟢 vs optN8；⚠️ vs optN7d 134 |
| HE LargeList | optN8 1299 · 1 | **~1007 · 1** | 🟢 vs optN8；⚠️ vs optN7d 799 |
| H2C / H2 short alloc | optN9 110 / 197 | **~111 / ~197** | ⚪ |

**产品 thruput headline 不变**：H2C ~224–226 · H2+TLS ~84 · stream-up ~262 · stream-one ~213。

**剩余能继续优化的点（按 ROI）**

1. **P1 H2+TLS / packet-up alloc gap（~197 vs H2C ~111）**：header 浅拷贝、`Request.WithContext`、`client.Do`、TLS/http2 栈；XHTTP 内继续只剩小数。
2. **P2 XMUX 多池扫描结构**：pool_32 仍 O(n)+RLock；可分层/堆，但别砍 score/probe/over-admit；勿 sticky 绑死。
3. **P2 stream-one residual**：相对 stream-up / P0 峰值仍有空间。
4. **P2 HE SortIPs/LargeList quiet reconfirm**：相对 optN7d 峰值仍有窗差，需多窗再定。
5. **P3 common/buf 其它 write micro**：Copy 已不再是上游缺口；其余性价比低。

详见 `bench_results/run_20260725_optN10/SUMMARY.md`。
---
## 1. Common Modules: Bray now vs Upstream Xray-core v26.6.22

来源：`run_20260724_quiet/new_{buf,crypto,dice,serial,mux}.txt`，median of 5。

| Benchmark | Upstream | Bray now (med) | Delta vs Upstream | Trend |
|-----------|---------:|---------------:|------------------:|-------|
| **common/buf** | | | | |
| `NewBuffer` | 47.15 ns/op | 48.18 | **-2.2%** | ⚪ |
| `NewBufferStack` | 30.06 ns/op | 31.83 | **-5.9%** | 🔴 |
| `Write2` | 1.520 ns/op | 1.654 | **-8.8%** | 🔴 |
| `Write8` | 1.814 ns/op | 2.041 | **-12.5%** | 🔴 |
| `Write32` | 1.834 ns/op | 1.986 | **-8.3%** | 🔴 |
| `WriteByte2` | 1.146 ns/op | 1.265 | **-10.4%** | 🔴 |
| `WriteByte8` | 4.180 ns/op | 4.348 | **-4.0%** | 🔴 |
| `Copy` | 98.13 ns/op | **79.9** (optN10 med) / 126.5 (quiet) | **+18.6%** vs upstream (optN10) | 🟢 optN10；quiet 历史 🔴 |
| `SplitBytes` | 159.4 ns/op | 175.0 | **-9.8%** | 🔴 |
| **common/crypto** | | | | |
| `ChaCha20` | 625 MB/s | 629.7 | **+0.7%** | ⚪ |
| `ChaCha20IETF` | 624 MB/s | 632.4 | **+1.3%** | ⚪ |
| `AES Encryption` | 1006 MB/s | 977 | **-2.9%** | ⚪ |
| `AES Decryption` | 1148 MB/s | 1099 | **-4.3%** | 🔴 borderline |
| **common/dice** | | | | |
| `Roll1` | 0.102 ns/op | 0.104 | **-2.0%** | ⚪ |
| `Roll20` | 6.28 ns/op | 6.572 | **-4.6%** | 🔴 |
| `Intn1` | 6.48 ns/op | 6.715 | **-3.6%** | 🔴 |
| `Intn20` | 6.29 ns/op | 6.586 | **-4.7%** | 🔴 |
| `Int63` | 5.21 ns/op | 5.351 | **-2.7%** | ⚪ |
| `Int31` | 5.03 ns/op | 5.196 | **-3.3%** | 🔴 |
| **common/serial** | | | | |
| `ReadUint16` | 11.12 ns/op | 11.85 | **-6.6%** | 🔴 |
| `WriteUint64` | 9.19 ns/op | 10.43 | **-13.5%** | 🔴 |
| `Concat` | 59.65 ns/op | 63.79 | **-6.9%** | 🔴 |
| **common/mux** | | | | |
| `FrameWrite` | 47.93 ns/op | 49.12 | **-2.5%** | ⚪ |

**Verdict**: ⚪/**🟢 on Copy**。  
- crypto 仍接近 stdlib 天花板（⚪）。  
- `buf.Copy`：07-24 quiet 曾 **126.5**（落后上游）；**optN10 `copyPlain`** med **~79.9 ns · 4 alloc**，**优于**上游 **98.13**。  
- 若干 write micro 仍可能略慢；这些 common 微基准**不是** XHTTP 吞吐主战场，但 Copy 已不再是“全面回退”证据。

---

## 2. XMUX Connection Pool（Bray-only self-baseline）

| Benchmark | 2026-06-24 Run2* | 2026-07-24 quiet (med) | optN5 clean med | **optN6 clean med** | vs quiet | Notes |
|-----------|-----------------:|-----------------------:|----------------:|--------------------:|---------:|-------|
| `XMUXGetXmuxClient` | 17.06* | 129.3 | ~60–62 | **~62** | 🟢 **~−52%** | 0 B/op；poolLen=1 快路径 + unreusableAtUnix/probeDone；get2 n=8 med **61.8** |
| `XMUXGetXmuxClientParallel` | 25,821 | **27,016** | ~28k 级 | — | ⚪ | 以 quiet 并行基线为主 |
| `XMUXRTTEWMA` | 8.41 | 15.35 | **18.2** | — | ⚪/🔴 | 逻辑更重；仍 ns 级 |
| `XMUXPoolScheduling/pool_1` | 23.80* | 160.3 | 105–112 | **102.2** | 🟢 **~−36%** | 0 B/op |
| `XMUXPoolScheduling/pool_4` | 32.27* | 186.8 | 180–186 | **137.4** | 🟢 **~−26%** | 超 optN4/optN5 |
| `XMUXPoolScheduling/pool_8` | 44.00* | 242.7 | 276.5 ⚠️ | **192.6** | 🟢 **~−21%** | optN5 噪声波作废；干净波赢 optN4 213.7 |
| `XMUXPoolScheduling/pool_16` | 78.03* | 242.1 | 264–278 ⚠️ | **189.9** | 🟢 **~−22%** | 超 optN4 204 |
| `XMUXPoolScheduling/pool_32` | 148.3* | 357.2 | 464.5 ⚠️ | **308.8** | 🟢 **~−14%** | 仍 > 旧 148 算法不可比 |
| `XMUXMetrics` | 10.97 | **11.34** | — | — | ⚪ | quiet 基线 |
| `XMUXConcurrentReadWrite/workers_1` | 519,873 | **536,519** | — | — | ⚪ | quiet 基线 |
| `.../workers_4` | 128,968 | **134,042** | — | — | ⚪ | |
| `.../workers_8` | 64,672 | **67,079** | — | — | ⚪ | |
| `.../workers_16` | 32,269 | **33,682** | — | — | ⚪ | |

* **Run2 不可当 Bray 目标**：当时是近“随机挑可用连接”的轻量路径；当前 Bray 含 probe 就绪、cached score、idle 淘汰、AIMD 连接/并发上限、over-admit 保护、MarkDead/cooldown。为 ns 去功能 = 假优化。

**Verdict**: 相对 **07-24 quiet**，optN6 **Get ~−52%**；**pool_1..32 全部 🟢**。optN5 中/大池“回退”是噪声波。相对 06-24 旧 17/23ns：**语义不同，禁止写“全面落后 upstream XMUX”**。


## 3. Happy Eyeballs v3

| Benchmark | 2026-06-24 Run2 | 2026-07-24 quiet (med) | optN6 clean med | **optN7 clean med** | vs quiet | vs Run2 |
|-----------|----------------:|-----------------------:|----------------:|--------------------:|---------:|--------:|
| `ScoreIPs` | 1,124 | 1,375 | 1,003 · 4 allocs | **905** · **1 alloc** | 🟢 | 🟢 **优于 Run2** + **−3 allocs** |
| `ScoreIPs_WithSVCB` | 1,232 | 1,594 | 1,157 · 4 allocs | **1,180** · **1 alloc** | 🟢 | 🟢 ns≈optN6；**4→1 alloc** |
| `ScoreIPs_V6Prioritized` | 585 | 744 | 363 · 4 | **268** · **1 alloc** | 🟢 | 🟢 **远优于 Run2** |
| `SortIPScores` | 422 | 461 | 271 · 3 | **274** · **0 alloc / 0 B** | 🟢 | 🟢 alloc residual 关闭 |
| `SortIPs` | 277 | 613 | 154 · 1 | **134** · **1 alloc** | 🟢 | 🟢 |
| `SortIPs_LargeList` | 1,965 | 4,517 | 911 · 1 | **799** · **1 alloc** | 🟢 | 🟢 **~2.5× 优于 Run2** |
| `HappyIPDB_Get` | ~12–14 | ~12–14 | — | — | ⚪ | ⚪ |
| `ClampRTT` | 0.10 | **0.106** | — | — | ⚪ | ⚪ |
| `HappyIPScore_Score` | 0.21 | **0.147** | ~0.26 | **~0.25** | ⚪ | ⚪ tiny |
| `ScoreWithHighFailRate` | 0.10 | **0.127** | — | **~0.20** | ⚪/🔴 tiny | ⚪ tiny |

**工程点（optN6→optN7）**：`sortIPScores` 从 `sort.Slice` 改为 `slices.SortFunc`（去掉 reflection/interface 分配；SortIPScores **3→0 alloc**，Score* **4→1 alloc** 仅剩结果切片）；`newPacketRequest` 手构 `http.Request` shell（去掉 dummy URL parse）。

**Verdict**: “HE 比 2026-06-24 Run2 还慢”**已不成立**。Score/SVCB/V6/Sort/LargeList **全部优于 Run2**；alloc residual 是本轮主收益。


## 4. REALITY Handshake Microbenches

| Benchmark | 2026-06-24 | 2026-07-24 quiet (med) | B/op | allocs | Trend |
|-----------|-----------:|-----------------------:|-----:|-------:|-------|
| `RealityHandshakeKeyExchange` | 24,820 | **25,805** | 32 | 1 | ⚪ |
| `RealityAEADSeal` | 64.08 | **67.3** | 32 | 1 | ⚪ |
| `RealityAEADOpen` | 53.11 | **55.41** | 16 | 1 | ⚪ |
| `RealityHKDF` | 26,651 | **34,380** | 17,064 | 217 | 🔴 / noisy |
| `RealityECDSA` | 22,308 | **23,938** | 6,064 | 59 | ⚪ |
| `RealityMLDSA65Verify` | 24,065 | **25,061** | 450 | 3 | ⚪ |
| `RealityMLDSA65Sign` | — | **292,297** | 450 | 3 | 🆕 |

**Verdict**: AEAD / KeyExchange / ECDSA / MLDSA verify 与 06-24 **同量级**。HKDF 波动大，不单独定罪。REALITY 服务端 amortize 等优化的主收益在**握手正确性与并发**，不在这条 micro 表上“翻倍”。

---

## 5. XHTTP Throughput — 场景隔离（2026-07-24 · P0 后）

不同 mode / 连接数 / TLS 会得到完全不同的 MB/s，**禁止**横向硬比。  
`b.SetBytes(payload*2)` → MB/s 含双向载荷；`ConcurrentConnections` 再 × `numConns`。

### 5.1 修复前对照（`run_20260724_quiet`，median count=5）

| Scenario | Metric | quiet med | Notes |
|----------|--------|----------:|-------|
| `H2C_Throughput` packet-up | MB/s | **2.17** | ns/op≈**30.2e6**（默认 30ms 间隔钉死） |
| `Modes/packet-up` | MB/s | **2.18** | 同上 |
| `Modes/stream-up` / `stream-one` | MB/s | **240.7 / 263.8** | stream 路径本已健康 |
| `conns_1` / `conns_16` | MB/s | **1.08 / 282.6** | 多连接可绕开单会话 30ms |

### 5.2 修复后（`run_20260724_p0_pace`，median of count=3，`-benchtime=500ms`）

| Scenario | Metric | P0 med | vs quiet | Notes |
|----------|--------|-------:|---------:|-------|
| `H2C_Throughput` (packet-up) | MB/s | **222.6** | **~100×** | ~294 µs/op；bulk≥8KiB 跳过 pace |
| `H2_Throughput` (packet-up+TLS) | MB/s | **86.5** | **~40×** | 短 benchtime 更噪；仍远高于 2.17 |
| `Modes/packet-up` | MB/s | **186.0** | **~85×** | 与 H2C 同向 |
| `Modes/stream-up` | MB/s | **241.3** | ⚪ | 与 quiet 同量级 |
| `Modes/stream-one` | MB/s | **261.8** | ⚪ | 与 quiet 同量级 |
| `ConcurrentConnections/conns_1` | MB/s | **170.5** | **~158×** | 16KiB payload；单连接已可用 |
| `.../conns_4` | MB/s | **2034** | 🟢 | 聚合吞吐（×4） |
| `.../conns_8` | MB/s | **6310** | 🟢 | 聚合吞吐（×8） |
| `.../conns_16` | MB/s | **17994** | 🟢 | 聚合吞吐（×16）；本机 loopback 量级 |

**代码点**：`packetUploadLaunchIntervalMs` + dialer upload loop；`packetUploadBulkPaceBytes=8KiB`；`recentFlow`（距上次 launch <50ms）。默认 `scMinPostsIntervalMs=30` **未删**，仅 idle/tiny 仍 pace。

### 5.3 `TestBenchmark_UpstreamCompare`（128 KB × 10，日志 Mbps）

| Label | Mode | quiet Mbps | **P0 Mbps** | Wall (P0) |
|-------|------|----------:|------------:|----------:|
| H2C | packet-up | 37.9 | **400.2** | 0.03s |
| H2 | packet-up | 38.3 | **151.5** | 0.07s |
| H2 | stream-up | 243.8 | **271.6** | 0.04s |

**解读**

1. **P0 根因**：`scMinPostsIntervalMs` 默认 30ms；32KiB 写相对 1MB `scMaxEachPostBytes` 既非 fullChunk，req/resp 间 pipe 又常空 → 每 POST 硬睡 30ms。  
2. **P0 后 packet-up 单连接**进入与 stream 同数量级（H2C micro ~220 MB/s；UpstreamCompare ~400 Mbps）。  
3. **stream-* 保持强项，未被 pacing 改动拖累。  
4. **P1（多连接）**：1-conn 已不再是 ~1 MB/s；1–16 连接聚合随 `numConns` 扩展。剩余优化空间在 H2 TLS 单连接、XMUX probe 噪声、H1 pipeline，而非 30ms 间隔。  
5. 旧文档 “H2 268 MB/s” 仍 **not comparable** 到错误场景标签；P0 后真实 `H2_Throughput` 是 packet-up+TLS ~87 MB/s（短跑 median），stream 另表。

---

### 5.4 post-H1 quiet2（2026-07-24 晚 · 干净日志）

**方法**：`benchNopLogHandler` 静音默认 log；`run_20260724_quiet2` count=3 `-benchtime=500ms`；stream/H2/conns 加跑 `quiet2_long` count=5 `-benchtime=1s`。  
**勿用**：`run_20260724_h1_pipeline`（~514KB probe/teardown 日志污染）。

| Scenario | Metric | P0 med | quiet2 med (500ms) | long med (1s×5) | vs pre-P0 | vs P0 |
|----------|--------|-------:|-------------------:|----------------:|----------:|------:|
| `H2C_Throughput` packet-up | MB/s | 222.6 | **224.5** | iso 噪声大 | ~100× | ⚪ |
| `H2_Throughput` packet+TLS | MB/s | 86.5 | 68.7 | **71.9** | ~33× | 🔴 ~−17% |
| `Modes/packet-up` | MB/s | 186.0 | **144.7** | **138.5** | ~65× | 🔴 ~−25% |
| `Modes/stream-up` | MB/s | 241.3 | 172.3 | **180.8** | soft vs peak | 🔴 ~−25% |
| `Modes/stream-one` | MB/s | 261.8 | 199.2 | **173.7** | soft vs peak | 🔴 ~−34% |
| `StreamUp_Throughput` TLS | MB/s | — | 100.5 | **109.4** | 🆕 residual | ⚠️ |
| `conns_1` | MB/s | 170.5 | 109.6 | **108.4** | ~100× | 🔴 ~−36% |
| `conns_4` / `8` / `16` | MB/s agg | 2034 / 6310 / 17994 | 1620 / 4260 / 10797 | **1311 / 4086 / 8278** | still scales | 🔴 vs P0 peak |

**UpstreamCompare（quiet2 后 3 次，Mbps）**

| Label | P0 | now (3 runs) |
|-------|---:|--------------|
| H2C packet-up | 400.2 | **768 / 797 / 505** |
| H2 packet-up | 151.5 | **197 / 151 / 184** |
| H2 stream-up | 271.6 | **120 / 142 / 72** |

**XMUX（quiet2 vs 07-24 morning quiet）**：Get **126 ns**（was 129）、pool_1 **159**（was 160）、ConcurrentRW workers_1 **537 µs** → ⚪。

**本轮工程点**

| 项 | 作用 | 吞吐预期 |
|----|------|----------|
| H1 `pipelinePost` depth=2 + 释放 `mu` 再 `ReadResponse` | 修并发 depth-2 **自死锁** | 正确性；不单独抬 H2C micro |
| `hotH1` 复用 | 串行 POST 少 dial | 小；bench 多为 H2 |
| probe skip `IsClosed`/stopCh | 减 teardown MarkDead 噪声 | 主要改善可复现性 |
| H2 `MaxReadFrameSize=256KiB` + `DisableCompression` | 帧/压缩路径 | 小；未单独 A/B |
| nop log handler（bench package） | 结果行可解析 | 文档可信度 |

**解读**

1. **P0 悬崖仍在修完状态**：H2C ~224 MB/s，不是 ~2 MB/s。  
2. **stream / 多连接相对 P0 峰值偏软是诚实事实**；可能因素含 OpenStream 等待、splitConn deadline 路径、TLS 分配（H2 ~209 allocs vs H2C ~122）、短 bench 与机器热噪声。  
3. H1 工作是 **可靠性**；用污染跑次写成“全面回退”是错的。  
4. 下一步可优化点见 §7.1。

## 6. 与 2026-06-24 文档差异（必须透明）

| 旧结论 | 07-24 实测 | 处理 |
|--------|------------|------|
| common “No regression / buf 略快” | Copy -29%，writes 偏慢 | 改写为 ⚪/🔴；非主收益区 |
| HE LargeList +46% | quiet 曾 4.5µs；**optN7 ~0.80µs · 1 alloc** 远优于 Run2 | 撤销旧夸张宣传；记 optN7 全面回升 + Score 1 alloc |
| XHTTP H2 268 MB/s | quiet 误把 packet 写成 2.17；**P0 后** packet H2C ~223 MB/s、stream ~241–262 | 场景拆开；旧 268 仍 not comparable |
| XMUX pool 20–150 ns / Get 17ns | quiet 160–360 / 129；**optN6 Get ~62 / pool_1~102 / pool_16~190 / pool_32~309** | 相对 quiet 🟢；相对旧 17ns **算法不可比** |

---

## 7. Summary

| Category | Status | Notes |
|----------|--------|-------|
| vs Upstream common | ⚪/**🟢 Copy** | crypto ⚪；**buf.Copy optN10 ~80 ns 优于上游 98**；其它 write micro 次要 |
| XMUX Get | 🟢 **optN10 vs quiet** | Get **~59 ns**（was 129；optN8 ~63）；≠ 旧 17ns 算法 |
| XMUX Pool | 🟢 **optN10 pool_1..32** | ~62 / 85 / 153 / 218 / 377；相对 quiet/optN8 中大池再降；≠ 旧 17/23ns |
| XMUX ConcurrentRW | ⚪ stable | quiet 基线 |
| HE v3 | 🟢 **optN10 再压 Score/SVCB/V6** | Score **~527 · 0 alloc**（optN8 667）；SVCB ~811；V6 ~153；Sort/LargeList 优于 optN8 软窗 |
| REALITY micro | ⚪ stable | 同量级（本轮未重跑） |
| XHTTP packet-up H2C | 🟢 **~225–226 MB/s** 产品 | quiet2 / **optN3** |
| XHTTP packet-up+TLS | ⚠️/**🟢 ~84 MB/s** 产品 optN3 | 单连接 TLS 天花板仍是 P1 |
| XHTTP stream-* micro | 🟢 residual 回暖 | optN3 Modes stream-up ~262 / stream-one ~213 |
| XHTTP multi-conn | 🟢 vs pre-P0；🔴 vs P0 peak | conns_N 仍以 quiet2 记 |
| UpstreamCompare now | H2C **500–800** / H2 packet **150–200** / stream **70–140** Mbps | 见 quiet2 |
| H1 pipeline | 🟢 **correctness** | depth=3 有序 + first-dial 串行 |

### Bray-only 数据面（时间线）

1. **pre-P0 quiet**：packet-up ~2 MB/s（30ms/POST）；stream 已强。  
2. **P0 pace**：packet-up H2C ~223 MB/s；stream 峰值 ~241–262。  
3. **post-H1 quiet2**：H2C 仍 ~225；stream/TLS/多连接相对 P0 峰值回落——**记软项，不抹掉 P0**。  
4. **optN3 residual**：H2C ~226 / H2+TLS ~84 / Modes stream 回暖；URL base + header pool + H1 dial serialize。  
5. **optN4 micro residual**：XMUX Get **129→77 ns**；Pool 明显回升；HE Sort/V6/LargeList 扳回 quiet 回退。  
6. **optN5 micro residual**：XMUX Get **~60–62 ns**；HE SVCB **24→4 allocs**；Score/LargeList 贴/超 Run2。  
7. **optN6 micro residual**：HE **全面优于 Run2**；XMUX pool_4..32 干净波全面优于 quiet/optN4；Get ~62。  
8. **optN7 micro residual**：HE Score **4→1 alloc** / SortIPScores **3→0**；ScoreIPs **905**、V6 **268**、LargeList **799**；packet request shell 手构。  
9. common/buf **不要**当 200-commit 主卖点；XMUX 也不要用上游/旧 17ns 当 KPI。
10. **optN8–optN10 residual**：XMUX score fold → packet body zero-copy → path/HE/buf micro；optN10 关 `buf.Copy` 上游缺口，HE Score/XMUX 中池再降；**H2+TLS ~197 alloc 仍见顶**。

### 7.1 还能优化的点（按收益/风险）

| 优先级 | 点 | 为什么 | 风险 |
|--------|----|--------|------|
| P1 | **H2 packet-up+TLS 剩余 ~90 allocs vs H2C** | 单连接产品天花板；optN3 ~84 MB/s；optN9/optN10 short alloc **~197** | Header/指纹语义；`WithContext`/Do/TLS 栈 |
| P1 | **Modes/stream-one vs P0 ~262** | OpenStream header wait + 双向 body | deadline / 半关 |
| P2 | **XMUX pool_32 residual** | optN10 ~377（仍 O(n)）；相对 quiet/optN8 已降；≠ 旧 148 算法 | 勿砍 score/probe/over-admit；勿 sticky |
| P2 | **scoreIPs 结果切片 1 alloc** | optN7 已 4→1；再降需 caller-owned buffer API | 保持 score 语义 |
| P2 | **packet-up 多连接聚合** 回落调查 | conns_N 相对 P0 峰值 −20..−50% | bench 定义/热噪声 |
| P3 | common/buf 其它 write micro | **Copy 已 🟢 优于上游**；其余 write 次要 | 性价比低 |
| Keep | probe / MarkDead / over-admit / H1 ordered / session IP | 稳定性资产 | **禁止为 ns 拆除** |

**稳定性优先**：有序 H1 pipeline、first-dial 串行、session IP 绑定、MarkDead 关传输、pace 仅 idle tiny——这些正确性资产保留。


## 8. 自动化与历史

| 路径 / 动作 | 说明 |
|-------------|------|
| `.github/workflows/benchmark.yml` | push/PR 跑 XMUX / HE / Warmup / VLESS / buf |
| `scripts/format_bench_report.py` | 输出统一 Markdown 表 + 🟢⚪🔴🆕 + `summary.svg` |
| `bench_results/report.md` | 当次完整报告（CI artifact） |
| `bench_results/summary.json` | 机器可读 summary |
| `bench_results/history/*.json` | 快照（含本机 `20260724T0615-local.json`） |
| `bench_results/history/latest.md` | 最近一次短摘要 |
| `bench_results/upstream/xray-core-v26.6.22.json` | 固定上游对照 |
| `bench_results/run_20260724_quiet/` | pre-P0 对照 |
| `bench_results/run_20260724_p0_pace/` | P0 峰值 |
| `bench_results/run_20260724_quiet2/` | post-H1 干净 re-bench（产品基线） |
| `bench_results/run_20260724_quiet2_long/` | H2/Modes/Conns 1s×5 |
| `bench_results/run_20260724_optN2/` | residual 中途（H1 depth3 / probe） |
| `bench_results/run_20260725_optN10/` | **optN10 residual**（buf.Copy ~80；XMUX Get ~59 / pool 中大池再降；HE Score ~527 · 0） |
| `bench_results/run_20260725_optN9/` | **optN9 packet residual**（H2C/H2 alloc 110/197；PostPacketBytes） |
| `bench_results/run_20260725_optN8/` | **optN8 micro residual**（XMUX score fold；HE Score 0 alloc） |
| `bench_results/run_20260725_optN6/` | **optN6+optN7 micro residual**（XMUX Get ~62；HE Score 1 alloc / SortIPScores 0） |
| `bench_results/run_20260724_optN5/` | **本轮 micro residual**（XMUX Get ~60ns + HE SVCB 4 allocs） |
| `bench_results/run_20260724_optN4/` | 上轮 micro residual（XMUX Get/Pool + HE v3） |
| `bench_results/run_20260724_optN3/` | 产品吞吐 residual（URL/header pool + dial serialize） |
| `bench_results/run_20260724_h1_pipeline/` | 污染跑次，勿用 |
| `./benchmark.sh` / `benchmark.bat` | 本地全套 |

```bash
# 本地快速格式化（在已有 new_*.txt / base_*.txt 时）
python scripts/format_bench_report.py --history \
  --sha "$(git rev-parse --short HEAD)" \
  --runner local --go "$(go env GOVERSION)"
```

```powershell
# 安静重跑（本机已验证 env）
$env:GOCACHE="D:\UGit\Bray-Core\.gocache"
$env:GOMODCACHE="$env:USERPROFILE\go\pkg\mod"
$env:GOTMPDIR="D:\UGit\Bray-Core\.gotmp"
$env:GOPROXY="https://goproxy.cn,direct"
# 见 bench_results/run_20260724_quiet/run_quiet.ps1
```

