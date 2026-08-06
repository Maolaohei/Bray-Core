# benchcompare — 通用式性能对比套件（完整固化版）

Bray-Core 与上游 Xray-core 的**通用性能对比套件**：同一份 benchmark 代码注入到
每个目标仓库的独立 worktree，跑完后按 benchmark 名自动对齐，输出覆盖矩阵 +
数值对比报告（Markdown + JSON）。**默认多轮执行**，避免单次测量误差影响结论。

## 完整套件（无参数默认命令）

```bash
go run ./tools/benchcompare
```

等价于：`--suite all --targets bray,upstream --count 3 --repeats 3`，覆盖
**6 个 suite、50+ 场景、双侧各 9 样本/场景**：

| Suite | 包 | 场景数 | 场景 | 目标 | 备注 |
|---|---|---|---|---|---|
| `xhttp` | `transport/internet/splithttp` | 17 | H2C/H2/StreamUp 吞吐、Parallel、Modes×3、TTFB、Burst_64KB/1MB、ConnectionStorm、Concurrent×4、MemoryAllocations、**ColdStart**（含 Dial）、**TCP_Echo**（网络基线） | both | 全场景放开 |
| `vless` | `proxy/vless/encoding` | 3 | Encode/Decode Request/Response Header | both | 公共 API 子集 |
| `xmux` | `transport/internet/splithttp` | 13 | 池调度/并发/EWMA 等 | bray-only | 上游 API 分叉 |
| `buf` | `common/buf` | 9 | NewBuffer/Write/Copy/SplitBytes | both | repo-native（双侧自带） |
| `reality` | `transport/internet` | 7 | X25519/AEAD/HKDF/ECDSA/MLDSA-65 | both | **默认 benchtime 5s**（MLDSA 长任务） |
| `dns` | `app/dns` | 7 | Parse/BuildReqMsgs/EDNS0/Fqdn/RecordAlloc | both | 基础逻辑基准（Bray 池化路径另见下） |

## 固化要点（本轮测试的方法论沉淀）

1. **默认多轮**：`--repeats 3`（默认）—— 每个 suite×target 独立 go test 进程跑
   3 次，样本合并后取中位数（9 样本/场景）。冷启动/短连接类场景单次波动可达
   ±30%（实测 ColdStart 2%↔31%），多轮是结论可信的前提。
2. **长任务默认 benchtime**：`reality` suite 默认 `5s`（MLDSA65Sign ~250µs/op，
   默认 1s 迭代会导致假信号：曾测得 +10.9%→复测实为 tie）。`--benchtime` 可显式覆盖。
3. **网络基线对照**：`BenchmarkTCP_Echo`（标准库 TCP 回环）隔离环境偏置 ——
   若该场景两侧差 >5%，其余差距需先扣环境因素再下结论。
4. **池化差异标注**：`dns` suite 注入版不含 Bray 特有池化回收（公平基础基准）；
   Bray 真实路径（含池化）比该基准快 4-5 倍、比上游快 54-65%。对比结论以仓库
   原版 bench（`go test -bench='Benchmark(BuildReqMsgs|GenEDNS0)' ./app/dns/`）为准。

## 快速开始

```bash
go run ./tools/benchcompare                        # 完整套件（默认多轮）
go run ./tools/benchcompare --quick                # 冒烟（单轮轻量子集）
go run ./tools/benchcompare --suite xhttp          # 单 suite
go run ./tools/benchcompare --bench-re 'BenchmarkXHTTP_(TTFB|ColdStart)' --repeats 5   # 精确复测
go run ./tools/benchcompare --upstream-path D:/UGit/Xray-upstream   # 复用已有上游 checkout
go run ./tools/benchcompare --cpuprofile --benchtime 5s --count 1   # 热点 profile（自动单轮）
```

## 报告与产物（`bench_results/compare/`）

- `report.md` — 覆盖矩阵 + 对比表（中位数 ns/op、**MB/s 速率**、B/op、allocs/op）+ 判定
- `report.json` — 结构化数据（CI 归档）
- `<suite>_<target>.raw` — 过滤后的 go test 输出（benchstat 可消费）
- `<suite>_<target>.prof` — `--cpuprofile` 时的 pprof 文件

## 扩展场景

三步：1) `benches/<common|bray>/` 加 bench 文件；2) `scenarios.go` 的
`defaultSuites()` 加一条（含 Targets/Inject/Remove/可选 BenchTime）；3) 跑 `--suite <name>`。
注意：注入式 bench 对"一侧有池化回收"的场景会失真（见 dns），需按目标评估生命周期语义。

## 最近一次完整对比基线（2026-08-06 · bray 2910e126 vs upstream 5ca6f4b7 · repeats 3 × count 3）

`go run ./tools/benchcompare` 无参数默认命令的完整结果（9 样本中位数）：

| Suite | 判定 | 要点 |
|---|---|---|
| `xhttp` (17) | **Bray 14 · 上游 3** | packet-up 系快 1-2 个数量级：H2C -99.2%、H2 -97.4%、MemoryAllocations -99.5%（allocs 92 vs 140）；StreamUp -10.9%、Modes/stream-up -5.8%；落后项均为短连接：ColdStart +20.8%、ConnectionStorm +17.7%、TTFB +16.7%（波动大，需结合 TCP_Echo 基线判读） |
| `vless` (3) | **Bray 3** | Encode -29~-31%（Decode 栈缓冲优化后） |
| `xmux` (13) | bray-only | 上游 API 分叉无对比 |
| `buf` (9) | Bray 1 · tie 7 · 上游 1 | Copy -13.5%；NewBufferStack 上游快（结构体含 UDP 字段，安全-性能权衡） |
| `reality` (7) | Bray 1 · tie 2 · 上游 4 | **MLDSA65Sign 5s benchtime 下波动仍大**，多轮方向翻转（+10.9%↔-16.3%），判定为不可比；X25519/AEAD/Verify Bray 快 |
| `dns` (7) | 基础基准上游快 5 | 注入版不含 Bray 池化（公平基础基准）；**Bray 真实池化路径快 54-65%**（BuildReqMsgs 102ns vs 295ns） |

判读原则：冷启动/短连接三场景（TTFB/ConnectionStorm/ColdStart）跨轮波动 ±20%，
单轮结论不可信；**必须以默认 repeats 3 + TCP_Echo 基线对照**。

## 相关质量测试（性能套件之外，随测试固化）

```bash
# DNS 安全性（防投毒 RFC5452 / SeededID，Bray 特有）
go test -run 'TestAttack' -count=5 ./app/dns/
# REALITY 生产级稳定性（并发/韧性/大文件/goroutine）
go test -count=2 -timeout=20m -run 'TestREALITY' ./testing/scenarios/
```

## 注意事项

- 上游目标需要网络（worktree + REALITY 子模块）；输出目录已 gitignore。
- 每轮全新 worktree，主工作区零污染；`--keep` 保留调试。
- 长任务/高波动场景请勿降低 `--repeats`。
