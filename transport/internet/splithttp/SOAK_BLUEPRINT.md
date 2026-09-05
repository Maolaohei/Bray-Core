# Bray-Core splithttp 分层测试蓝图（soak）

> 版本 2026-09-05 · 对应代码 `transport/internet/splithttp/`
> 背景：专家蓝图咨询（故障注入层 / 负载画像 / 三判据 / 可复现性 / 真实环境层 / CI 分层 / 从 bug 反推测试）的落地与差距分析。

## 0. 总览：L0–L4 分层

| 层 | 内容 | 时长预算 | 触发 | 状态 |
|---|---|---|---|---|
| L0 | 流水线/包边界单元与 e2e 边界 | <1 min | 每次 push | ✅ `h1_pipeline_boundary_test.go`（默认套件） |
| L1 | 混沌故障 e2e（RST/高 RTT） | ~2 min | 每次 push | ✅ `chaos_proxy_test.go`（默认套件） |
| L2 | soak 三判据场景（ChatIdle/MobileUser/Shape） | ~2.5 min | `-tags soak`（本地/定期） | ✅ `soak_harness_test.go` + `soak_scenarios_test.go` |
| L2+ | 耐久无丢弃系列 | ~10 min | `-tags endurance` | ✅ `endurance_test.go`（既有） |
| L3 | 长时 soak（30 min–4 h，放大时长参数） | 夜间 | CI nightly | ⬜ 结构已备：调 `FaultPlan`/轮次常量即可 |
| L4 | netem 网络损伤 / CDN 真实链路 / 真机 | 夜间/周 | 手动+nightly | ⬜ 见 §4 |

## 1. 故障注入层（已实现）

**载具**：用户态 TCP 故障代理 `faultProxy`（`soak_harness_test.go`），置于客户端与产品 hub 之间，由种子化 `FaultPlan` 驱动：

| 故障 | 参数 | 命中产品路径 |
|---|---|---|
| RST | `RSTLifetime[2]`（每连寿命抽取，SO_LINGER 0） | 错误路径、重连、dup-seq 幂等 |
| Stall | `Stall[2]`+`StallChance`（中途静默 hold，无错误） | 超时/背压逻辑（对 4xx 错位类 bug 的回归） |
| Blackhole | `Blackhole[]{Start,End}`（全局时间窗，静默 hold） | 大规模重连/退避、seq-gap 拆会话 |
| Truncate | `TruncateChance`（S→C 中途 RST） | 下行重建 |
| HalfClose | `HalfCloseChance`（C→S CloseWrite） | 方向性 EOF 处理 |
| 限速 | `BWLimitBytes`（令牌桶） | 慢链路背压 |
| 拨号失败/延迟 | `DialFailRate`（+`RTTMean` 逐块延迟） | 拨号池、探测超时 |

**设计约束**：
- `GracePeriod`：故障在宽限期后才开始 —— 会话必须先建立（真实用户进电梯前会话已存在；握死于故障属拨号器 fail-closed 设计，不是 soak 发现项）。
- 丢失/乱序/时钟跳变 **不在用户态层做**：进程内丢字节=流损坏而非"丢包"；TCP 重传/排序归内核 —— 归 netem 层（§4.1）。
- 单写者契约：soak 场景一律顺序写（并发应用层 `conn.Write` 会撕裂 chunk 载荷，见 L0 测试注释）。

## 2. 三判据（oracle，已实现）

1. **数据正确性**：seq 帧 `[u32 seq][u32 len][payload]`，载荷按位置推导 `byte(seq*131+i*7+0x5A)`（无共享 PRNG），echo 逐帧校验连续性+内容 —— dup/丢失/乱序/截断全部可定位。
2. **Liveness（恢复 SLA）**：每个故障事件（事件结束时刻）→ 下一个字节到达的时延，断言 < SLA（15s），而非只看"没崩"。完成时间之后的事件跳过（无在飞数据可恢复）。
3. **资源有界性**：周期采样 NumGoroutine/HeapAlloc；终态泄漏守卫（基线+30，先 settle 2s）；峰值界（基线+120）。无 goleak 依赖。

**行为形状判据**（第 4 判据，`TestSoak_ShapeStatistics`）：线上 burst 大小变异系数 CV ≥ 0.10（无固定 body 尺寸指纹）+ burst 间隔自相关 |AC(1..5)| ≤ 0.6（无固定节拍指纹）。阈值宽松+固定种子，防 flaky；一旦 chunk 定尺寸/定间隔化回潮即报警。

## 3. 负载画像（personas，已实现）

- **ChatIdle**（`TestSoak_ChatIdle`）：16 轮"小消息→空闲 1–3s"，全故障混合。直接对应"挂一晚上第二天发不出消息"类 bug：断言每次空闲后首消息时延 <10s SLA + 全部恢复 SLA + 泄漏守卫。
- **MobileUser**（`TestSoak_MobileUser`）：5 轮 [web burst → video steady]，预排 3 个电梯黑窗（10/17/24s，均 <3.4s < maxSeqGapWait 5s —— 设计承诺内必须存活，SLA 断言恢复速度），writer **黑窗期间持续写**（写阻塞=真实背压）。
- **ShapeStatistics**：300 条聊天尺寸消息驱动 pacing 带，线上形状统计。

**可复现性**：全场景单一种子（`BRAY_SOAK_SEED`，默认时间戳，每次运行打印 `SEED=N`）；失败重跑 `BRAY_SOAK_SEED=<N> go test -tags soak -run <name>` 复现同一计划。

## 4. L4 真实环境层（设计，未实现）

### 4.1 netem（Linux 夜间）

```bash
# 真实丢包/乱序/时延抖动（用户态层不模拟这些）
sudo tc qdisc add dev lo root netem delay 100ms 30ms loss 2% reorder 5%
go test -tags soak ./transport/internet/splithttp -run TestSoak
sudo tc qdisc del dev lo root
```

Windows 本地等价：clumsy（https://jagt.github.io/clumsy/ ）—— drop/order/delay/throttle。

### 4.2 CDN nightly（MobileUser × 真链路）

同一 persona 代码，把 `soakServer`+`faultProxy` 换成真实 CDN 前置：故障=CDN 侧 455/429/超时（已有单测覆盖判定逻辑）+ 客户端 Wi-Fi 开关。跑 30 min，判据不变（完整性+恢复 SLA+资源界）。

### 4.3 真机层

Android/iOS 客户端 × 真实基站切换/锁屏省电：抓包对线上形状（burst 分布）与 ShapeStatistics 基线对比，防"实验室形状≠野外形状"。

### 4.4 虚拟时钟（路线图，暂缓）

产品时间源重构（全局时钟接口）后，soak 场景可注入跳变；当前不做 —— 时钟跳变仅出现在 netem/真机层。

## 5. 从 bug 反推测试（本蓝图的验证）

| 历史 bug | 现在由哪个测试防守 |
|---|---|
| Huorong WFP 环境致 4xx 错位（400 错位是环境问题非产品 bug） | `host_http_sanity_test.go`（环境归因）+ Stall 故障回归超时类逻辑 |
| seq-gap 拆会话（设计 032ca7dc/59380fde，fail-closed） | MobileUser 黑窗 <maxSeqGapWait 断言存活；ChatIdle 空闲后再发 |
| 并发写撕裂 chunk（单写者契约） | L0 `h1_pipeline_boundary_test.go` 注释+顺序化 |
| 固定 chunk 尺寸/节拍（A/B 已采纳全尺寸+抖动） | ShapeStatistics CV+自相关判据 |
| 空闲后首包迟滞 | ChatIdle 首消息 SLA |

## 6. 运行手册

```bash
# L2 soak 全套（~2.5 min）
go test -tags soak ./transport/internet/splithttp -run TestSoak -v

# 复现失败
BRAY_SOAK_SEED=<printed> go test -tags soak ./transport/internet/splithttp -run <TestName> -v

# 竞态
go test -tags soak -race ./transport/internet/splithttp -run TestSoak_ChatIdle

# L3 放大：改 soak_scenarios_test.go 的 rounds/窗口/SLA 常量（结构不变）
```
