# Bray-Core

> 基于 [Xray-core](https://github.com/XTLS/Xray-core) v26.6.1 的高性能增强分支，专注于 TCP 优化、连接调度与智能网络决策，在保持 100% 协议兼容性的前提下提升复杂网络环境下的稳定性与连接成功率。

### V2.2 — 代码质量与性能优化 ✅

| 模块 | 优化 | 状态 |
|------|------|------|
| XMUX mux.go | defer Unlock 防死锁、nil 检查、CAS 替代竞态 | ✅ |
| XMUX mux.go | leftUsage atomic 化、Close 清理连接池 | ✅ |
| VLESS encoding | Sentinel Errors 消除热路径分配 | ✅ |
| VLESS encoding | In-place Modification 零拷贝解码 | ✅ |
| VLESS encoding | flowString 预分配常量、copySeed pool 复用 | ✅ |
| tcpinfo | computeQuality 跨平台去重 | ✅ |
| xpadding.go | parsedURLCache 有界 LRU 替代 sync.Map | ✅ |
| connection.go | onClose 顺序修复、Deadline 返回 error | ✅ |
| collector_fallback.go | FeedRTT 零值保护 | ✅ |
| mux.go | healthCheckLoop 提取 helper + defer Unlock | ✅ |
| mux.go | getNetworkHash strings.Builder 优化 | ✅ |
| mux.go | behaviorCounts 固定数组替代 map | ✅ |

---

## 快速开始

### 下载

| 平台 | 下载 |
|------|------|
| **Windows** | [V2rayN (个人修改版本)](https://github.com/Maolaohei/v2rayN) | 
| **Android** | [V2rayNG (Bray-Core 内核)](https://github.com/Maolaohei/v2rayNG/releases) |

Windows版本需要自己在bin/xray下替换内核


### 编译

```bash
# Linux amd64 (推荐 v3 指令集)
CGO_ENABLED=0 GOAMD64=v3 go build -o xray -trimpath -ldflags="-s -w -buildid=" ./main

# Windows amd64
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 GOAMD64=v3 go build -o xray.exe -trimpath -ldflags="-s -w -buildid=" ./main
```

### Linux 服务端优化

```bash
sysctl -w net.core.default_qdisc=fq
sysctl -w net.ipv4.tcp_congestion_control=bbr
sysctl -w net.ipv4.tcp_fastopen=3
sysctl -w net.core.rmem_max=16777216
sysctl -w net.core.wmem_max=16777216
```

---

## Roadmap

### V1.x — Performance & Stability ✅

- TCP 网络栈优化 — BBR / TCP_NOTSENT_LOWAT / DEFER_ACCEPT / NODELAY / QUICKACK
- XHTTP / VLESS 优化 — Vision Fast Path、手写 protobuf、AddonsPool
- XMUX v3 — Min-Inflight Scheduling、RTT-aware Scheduling、Connection Reuse Metrics
- Happy Eyeballs v3 — Dynamic Parallelism、Historical Learning、Score-based IP Selection
- Warmup Pipeline — 连接预热 + DNS 预热 + 健康检查

### V2.0 — Transport Intelligence Layer ✅

核心原则：Observe → Decide → Control

| 模块 | 状态 |
|------|------|
| TransportProfile — RTT / Loss / Retrans / Confidence / Source / Timestamp | ✅ |
| Quality Model — Overall / Latency / Loss / Stability 多维评分 | ✅ |
| XMUX V2.0 — QualityScore / Trend / Graceful Drain | ✅ |
| HEv3 V2.0 — EWMA Failure Tracking / Quality-aware Scoring | ✅ |
| Debug API — Snapshot / Reason / History (64-sample ring buffer) | ✅ |
| NetworkLearner — Behavior Classification (6 types) | ✅ |
| Pipeline Integration — Profile → UpdateQuality → scoreClient | ✅ |
| Windows Fallback — HTTP RTT → estimated quality | ✅ |

### V2.1 — Adaptive Transport ✅

| 功能 | 状态 |
|------|------|
| Adaptive XMUX — behavior-aware penalty scaling | ✅ |
| Dynamic Connection Scaling — AIMD pool sizing | ✅ |
| Oscillation Prevention — debounce (3 observations) | ✅ |
| Connection Migration — proactive pool refill | ✅ |

### V3.x — Network Learning (研究阶段)

- IPv4/IPv6 行为学习
- 运营商行为识别
- 链路退化趋势预测

---

## 核心架构

### TCP 网络栈（默认即最优）

| 优化 | 效果 | 上游行为 |
|------|------|----------|
| BBR 拥塞控制 | 出站/入站自动设置 | 需手动配置 |
| TCP_NOTSENT_LOWAT | 按内存自动选择 8K/16K/32K | 需手动配置 |
| TCP_DEFER_ACCEPT (3s) | 省一次上下文切换 | 未启用 |
| TCP_NODELAY | 禁用 Nagle，延迟降低 | 未启用 |
| TCP_QUICKACK | 禁用延迟 ACK，TLS 加速 | 未启用 |

### XMUX 连接池

| 特性 | 说明 |
|------|------|
| Min-Inflight Scheduling | 选择最少在途请求的连接 |
| RTT-aware Scheduling | 基于 EWMA 平滑 RTT 调度 |
| QualityScore V2.1 | inflight×10000 + rttMs×10 + (retrans×50 + loss/20) × behaviorScale × confidenceScale |
| Behavior-aware Scaling | LowLatency 0.5x / Lossy 1.5x / Aggressive 1.2x 惩罚系数 |
| Dynamic Connection Scaling | AIMD: 改善 +1, 恶化 ×0.5, 平滑过渡 |
| Connection Migration | 断线/质量下降时主动创建替代连接 |
| Graceful Drain | 连续 5 次质量下降 + confidence≥30 时优雅移除 |
| Oscillation Prevention | 连续 3 次观察到同一行为才切换，防止反复横跳 |
| Pre-connect | 后台每 5s 检查池，空池自动创建 |
| Health Check | 5s 周期，主动迁移低于 effectiveConnections 的池 |

### Happy Eyeballs v3

| 特性 | 说明 |
|------|------|
| Score-based Selection | priority×1e9 + rttTerm×(1 + failRate×10 + lossPenalty) |
| EWMA Failure Tracking | 0.95 衰减，无双计数器，无清理协程 |
| Quality-aware Scoring | Loss penalty 来自 TransportProfile |
| Dynamic Parallelism | 自适应并行连接数 |

### Transport Intelligence

```
TCP socket → Profile.Collect(TCP_INFO) → Snapshot
    ↓ (Linux: getsockopt, Windows: HTTP RTT estimation)
Quality Score (Overall/Latency/Loss/Stability)
    ↓
NetworkLearner.Record() → ClassifyBehavior (6 types)
    ↓
scoreClient() × behaviorScale × confidenceScale → scheduling decision
    ↓
Dynamic Connection Scaling (AIMD) → pool sizing
    ↓
Connection Migration → proactive pool refill
```

---

## 性能数据

**测试环境**: i5-13600KF, Windows, amd64, Go 1.26

### Bray vs 上游 Xray-core

| Benchmark | 上游 (ns/op) | Bray (ns/op) | Delta |
|-----------|-------------|-------------|-------|
| NewBuffer | 47.15 | 39.15 | **-16.9%** |
| NewBufferStack | 30.06 | 24.05 | **-20.0%** |
| Copy | 98.13 | 89.02 | **-9.3%** |
| SplitBytes | 159.4 | 143.8 | **-9.8%** |

### XMUX Hot Paths

| Benchmark | ns/op | allocs |
|-----------|-------|--------|
| RTTEWMA | 8 | 0 |
| WarmupEnqueue | 11 | 0 |
| Metrics | 11 | 0 |
| PoolScheduling (pool_1) | 109 | 0 |
| PoolScheduling (pool_4) | 153 | 0 |
| PoolScheduling (pool_8) | 207 | 0 |
| PoolScheduling (pool_16) | 307 | 0 |
| PoolScheduling (pool_32) | 494 | 0 |
| ConcurrentR/W (workers_16) | 35 us | 0 |

### VLESS Decode (优化后)

| Benchmark | ns/op | B/op | allocs |
|-----------|-------|------|--------|
| DecodeHeaderAddons | 28 | 32 | 1 |
| DecodeHeaderAddonsParallel | 23 | 104 | 3 |
| MarshalAddons Vision | 14.4 | 24 | 1 |

### Happy Eyeballs v3

| Benchmark | ns/op | allocs |
|-----------|-------|--------|
| Score | 0.19 | 0 |
| ClampRTT | 0.10 | 0 |

---

## 多租户说明

### 连接池隔离

| 部署方式 | 池隔离 | V2.0/V2.1 效果 |
|----------|--------|---------------|
| **客户端** (每用户独立实例) | ✅ 天然隔离 | 每个用户独立池，互不影响 |
| **服务端** (多用户共享出站) | ⚠️ 按目标共享池 | 池质量反映网络质量，非用户质量 |

### 服务端多用户行为

- 连接池按 `destination + streamSettings` 隔离，不同目标的池互不影响
- 同一目标的多用户共享池 — 这是正确行为：如果到某目标的网络丢包，所有连接都应被视为 lossy
- NetworkLearner 跟踪每个连接的行为，池行为取活跃连接的主导行为
- 单个差连接不会污染整个池（debounce + 取主导行为）

### 建议

- 客户端部署：每个用户独立 Xray 实例 → 完全隔离
- 服务端部署：共享池是预期行为，网络级别的调度决策对所有用户公平

---

## 兼容性

| 场景 | 兼容性 |
|------|--------|
| 上游客户端 → Bray 服务器 | ✅ 100% |
| Bray 客户端 → 上游服务器 | ✅ 100% |
| 配置格式 | ✅ 向后兼容 |
| 跨平台编译 | ✅ Linux/Windows/macOS/Android/FreeBSD/OpenBSD |

---

## 测试

```bash
# 全量单元测试
go test -short -timeout 60s ./transport/internet/tcpinfo/ ./transport/internet/quality/ ./transport/internet/splithttp/

# XMUX benchmarks
go test -bench "^BenchmarkXMUX" -run "^$" -timeout 30s ./transport/internet/splithttp/

# 全量 benchmarks
go test -bench "BenchmarkNewBuffer|BenchmarkCopy|BenchmarkSplitBytes" -run "^$" -timeout 60s ./common/buf/
```

---

## 许可证

[Mozilla Public License Version 2.0](https://github.com/XTLS/Xray-core/blob/main/LICENSE)

---

*上游同步基准：Xray-core [v26.6.1](https://github.com/XTLS/Xray-core/releases/tag/v26.6.1)*
