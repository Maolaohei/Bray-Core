# Bray-Core

> 基于 [Xray-core](https://github.com/XTLS/Xray-core) v26.6.1 的高性能增强分支，专注于 TCP 优化、连接调度与智能网络决策，在保持 100% 协议兼容性的前提下提升复杂网络环境下的稳定性与连接成功率。

---

## 快速开始

### 下载

| 平台 | 下载 |
|------|------|
| **Windows** | [V2rayN (原版内核)](https://github.com/2dust/v2rayn/releases) |
| **Android** | [V2rayNG (Bray-Core 内核)](https://github.com/Maolaohei/v2rayNG/releases) |

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

**已完成：**

- TCP 网络栈优化 — BBR / TCP_NOTSENT_LOWAT / DEFER_ACCEPT / NODELAY / QUICKACK
- XHTTP / VLESS 优化 — Vision Fast Path、手写 protobuf、AddonsPool
- XMUX v3 — Min-Inflight Scheduling、RTT-aware Scheduling、Connection Reuse Metrics
- Happy Eyeballs v3 — Dynamic Parallelism、Historical Learning、Score-based IP Selection
- Warmup Pipeline — 连接预热 + DNS 预热 + 健康检查
- 安全性强化 — CSPRNG 缓冲池、rejection sampling、Huffman 缓存

### V2.0 — Transport Intelligence Layer ✅

**状态：已完成**

核心原则：Observe → Decide → Control

TransportProfile 仅负责观测网络状态，不参与决策逻辑。

**已实现：**

| 模块 | 状态 |
|------|------|
| TransportProfile — RTT / Loss / Retrans / Confidence / Source / Timestamp | ✅ |
| Quality Model — Overall / Latency / Loss / Stability 多维评分 | ✅ |
| XMUX V2.0 — QualityScore / Trend / Graceful Drain | ✅ |
| HEv3 V2.0 — EWMA Failure Tracking / Quality-aware Scoring | ✅ |
| Debug API — Snapshot / Reason / History (64-sample ring buffer) | ✅ |
| NetworkLearner — Behavior Classification (6 types) | ✅ |
| Pipeline Integration — Profile → UpdateQuality → scoreClient | ✅ |

### V2.1 — Adaptive Transport (规划中)

- Adaptive XMUX — Dynamic Concurrency / Dynamic Connection Scaling
- Feedback-driven Scheduling
- 需建立在成熟的 TransportProfile 与 Quality Model 之上

### V3.x — Network Learning (研究阶段)

- IPv4/IPv6 行为学习
- 运营商行为识别
- 链路退化趋势预测
- Transport Behavior Recognition（不依赖算法名称，基于真实链路行为）

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
| QualityScore V2.0 | inflight×10000 + rttMs×10 + retrans×50 + lossRate/20 |
| Graceful Drain | 连续 5 次质量下降 + confidence≥30 时优雅移除 |
| Pre-connect | 后台每 5s 检查池，空池自动创建 |
| Health Check | 5s 周期，RTT>5s / Closed / Drain 三重检测 |

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
    ↓
Quality Score (Overall/Latency/Loss/Stability)
    ↓
XMUX.UpdateQuality() → scoreClient() → scheduling decision
    ↓
HEv3.UpdateQuality() → score() → IP selection
```

---

## 性能数据

**测试环境**: i5-13600KF, Windows, amd64, Go 1.26

### Bray vs 上游 Xray-core

| Benchmark | 上游 (ns/op) | Bray (ns/op) | Delta |
|-----------|-------------|-------------|-------|
| NewBuffer | 47.15 | 43.13 | **-8.5%** |
| NewBufferStack | 30.06 | 28.16 | **-6.3%** |
| Copy | 98.13 | 90.71 | **-7.6%** |
| SplitBytes | 159.4 | 156.0 | **-2.1%** |
| ChaCha20 | 625 MB/s | 598 MB/s | ~0 |
| AES Encryption | 1006 MB/s | 1004 MB/s | ~0 |
| FrameWrite | 47.93 | 47.34 | ~0 |

### XMUX Hot Paths

| Benchmark | ns/op | allocs |
|-----------|-------|--------|
| GetXmuxClient | 17 | 0 |
| RTTEWMA | 8.4 | 0 |
| WarmupEnqueue | 10.8 | 0 |
| Metrics | 11.0 | 0 |
| ConcurrentRW (16 workers) | 32,269 | 0 |

### Happy Eyeballs v3

| Benchmark | ns/op | allocs |
|-----------|-------|--------|
| Score | 0.21 | 0 |
| ClampRTT | 0.10 | 0 |
| SortIPs (large) | 1,965 | - |
| ScoreIPs | 1,124 | - |

---

## 兼容性

| 场景 | 兼容性 |
|------|--------|
| 上游客户端 → Bray 服务器 | ✅ 100% |
| Bray 客户端 → 上游服务器 | ✅ 100% |
| 配置格式 | ✅ 向后兼容 |

---

## 测试

```bash
# XMUX 单元测试
go test -run "TestXMUX|TestMetrics" ./transport/internet/splithttp/

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
