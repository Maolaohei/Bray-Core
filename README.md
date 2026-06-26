# Bray-Core

> 基于 [Xray-core](https://github.com/XTLS/Xray-core) v26.6.1 的高性能增强分支，专注于 TCP 优化、连接调度与智能网络决策，在保持 100% 协议兼容性的前提下提升复杂网络环境下的稳定性与连接成功率。

<p align="center">
  <a href="https://github.com/Maolaohei/Bray-Core">
    <img src="https://img.shields.io/badge/BRAY--CORE-FF6B35?style=for-the-badge&logo=go&logoColor=white" alt="BRAY-CORE">
  </a>
  &nbsp;
  <a href="https://github.com/Maolaohei/REALITY">
    <img src="https://img.shields.io/badge/BRAY--REALITY%20v3-4A90D9?style=for-the-badge&logo=shield&logoColor=white" alt="BRAY-REALITY">
  </a>
</p>

### 魔改 REALITY 特有特性

| 特性 | 说明 | 上游 REALITY |
|------|------|:------------:|
| TLS 1.3 握手伪造 | 从目标服务器实时捕获 ServerHello + record 长度，构建与目标一致的握手指纹 | ✅ |
| 持久化缓存 | profiles.json 原子写入，服务重启后秒级恢复，无需等待首次握手 | ❌ |
| HotSwap 热替换 | 目标 CipherSuite 变更时新旧 profile 无缝切换，已有连接不受影响 | ❌ |
| Stale-While-Revalidate | 过期 profile 仍可用于握手，后台异步刷新，不阻塞新连接 | ❌ |
| 负缓存退避 | 探测失败后指数退避（1/2/4/8min），避免对目标产生无效请求 | ❌ |
| Pin/Unpin 引用计数 | 连接级引用计数保护，正在使用的 profile 不会被误删 | ❌ |
| EventBus 事件总线 | Observer 模式解耦缓存、持久化、刷新三大模块 | ❌ |
| 证书链扩容 | 支持 64KB 证书链（原 8KB），兼容更多目标站点 | ❌ |
| RefreshManager | 单调度器统一管理所有目标探测，替代 per-target goroutine | ❌ |

### 缓存实测数据（i5-13600KF, Go 1.24）

| 指标 | 结果 |
|------|------|
| 缓存命中查询 | 13 ns/op，0 B/op，单核吞吐 77M ops/s |
| 缓存未命中 | 6 ns/op，0 B/op，单核吞吐 167M ops/s |
| Fingerprint 计算 | 3.8 ns/op，0 B/op |
| Soak 2000 次握手 | 内存增长 0.14 MB（15.78%），GC 仅 1 次 |
| 单元测试 | 37/37 全通过，0 data races |

### 魔改解决的问题

| 原版问题 | 魔改方案 | 实际效果 |
|----------|----------|----------|
| 重启后需重新握手，首次连接慢 | 持久化缓存 | 重启后秒级恢复，用户无感知 |
| 目标换证书后所有连接断开 | HotSwap + Stale-While-Revalidate | 已有连接不受影响，新连接自动切换 |
| 探测失败后反复重试 | 负缓存退避 | 指数冷却，减少无效请求 |
| 正在用的 profile 可能被清理 | Pin/Unpin 引用计数 | 连接级保护，杜绝误删 |
| 证书链超 8KB 的站点无法使用 | 证书链扩容至 64KB | 兼容更多目标站点 |
| 多目标探测各自为战 | RefreshManager 单调度器 | 资源统一管理，降低开销 |

---

### REALITY v3 — TLS 指纹伪装与缓存体系 ✅

| 特性 | 说明 | 状态 |
|------|------|------|
| 持久化缓存 | 重启后秒级恢复，profiles.json 原子写入 | ✅ |
| 后台刷新 | RefreshManager 定期探测目标，CipherSuite 变更自动热替换 | ✅ |
| HotSwap | 新旧 profile 无缝切换，正在使用的连接不受影响 | ✅ |
| Stale-While-Revalidate | 过期 profile 仍可使用，后台异步刷新 | ✅ |
| 负缓存 | 探测失败指数退避，避免无效重试 | ✅ |
| Pin/Unpin | 连接级引用计数，防止正在使用的 profile 被误删 | ✅ |
| EventBus | Observer 模式解耦缓存/持久化/刷新逻辑 | ✅ |
| 证书限制解除 | 支持 64KB 证书链（原 8KB 限制） | ✅ |
| TLS 1.3 握手伪造 | 从目标服务器捕获记录长度，构建假握手 | ✅ |
| Soak 测试 | 2000 次连接，GC 仅 1 次，内存增长 15.79% | ✅ |
| Race-free | 0 data races，37/37 测试全通过 | ✅ |

### V2.2 — 代码质量与性能优化 ✅

| 模块 | 优化 | 状态 |
|------|------|------|
| XMUX mux.go | defer Unlock 防死锁、nil 检查、CAS 替代竞态 | ✅ |
| XMUX mux.go | leftUsage atomic 化、Close 清理连接池 | ✅ |
| VLESS encoding | Sentinel Errors 消除热路径分配 | ✅ |
| VLESS encoding | In-place Modification 零拷贝解码 | ✅ |
| VLESS encoding | flowString 预分配常量、copySeed pool 复用 | ✅ |
| VLESS encoding | Decode 33x 性能提升（零分配解码） | ✅ |
| tcpinfo | computeQuality 跨平台去重 | ✅ |
| xpadding.go | parsedURLCache 有界 LRU 替代 sync.Map | ✅ |
| connection.go | onClose 顺序修复、Deadline 返回 error | ✅ |
| collector_fallback.go | FeedRTT 零值保护 | ✅ |
| mux.go | healthCheckLoop 提取 helper + defer Unlock | ✅ |
| mux.go | getNetworkHash strings.Builder 优化 | ✅ |
| mux.go | behaviorCounts 固定数组替代 map | ✅ |

### V2.3 — 安全加固与协议扩展 ✅

| 模块 | 优化 | 状态 |
|------|------|------|
| NetBridge | 新增 inbound 代理协议（ProxyBridgeCore 流量接入） | ✅ |
| XHTTP/WS/HU/gRPC | 服务端强制 `trustedXForwardedFor` 校验 | ✅ |
| WireGuard | 大规模重构，优化代码质量与可维护性 | ✅ |
| Loopback | 新增 sniffing 支持 | ✅ |
| XMUX | 死锁修复、QUIC/UDP 主动关闭 | ✅ |
| XHTTP/3 | 客户端主动关闭 QUIC & UDP 资源 | ✅ |
| Benchmark CI | 建立系统化性能基准测试流水线 | ✅ |

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

### V2.3 — Security & Protocol Extensions ✅

| 功能 | 状态 |
|------|------|
| NetBridge — inbound 代理协议（ProxyBridgeCore 流量接入） | ✅ |
| trustedXForwardedFor — XHTTP/WS/HU/gRPC 服务端强制校验 | ✅ |
| WireGuard — 大规模重构 | ✅ |
| Loopback Sniffing — 出站嗅探支持 | ✅ |
| Benchmark CI — 系统化性能基准测试流水线 | ✅ |

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

### NetBridge 协议

| 特性 | 说明 |
|------|------|
| 流量接入 | 接收来自 ProxyBridgeCore 的代理流量 |
| 协议支持 | TCP + UDP 双协议 |
| 认证机制 | Token 认证，防止未授权访问 |
| 配置格式 | Protobuf 定义 + JSON 配置解析 |
| 测试覆盖 | 完整单元测试（344 行） |

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

### REALITY v3 缓存架构

```
ClientHello → Handshake Engine → MirrorConn (client↔target)
                    ↓
            EventBus (Observer 模式)
            ├─ CacheHandler → CacheManager (sync.Map)
            ├─ PersistHandler → profiles.json (原子写入)
            └─ RefreshHandler → RefreshManager (20-30min 探测)
                    ↓
            HotSwap: 新 profile Store → 旧 profile PendingDelete
                    ↓
            Pin/Unpin: 连接级引用计数 → refCount=0 自动清理
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

# VLESS Decode benchmarks
go test -bench "BenchmarkDecode|BenchmarkMarshal" -run "^$" -timeout 30s ./proxy/vless/encoding/

# Happy Eyeballs benchmarks
go test -bench "BenchmarkScore|BenchmarkClampRTT" -run "^$" -timeout 30s ./transport/internet/

# 全量 benchmarks
go test -bench "BenchmarkNewBuffer|BenchmarkCopy|BenchmarkSplitBytes" -run "^$" -timeout 60s ./common/buf/

# NetBridge 单元测试
go test -short -timeout 30s ./proxy/netbridge/
```

---

## 许可证

[Mozilla Public License Version 2.0](https://github.com/XTLS/Xray-core/blob/main/LICENSE)

---

*上游同步基准：Xray-core [v26.6.1](https://github.com/XTLS/Xray-core/releases/tag/v26.6.1)*

*最后更新：2026-06-26*
