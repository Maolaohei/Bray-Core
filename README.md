# Bray-Core

面向复杂网络环境的 Xray-core 深度改造内核：以 **VLESS + XHTTP + XMUX + REALITY** 为核心栈，在传输稳定性、调度与性能上持续投入。基于 [Xray-core](https://github.com/XTLS/Xray-core)，**两端均须使用本仓库构建**。

---

## 快速开始

```bash
git submodule update --init --recursive   # 初始化 REALITY 子模块

# Linux amd64（推荐 GOAMD64=v3）
CGO_ENABLED=0 GOAMD64=v3 go build -o xray -trimpath -ldflags="-s -w -buildid=" ./main

# Windows amd64
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 GOAMD64=v3 go build -o xray.exe -trimpath -ldflags="-s -w -buildid=" ./main
```

`go.mod` 通过 `replace github.com/Maolaohei/REALITY => ./REALITY` 固定本地子模块，请勿在未对齐子模块提交时单独改写该依赖。Linux 服务端 TCP 调优（可选）见 [`docs/server-tuning.md`](docs/server-tuning.md)。

### 客户端

| 平台 | 方式 |
|------|------|
| Windows | [v2rayN](https://github.com/2dust/v2rayN) 替换内核二进制（`bin/xray`），对端须为 Bray-Core |
| Android | [v2rayNG（Bray-Core 内核发布）](https://github.com/Maolaohei/v2rayNG/releases) |
| 通用 | 任意可替换 Xray 二进制的客户端，**两端均使用本仓库构建** |

---

## 项目状态

| 项目 | 值 |
|------|-----|
| 基线版本 | Xray-core **26.8.3** |
| 语言版本 | Go **1.26.5** |
| REALITY | [Maolaohei/REALITY](https://github.com/Maolaohei/REALITY) v0.5.5（子模块） |
| 兼容策略 | **Bray-only**：两端均须本仓库内核 |

分支：**`main`** = 当前主干（Bray 完全体）；**`v1`** = 旧主干快照（仅回滚/对比）。

---

## 特性

### 传输

- **REALITY v0.5.5**：TLS 1.3 握手形态基于目标站点观测构造，Profile 内存 + 持久化缓存，HotSwap 热切换；摊销 L0/L1/L2（默认 L2，证据充分后 zero-dial），降低对观测站点的重复访问。
- **Happy Eyeballs v3**：基于历史失败率与质量评分的并行拨号与选路；DNS 缓存含正常命中、过期策略与并发查询加固；域名在 DNS 替换 IP 后保留（供 SNI 与目标使用）。
- **TCP**：支持平台上默认更积极的 socket 参数（BBR、NODELAY 等，依 OS 能力）。

### XHTTP / XMUX 数据面

- 连接池调度：Min-inflight、RTT/质量评分、行为感知惩罚、AIMD 池规模；Active → Draining → Closed 生命周期，致命错误快速驱逐。
- 身份隔离：`MuxKey` 含 `OriginalDomain` 等目的地身份；H2 连接按 TLS/主机身份隔离，同 IP 多域名不串池。
- Packet-up：RTT 自适应窗口（默认 12 / 上限 24）与 chunk 档位；bulk 大包跳过 pacing。
- 会话完整性：`sessionId` 带 HMAC 签名（密钥默认由 VLESS UUID 派生，零配置），服务端拒收未签名/错误会话。
- 模式级联：stream-one → stream-up → packet-up 自动演进，失败可自愈；sticky 记住 last-good 模式与多落地赢家。
- 热路径：零分配 seq、共享 header、池化缓冲、自适应 padding——每包 1 次堆分配消除（缓冲池指针化，Buffer 往返 -29%）。

### 传输形态

- 请求头与路径等 wire 形态经多轮改造，去除固定模式（详见 [CHANGELOG.md](CHANGELOG.md)）；每项改造均以实测 wire 审计 + 回归测试验收，并有兼容性说明。
- 空闲连接以低频、不规则间隔维持轻量活动，连接生命周期与空闲驱逐参数均按真实客户端分布取值。

---

## 性能

- **回归门禁**：`scripts/bench_compare.sh` —— 对比工作区 vs 任意基线 commit，benchstat 统计显著（时间 ≥+3% 或吞吐 ≤-3%）即失败；每轮改造以此验收。
- **基准套件**：每次推送 main 自动运行核心基准套件（XHTTP / XMUX / Happy Eyeballs / VLESS / Buffer），结果随 CI 报告归档；对比口径只信同名 Benchmark + 同 count + median。

---

## 安全模型：会话签名与 `x-bray-*` 头

配置里的 `headers` / `x-bray-*` **不是**「再造一套用户口令」，也 **不会** 原样出现在公网 HTTP 上：

- `headers` 普通项（User-Agent 等）进入 TLS 内的 HTTP 明文；有 TLS/REALITY 时链路加密，中间人不可见。
- `x-bray-*`（`session-secret` / `session-uuid` / `mode-degrade` / `multi-endpoint` / sticky TTL）：**本地控制头，永不上线**（构建请求时剥离），默认无需手配。
- **与 UUID 的关系**：VLESS UUID 是账号身份；会话签名是传输层对 `sessionId` 的认证。默认从同一 VLESS UUID 派生签名密钥，**不必**手工配 `x-bray-session-secret`；仅非 VLESS 或需多入站统一密钥时才写显式 secret。
- 线上只有已签名的会话路径分量，签名密钥不出进程。

---

## 兼容性

| 场景 | 预期 |
|------|------|
| **Bray 客户端 → Bray 服务端** | **支持（唯一正式保证）** |
| 上游 Xray 客户端 → Bray 服务端 | 不保证（会话签名、传输形态、XMUX 语义可能拒绝） |
| Bray 客户端 → 上游 Xray 服务端 | 不保证 |
| 配置 JSON | 沿用 Xray 字段外形，语义以 Bray 为准 |
| 平台 | Linux / Windows / macOS / Android 等 Go 支持的目标 |

> 传输形态改造（会话路径结构、padding 取值）需要**两端同步升级**：新客户端对旧服务端可能因校验不匹配被拒。

---

## 文档

| 文档 | 内容 |
|------|------|
| [CHANGELOG.md](CHANGELOG.md) | 版本与变更记录 |
| [docs/README.md](docs/README.md) | 文档索引 |
| [docs/presets/README.md](docs/presets/README.md) | 传输预设与 `x-bray-*` 控制头 |
| [docs/architecture-connection-lifecycle.md](docs/architecture-connection-lifecycle.md) | XMUX / 连接生命周期 |
| [docs/server-tuning.md](docs/server-tuning.md) | Linux 服务端 TCP 调优 |
| [SECURITY.md](SECURITY.md) | 安全策略 |
| [REALITY](https://github.com/Maolaohei/REALITY) | 独立 REALITY 实现与发布 |

问题反馈请使用 GitHub Issues，并附带：客户端/内核版本、传输组合、XHTTP mode、是否可稳定复现、服务端日志片段。

---

## 致谢与许可

Bray-Core 建立在 [XTLS/Xray-core](https://github.com/XTLS/Xray-core) 及社区生态之上，并集成 [Maolaohei/REALITY](https://github.com/Maolaohei/REALITY)。许可与上游一致，详见 LICENSE。
