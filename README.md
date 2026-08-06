# Bray-Core

Bray-Core 是基于 [Xray-core](https://github.com/XTLS/Xray-core) 的 **Bray 专属魔改内核**：以 **VLESS + XHTTP + XMUX + REALITY** 为核心栈，面向复杂网络下的稳定性、调度与性能。  
**当前 `main` 默认只保证 Bray 客户端 ↔ Bray 服务端互通**；为性能/安全做的协议侧加固已与上游 Xray 行为分叉，**不再承诺与上游内核互访兼容**。

| 项目 | 当前状态 |
|------|----------|
| 基线版本 | Xray-core **26.7.23** |
| 语言版本 | Go **1.26.5** |
| 模块路径 | `github.com/xtls/xray-core` |
| REALITY | [Maolaohei/REALITY](https://github.com/Maolaohei/REALITY) **v0.5.5**（子模块 `./REALITY`） |
| 默认 REALITY 摊销 | **L2**（证据充分后 zero-dial；不满足条件时回退 L1/L0） |
| 兼容策略 | **Bray-only**（两端均须本仓库内核 / 配套客户端） |

---

## 定位

- **Bray-only**：session MAC、packet-up 窗口/分片、padding/指纹加固、OpenStream 超时驱逐等按 Bray 两端语义设计；不要假设能挂原版 Xray 对端。
- **传输智能**：TCP 观测、XMUX 连接池、Happy Eyeballs、DNS 缓存、mode cascade / sticky / multi-endpoint。
- **REALITY 摊销**：服务端在安全约束下复用目标指纹观测，降低对伪装站点（RA）的重复拨号成本。
- **数据面性能**：packet-up RTT 自适应窗口与 chunk、零分配 seq、共享 header、deadline 路径 `bytespool`、大包 padding 收缩等。

推荐组合：**VLESS + XHTTP（stream-one / stream-up / packet-up）+ REALITY**。

---

## 快速开始

```bash
git submodule update --init --recursive   # 先初始化子模块（REALITY）

# Linux amd64（推荐 GOAMD64=v3）
CGO_ENABLED=0 GOAMD64=v3 go build -o xray -trimpath -ldflags="-s -w -buildid=" ./main

# Windows amd64
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 GOAMD64=v3 go build -o xray.exe -trimpath -ldflags="-s -w -buildid=" ./main
```

`go.mod` 通过 `replace github.com/Maolaohei/REALITY => ./REALITY` 固定本地子模块，请勿在未对齐子模块提交时单独改写该依赖。

Linux 服务端 TCP 调优（消除慢启动冷启动，可选）见 [`docs/server-tuning.md`](docs/server-tuning.md)。

### 客户端与替换内核

| 平台 | 说明 |
|------|------|
| Windows | [v2rayN（原版）](https://github.com/2dust/v2rayN) — 将编译产物替换到客户端 `bin/xray`（或对应内核目录）。**对端服务端须为 Bray-Core**。 |
| Android | [v2rayNG（Bray-Core 内核发布）](https://github.com/Maolaohei/v2rayNG/releases) |
| 通用 | 任意能换 Xray 二进制的客户端；**客户端与服务端都请使用本仓库构建** |

> 历史配套修改版 v2rayN 仓库不再作为默认推荐路径；以替换内核 + Bray 服务端为准。

---

## 分支策略

| 分支 | 说明 |
|------|------|
| **`main`** | **当前默认主干** = Bray 完全体（原功能分支 `Bray-V2`，Wave 1–7 + 后续 Bray-only 安全/性能）。克隆 / CI / 发版均以此为准。 |
| **`v1`** | 升级前的旧主干快照（原 `main`），仅用于回滚与对比。 |

```bash
git clone https://github.com/Maolaohei/Bray-Core.git
cd Bray-Core          # 默认已在 main（完全体）
git checkout v1       # 仅当需要旧线行为时
```

---

## 能力速览

### Bray 完全体（main）要点

| 能力 | 说明 |
|------|------|
| XMUX 浏览器默认 | 未配置 `xmux` 时使用 8–16 并发、2–4 连接、有界复用寿命（可显式写 0 恢复无限） |
| Mode cascade | auto 等路径下 stream-one → stream-up → packet-up；失败可自愈 |
| Sticky | 记住 last-good mode / multi-endpoint 赢家（TTL，可 opt-out） |
| Multi-endpoint | 可选多落地竞速（`x-bray-multi-endpoint` + endpoints） |
| 坏会话驱逐 | fatal open 时 MarkDead；OpenStream 等 header 超时累计后驱逐黑洞 H2；cascade 时刷新 HTTP/XMUX client |
| Session MAC | XHTTP session id 带 HMAC；密钥优先由 **VLESS UUID 派生**，无需额外配置 |
| 可观测 | `bray-v2>>>` 指标与比率；控制头 `x-bray-*` **仅本地配置注入，永不出现在线上 HTTP** |

### REALITY（v0.5.5）

- 指纹伪装：TLS 1.3 握手形态基于目标站点实时/缓存观测构造；Profile 内存 + 持久化缓存，HotSwap / SWR 热切换。
- 摊销 L0/L1/L2：**L2（默认）** 证据门槛后 zero-dial；证据不足、失败窗口触发时自动降级（不缓存不可安全复用的 R0）。
- 发布说明：[REALITY v0.5.5](https://github.com/Maolaohei/REALITY/releases/tag/v0.5.5)

### XHTTP / XMUX（Bray-only 数据面）

- 连接池调度：Min-inflight、RTT/质量评分、行为感知惩罚、AIMD 池规模；Active → Draining → Closed，致命错误快速驱逐。
- 身份隔离：`MuxKey` 含 `OriginalDomain` 等目的地身份；H2 连接按 TLS/主机身份隔离，避免同 IP 多域名串池。
- Packet-up：RTT 自适应窗口（默认 12 / 上限 24）与 chunk 档位（≤`scMaxEachPostBytes`）；bulk≥8KiB / recentFlow 跳过 30ms pacing。
- Session MAC：`sessionId = raw + "." + base64url(HMAC-SHA256(secret, raw)[:8])`；服务端拒收未签名/错误 MAC，防 hub 被未认证冲垮。
- 热路径减配：零分配 seq、共享 header、大包 padding 收缩、deadline 缓冲走 `bytespool`；OpenStream 等响应头超时可累计并 MarkDead。

### 出站拨号与 DNS

- 域名保留：DNS / Happy Eyeballs 替换 IP 时保留 `OriginalDomain`，供 SNI 与 VLESS 目标使用。
- Happy Eyeballs v3：基于历史失败率与质量评分的并行拨号与选路；DNS 缓存含正常命中、过期策略与并发查询路径加固。
- TCP 默认策略：支持平台上默认更积极的 socket 参数（BBR、NODELAY 等，依 OS 能力）。

> 传输预设与 `x-bray-*` 本地控制头详解见 [`docs/presets/README.md`](docs/presets/README.md)；
> XMUX / 连接生命周期架构见 [`docs/architecture-connection-lifecycle.md`](docs/architecture-connection-lifecycle.md)。

---

## Headers / `x-bray-*` 说明（常见疑问）

配置里的 `headers` / `x-bray-*` **不是**「再造一套用户口令」，也 **不会** 原样出现在公网 HTTP 上。

- `headers` 普通项：伪装用 HTTP 请求头（如 User-Agent），会进入 TLS 内的 HTTP 明文；有 TLS/REALITY 时链路加密，中间人看不到明文内容。
- `x-bray-*`（`session-secret` / `session-uuid` / `mode-degrade` / `multi-endpoint` / sticky TTL 等）：**本地控制头，永不上线**（`GetRequestHeader` 剥离所有 `x-bray-*`），默认无需手配。
- **和 UUID 的关系**：VLESS UUID 仍是代理层账号身份；Session MAC 是 XHTTP 传输层对 `sessionId` 的签名。Bray 默认从**同一 VLESS UUID 派生** MAC 密钥，因此**不必**再手工配 `x-bray-session-secret`；仅非 VLESS 或需对多入站强制统一密钥时才写显式 secret。
- 中间人看不到 session secret：secret 与 `x-bray-*` 只在本机配置与进程内使用；线上只有已签名的 session 路径分量，且外层通常还有 TLS/REALITY。

---

## 兼容性（Bray-only）

| 场景 | 预期 |
|------|------|
| **Bray 客户端 → Bray 服务端** | **支持（唯一正式保证）** |
| 上游 Xray 客户端 → Bray 服务端 | **不保证**（session MAC、padding/指纹、XMUX 等可能拒绝或行为不一致） |
| Bray 客户端 → 上游 Xray 服务端 | **不保证** |
| 配置 JSON 外形 | 仍尽量沿用 Xray 字段，便于替换二进制；**语义以 Bray 为准** |
| 平台 | Linux / Windows / macOS / Android 等 Go 支持的目标 |

若你仍持有仅上游内核的一端，请升级到 Bray-Core，或使用历史分支 `v1` 自行评估（`v1` 也不再接收大型行为变更）。

---

## 性能基准

性能对比快照与历史见 [`bench_results/COMPARISON_REPORT.md`](bench_results/COMPARISON_REPORT.md)（历史 [history/](bench_results/history/README.md)）；每次 push/PR 的 CI 生成 `Benchmark Tracking` artifact（`report.md` / `summary.svg`，含 **Advantage Highlights**）。

读表口径：**Regression** = 有没有明显变慢（CI Summary 计数）；**Advantage** = Bray 数据面强在哪（XHTTP/XMUX 热路径）。**Advantage ≠ Upstream 对打**；无法同配置复现的旧数字直接忽略，只信同名 Benchmark + 同 count + median。

---

## 文档与仓库

| 文档 | 内容 |
|------|------|
| [CHANGELOG.md](CHANGELOG.md) | 版本与变更记录 |
| [docs/README.md](docs/README.md) | 文档索引 |
| [docs/presets/README.md](docs/presets/README.md) | 传输预设与 `x-bray-*` 本地控制头 |
| [docs/architecture-connection-lifecycle.md](docs/architecture-connection-lifecycle.md) | XMUX / 连接生命周期架构 |
| [docs/server-tuning.md](docs/server-tuning.md) | Linux 服务端 TCP 调优（可选） |
| [bench_results/COMPARISON_REPORT.md](bench_results/COMPARISON_REPORT.md) | 性能对比快照 |
| [SECURITY.md](SECURITY.md) | 安全策略 |
| [docs/archive/README.md](docs/archive/README.md) | 历史 / 一次性文档归档 |
| [REALITY](https://github.com/Maolaohei/REALITY) | 独立 REALITY 实现与发布 |

问题反馈请使用本仓库 GitHub Issues。提交时请尽量附带：客户端/内核版本、传输组合（如 VLESS+XHTTP+REALITY）、XHTTP mode、是否可稳定复现、以及证书 CN/SAN 或服务端日志片段。

---

## 友情链接

- [Linux DO](https://linux.do/)

## 致谢与许可

Bray-Core 建立在 [XTLS/Xray-core](https://github.com/XTLS/Xray-core) 及社区生态之上，并集成 [Maolaohei/REALITY](https://github.com/Maolaohei/REALITY)。

许可协议与上游保持一致，详见仓库内 LICENSE。
