# Bray-Core

Bray-Core 是基于 [Xray-core](https://github.com/XTLS/Xray-core) 的 **Bray 专属魔改内核**：以 **VLESS + XHTTP + XMUX + REALITY** 为核心栈，面向复杂网络下的稳定性、调度与性能。  
**当前 `main` 默认只保证 Bray 客户端 ↔ Bray 服务端互通**；为性能/安全做的协议侧加固已与上游 Xray 行为分叉，**不再承诺与上游内核互访兼容**。

| 项目 | 当前状态 |
|------|----------|
| 基线版本 | Xray-core **26.8.2** |
| 语言版本 | Go **1.26.4** |
| 模块路径 | `github.com/xtls/xray-core` |
| REALITY | [Maolaohei/REALITY](https://github.com/Maolaohei/REALITY) **v0.5.5**（子模块 `./REALITY`） |
| 默认 REALITY 摊销 | **L2**（证据充分后 zero-dial；不满足条件时回退 L1/L0） |
| 兼容策略 | **Bray-only**（两端均须本仓库内核 / 配套客户端） |

---

## 定位

- **Bray-only**：session MAC、packet-up 窗口/分片、padding/指纹加固、OpenStream 超时驱逐等按 Bray 两端语义设计；不要假设能挂原版 Xray 对端。
- **传输智能**：TCP 观测、XMUX 连接池、Happy Eyeballs、DNS 缓存、mode cascade / sticky / multi-endpoint。
- **REALITY 摊销**：服务端在安全约束下复用目标指纹观测，降低对伪装站点（RA）的重复拨号成本。
- **数据面性能**：packet-up RTT 自适应窗口与 chunk、零分配 seq、共享 header、deadline 路径 `bytespool`、大包 padding 收缩等（见下方「近期性能」）。

推荐组合：**VLESS + XHTTP（stream-one / stream-up / packet-up）+ REALITY**。

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

详细能力见 [`docs/bray-v2-full.md`](docs/bray-v2-full.md) 与 [`docs/presets/README.md`](docs/presets/README.md)。

## Bray 完全体（main）要点

| 能力 | 说明 |
|------|------|
| XMUX 浏览器默认 | 未配置 `xmux` 时使用 8–16 并发、2–4 连接、有界复用寿命（可显式写 0 恢复无限） |
| Mode cascade | auto 等路径下 stream-one → stream-up → packet-up；失败可自愈 |
| Sticky | 记住 last-good mode / multi-endpoint 赢家（TTL，可 opt-out） |
| Multi-endpoint | 可选多落地竞速（`x-bray-multi-endpoint` + endpoints） |
| 坏会话驱逐 | fatal open 时 MarkDead；OpenStream 等 header 超时累计后驱逐黑洞 H2；cascade 时刷新 HTTP/XMUX client |
| Session MAC | XHTTP session id 带 HMAC；密钥优先由 **VLESS UUID 派生**，无需额外配置 |
| 可观测 | `bray-v2>>>` 指标与比率；控制头 `x-bray-*` **仅本地配置注入，永不出现在线上 HTTP** |

---

## 主要能力

### REALITY（v0.5.5）

| 能力 | 说明 |
|------|------|
| TLS 1.3 指纹伪装 | 基于目标站点实时/缓存观测构造与目标一致的握手记录形态 |
| Profile 缓存 | 内存缓存 + 持久化（`profiles.json` 原子写入），重启后可快速恢复 |
| HotSwap / SWR | CipherSuite 或证书变更时热切换；过期 profile 可边用边刷 |
| 负缓存退避 | 探测失败指数退避，避免对目标站点无效重试 |
| 证书链容量 | 支持更大证书链，兼容更多目标站点 |
| 摊销模式 L0/L1/L2 | L0 全量实拨；L1 可复用 R1–R6；**L2（默认）**在证据门槛满足后 zero-dial |
| L2 安全边界 | 不缓存不可安全复用的 R0；证据不足、失败窗口触发时自动降级 |

相关发布说明：[REALITY v0.5.5](https://github.com/Maolaohei/REALITY/releases/tag/v0.5.5)

### XHTTP / XMUX（Bray-only 数据面）

| 能力 | 说明 |
|------|------|
| 连接池调度 | Min-inflight、RTT / 质量评分、行为感知惩罚、AIMD 池规模 |
| 生命周期 | Active → Draining → Closed；致命错误快速驱逐，非致命失败不误杀整池 |
| 身份隔离 | `MuxKey` 含 `OriginalDomain` 等目的地身份，避免同 IP 多域名串池 |
| H1 上传池 | 有界 idle 池；写后跟踪未读响应并在复用前排空，失败连接不回池 |
| HTTP/2 身份 | 缓存连接按 TLS/主机身份隔离，降低串站风险 |
| Session MAC | `sessionId = raw + "." + base64url(HMAC-SHA256(secret, raw)[:8])`；服务端拒收未签名/错误 MAC，防 hub 被未认证冲垮 |
| Packet-up 窗口 | 默认 in-flight **12**，RTT 放大上限 **24**，且不超过 `scMaxBufferedPosts/2` |
| Packet-up chunk | 按 RTT 选择 POST body 上限（约 256KB / 512KB / 满配置）；`rtt==0` 冷启动不砍；**永不超** `scMaxEachPostBytes` |
| Packet-up pacing | 默认 `scMinPostsIntervalMs=30` 仅用于 **idle/tiny** 伪装；**≥8KiB bulk** 或 **recentFlow(<50ms)** 跳过间隔，避免 32KiB 写被钉在 30ms/POST |
| 热路径减配 | `formatSeqInt64` 无堆分配、共享单值 header 切片、大包 X-Padding 收缩、splitConn deadline 缓冲走 `bytespool` |
| OpenStream 防挂死 | 等响应头超时可累计并 MarkDead，避免黑洞 H2 占池导致内核「无响应」 |

### 出站拨号与 DNS

| 能力 | 说明 |
|------|------|
| 域名保留 | DNS / Happy Eyeballs 将地址替换为 IP 时保留 `OriginalDomain`，供 SNI 与 VLESS 目标使用 |
| Happy Eyeballs v3 | 基于历史失败率与质量评分的并行拨号与选路 |
| DNS 缓存 | 正常命中、过期策略与并发查询路径加固；避免缓存对象错误复用 |
| TCP 默认策略 | 在支持平台上默认更积极的 socket 参数（如 BBR、NODELAY 等，依 OS 能力） |

### 其他

- **NetBridge**：面向配套客户端的入站桥接（需修改版 v2rayN 等配合）。
- **架构说明**：连接池与生命周期细节见 [`docs/architecture-connection-lifecycle.md`](docs/architecture-connection-lifecycle.md)。

---

## Headers / `x-bray-*` 说明（常见疑问）

配置里的 `headers` / `x-bray-*` **不是**「再造一套用户口令」，也 **不会** 原样出现在公网 HTTP 上。

| 名称 | 作用 | 是否上线（明文可见） | 是否要手配 |
|------|------|----------------------|------------|
| `headers` 普通项 | 伪装用 HTTP 请求头（如 User-Agent） | 会进入 TLS 内的 HTTP 明文；有 TLS/REALITY 时链路加密，中间人看不到明文内容 | 按伪装需要 |
| `x-bray-session-secret` | 本地控制：session MAC 密钥材料（显式覆盖） | **不上线**（`GetRequestHeader` 剥离所有 `x-bray-*`） | **通常不需要** |
| `x-bray-session-uuid` | 本地控制：用 VLESS UUID 派生 MAC 密钥（多用户可逗号分隔） | **不上线** | 走 VLESS 时由配置链路注入即可 |
| `x-bray-mode-degrade` / `x-bray-multi-endpoint` / sticky TTL 等 | 本地控制：级联、多落地、粘滞策略 | **不上线** | 高级场景 opt-in |
| 默认回退 `bray-default-session-key` | 无 UUID、无显式 secret 时的零配置种子 | 不上线；仅用于本机派生 | 自动注入 |

**和 UUID 的关系**：VLESS UUID 仍是代理层账号身份。Session MAC 是 **XHTTP 传输层** 对 `sessionId` 的签名，防止未认证客户端灌满服务端 session hub。Bray 默认从 **同一 VLESS UUID 派生** MAC 密钥，因此 **不必** 再手工配一份 `x-bray-session-secret`；仅在非 VLESS 或要对多入站强制统一密钥时才写显式 secret。

**中间人能否看到 session secret？**  
不能：secret 与 `x-bray-*` 控制头只在本机配置与进程内使用；线上只有已签名的 session 路径分量等，且外层通常还有 TLS/REALITY。

---

## 客户端与替换内核

| 平台 | 说明 |
|------|------|
| Windows | [v2rayN（原版）](https://github.com/2dust/v2rayN) — 将编译产物替换到客户端 `bin/xray`（或对应内核目录）。**对端服务端须为 Bray-Core**。 |
| Android | [v2rayNG（Bray-Core 内核发布）](https://github.com/Maolaohei/v2rayNG/releases) |
| 通用 | 任意能换 Xray 二进制的客户端；**客户端与服务端都请使用本仓库构建** |

> 历史配套修改版 v2rayN 仓库不再作为默认推荐路径；以替换内核 + Bray 服务端为准。

---

## 构建

要求：Go **1.26.4+**，并初始化子模块。

```bash
git submodule update --init --recursive

# Linux amd64（推荐 GOAMD64=v3）
CGO_ENABLED=0 GOAMD64=v3 go build -o xray -trimpath -ldflags="-s -w -buildid=" ./main

# Windows amd64
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 GOAMD64=v3 go build -o xray.exe -trimpath -ldflags="-s -w -buildid=" ./main
```

`go.mod` 通过 `replace github.com/Maolaohei/REALITY => ./REALITY` 固定本地子模块，请勿在未对齐子模块提交时单独改写该依赖。

### Linux 服务端参考（可选）

以下参数消除 **TCP 慢启动冷启动**（新建连接首段吞吐爬升），对高码率流（4K 视频、大文件下载）首段卡顿改善明显；下行由服务端发送，故调优在服务端生效。

**方式一：当前生效（重启后失效）**

```bash
sysctl -w net.ipv4.tcp_initcwnd=60
sysctl -w net.ipv4.tcp_slow_start_after_idle=0
sysctl -w net.ipv4.tcp_fastopen=3
```

**方式二：永久生效（写入配置文件并加载）**

```bash
cat > /etc/sysctl.d/99-bray.conf <<'EOF'
net.ipv4.tcp_initcwnd=60
net.ipv4.tcp_slow_start_after_idle=0
net.ipv4.tcp_fastopen=3
EOF
sysctl --system
```

说明：`tcp_initcwnd=60` 将初始拥塞窗口提到约 60 段（~85KB，MSS 1460B），首 RTT 即可发送更多数据；`tcp_slow_start_after_idle=0` 空闲后不再重置 cwnd（连接保活期间吞吐不回退）；`tcp_fastopen=3` 启用 TFO 握手减 1 RTT。需 Linux 4.9+（默认内核即可）。

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

## 近期变更摘要

下列项已合入 `main`（细节见 [CHANGELOG.md](CHANGELOG.md) 与提交说明）：

### 可靠性 / 安全（Bray-only）

1. **HTTPS 证书串站**：`OriginalDomain`、MuxKey / H2 身份隔离、MultiBuffer 与 DNS 缓存生命周期。
2. **XHTTP H1 上传池**：响应排空、致命错误判定、有界池。
3. **Session MAC**：UUID 派生或显式 secret；拒绝未签名 session；`x-bray-*` 永不发送。
4. **OpenStream / XMUX 防挂死**：header 等待超时累计 → MarkDead，降低「核无响应需重启」类卡死。
5. **REALITY L2**：默认摊销 + 证据/失败窗口降级。

### 性能（数据面，稳定优先）

1. **packet-up window**：默认 12 / 上限 24，RTT 缩放，半缓冲硬顶。
2. **packet-up chunk**：RTT 档位 256KB / 512KB / 满配置；不超服务器 `scMaxEachPostBytes`。
3. **packet-up pacing**：bulk≥8KiB / recentFlow 跳过 `scMinPostsIntervalMs`（修 30ms/POST 吞吐钉死）。
4. **零分配 seq + 共享 header + 大包 padding 收缩**。
5. **splitConn deadline**：Read/Write 中间缓冲 `bytespool` 复用。

**未强行改动的取舍**：XMUX 选路大重构、REALITY 握手路径再抠微秒、服务端 body 所有权硬进 pool 等高回归风险项保持不动。

#### Benchmark 结论（摘要）

**读表口径（回归轨 vs 优势轨）**

| 轨道 | 回答的问题 | 看哪里 |
|------|------------|--------|
| **Regression** | 有没有明显变慢？ | CI Summary 计数 + common/XMUX/VLESS/buf 全表 |
| **Advantage** | Bray 数据面强在哪？ | CI **Advantage Highlights** + suite `xhttp_core`（TTFB / Burst / Modes / allocs / header·seq）+ XMUX 热路径 |
| **Local product snapshot** | 同机手工产品吞吐 | 下表 + [COMPARISON_REPORT.md](bench_results/COMPARISON_REPORT.md)（P0/optN 时间线） |

> common/buf 与固定 Upstream 对照证明「底层没做烂」；**卖点请看 XHTTP/XMUX**，不要把 AES/ChaCha 或混场景 MB/s 当 Bray 优势。

**快照**：2026-08-01 optN12（i5-13600KF / go1.26.4；XHTTP short alloc 800ms×3、micro 干净串行）· 全文 [bench_results/COMPARISON_REPORT.md](bench_results/COMPARISON_REPORT.md)

| 类别 | 轨道 | 结论 | 说明 |
|------|------|------|------|
| vs 上游 common（buf/crypto/…） | Regression | ⚪/🔴 **混合 / 非主战场** | crypto 持平；`Copy` 波动不代表 XHTTP 产品吞吐 |
| XMUX ConcurrentRW / Pool | Regression + Advantage | ⚪/**🟢** | Get/Pool 以近期 self-baseline 演进；见 CI XMUX + Advantage |
| Happy Eyeballs | Regression | ⚠️ **撤销旧 +46% 叙事** | LargeList 无法复现 06-24 大胜 |
| XHTTP stream-one / stream-up | Advantage (local) | 🟢 **~450 / ~460 MB/s**（optN12 短窗） | 差距收窄至 ~4%，profile 无 XHTTP 热点；产品 headline 仍以 optN3 为准 |
| XHTTP packet-up 单连接 | Advantage (local) | 🟢 **~330 MB/s H2C**（optN12 短窗） | 原 ~2.17 MB/s（30ms 间隔）P0 解锁，约 **100×** |
| XHTTP multi-conn (16) | Advantage (local) | 🟢 **聚合 ~23 GB/s 量级** | conns_1..16 超 P0 峰值 +32~60%，回落已不存在 |
| HE SortIPs / LargeList | Regression + Advantage | 🟢 **0 alloc**（SortIPs ~59ns / LargeList ~348ns） | `sortIPsInto` caller-owned buffer（optN12） |
| XHTTP TTFB / Burst / allocs | Advantage (CI `xhttp_core`) | 📈 **CI 跟踪** | 每次 push 写入 `new_xhttp_core.txt` → report Advantage 区 |
| REALITY micro | Regression | ⚪ **同量级** | AEAD / KeyExchange 稳定 |

- 完整表格与图例：[bench_results/COMPARISON_REPORT.md](bench_results/COMPARISON_REPORT.md) · 历史快照 [bench_results/history/](bench_results/history/README.md)
- 原始输出：`bench_results/run_20260801_optN12/`（optN12）· `run_20260724_p0_pace/`（P0 后）· 修复前对照 `bench_results/run_20260724_quiet/`
- 每次 push/PR 的 CI 表格 + SVG：`Benchmark Tracking` → artifact `bench-report-<sha>`（`report.md` / `summary.svg` / `history/`；含 **Advantage Highlights**）
- 本地格式化：`python scripts/format_bench_report.py --history --suites xhttp_core,xmux,happy,warmup,vless,buf`
- **无法同配置复现的旧数字直接忽略**；只信同名 Benchmark + 同 count + median。
- **Upstream 列** = 固定外部对照快照 [`bench_results/upstream/xray-core-v26.6.22.json`](bench_results/upstream/xray-core-v26.6.22.json)（2026-06-24 同机 Xray-core，偏 common/*）；**Self-baseline** = CI 上次 `main` 缓存；**Advantage** ≠ Upstream 对打。

本地 HTTPS 代理若叠加系统/浏览器 MITM（例如部分 DNS/广告拦截的 HTTPS 解密），会表现为独立 CA 签发的证书，属于链路外侧因素，与上述内核串站修复无关。

---

## 文档与仓库

| 文档 | 内容 |
|------|------|
| [CHANGELOG.md](CHANGELOG.md) | 版本与变更记录 |
| [bench_results/COMPARISON_REPORT.md](bench_results/COMPARISON_REPORT.md) | 性能对比快照（表格 + 图例） |
| [scripts/format_bench_report.py](scripts/format_bench_report.py) | CI/本地 bench → Markdown/SVG/history |
| [docs/README.md](docs/README.md) | 文档索引 |
| [docs/bray-v2-full.md](docs/bray-v2-full.md) | Bray 完全体总览 |
| [docs/presets/README.md](docs/presets/README.md) | 传输预设与控制头 |
| [docs/architecture-connection-lifecycle.md](docs/architecture-connection-lifecycle.md) | XMUX / 连接生命周期架构 |
| [SECURITY.md](SECURITY.md) | 安全策略 |
| [REALITY](https://github.com/Maolaohei/REALITY) | 独立 REALITY 实现与发布 |

问题反馈请使用本仓库 GitHub Issues。提交时请尽量附带：客户端/内核版本、传输组合（如 VLESS+XHTTP+REALITY）、XHTTP mode、是否可稳定复现、以及证书 CN/SAN 或服务端日志片段。

---

## 友情链接

- [Linux DO](https://linux.do/)

## 致谢与许可

Bray-Core 建立在 [XTLS/Xray-core](https://github.com/XTLS/Xray-core) 及社区生态之上，并集成 [Maolaohei/REALITY](https://github.com/Maolaohei/REALITY)。

许可协议与上游保持一致，详见仓库内 LICENSE。
