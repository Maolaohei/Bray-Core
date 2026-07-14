# Bray-Core

Bray-Core 是基于 [Xray-core](https://github.com/XTLS/Xray-core) 的兼容增强分支，面向复杂网络环境下的传输层稳定性、连接调度与 REALITY 握手开销优化。协议与配置面保持与上游一致，可直接替换客户端/服务端内核二进制使用。

| 项目 | 当前状态 |
|------|----------|
| 基线版本 | Xray-core **26.6.22** |
| 语言版本 | Go **1.26.4** |
| 模块路径 | `github.com/xtls/xray-core` |
| REALITY | [Maolaohei/REALITY](https://github.com/Maolaohei/REALITY) **v0.5.5**（子模块 `./REALITY`） |
| 默认 REALITY 摊销 | **L2**（证据充分后 zero-dial；不满足条件时回退 L1/L0） |

---

## 定位

- **兼容优先**：VLESS / XHTTP / REALITY 等与上游配置互通；客户端与服务端可混用（增强能力在本端生效）。
- **传输智能**：在 TCP 观测、XMUX 连接池、Happy Eyeballs、DNS 缓存等路径上做调度与容错，而不是改写应用层协议语义。
- **REALITY 摊销**：服务端在安全约束下复用目标指纹观测结果，降低对伪装站点（RA）的重复拨号与握手成本。

推荐组合：**VLESS + XHTTP（stream-one / stream-up / packet-up）+ REALITY**。

---


## 分支策略

| 分支 | 说明 |
|------|------|
| **`main`** | **当前默认主干** = Bray 完全体（原功能分支 `Bray-V2`，Wave 1–7）。克隆 / CI / 发版均以此为准。 |
| **`v1`** | 升级前的旧主干快照（原 `main`），仅用于回滚与对比。 |

```bash
git clone https://github.com/Maolaohei/Bray-Core.git
cd Bray-Core          # 默认已在 main（完全体）
git checkout v1       # 仅当需要旧线行为时
```

详细能力见 [`docs/bray-v2-full.md`](docs/bray-v2-full.md) 与 [`docs/presets/README.md`](docs/presets/README.md)。

## Bray 完全体（main）相对 v1 的要点

在保持 VLESS / XHTTP / REALITY **协议与配置兼容** 的前提下，`main` 额外强化：

| 能力 | 说明 |
|------|------|
| XMUX 浏览器默认 | 未配置 `xmux` 时使用 8–16 并发、2–4 连接、有界复用寿命（可显式写 0 恢复无限） |
| Mode cascade | auto 等路径下 stream-one → stream-up → packet-up；失败可自愈 |
| Sticky | 记住 last-good mode / multi-endpoint 赢家（TTL，可 opt-out） |
| Multi-endpoint | 可选多落地竞速（`x-bray-multi-endpoint` + endpoints） |
| 坏会话驱逐 | fatal open 时 MarkDead；cascade 时刷新 HTTP/XMUX client |
| 可观测 | `bray-v2>>>` 指标与比率；控制头 `x-bray-*` 仅本地、不上线 |

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
| L2 安全边界 | 不缓存不可安全复用的 R0；证据不足、失败窗口触发时自动降级，避免客户端/服务端状态错位 |

相关发布说明：[REALITY v0.5.5](https://github.com/Maolaohei/REALITY/releases/tag/v0.5.5)

### XHTTP / XMUX

| 能力 | 说明 |
|------|------|
| 连接池调度 | Min-inflight、RTT / 质量评分、行为感知惩罚、AIMD 池规模 |
| 生命周期 | Active → Draining → Closed；致命错误快速驱逐，非致命失败不误杀整池 |
| 身份隔离 | `MuxKey` 含 `OriginalDomain` 等目的地身份，避免同 IP 多域名串池 |
| H1 上传池 | 有界 idle 池；写后跟踪未读响应并在复用前排空，失败连接不回池 |
| HTTP/2 身份 | 缓存连接按 TLS/主机身份隔离，降低串站风险 |

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

## 客户端与替换内核

| 平台 | 说明 |
|------|------|
| Windows | [v2rayN（配套修改版）](https://github.com/Maolaohei/v2rayN) — 将编译产物替换到客户端 `bin/xray`（或对应内核目录） |
| Android | [v2rayNG（Bray-Core 内核发布）](https://github.com/Maolaohei/v2rayNG/releases) |
| 通用 | 任意兼容 Xray 配置的客户端，均可替换为 Bray-Core 二进制 |

服务端与客户端均可独立升级；仅当使用本仓库 REALITY 摊销等增强时，对应端需使用本内核及配套 REALITY 子模块版本。

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

```bash
sysctl -w net.core.default_qdisc=fq
sysctl -w net.ipv4.tcp_congestion_control=bbr
sysctl -w net.ipv4.tcp_fastopen=3
sysctl -w net.core.rmem_max=16777216
sysctl -w net.core.wmem_max=16777216
```

---

## 兼容性

| 场景 | 预期 |
|------|------|
| 上游 Xray 客户端 → Bray 服务端 | 协议兼容 |
| Bray 客户端 → 上游 Xray 服务端 | 协议兼容 |
| 配置 JSON / 分享链接 | 与上游字段兼容；未识别字段按实现忽略或报错策略与上游一致 |
| 平台 | Linux / Windows / macOS / Android 等 Go 支持的目标 |

说明：传输层增强（XMUX 策略、REALITY L2 等）只影响本端行为；对端为上游实现时，连接仍按标准 REALITY / XHTTP 语义工作，但不会获得本端独有摊销或调度收益。

---

## 近期可靠性相关变更（摘要）

下列项已合入主干，详细条目见 [CHANGELOG.md](CHANGELOG.md)：

1. **HTTPS 证书串站（内核路径）**  
   VLESS 保留 `OriginalDomain`；XMUX / H2 缓存按目的地身份隔离；MultiBuffer 所有权与 DNS 缓存记录生命周期修复，避免随机出现错误对端证书。
2. **XHTTP H1 上传连接池**  
   响应排空、致命错误判定、有界池，降低半死连接与上传抖动。
3. **DNS**  
   取消上下文噪声与缓存 UAF 类问题处理；双栈查询路径改进。
4. **REALITY L2**  
   子模块升级至 v0.5.5 摊销实现；默认 L2，证据与失败窗口约束下的 zero-dial。

本地 HTTPS 代理若叠加系统/浏览器 MITM（例如部分 DNS/广告拦截的 HTTPS 解密），会表现为独立 CA 签发的证书，属于链路外侧因素，与上述内核串站修复无关。

---

## 文档与仓库

| 文档 | 内容 |
|------|------|
| [CHANGELOG.md](CHANGELOG.md) | 版本与变更记录 |
| [docs/architecture-connection-lifecycle.md](docs/architecture-connection-lifecycle.md) | XMUX / 连接生命周期架构 |
| [SECURITY.md](SECURITY.md) | 安全策略 |
| [REALITY](https://github.com/Maolaohei/REALITY) | 独立 REALITY 实现与发布 |

问题反馈与讨论请使用本仓库 GitHub Issues。提交缺陷时请尽量附带：客户端/内核版本、传输组合（如 VLESS+XHTTP+REALITY）、XHTTP mode、是否可稳定复现、以及证书 CN/SAN 或服务端日志片段。

---

## 友情链接

- [Linux DO](https://linux.do/)

## 致谢与许可

Bray-Core 建立在 [XTLS/Xray-core](https://github.com/XTLS/Xray-core) 及社区生态之上，并集成 [Maolaohei/REALITY](https://github.com/Maolaohei/REALITY)。

许可协议与上游保持一致，详见仓库内 LICENSE。