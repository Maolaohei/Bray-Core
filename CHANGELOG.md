# Bray-Core Changelog

基于 [Xray-core](https://github.com/XTLS/Xray-core) 的兼容增强分支变更记录。  
基线版本以 `core` 包中的版本号为准（当前 **26.6.22**）。REALITY 独立版本见 [Maolaohei/REALITY Releases](https://github.com/Maolaohei/REALITY/releases)。

格式大致遵循 [Keep a Changelog](https://keepachangelog.com/) 语义：新增 / 变更 / 修复 / 文档。

---

## [Unreleased]

### 文档

- 重写项目 `README.md`：对齐当前基线、REALITY v0.5.5 / L2 默认摊销、客户端替换方式与构建说明；移除过时的 “REALITY v3” 版本矩阵与营销式路线图表述。
- 本文件补充 2026-07 可靠性相关变更摘要。

---

## 2026-07 — 可靠性与 REALITY L2

对应主干近期提交（摘要）：`713ec667`、`a4b5b619`、`6ef8344f`、`e3abf54f`、`d246d527` 及同线修复。

### 修复

#### HTTPS 证书串站（VLESS + XHTTP + REALITY 路径）

- **域名身份保留**：DNS / Happy Eyeballs 将目标解析为 IP 后继续携带 `OriginalDomain`；VLESS 编码侧保留原始主机名，避免 CDN 多域名共用 IP 时服务端拨号/SNI 身份丢失。
- **XMUX 池隔离**：`MuxKey.destIdentity` 纳入 `OriginalDomain` 等字段，禁止「同 IP、不同域名」共用连接池。
- **HTTP/2 缓存身份**：缓存连接按 TLS / 主机身份隔离，降低串站复用概率。
- **MultiBuffer 生命周期**：写入移交所有权后不再错误归还 buffer 池，避免读侧消费前被复用。
- **DNS 缓存记录**：移除不安全的 `*record` 对象池复用；查询结果返回快照，消除缓存 UAF 导致的随机错误解析/后续 TLS 异常。

说明：系统或浏览器侧 HTTPS 中间人（例如部分广告拦截的 HTTPS 解密）会呈现独立 CA 证书，属于链路外侧因素，与上述内核修复相互独立。

#### XHTTP

- **H1 上传连接池**：成功写入后正确累计未读响应；复用前排空响应；失败连接不回池。
- **有界 idle 池**：限制 H1 upload 空闲连接数量，避免 `sync.Pool` 无限堆积。
- **连接关闭策略**：仅在致命连接错误时标记整连接关闭；取消/超时等非致命错误只失败当前流，避免 XMUX 误弃健康连接。
- **PostPacket**：与 stream 路径对齐致命错误判定，避免单次 POST 失败即杀死整连接。

#### DNS

- 查询被取消时的噪声日志与竞态路径处理。
- 双栈查询等待路径改进，降低单侧慢查询拖死整次解析的概率。
- 缓存对象生命周期与 serve 路径加固（见上「DNS 缓存记录」）。

### 变更

#### REALITY 子模块

- 指向 [Maolaohei/REALITY](https://github.com/Maolaohei/REALITY) **v0.5.5**（提交 `cfb09a0` 一线）。
- **摊销模式**：L0 全量实拨；L1 可复用 R1–R6；**L2 为默认**——在连续匹配观测达到证据门槛后可 zero-dial；证据不足或失败窗口触发时回退。
- **L2 安全约束**：不对不可安全复用的 R0 做 L2 缓存复用，避免客户端与服务端握手状态不一致。

### 依赖

- 常规依赖升级（含 `golang.org/x/net` 等），详见对应 chore 提交。

---

## 2026-06 — 基线与传输增强（历史摘要）

> 以下为 2026-06 前后相对上游的能力与修复摘要，供对照；细节以当时提交为准。

### 基线

| 项目 | 数值 |
|------|------|
| 上游基线 | Xray-core v26.6.22 |
| 协议兼容 | 与上游配置/协议面兼容 |

### 传输与调度

| 方向 | 说明 |
|------|------|
| XMUX | 多路复用连接池、min-inflight / RTT 与质量评分调度、优雅排空与健康检查 |
| Happy Eyeballs v3 | 历史失败率与质量感知的并行拨号与选路 |
| Warmup | 连接/DNS 预热与健康检查管道 |
| TCP 默认策略 | 支持平台上更积极的 socket 默认（如 BBR、NODELAY 等，依 OS） |
| Transport Intelligence | TCP_INFO / 估算 RTT → 质量评分 → 行为分类 → 调度与池规模（AIMD） |

### REALITY（子模块演进，至 2026-06）

| 方向 | 说明 |
|------|------|
| 指纹与缓存 | TLS 1.3 目标观测、profile 持久化、HotSwap、SWR、负缓存、引用计数保护 |
| 证书链 | 更大证书链容量以兼容更多目标站点 |
| 后续（2026-07） | 见上文 L2 摊销 / zero-dial |

### 安全与协议扩展（摘要）

| 方向 | 说明 |
|------|------|
| 随机数与 padding | 密码学安全随机池、相关 padding 热路径优化 |
| NetBridge | 配套客户端入站桥接 |
| 反代头 | XHTTP / WS / HTTPUpgrade / gRPC 等路径对 `trustedXForwardedFor` 的校验策略 |
| 其他 | WireGuard 可维护性重构、Loopback sniffing、若干关闭/泄漏类修复 |

### 已知审计项

历史缺陷审计快照见 [`DEFECT_REPORT.md`](DEFECT_REPORT.md)（**2026-06-27**）。该文件为当时审计记录，不保证与当前主干一一对应；修复状态以提交历史与测试为准。

---

## 性能数据说明

历史 README / 文档中曾收录局部 micro-benchmark（buffer、XMUX 调度、VLESS decode 等）。这些数字高度依赖 CPU、Go 版本与编译参数，**不应视为跨版本 SLA**。若需对比，请在同一机器上对目标提交重新执行：

```bash
go test -bench=. -benchmem ./transport/internet/splithttp/ ./proxy/vless/encoding/ ./common/buf/
```

---

## 链接

- 仓库：<https://github.com/Maolaohei/Bray-Core>
- REALITY：<https://github.com/Maolaohei/REALITY>
- 上游：<https://github.com/XTLS/Xray-core>
- 架构文档：[`docs/architecture-connection-lifecycle.md`](docs/architecture-connection-lifecycle.md)