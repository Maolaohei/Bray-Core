# Bray-Core Changelog

基于 [Xray-core](https://github.com/XTLS/Xray-core) 的 **Bray 专属魔改**变更记录。  
基线版本以 `core` 包中的版本号为准（当前 **26.8.3**）。REALITY 独立版本见 [Maolaohei/REALITY Releases](https://github.com/Maolaohei/REALITY/releases)。

**兼容策略（2026-07 起）**：`main` 默认 **Bray 客户端 ↔ Bray 服务端**；不再承诺与上游 Xray 内核互访兼容。

格式大致遵循 [Keep a Changelog](https://keepachangelog.com/) 语义：新增 / 变更 / 修复 / 文档。

---

## [2026.09.01] — 2026-09-01（2026-09-02 重发：补入 dseg 交付延迟与 EOF 竞争修复）

对应提交：`5b0d9b33` 至 `581d67b0`（VLESS/Vision、DNS、XMUX、REALITY 四专项修复 + 8.31 断流风暴止血 + 日志膨胀修复 + dseg 性能修复）。

### ⚠️ 升级注意（8.31 断流报告的定性）

- 社区报告"2026.08.31 XMUX 探针与 XHTTP uTLS 未继承 TLS 配置导致 x509 断流"经双端代码取证与 E2E 复现定性为：**探针与业务流量共用同一 Transport/DialTLSContext/TLS 配置，不存在配置丢失**；`XHTTP dial: uTLS failed` 日志仅出现在标准 TLS 拨号路径（REALITY 路径失败会打出 `REALITY failed` 且提前返回），提示客户端 outbound `security`/`tlsSettings` 与服务端 REALITY 不匹配。该配置不匹配触发的是下述探针风暴放大器，本版本已消除放大器；如仍见 x509 请核对双端 streamSettings。

### 修复（XHTTP 断流风暴止血 — `0763c08f`）

- **XMUX 探针冷却全类别生效**：探针失败分类此前只对 dial-dead 类错误（connection refused 等）推进冷却计数，x509/超时等非 dial-dead 失败走无冷却分支直接逐出连接，形成 `connect → probe 失败 → MarkDead → 池空 → 重连` 紧循环，池中永无可用连接 → `all retry attempts failed` 大面积断网。现所有类别探针失败均推进失败计数（连续 3 次 → 2s 冷却），冷却期内探针短路。
- **上传续借限流**：packet-up 上传客户端续借重试增加 1s backoff，持久失败不再每 32KiB chunk 一次完整 TLS 握手 + 一条日志。
- POC 回归护栏：x509 注入驱动真实 probeConnection（修复前 6 轮全烧连接、冷却永不激活；修复后第 3 轮起冷却生效）。

### 修复（日志膨胀 — `1735425d`）

- **风暴类诊断日志降级 Debug**：`loglevel: info` 时 x509/探针风暴会以 ~12 行/请求的速率把 Bray 专属诊断（Fast Eviction、probe removing、packet-up retry/rescue、H3 回退、cascade）写满错误日志（实测 15 请求 184 行）。全部降级 Debug 后 info 模式仅剩上游标准每请求 3 行（dispatcher/socks/vless），warning 模式仅启动 1 行，debug 模式诊断能力无损。级别过滤机制本身经 8.27/8.31 原版二进制对照 E2E 证实无回归。
- **AST 白名单护栏**：splithttp 包内新增 `errors.LogInfo*` 调用若不在 8 条低频白名单（listening/CDN/网络变更/N 连续逐出）内即测试失败，防止风暴日志再被提回 Info。

### 修复（VLESS / Vision 专项）

- B1：`XtlsPadding` 负 paddingLen 触发 `Extend` panic。
- B2：`VisionReader` 在 `input/rawInput` 为 nil 时 `ReadFrom` 空指针 panic。
- B4：预连接通道提前创建消除竞态与泄漏；B8：无效目标 nil 指针 panic。

### 修复（DNS 专项）

- D1 nameserver 竞态、D2 双栈合并丢 IP、D4 漏 return（均 POC 复现）。

### 修复（XMUX 专项）

- M1：`xudpEstablish` 锁外读写 `x.Mux/x.Status` 数据竞争。
- M2：空闲关闭不复用泄漏（POC 已证 FALSE POSITIVE，护栏留档）。

### 修复（REALITY 专项）

- R1：ML-KEM/REALITY 握手无读超时导致 Slowloris DoS。
- R2：共享 keylog `*os.File` 并发写竞争。

### 性能（XHTTP 下行分段 dseg — `3e85f696`）

- **交付延迟 20ms → 3ms（吞吐 9.2×）**：dseg 开启时（H2/H3 + packet-up/stream-up），下行分段对客户端的交付以固定 ~20ms 轮询节奏到达（读相 100% >3ms、82% 落在 10-21ms 频带，写入相零阻塞——纯交付侧病理）。修复：段就绪事件广播（close-and-replace channel）取代 20ms `Sleep` 轮询 + frontier 1ms 短提交窗 + 快路径 404 收紧到"段从未开始接收"。基准（count=3）：dseg_on 20.3→2.2 ms/op（3.2→29.6 MB/s）；H2_Throughput 20.3→2.2 ms/op（29.2 MB/s）；dseg_off / H2C / stream 模式无回归。纯服务端修复，双端无需同步升级。
- POC：`downseg_delivery_latency_poc_test.go`（驱动真实 `handleDownSegment`，修复前 20.5ms → 修复后 3.0ms，5/5 稳定；负向对照回退轮询即 RED）。

### 修复（dseg EOF 竞争 — `581d67b0`）

- **静默截断**：拉取方 `get(seq)` 未命中瞬间生产者 `finalize` 提交末段（dump 铁证：段仍在缓存 `segs=[4]`、EOF 标记已服务、客户端停拉 → 丢 262144 字节恰一段）。修复：EOF 判定改 `eofForSeq()` 单锁原子决策（final && 段不在 segs && 不在 repull 窗 && seq≥produced 才发 EOF），否则继续取段。ubuntu -race 抓到、count=30 全绿闭环。

---

## [2026.08.30] — 2026-08-30

对应提交：`6bd9e448` 至 `d2bcd2a85`（上游同步 7 提交 + XHTTP dseg 静默截断修复 + POC）。

### ⚠️ 升级注意（安全校验生效）

- `65458e91`（Config: Fix some issues）让原本**完全失效**的"禁止公网明文 VLESS/Trojan"校验**真正生效**（fork 此前读取了恒为 nil 的顶层 `Address` 字段）。
  **升级后，任何指向公网地址且未启用 TLS 的 VLESS/Trojan 出站将无法启动。**

### 安全

- 出站传输安全校验地址源改为 `Vnext[0].Address` / `Servers[0].Address`，公网明文 VLESS/Trojan 出站被拒绝（域名公私按子域判定：`example.com` 为**公网**、`host.example` 为私有）。

### 上游同步（XTLS/Xray-core，7 个提交）

- `c1958dba` FreeBSD TUN `autoSystemRoutingTable` / `autoOutboundsInterface`。
- `540b9070` UDP 出站 socket 按目标地址族绑定（修复 IPv6 目标失败）。
- `dffc7ada` XHTTP packet-up `Request.GetBody()`，h2 GOAWAY 后可重放（已适配 fork 池化实现：非热路径急切拷贝 replay 快照、热路径零拷贝 durable body）。
- `aa3d6589` macOS TUN `Wait()` 改用 kqueue 阻塞，消除忙等占满一核。
- `c7e569b0` WireGuard 出站 `remoteDNS` 并尊重 TTL。
- `ada99a4e` Hysteria 升级至官方 v2.12.2（配置层手工平移回未拆分的 `transport_internet.go`；`apernet/quic-go` 顶到 `v0.61.1-0.20260806010916-184d081eef3e`）。

### 修复（XHTTP 下行分段 dseg）

- **慢速消费者静默截断**：dseg 开启时大文件下载静默丢字节（服务端生产腿 finalize 即拆会话 + 客户端 `Read()` 先判 fatal）。修复：
  - 服务端 `holdDrainLeg()` 等到 `drained()=final && eofServed && len(segs)==0`（上限 2min，`idleFor()` 判定客户端存活）；
  - 客户端 `Read()` 改为 `buf → skip → eofAt → fatal → prodErr`，`failProductionLeg` 延后 + 30s 停摆检测取代快速失败。
- POC（负向对照已做）：`downseg_drain_poc_test.go`（5 例）+ `testing/scenarios/dseg_slow_reader_poc_test.go`（64 MiB 端到端门）。2×2 验证：新/新 PASS@67.1M，移除任一修复即 FAIL。

### 文档

- `docs/xhttp-dseg-truncation-fix-2026-08-30.md`：根因、探针实测、修复、2×2 分量验证、性能影响。
- `docs/upstream-sync-review-2026-08-30.md` / `upstream-merge-2026-08-30.md`：上游同步审查与合并报告。

---

## [26.8.3] — 2026-08-05

对应提交：`9de077d9` 至 `b7f24639`（XHTTP 热路径性能批次 + Go 工具链升级）。

### 性能（XHTTP packet-up 热路径）

- **每包分配 -15%**（MemoryAllocations 110 → 93 allocs/op）：
  - packet-up body 读取改为 `ResponseController.SetReadDeadline` 同步读（去掉每次 goroutine + channel + context；h3 无读截止线时保留原超时守卫）；
  - session MAC 验证改为 per-listener `sessionMacVerifier`（sync.Pool 键控 HMAC-SHA256 实例复用，常量时间比较语义不变）；
  - MAC tag base64 改为栈上编码（消除 EncodeToString 分配）。
- **h1 并发 POST 批量写**：`H1Conn` 双缓冲零分配队列把同批并发请求合并为单次 writev/WSASend（单请求快速路径不变，零新增延迟，wire 格式与 stock Xray 兼容）；并发吞吐峰值 1494-1554 MB/s（历史最佳）。
- 回测确认：延迟 / 吞吐 / allocs 无回退（Windows 1.26.5）。

### 构建

- Go 工具链升级 1.26.4 → **1.26.5**（CVE-2026-42505 crypto/tls ECH、CVE-2026-39822 os.Root 逃逸、Green Tea GC AVX-512 span 扫描 SIGILL 崩溃修复）。

### 上游同步

- XHTTP & gRPC 服务端获取准确 localAddr（`session.Inbound.Local` 不再是 0.0.0.0:0，routing `local` 规则恢复正确）。
- Routing `process` 排除 iOS（Go `darwin` tag 在 iOS 下亦为真，iOS 构建不再带 macOS 专属实现）。

---

## [26.8.2] — 2026-08-04

对应提交：`bd835bc5` 至 `3f807644`（XHTTP / VLESS / REALITY / DNS 深度修复批次）。

### ⚠️ 升级注意（兼容性破坏）

- **XHTTP 会话认证 fail-closed**：移除了**无密钥时的公开默认密钥** `bray-default-session-key` 自动注入（该常量公开、任何人都能伪造 MAC）。行为变化：
  - **VLESS + XHTTP 用户：无需任何配置**——conf 构建层仍自动从 VLESS 账号 UUID 派生 `x-bray-session-uuid`（会话 MAC 与账号绑定，继承 128 位熵），与之前一致。
  - **非 VLESS / 绕过 conf 层直接构造配置**（如纯 XHTTP、API 直构）：`packet-up` / `stream-up` 需要显式配置高熵 `x-bray-session-secret`（≥32 随机字节），否则只支持 `stream-one`（锁定会话模式会报错 `XHTTP: session wire modes require x-bray-session-secret`）。
  - **旧客户端（默认密钥）连接新服务端时，携带 sessionId 的请求会被拒绝**——这是 fail-closed 的预期行为。

### 安全

- REALITY：握手读超时 10s（消除信号量 DoS 面）；mirror 路径 per-IP 限速（60s/20 次）；short_id / Mldsa65Seed 长度校验（消除配置 panic）；ReplayGuard 16 分片化；Show 日志脱敏（去 SNI/evidence/ClientTime）；客户端证书校验移除 reflect+unsafe；Spider 同 dest 60s 冷却。
- DNS：UDP reqID 每查询 crypto/rand 随机 + 回绕冲突重试；响应 question 域名校验（不匹配/缺失即丢弃，RFC 5452）；TTL=0 不缓存、TTL 上限 86400、负缓存 60s；DoH 响应 64KB 上限；解析记录数上限 32；fakedns 改为 keyed hash（消除时间可预测性）；singleflight leader ctx 解耦；warmup 8 并发限流。
- XHTTP：会话认证 fail-closed + 空 sessionId 严格拒绝；日志去敏（剥离 sessionId/seq）；CF 检测移除可伪造的 `Server` 请求头；packet-up body 30s 读取超时（含 chunked）；stream-one 并发/时长上限；padding 与 payload 解耦 + 样本 8。
- VLESS：Addons 池归还；UUID 规范化冲突拒绝；UDP 大包显式报错；NfsKeys 每 ticket 上限；错误日志去敏；preconnect 用 math/rand/v2。

### 性能

- XHTTP：下行写聚合 Flush（孤立写直发）；H1 上传路径去死参数；高 RTT 窗口 24→12；H3 竞速 drain goroutine 泄漏修复；header 固定顺序（免 map 随机序）。实测（本机）：H2C 吞吐 82.5→360 MB/s、StreamUp 90.7→283 MB/s、TTFB 3.29ms→0.82ms、MemoryAlloc 23.9→112.5 MB/s。

### 清理

- `go vet ./...` 全仓库零告警（context cancel 泄漏、vmess/xray 死代码、unsafe.Pointer misuse 共 6 处）。

---

## [26.8.1] — 2026-08-01

对应提交：`23b7ea94`、`569b92f3`、`dcbbad33`、`f1bbc674`、`66567499`（`main` 相对 `origin/main` 领先）。

### 性能（XHTTP / XMUX / HE）

- **OpenStream URL 指针化**（`23b7ea94`）：`DialerClient.OpenStream` 改传 `*url.URL`，省去每流 `url.String()` + `NewRequestWithContext` Parse 往返（alloc + 重编码）；手构 `http.Request` shell 与 `newPacketRequest` 同模式。
- **错误日志数据竞争修复**（`569b92f3`）：OpenStream 失败日志改用请求本地 URL 拷贝 `u.String()`，消除与 dialer 模式级联重试并发写 `requestURL.Path` 的 string 头撕裂（security_review 发现，MEDIUM）。
- **HE `sortIPsInto` caller-owned buffer**（`dcbbad33`）：SortIPs **98→~59 ns · 1→0 alloc**、LargeList **578→~348 ns · 1→0 alloc**（optN12 实测）；`tcpRaceDialV2` 同步受益；单族输入保留零拷贝快路径。
- **清理**（`f1bbc674`）：删除无调用者的 `sortIPs` 死代码；补 4 个 `sortIPsInto` 三态契约单测（空/单族/混合族别名+0 alloc/v6 优先）。

### 基准（optN12，无回退）

- buf.Copy ~74.7ns·4、XMUX Get ~34.1ns·0 / pool_1..32 ~46.4/~51.0/~60.4/~87.6/~148.4ns·0、HE Score ~532ns·0、H2C packet-up alloc **111**、H2+TLS alloc **198**、Modes 111/18——与 optN11 持平。
- 多连接聚合 conns_1..16（~272 / 3053 / 9270 / 23666 MB/s）超 P0 峰值 +32~60%，历史 quiet2 回落已不存在。
- stream-one vs stream-up 差距收窄至 ~4%（profile 无 XHTTP 热点，http2 双工固有开销，不动）。

### 安全 / 上游跟进（45cf2898 v26.6.27 → 5ca6f4b7 v26.7.28）

- **QUIC sniffer 越界防护**（`66567499`，对应上游 `8f15190c` / GHSA-xqmr-94vq-cxvx / GHSA-9h6x-9r9m-5qw2）：伪造 Initial 包 `4≤packetLen<20` 会触发 `b[hdrLen+4:hdrLen+4+16]` 越界 panic 的 DoS 已修复；额外补 `TestSniffQUICShortInitialPacketNoPanic` 回归测试（上游仅有修复无测试）。
- **Stats GetOrRegister 原子化**（`66567499`，对应上游 `0bafca94`）：`Manager` 接口新增原子 `GetOrRegisterCounter/OnlineMap/Channel`，删除非原子包级 Get-then-Register 辅助函数（并发首用会撞 "already registered"）；16 处调用点迁移（含 Bray-only `bray_stats.go`）；新增 32-goroutine 并发回归测试。
- 上游其余候选（XHTTP maxConnections 6→3、尾斜杠条件化、准确 localAddr、x/net 0.57、ECH 解析健壮化、HTTPUpgrade 容错）经核对已等价存在，无需合并；公网明文出站禁令（破坏性变更）**不跟进**。

---

## [Unreleased]

### 文档

- 重写 `README.md`：明确 **Bray-only**、Session MAC / UUID 派生、`x-bray-*` 本地控制头、packet-up 窗口与 RTT chunk、OpenStream 防挂死与数据面性能摘要；移除「与上游双向协议兼容」表述。
- 本文件补充 Bray-only 安全与性能提交摘要。

### 变更

#### 分支：Bray 完全体并入 `main`（原 main → `v1`）

- **`main`**：现为 Bray 完全体默认主干（历史功能分支 `Bray-V2`，Wave 1–7：XMUX 浏览器默认、mode cascade、sticky、multi-endpoint、指标与 review 修复等）。
- **`v1`**：原 `main` 线冻结为回滚/对比分支（升级前基线）。
- 文档：`docs/bray-v2-full.md`、`docs/presets/`、各 wave 说明中的 Branch 字段已改为指向 `main`。

### 新增 / 安全（Bray-only）

#### XHTTP Session MAC（UUID 派生，零额外配置）

- Session id：`raw + "." + base64url(HMAC-SHA256(secret, raw)[:8])`；服务端拒收未签名或错误 MAC，降低 hub 被未认证灌满风险。
- 密钥解析顺序：显式 `x-bray-session-secret` → `x-bray-session-uuid`（VLESS UUID，可多用户逗号分隔）→ 默认种子 `bray-default-session-key`。
- 所有 `x-bray-*` 控制头仅本地配置注入，`GetRequestHeader` **永不发送到线上**。
- 相关提交：`fbaae44d` / `737c38e0` 一线（rebase 后 SHA 以仓库为准）。

#### OpenStream / XMUX 防挂死

- OpenStream 等响应头超时累计后 MarkDead，驱逐黑洞 H2，减轻「内核无响应需重启」类故障。
- XMUX open 超时与坏会话驱逐策略保留 hard cap（over-admit 等），避免单连接拖垮池。

### 性能（数据面，稳定优先）

#### packet-up / hotpath（2026-07-23）

- **窗口**：默认 in-flight 12，RTT 放大上限 24，硬顶不超过 `scMaxBufferedPosts/2`（`b45291b8` 等）。
- **chunk**：`packetUploadChunkSize` 按 RTT 选择约 256KB / 512KB / 满配置；`rtt==0` 冷启动保持配置上限；永不超 `scMaxEachPostBytes`（`293c0d66`）。
- **减配**：`formatSeqInt64` 零堆分配、共享单值 header 切片、大包 X-Padding 上界收缩。
- **splitConn deadline**：Read/Write 中间缓冲改 `bytespool`，超时语义与 partial read re-park 不变（`293c0d66`）。

### 修复

#### XMUX 连接池 panic 修复（`GetXmuxClient` nil pointer dereference）

- **根因**：`getHTTPClient()` / `GetXmuxClient()` 在 probe 失败或连接创建失败时返回 `(nil, nil)`，`Dial()` 未判空直接调用 `httpClient.OpenStream()` 导致 panic。
- **接口改进**：`GetXmuxClient` 返回 `(*XmuxClient, error)`，`getHTTPClient` 返回 `(DialerClient, *XmuxClient, error)`，错误信息包含 probe 失败原因。
- **快速淘汰**：probe 失败时立即从 pool 移除 broken client（`MarkDead` + `pool.Remove`），避免下次 `GetXmuxClient` 再次拿到并重试同一个坏连接。
- **自动重试**：Phase 2 创建新连接 probe 失败后，移除坏连接并自动重试一次（`maxAttempts=2`），大多数 transient failure 在 XMUX 内部恢复，调用方无感知。
- **probe 超时**：`probeConnection` 从 `context.Background()`（无超时）改为 `context.WithTimeout(ctx, 10s)`，避免服务器半开时 probe 挂死。
- **资源清理**：`Dial()` 中多个 error return 路径统一用 `cleanup()` 函数处理 `reader.Close()` + `conn.Close()`，`conn.Close()` 的 `onClose` 回调负责 Release 已 Borrow 的 XMUX client。
- **性能优化**：`addToPool` 直接返回 `*XmuxClient`，Phase 2 省掉一次 `pool.mu.RLock()` + pool 遍历。

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
