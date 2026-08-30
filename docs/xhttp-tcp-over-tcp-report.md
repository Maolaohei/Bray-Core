# Bray-Core XHTTP TCP-over-TCP 拥塞互扰研究报告

> 研究日期：2026-08-14　｜　研究对象：`D:\UGit\Bray-Core`（Xray-core fork，XHTTP+XMUX）
> 范围：只读研究，未改任何代码。所有行号基于当前 HEAD。

---

## 0. 结论摘要（TL;DR）

1. **Bray-Core 的 XHTTP 是"内层 TCP 字节流 + 外层 TCP(HTTP/2)"的双层可靠传输**。内层 TCP 终止于隧道两侧（本地 inbound / 远端 outbound），内层 ACK/重传只跑在两侧本地段，隧道里只搬字节流（`proxy/vless/outbound/outbound.go:403,443` 的 `buf.Copy`）。因此这不是教科书式的"双重拥塞控制互相退避"（内层收不到隧道丢包信号），而是 **RTT 耦合 + 缓冲耦合**：外层丢包/stall → 隧道延迟尖峰 → 内层 RTO 触发 → 重复字节再次注入隧道（放大）；隧道缓冲（io.Pipe / H2 4MiB 窗口 / 外层 TCP 缓冲）把延迟进一步撑高 → 正反馈。
2. **下行是三种模式共同的单点瓶颈**：stream-one/stream-up/packet-up 的下行都是单一 GET 长连接字节流（`hub.go:686-703`），packet-up 只拆了上行（`dialer.go:1174+`）。下行完全没有并发隔离与有限重试。
3. **XMUX 加剧队头阻塞**：每 XmuxClient = 一条独立外层连接（`mux.go:1238-1256`），内层连接 = 其上的 HTTP/H2 流。HTTP/2-over-TCP 是连接级 HoL——一条外层连接上一次段丢失，会阻塞其上全部 8-16 个内层流（`config.go:123`）。2-4 条连接池（`config.go:124`）是现有的隔离手段，但质量驱动的 AIMD 在 lossy 时**减半**连接数（`mux.go:1090-1093`），方向与吞吐目标相反。
4. **影响阈值**：外层丢包 ≥0.1% 出现可见延迟尖峰；≥1% 吞吐塌陷明显；RTT ≥100ms 放大显著。跨境线路（1-3% 丢包、150-250ms RTT）是重灾区；内层 QUIC/UDP 类应用（游戏、DNS、QUIC 视频）在可靠隧道下失去丢包信号，内层拥塞窗口永不收敛，是 bufferbloat 的主要来源之一。
5. **优化方向排序**（详见 §5）：
   - **低风险**：lossy 时反向 AIMD（升连接数、降每连接并发流数）；packet-up 窗口/重试参数联动微调；外层 socket 缓冲参数。
   - **中风险**：packet-up 下行多段 GET（把下行也拆包）；外层 TCP 换 BBR（Linux）；XMUX 参数配置化。
   - **大工程**：XHTTP/3（外层 QUIC）转正为主通道——代码已基本存在（`hub.go:937-991`、`dialer.go:566-682`、`h3_fallback.go`），目前只做 fallback。
   - "内层禁用重试"在代理架构下**无法字面实现**（内层 TCP 栈属于用户 app），正确近似是：外层保证可靠（已满足）+ 外层平滑化降低内层 RTO 触发率 + 会话级失败粒度收敛。

---

## 1. 现状机制（带文件:行号证据）

### 1.1 完整数据链

```
app ──内层TCP/UDP──▶ 本地inbound ──buf.Copy──▶ VLESS outbound ──▶ XHTTP dialer
                                                            │  (dialer.go)
                          ◀──外层 TCP(REALITY/TLS) + HTTP/1.1/2/3──▶ CDN/服务器
                                                            │
app ◀──内层TCP/UDP── 远端outbound ◀─buf.Copy── VLESS inbound ◀── XHTTP hub (hub.go)
```

- 内层 TCP：`app ↔ 本地 inbound`、`远端 outbound ↔ target` 各是一条**真实 TCP**，终止在隧道两端。隧道里流动的是内层负载字节流（`proxy/vless/outbound/outbound.go:403,443` `buf.Copy(clientReader, serverWriter, ...)`）。
- 内层 UDP：VLESS 在字节流内做 **2 字节长度前缀帧**（`proxy/vless/encoding/addons.go:356-395` `MultiLengthPacketWriter`，数据报 ≤ `buf.Size-2`；对端 `NewLengthPacketReader` 同文件 :397-427）。
- 内层 TCP 的 ACK/重传只发生在两侧本地段：**内层 cwnd 几乎不因隧道丢包回退**（本地段无丢包），但内层 RTT = 本地段 + 隧道往返 + 目标段，隧道延迟直接注入内层 RTT 估计。

### 1.2 XHTTP 三种模式

| 模式 | 上行 | 下行 | 关键代码 |
|---|---|---|---|
| stream-one | 请求 body（单请求双向） | 同请求 response body | client.go:334-527 `OpenStream`；hub.go:630-737 |
| stream-up | POST 上行流（seqStr==""） | 独立 GET response | dialer.go:1075-1091；hub.go:401-464 |
| packet-up | 编号 POST 包（seq 递增） | 独立 GET response | dialer.go:1174-1350；hub.go:466-629 |

- **下行（三种模式共同）**：服务端 GET 分支（`hub.go:630-737`）构造 `splitConn{writer: httpSC, reader: uploadQueue|request.Body}`（`hub.go:695-703`）——sessioned 模式下 `conn.reader = uploadQueue`（`hub.go:702`），客户端 `conn.reader = OpenStream 返回的 response body`（`dialer.go:1049,1064`）。下行是**单一连续字节流**，无分片、无重试。
- **下行写聚合**：`httpServerConn.Write` 聚合到 16KiB / 10ms flush（`hub.go:768-771` 常量、:788-831 实现），只减少帧数，不改变流语义。
- **上行**：stream 模式经 `io.Pipe()` 同步背压（`dialer.go:1013`）；packet-up 经有界管道 `pipe.WithSizeLimit(maxUploadSize-buf.Size)`（`dialer.go:1200`）→ 上传循环按 `maxUploadSize` 切块（`dialer.go:1257`）→ `postPacketReliable`（`packet_upload.go:263-326`）。

### 1.3 packet-up 窗口与分块（RTT 自适应）

- 窗口：默认 **12**，上限 **24**（`packet_upload.go:28,32`）；RTT 分档：≥200ms→12、≥80ms→18、≥20ms→12、<20ms→8，且 ≤ 服务端重排缓冲一半（`packet_upload.go:129-163` `packetUploadWindow`）。
- 分块：32KiB/256KiB/512KiB 分档（`packet_upload.go:168-217` `packetUploadChunkSize`），硬顶为配置 `scMaxEachPostBytes`。
- 重试：每 POST 最多 **3 次**（25ms/50ms 退避，`packet_upload.go:20-23`）；同 seq 重发（`packet_upload.go:256-257` 注释），服务端按 seq 重排（`upload_queue.go:130-190`），**2s gap 超时**判丢包并终止会话流（`upload_queue.go:37,161-165`）。
- 会话级失败：任一 POST 3 次失败 → `failUpload` 中断整个上传管道（`dialer.go:1227-1235`）→ 该逻辑连接（及其上所有内层连接）一起断。

### 1.4 XMUX（多路复用）结构

- **XmuxClient = 一条独立外层 HTTP 连接**：`newXmuxClientLocked` 调 `newConnFunc`（= `createHTTPClient`，`dialer.go:250-252`），即 XMUX 池是"外层连接池 + 每连接多 HTTP 请求"（`mux.go:1238-1256`）。
- **内层连接 = 外层连接上的一个 HTTP 请求 / H2 流**：`dialer.go:1049` `httpClient.OpenStream(...)`。没有 common/mux 的 6 字节帧头（`common/mux/frame.go:45-56` 只用于 VLESS mux 协议：`proxy/vless/inbound/inbound.go:661`、`outbound/outbound.go:508`）。
- 参数：每连接并发流 **8-16**（`config.go:123`，jitter 后 4-32），连接池 **2-4**（`config.go:124`，jitter 后 1-8），burst 硬顶 **16**（`mux.go:751`），单连接 over-admit 硬顶 4x/32（`mux.go:96-97`）。
- 质量反馈：TCP_INFO profile（`mux.go:199-203` lastRetrans/lastLoss/qualityScore；`dialer.go:287-306` 接线）、行为学习 NetworkLearner（`mux.go:210-211`）、AIMD 动态连接/并发（`mux.go:1068-1113`：**lossy/saturated 时连接与并发减半**，`mux.go:1127-1128`）、连续 5 次质量下降 → drain（`mux.go:418-420`）。
- 池生命周期：probe HEAD（`mux.go:1246-1256`）、健康巡检 5s（`mux.go:845-857`）、Fast Eviction（`mux.go:300-307`）、idle eviction 90-240s（`mux.go:73-74`）、idle beacon（`mux.go:781-838`）。

### 1.5 外层传输

- H2：`http2.Transport`，`DisableCompression:true`、`MaxReadFrameSize:16384`（伪装对齐，`dialer.go:720-735`）；x/net 客户端初始流窗口固定 **4MiB**（`dialer.go:702-705` 注释明说不可配置）。
- H1：`http.Transport` + `DisableKeepAlives:true`（`dialer.go:741-748`）；packet-up 上传另有 H1 raw 连接池（`dialer.go:754`、`h1_conn.go`、`h1_pool.go:1-51`）。
- H3：`http3.Transport` + QUIC，可配 BBR/Brutal（`dialer.go:566-682`）；服务端 `QListener` 同样支持（`hub.go:1104-1120`）。H3 目前只作 fallback（`dialer.go:684-712` happy-eyeballs + `h3_fallback.go`）。
- 原始 socket 被 trackConn 捕获（`dialer.go:510`），可挂 socket 级参数（当前用于 TCP_INFO 档案）。

---

## 2. 问题机制分析

### 2.1 耦合路径：不是经典 meltdown，而是 RTT 耦合 + 缓冲耦合

经典 "TCP meltdown"（两层 TCP 各自按丢包减窗、互相放大）在本架构下**大部分被架构本身化解**：内层 TCP 的丢包信号来自本地段（无丢包），内层 cwnd 不因隧道丢包回退。真正的问题是：

1. **延迟注入（主路径）**：外层 TCP 段丢失 → 外层重传/停等 → 隧道单向延迟尖峰（可达数百 ms~数秒）。内层 RTT 估计吸收该尖峰：
   - 尖峰 < 内层 RTO：表现为内层 RTT 膨胀（应用层感知为卡顿、超时风险）。
   - 尖峰 > 内层 RTO：内层发送方 RTO 触发 → **重传 = 相同字节再次注入隧道**（放大）。内层窗口在本地段 autotuning 下可达 MB 级 → 单次 RTO 可注入大块重复数据 → 外层队列进一步上涨 → 正反馈。
2. **缓冲耦合（bufferbloat 放大器）**：外层可靠 ⇒ 内层数据不会丢，只会堆积在：io.Pipe（stream 模式，`dialer.go:1013`）→ H2 4MiB 流窗口（`dialer.go:702-705`）→ 外层 TCP 发送缓冲。堆得越满，延迟越高；外层拥塞控制（CUBIC）在丢包路径上减窗后恢复慢，吞吐锯齿。
3. **下行单流**：三种模式下行都是单一字节流（§1.2）。下行完全暴露在"外层 stall → 内层感知延迟"的裸路径上，没有任何拆包/并发/重试手段。packet-up 只优化了上行。

### 2.2 XMUX 加剧队头阻塞（连接级 HoL）

- HTTP/2 运行在单条 TCP 上：TCP 按序交付，**一次外层段丢失阻塞该连接上所有 H2 流**。每条外层连接承载 8-16 个内层连接（`config.go:123`）⇒ 一次丢包事件可同时卡住 8-16 条内层 TCP（网页多开、多任务场景的尾延迟放大）。
- 现有缓解：连接池 2-4 条（burst 16，`mux.go:751`）——每条连接独立 cwnd，HoL 在连接间隔离；`overAdmitHardMult` 防止单条连接被吸干（`mux.go:93-97`）。
- **与目标相反的行为**：质量驱动 AIMD 在 lossy/saturated 时把连接数**减半**（`mux.go:1127-1128`）——减少并行外层连接 = 更少总 cwnd + HoL 更集中。该逻辑更像"伪装优先"（浏览器不开很多连接），但以高丢包路径的吞吐为代价。
- 注意区分两级 HoL：**流级**（H2 一个流 stall 不影响同连接其他流——Go http2 调度独立）vs **连接级**（TCP 段丢失影响该连接全部流）。池化多连接解决的是连接级；提高单连接并发流数只会摊薄单流带宽，不解决连接级 HoL。

### 2.3 packet-up 拆包到底缓解了什么

- **并发隔离**：上传窗口 8-24 个并发 POST（可跨多条外层连接、H1/H2），一个 POST 的 stall 不阻塞其他（`packet_upload.go:129-163`）。
- **有界重试**：同 seq 重发 ≤3 次（25/50ms），避免外层故障时无限放大（`packet_upload.go:20-23,293-326`）。
- **但**：上传仍是字节流按 `maxUploadSize` 切块（`dialer.go:1257`），**不保留内层段边界**——所以"拆包"不改变内层 TCP 语义，只改变调度/重试粒度。且下行未拆（§2.1.3），上行拆包的收益在不对称路径（下载为主）场景被下行完全抵消。
- **新风险**：会话级失败语义——单 POST 3 次失败（`dialer.go:1227-1235`）或上行 2s gap 超时（`upload_queue.go:37`）→ **整会话终止** ⇒ 该逻辑连接上的所有内层 TCP 同时 reset、所有 UDP 会话消失。这比 stream 模式（连接整体断）更脆，且重排缓冲 `scMaxBufferedPosts`（默认 64，`config.go:537-543`）在突发乱序时直接报错断流（`upload_queue.go:153-156`）。

### 2.4 UDP / QUIC 内层的特殊问题

- 内层 UDP 无重传，由应用（或应用内 QUIC）负责。隧道可靠 ⇒ **内层 QUIC 永远收不到丢包信号** ⇒ 内层拥塞窗口只增不减（无 loss 收敛、无 ECN），把隧道缓冲撑满 → 延迟持续膨胀；而 QUIC 的 RTO 按膨胀后的 RTT 自适应，重传只在极长 stall 时触发。对游戏/DNS/实时流，表现为"零丢包但延迟抖大"。
- 数据报 ≤65533 的硬限制（`addons.go:370-375`），超大 UDP 直接报错——外层 chunk 切分不感知 VLESS 帧边界，但字节流保序重排后由 `LengthPacketReader` 重新成帧，语义无损。

### 2.5 内层"重试"的真实位置

内层 TCP 的重传由**用户 app 的 TCP 栈**控制（app↔本地 inbound 段），代理无法禁用。代理侧能影响的只有：内层数据何时进入隧道（背压）、隧道延迟的平滑度。因此"内层禁用重试、依赖外层可靠"在代理架构下的正确解读是：

- 外层可靠交付（已满足：TCP/H2/H3 都可靠）；
- **降低内层 RTO 触发率** = 让外层 stall 尽量短、尽量少（外层平滑化：BBR、多连接、QUIC）；
- **会话级失败粒度收敛**：外层故障时只断受影响的内层连接，而不是整棵会话树（当前 packet-up 会断整会话）。

---

## 3. 影响场景

| 场景 | 参数 | 表现 | 主导机制 |
|---|---|---|---|
| 跨境大流量下载（单流下行） | 丢包 1-3%，RTT 150-250ms | 吞吐 = 外层 TCP 锯齿（CUBIC 减窗-恢复循环）；内层 RTO 偶发放大 | §2.1.1+2.1.2 |
| 跨境网页/API（多小流） | 同上 | 尾延迟放大：一次外层段丢失卡住同连接 8-16 条内层流 | §2.2 连接级 HoL |
| 弱网移动端 | 丢包 2-5%，抖动大 | 外层 RTO 频繁 → 内层 RTO 联动 → 明显卡顿、应用超时 | §2.1.1 |
| 高 RTT 长肥管道 | RTT ≥200ms，BDP 大 | 内层窗口大（MB 级）→ 放大系数大；外层恢复慢 | §2.1.1 |
| 游戏/QUIC/DNS（UDP 内层） | 任意 | 无丢包但延迟膨胀；内层 QUIC 不收敛 → bufferbloat 主源 | §2.4 |
| 下载为主（packet-up） | — | 上行拆包收益被单流下行抵消 | §2.3 |

量化经验值（外层为唯一丢包源时）：
- 外层丢包 <0.1%：基本无感（外层重传被 SRTT 平滑吸收）。
- 0.1-1%：延迟尖峰可见；内层 RTO 触发率 ≈ P(外层 stall > 内层 RTO)。
- >1%：吞吐塌陷明显；每连接 8-16 流共享 ⇒ 影响面 ×8-16。
- 内层 RTO 默认 1s 起步（随 SRTT 增长），外层 RTO 在 200ms-数秒量级——两者在 1-3% 丢包路径上频繁交错。

---

## 4. 重点方案评估（任务指定四项）

### 4.1 内层用 UDP 承载（QUIC）
- **机制**：把外层从 TCP 换成 QUIC（即 XHTTP/3 转正；或自研 QUIC 承载层）。QUIC 天然流级隔离（丢包只影响受影响的流，无连接级 HoL）、自带 BBR/Brutal（`hub.go:1109-1118` 已有）、0-RTT。
- **改动位置**：`dialer.go:566-712`（H3 客户端 + happy-eyeballs，目前 fallback）、`hub.go:937-991`（H3 监听，已完整）。
- **收益**：彻底消除外层 TCP 拥塞互扰；外层平滑 → 内层 RTO 触发率大幅下降；HoL 从连接级降到流级。
- **风险**：UDP 被 QoS 限速/阻断的网络不可用（需 H2 fallback，已有）；QUIC 指纹面；测试面大。**注意**：内层仍是 TCP，RTT 耦合（§2.1.1）只缓解不消除——但这是目前能做到的最大缓解。

### 4.2 外层多连接池（已有 maxConnections 2-4）
- **机制**：连接池 = 并行外层连接数；每条连接独立 cwnd 与 HoL 域。当前 2-4（jitter 1-8）、burst ≤16（`config.go:124`、`mux.go:751`）。
- **问题**：lossy 时 AIMD 减半（`mux.go:1127-1128`），方向相反；上限 8 也偏保守（burst 16 只在饱和时短暂放开）。
- **建议**：lossy/saturated 行为反转——**升连接数（隔离 HoL、增加总 cwnd）、降每连接并发流数**（`mux.go:123` 8-16 → 4-8）；上限放开到 8-16 稳态 / 32 burst。这是低成本、高性价比的一档（见 §5 L1）。

### 4.3 内层禁用重试（依赖外层可靠）
- **结论：字面不可行**。内层 TCP 重传由 app 的栈控制（§2.5）。代理侧等价物：
  1. 外层可靠性已有（TCP/QUIC 都可靠）——内层数据不会因隧道丢包而丢，只会延迟；
  2. 降低内层 RTO 触发率：外层平滑化（§4.1、§5 M4）+ 减小隧道缓冲（H2 窗口不可配置但可减小 pipe/队列）；
  3. 会话级失败粒度：packet-up 断整会话（§2.3）应改为只断受影响内层连接（VLESS 层本身每次 dial 都是新连接，天然支持细粒度重连）。
- 收益排序：3 > 1 > 2（按当前代码的可改性与风险）。

### 4.4 拥塞控制参数调整
- 外层 TCP：trackConn 已握有 raw socket（`dialer.go:510`），可 setsockopt：Linux `TCP_CONGESTION=bbr`（若内核支持）对高丢包路径收益最大（无丢包减窗）；SO_SNDBUF/SO_RCVBUF 按 BDP 放大（默认缓冲在 200ms+ 路径偏小）；`TCP_NODELAY` Go 已默认。
- 外层 QUIC：已支持 BBR/Brutal（`dialer.go:670-678`、`hub.go:1109-1118`）。
- 内层不可控（app 栈）。H2 4MiB 客户端窗口是 x/net 固定值（`dialer.go:702-705` 注释），fork x/net 可改但代价大，收益中等（减小缓冲上限能压 bufferbloat，但降低突发吞吐）。

---

## 5. 优化方案分档表

### 低风险（小改，改参数/逻辑，不动协议）

| # | 方案 | 机制说明 | 改动位置 | 预期收益 | 风险 |
|---|---|---|---|---|---|
| L1 | lossy 反向 AIMD：升连接、降并发流 | 高丢包时增加外层连接数（HoL 隔离 + 总 cwnd 上升），同时降低单连接并发流（减少连接级 HoL 波及面） | `mux.go:1068-1113`（applyAIMD/computeTargetConns/Conc）、`config.go:123-124` 上限（2-4→4-8，burst 16→32）、`mux.go:751` | 1-3% 丢包路径吞吐与尾延迟显著改善 | 连接/流数分布指纹变化；与现有行为相反，必须 ABAB 基准验证（skill 规则 3）；CDN 连接数配额 |
| L2 | packet-up 窗口/重试联动微调 | 高 RTT+高丢包时窗口 12→8（降并发放大）、重试 3→4 次（覆盖外层偶发失败）、chunk 上限 512KiB（防放大） | `packet_upload.go:20-23`（MaxAttempts）、:129-163（window）、:168-217（chunk） | 弱网下会话中断率下降，放大减小 | 纯参数；需实测窗口-吞吐曲线 |
| L3 | stream 模式上行管道加有界容量 | io.Pipe 无界同步（`dialer.go:1013`）在 stall 时让内层背压更平滑、内存可控 | `dialer.go:1013` 换 `pipe.WithSizeLimit` | 弱网下内存/队列可控 | 极小；注意与 HTTP body 流控叠加 |
| L4 | 会话失败粒度：packet-up 断连只影响本连接 | `failUpload` 目前断整个上传管道（`dialer.go:1227-1235`），改为仅终止本逻辑连接并快速重连（VLESS 每次 dial 天然新连接） | `dialer.go:1215-1350` 上传循环错误处理 | 会话中断对多连接用户的影响面缩小 | 中低；涉及会话状态机，需测试 |

### 中风险（协议内扩展或系统参数）

| # | 方案 | 机制说明 | 改动位置 | 预期收益 | 风险 |
|---|---|---|---|---|---|
| M1 | packet-up 下行多段 GET | 下行也拆成编号段：客户端并发拉取 + 有限重试 + 服务端按段缓存/重排（复用 uploadQueue 语义）；每段独立 GET，段丢失只重试该段 | `hub.go:630-737`（GET 分支改段式）、`client.go:334-527`（下行循环）、`upload_queue.go` 语义复用 | 下行获得与上行同等的并发隔离与有界重试——这是"内层禁用重试"最接近的落地 | 协议扩展（服务端需双格式兼容，skill 规则 2）；CDN 缓存语义复杂；工作量大（接近大工程） |
| M2 | XMUX 参数配置化 + lossy 联动 | L1 做成配置项（连接数/并发流上下限、lossy 策略开关），默认值保持伪装友好 | `config.proto/config.go` 新增字段、`mux.go` 读配置 | 部署方可按线路调优 | 配置面膨胀；默认值需维持现状 |
| M3 | 外层 socket 参数 | trackConn 处 setsockopt：SO_SNDBUF/SO_RCVBUF 按 BDP 放大；Linux TCP_CONGESTION=bbr 选项 | `dialer.go:457-510`（dialRawTCP/dialContext） | 高 BDP 路径吞吐提升；BBR 下外层平滑 → 内层 RTO 触发率下降 | 非 Linux 无效；BBR 需内核支持；与 CUBIC 行为差异需实测 |
| M4 | H2 客户端窗口瘦身（fork x/net） | 4MiB 初始/最大窗口（`dialer.go:702-705`）→ 1MiB 级，压隧道内缓冲上限 | x/net http2.Transport 窗口参数（需 fork，本仓库已有 x/net 依赖面） | bufferbloat 上限下降，延迟更稳 | fork x/net 维护成本；突发吞吐可能下降；与伪装窗口（注释说非高价值指纹）权衡 |

### 大工程

| # | 方案 | 机制说明 | 改动位置 | 预期收益 | 风险 |
|---|---|---|---|---|---|
| B1 | XHTTP/3 转正为主通道 | 外层 QUIC（流级隔离 + BBR/Brutal + 0-RTT），H2 仅 fallback；内层仍 TCP 但外层平滑化后 RTT 耦合大幅缓解 | `dialer.go:566-712`（默认 H3）、`hub.go:937-991`（已完整）、`h3_fallback.go`（策略反转）、`mode_degrade.go` | 消除外层 TCP 拥塞互扰与连接级 HoL；弱网吞吐/延迟显著改善 | UDP 被 QoS/阻断的网络退化 H2（已有）；QUIC 指纹；全面测试面 |
| B2 | 隧道内部 QUIC 承载（跳过 HTTP 层） | 自研 QUIC 承载层替代 HTTP/3 语义（多路流 + 字节流 + 流控），保留 XHTTP 模式语义 | 新 transport 包 + dialer/hub 重构 | 同 B1，且省 HTTP 头开销 | 失去 HTTP 伪装；工程量大；与 B1 二选一 |
| B3 | 外层 FEC | 字节流包级前向纠错降低有效丢包率 → 内层 RTO 触发率下降 | 新传输层包装 | 高丢包（>3%）路径吞吐 | 带宽放大、指纹、复杂度——与伪装目标冲突，**不建议** |

### 方案间关系

- L1+L2+L3+L4 是当前架构下性价比最高的一组，全部在已有参数/逻辑内做文章；
- M1 是"下行拆包"，与上行拆包对称，是 L 档之上收益最大的协议内改动；
- B1 是终极形态：外层 QUIC 已具备 90% 代码，转正后"外层多连接池"问题自然消失（QUIC 连接内部多流无连接级 HoL）；
- "内层 QUIC（隧道内部协议换 QUIC）"与 B1/B2 等价，无需单独列项。

---

## 6. 验证建议（对齐仓库既有纪律）

1. 任何改动走 `scripts/bench_compare.sh` + ABAB 交替验证（skill 规则 3）；涉及 wire 形态的改动跑 `wire_audit_test.go` 与 `go test ./testing/scenarios/ -run 'TestVlessXtls'`（skill 规则 7c）。
2. M1 属协议改动，必须服务端双格式兼容 + `v21_test.go` 风格回归测试。
3. 高丢包场景建议用 tc netem（`loss 1% 5%`、`delay 150ms`）在本地起双端实测，比 bench 更接近真实；H3 转正（B1）前先跑通 `wave*_test.go` 全家族。

---

*本报告为纯研究产出，未修改任何源码。*
