# Bray-Core XHTTP 下行分段方案（M1）研究报告

> 研究日期：2026-08-14　｜　研究对象：`D:\UGit\Bray-Core`（Xray-core fork，XHTTP+XMUX）
> 范围：只读研究，未改任何代码。所有行号基于当前 HEAD（6d6222b7）。
> 关联文档：`docs/xhttp-tcp-over-tcp-report.md`（TCP-over-TCP 总体研究，M1 为其 §5 分档表中的中风险项）、skill `bray-core-dev/references/xhttp-architecture.md`。

---

## 0. 结论摘要（TL;DR）

1. **现状下行是三种模式共同的单点瓶颈**：stream-one / stream-up / packet-up 的下行都是**一条 GET 长连接字节流**（`hub.go:686-703` + `dialer.go:1062-1067`），客户端对下行**无并发、无分段、无重试**——外层连接一断，整条内层连接即死，重试粒度只能是"整个会话"。
2. **M1 目标**：下行也拆成编号段，客户端并发拉取 + 有限重试（≤3 次、25/50ms 退避，镜像上行 `postPacketReliable`），服务端按段缓存/重排（复用 `uploadQueue` 语义），获得与上行同等的**并发隔离与有界重试**。
3. **推荐协议（一句话）**：把服务端目前无人使用的 `GET + ?seq=N` 语义从"空包上行"（`hub.go:393-394,466-629`）**重定义为"下行段 N 拉取"**；客户端并发窗口 W=8~12 个段 GET（直接复用 `packetUploadWindow` 函数）、默认段大小 256KB（RTT 分档同上行，正好落在 `postBodyPool` size class 上）、服务端每会话 16 段滑动缓存 + 全局 256MB 上限；legacy 单流路径（纯 GET 无 seq）原样保留，老客户端零影响；新客户端用"首段 GET 拿到 0 字节响应"探测老服务端并自动回退 legacy。**只动 packet-up 下行腿，stream-one / stream-up 不动。**
4. **关键发现（可安全重定义的理由）**：bray 客户端下行 GET 恒不带 seq（`client.go:408` `FillStreamRequest(req, sessionId, "")`），全仓测试也无 `GET+seq` 用例——`GET+seq` 目前是一条**死代码路径**（空 body 推空包），重定义不会破坏任何现存客户端。
5. **最值得警惕的两个落地陷阱**：(a) XMUX 请求配额——现下行腿只消耗 1 个 LeftRequests（`dialer.go:1096`），分段后请求数膨胀为 `数据量/段大小`，必须让段 GET 走下载专用 XmuxClient 且豁免/摊销配额；(b) H1 路径 `DisableKeepAlives: true`（`dialer.go:747`）——每段一个 TCP+TLS 握手是性能灾难，M1 段拉取必须要求 H2（或 H1 keep-alive）。
6. **伪装面**：多段 GET 比"一条无限长 GET"更接近真实 Web（HLS/DASH 视频分段、视频元素 Range 请求都是同路径多请求、参数递增）；代价是"无 Manifest 的纯 seq 递增参数"本身可被归为代理特征，需加抖动。**建议默认保持 `Cache-Control: no-store`**（与 `hub.go:670` 现有决策一致），"段可被 CDN 缓存"作为可选控制头留给 CDN 场景（M1.5）。

---

## 1. 现状下行路径（带行号）

### 1.1 客户端：Dial → 单条 GET 响应体

```
app ──内层TCP──▶ 本地inbound ──buf.Copy──▶ VLESS outbound ──▶ XHTTP Dial (dialer.go:825)
                                                                  │
                        conn.reader = GET response body（单流、无重试、无并发）
```

- **Dial 入口与 URL**：`dialer.go:825`；requestURL 构造 `:837-862`；`getHTTPClient` 取客户端与 XmuxClient `:864`；模式级联 `BuildModeCascade` `:878`；下载腿独立 URL/客户端仅在配置了 `downloadSettings` 时存在：`requestURL2/httpClient2/xmuxClient2` `:905-907`，否则等于主腿。
- **tryMode 开下行腿（packet-up）**：`dialer.go:1062-1067` `conn.reader, ... = httpClient2.OpenStream(ctx, &requestURL2, sessionId, nil, false)` —— 一条 GET，`body=nil`。成功后在 `:1095-1098` 扣 `xmuxClient2.LeftRequests.Add(-1)`：**整条下行腿恰好消耗 1 个 XMUX 请求配额**。
- **OpenStream（client.go:334-527）**：`body==nil → method="GET"`（`:339-342`）；`FillStreamRequest(req, sessionId, "")`（`:408`，**seq 恒为空**）；200 后返回 `cancelOnClose{resp.Body}`（`:466-473`）；头部等待上限 `defaultOpenStreamHeaderTimeout = 20s`（`:29-34,491`）；连接级故障 `isFatalConnError` 会 markFatal 驱逐客户端（`:415-417`）。
- **下行消费**：`conn.reader`（GET 响应体）→ `splitConn.Read`（`connection.go:45+`）→ 内层 `buf.Copy`。响应体一断（RST/超时/服务器断流），内层连接直接报错，**没有重试粒度**。

### 1.2 服务端：GET 分支 → httpServerConn 聚合写

- **GET 分支**（`hub.go:630-737`）：sessioned GET（stream-up/packet-up 的下行腿）→ 建 `httpSC := &httpServerConn{...}`（`:686-690`）→ `conn := splitConn{writer: httpSC, reader: currentSession.uploadQueue}`（`:695-703`）→ `h.ln.addConn(stat.Connection(&conn))`（`:729`）把 splitConn 作为**内层连接**交给 VLESS inbound → 等 ctx 或 httpSC 结束（`:732-735`）→ `conn.Close()`（`:737`）。
- **下行写入路径**：VLESS inbound 把目标回包写进 splitConn → `httpServerConn.Write`（`hub.go:788-829`）：孤立写立即 flush（`:801-812`，保首字节延迟）；连续写按 **16KiB / 10ms** 聚合（常量 `:768-771`，`:816-829` + `flushPending` 兜底 `:828`）。
- **响应头**：`Cache-Control: no-store`（`:670`）；`X-Accel-Buffering` 默认不发（`:660-666`）；Content-Type 默认不设（仅 `x-bray-sse=1` 控制头时 `text/event-stream`，`:672-681`）。
- **关键语义：GET+seq 目前被当作"空包上行"**（`hub.go:392-397`：`case "GET": isUplinkRequest = seqStr != ""` → 进 packet-up POST 分支 `:466-629`，GET 无 body → 推一个空 payload 包进 uploadQueue）。**bray 客户端从不发送 GET+seq**（下行 GET 恒无 seq，见 1.1），测试亦无此用例 → 该路径是死代码，可安全重定义（详见 §2.1）。

### 1.3 可复用组件盘点（M1 的积木）

| 组件 | 位置 | 可复用点 |
|---|---|---|
| `uploadQueue`（seq 小顶堆重排 + 2s gap 超时判丢 + 队列上限 + 非阻塞 Push） | `upload_queue.go:39-193` | 服务端段队列可直接照抄/泛化；`maxSeqGapWait=2s`（`:37`）作为段等待上限基准 |
| `packetUploadWindow(scMaxBufferedPosts, rtt)`（默认 12、上限 24、RTT 分档 8/12/18/12） | `packet_upload.go:129-163` | 客户端段拉取并发窗口直接复用 |
| `packetUploadChunkSize`（RTT 分档 32K/256K/512K） | `packet_upload.go:168-218` | 段大小分档同构 |
| `postPacketReliable`（同 seq ≤3 次、25/50ms 退避、durable 快照） | `packet_upload.go:263-326` | 段 GET 重试逻辑照搬（换成 GET） |
| `formatSeqInt64` / `seqSmallCache`（无分配 seq 格式化） | `packet_upload.go:94-123` | 段号格式化复用 |
| `postBodyPool`（2K~4M 分档 pool） | `post_body_pool.go:20-60` | 段缓冲区直接复用 256K/512K/1M class |
| 下载专用腿机制 `requestURL2/httpClient2/xmuxClient2` | `dialer.go:905-907,1062-1067` | 段 GET 天然走独立下载连接（配额/生命周期隔离） |
| `slots` 通道窗口 + `inflight.WaitGroup` + `failOnce` 上传循环骨架 | `dialer.go:1215-1354` | 段拉取器骨架照搬 |
| 会话 TTL/MAC/上限 | `hub.go:62,134-211,336-348` | 段缓存生命周期挂在会话 TTL 上 |

### 1.4 会话与配额事实（设计约束）

- `httpSession{uploadQueue, isFullyConnected, timer}`（`hub.go:50-56`）；TTL 默认 45s、按平均处理时延可拉伸到 180s（`:64-81`）；每 handler 上限 65536 会话（`:62`）；任意合法请求（含段 GET）都会 `upsertSession` 续期（`:150-153`）——与上行 POST 行为一致，无新增问题。
- XMUX：`LeftRequests` 默认 MaxInt32、受 `hMaxRequestTimes` 配置（`mux.go:1479-1482`）；健康检查在 `LeftRequests<=0` 时 drain 连接（`:907-914`）；上传循环在配额耗尽时换客户端（`dialer.go:1306-1334`）。**段 GET 若每段扣 1 配额，会瞬间耗尽下载连接**——必须专门处理（§3.4）。
- 服务端 DoS pin 原则（Bray-only）：`uploadQueue.Push` 用 `select default` 非阻塞，满则 404（`upload_queue.go:84-92`）——段缓存必须继承"上限内驱逐、不阻塞 handler"。

---

## 2. 分段协议设计（M1 建议方案）

### 2.1 协议形态：`GET ?session=X&seq=N`（重定义 GET+seq）

- **线格式**：客户端对段 N 发 `GET <path>?session=<sid>&seq=N`（seq 经 `ApplyMetaToRequest` 走既有 path/query/header/cookie 四通道之一，`config.go:637-685`，与上行 POST 完全同构）。建议同时带一个 Bray-only 标记头（如 `x-bray-dseg: 1`）与"空包上行"死路径彻底隔离——虽然旧语义无人用，显式标记可防止未来上游行为漂移。
- **服务端分派**（`hub.go:392-397` 扩展）：`GET + seq + dseg 标记` → 下行段拉取；`GET + seq 无标记` → 保持现状（空包上行，死路径，兼容上游）；`GET 无 seq` → legacy 单流（老客户端，完全不变）。超集解析，符合用户既有"服务端=超集、客户端=新格式"原则。
- **段号空间**：独立于上行 seq 的下行段号（服务端每会话一个 `downSegSeq` 计数器，从 0 起）。**不复用上行 seq 空间**——上传与下载方向不同，混用会与 uploadQueue 的 nextSeq 冲突。
- **备选方案（不推荐首版，留 M1.5）**：`Range: bytes=[N*S,(N+1)*S)` 定长段拉取——最 HTTP 原生、CDN 最友好（浏览器视频元素就是这样），但需要服务端新增 Range 语义、且与"未产出段必须阻塞/404"的活流语义叠加更绕；seq 参数与上行完全对称、复用一个解析路径，首版成本最低。

### 2.2 客户端：段拉取器（segmentAssembler）

- **接入点**：packet-up 分支的 `conn.reader` 从"单条 GET 响应体"换成 `segmentAssembler`（实现 `io.Reader`，对内层 `buf.Copy` 完全透明）。首个段 GET（`seq=0`）兼任"开下行腿 + 模式协商"——**注意必须先于上传循环发出**（现状 `dialer.go:1062` 也是先开下行腿再起上传循环，顺序不变）。
- **并发窗口**：W = `packetUploadWindow(scMaxBufferedSegments, seedRTT)`（默认 12、上限 24、RTT 分档），骨架照搬上传循环的 `slots` 通道 + `inflight.WaitGroup`（`dialer.go:1222-1223`）。拉取器只维护 `[next, next+W)` 的窗口：消费推进 next 后补拉一段。
- **重试**：单段 GET 失败 → 同段重试 ≤3 次、25/50ms 退避（照搬 `postPacketReliable:293-325` 的循环，含 ctx 感知）。**头部段（next）重试耗尽 → 会话级 abort**（镜像上传的 `failUpload` → `uploadPipeReader.Interrupt()`，`dialer.go:1227-1235`）。
- **段级超时**：单段 GET 头部等待上限建议 2~5s（`defaultOpenStreamHeaderTimeout` 的 20s 对段粒度太长）；服务端对"未产出段"的阻塞响应最长 2s（对齐 `maxSeqGapWait`），超时返回 404 → 客户端幂等重拉（段字节不可变，同 seq 重拉无副作用）。
- **重排与投递**：按 seq 缓冲重排（客户端侧小堆，容量 W），顺序投递给内层；乱序时只等 next，与 uploadQueue.Read 同一语义。
- **EOF**：服务端在流结束时发"末段"（不足 S 的半段或 0 字节段 + EOF 标记头，如 `x-bray-dseg-eof: 1`）；客户端见 EOF 头或"非首段却 0 字节响应"即完成，向内层返回 EOF。双重信号任一生效，防丢。
- **老服务端回退**：新客户端首段 GET（`seq=0`）若收到 200 + Content-Length:0 且无 EOF 标记 → 判定对端是旧实现（把 GET+seq 当空包上行）→ 整条下行腿回退 legacy 单流 GET（新开一条无 seq GET），会话继续。配合发布节奏（§5.3）这只是保险丝。

### 2.3 服务端：段产出 + 段缓存 + 段响应

- **段产出（segmentWriter）**：`splitConn.writer` 从裸 `httpSC` 换成 segmentWriter（或给 httpSC 加 segment 模式）：内层写进来的字节先累积进当前段缓冲，**四种情况触发发布**：(a) 填满 S；(b) hold 定时器到期（建议 10~50ms，沿用 `streamFlushInterval` 语义，`hub.go:768-771`）；(c) **有 GET 正等着该段（waiter）→ 立即 flush 半段**（保 TTFB，这是比 legacy 单流还好的特性）；(d) 连接 Close（EOF，发末段+EOF 标记）。
- **段缓存（downloadQueue，照抄 uploadQueue 结构）**：每会话一个，按 seq 索引的滑动窗口：
  - 容量 K 段（建议默认 16 = 客户端窗口 2×，沿用"窗口 ≤ 服务端缓冲/2"原则，`packet_upload.go:126-128` 注释）。
  - 段数据用 `allocPostBody`（`post_body_pool.go`）的 size class 池，消费/驱逐即 `freePostBody`——与 uploadQueue 的 Pooled 语义（`upload_queue.go:20-33`）完全一致。
  - 查询语义：`N < 窗口起点`（已滑过/被驱逐）→ **410 Gone**（客户端应已消费，出现即失步，abort）；`N 在窗口内未产出` → **阻塞等待 ≤2s**（复用 gap 超时思想）→ 超时 404；`N ≥ 窗口终点`（客户端超前失步）→ 立即 404。
  - 非阻塞 Push 继承（`select default`，`upload_queue.go:84-92`）——段缓存满不阻塞产出路径。
- **全局内存上限**：per-listener 下行段缓存总字节上限（建议默认 256MB，新配置），超限按"最旧未取段"驱逐 → 被驱逐段后续请求 410 → 客户端 abort 会话（只在内存压力下发生，可接受）。**必须**：`session.close()`（`hub.go:58-60`）同时释放该会话段缓存，避免 TTL 回收滞后。
- **模式标记**：会话在首个 GET 请求定型（legacy 或 segment），存到 `httpSession` 新字段；后续段 GET 校验一致（段 GET 打到 legacy 会话 → 404）。

### 2.4 配置项（新增，全部走既有 RangeConfig 风格）

| 配置 | 默认 | 语义 |
|---|---|---|
| `scMaxEachDownloadBytes`（RangeConfig） | 256K（RTT 分档 32K/256K/512K/1M） | 段大小 S，上限对齐 `scMaxEachPostBytes` |
| `scMaxBufferedSegments`（int64） | 16 | 服务端每会话段缓存窗口 K |
| `scMaxDownloadCacheBytes`（int64） | 256MB | per-listener 段缓存全局上限 |
| 段 hold 间隔 | 10~50ms | 半段 flush 定时器（可先硬编码，不上配置） |

客户端窗口 W 由 `packetUploadWindow(scMaxBufferedSegments, rtt)` 推出（默认 12 ≤ K/2=8？——注意：K=16 时 W 上限应为 8 而非 12，默认取 min(12, K/2)=8；若想要 W=12 需 K=24。**默认建议 K=24、W=12**，与上行 `scMaxBufferedPosts=64/W=12` 的比例同构）。

---

## 3. 权衡分析

### 3.1 段粒度 S vs 重试粒度 vs 服务端缓存内存

| S | 请求数(1GB 下行) | 重试代价 | 服务端内存(×24 段) | TTFB | 评价 |
|---|---|---|---|---|---|
| 64K | 16,384 | 极小 | 1.5MB/会话 | 好 | 段数过多：XMUX 配额、日志、伪装面全面膨胀 |
| **256K** | **4,096** | **小** | **6MB/会话** | **好** | **推荐**：postBodyPool 有现成 class、与上行 chunk 中档对称 |
| 1M | 1,024 | 大（一次丢 1MB） | 24MB/会话 | 一般（hold 定时器兜底） | 只有高 RTT/大缓存场景值得 |

- 256KB 的另一个理由：**上下行对称**——观察者看到的 POST 与 GET 载荷大小分布一致，比"上行 256K、下行无限长"更不显眼。
- 重试粒度 = 段粒度：256KB 段重拉一次的成本 ≈ 一个上行 chunk 重发一次的成本，语义完全对称。
- 内存公式：峰值 ≈ 活跃会话数 × K × S。1000 活跃会话 × 24 × 256K ≈ 6GB —— **必须全局上限 + 驱逐**；256MB 上限意味着极端并发下最多 ~40 个会话持有满窗口（现实流量远低于此，驱逐是保险丝不是常态路径）。

### 3.2 并发窗口 W 与缓存窗口 K 的耦合

- 客户端 W 必须 ≤ 服务端 K/2（沿用 `packet_upload.go:126-128` 注释的论证：乱序突发要装得下）。默认 K=24、W=12。
- W 太大（≥16）：服务端缓存压力、H2 并发流数超浏览器常态（`config.go:123` 的 4-32 jitter 上限内但贴近上限）、失步时重排延迟放大。
- W 太小（≤4）：高 RTT 下 BDP 填不满（与上行窗口同款论证，`packet_upload.go:141-155`）。RTT 分档复用即可。

### 3.3 HTTP 语义与 CDN 交互

- **多段 GET 更 CDN 友好（结构性优势）**：段 URL（session 固定 + seq 递增）对应的字节**不可变**（产出后不再改），天然可缓存；断线重试可命中 CDN 缓存、绕开源站；每段响应小、可被 CDN 单段缓存而不用 tee 整条长流（`hub.go:667-670` 注释正是抱怨 CDN tee 长流导致 slowdown）。
- **但代价与冲突**：(a) 启用缓存需去掉/弱化 `Cache-Control: no-store`（`hub.go:670`），这是 Bray 现状的刻意决策；(b) 段内容留在 CDN 侧形成缓存侧信道（段内是内层协议字节流，无明文机密，但多了留存面）；(c) 段 GET 带 sessionId（MAC 保护的会话标识）——CDN 缓存 key 含 sessionId，若 CDN 公开缓存（public）会被跨客户端探到。
- **建议**：首版**保持 no-store**（伪装面优先，符合仓库一贯决策）；把"段可缓存"做成可选控制头（如 `x-bray-seg-cache: 1` 时对段响应发 `Cache-Control: private, max-age=60`）留给明确需要 CDN 抗断流的部署（M1.5）。Range 方案（§2.1 备选）若未来要最大化 CDN 友好度，可作为 M1.5 叠加（浏览器视频元素同款形态）。

### 3.4 XMUX 配额耦合（最容易踩的坑）

- 现状：下行腿 = 1 个请求、扣 1 个 LeftRequests（`dialer.go:1096`）。分段后：请求数 = `下行字节数/S`，1GB 下行 = 4096 个段 GET。若每段扣 1，`hMaxRequestTimes` 配置的连接配额秒空 → 健康检查 drain（`mux.go:907-914`）→ 下载连接频繁重建。
- **推荐**：段 GET 一律走 `xmuxClient2`（下载专用连接，机制已存在，`dialer.go:905-907`）且**不扣 LeftRequests**（下载连接的寿命由 `maxConnectionAge`/idleTimeout 管理，`mux.go:926-933`）；或摊销扣减（每 K 段扣 1）。上传侧配额逻辑（`dialer.go:1306-1334`）不动。
- 附带收益：下载流量与上传流量天然分连接，段 GET 的 H2 流并发只与下载侧共享，互不挤占。

### 3.5 伪装面（与浏览器行为对比）

| 观察点 | 现状（单条长 GET） | M1（多段 GET） | 浏览器参考 |
|---|---|---|---|
| 单请求时长 | 无限长（数小时）——**最不像浏览器的形态** | 每段 ≤ 几秒 | 视频段请求秒级 |
| 同路径请求数 | 1 | 数据量/S（如 4096） | HLS/DASH 每 2-10s 一段 |
| 并发请求数 | 1 | W=8-12（建议抖动 6-10） | 每域 6-8 并行子资源 |
| 参数形态 | 无 seq | seq 单调递增（与上行 POST 同构） | 视频 CDN 的段序号参数常见 |
| 响应大小 | 流式不定 | 固定 S±抖动 | 视频段定长常见 |

- **净评估**：M1 把"单条无限长 GET"（最可疑）换成"同路径参数递增的多段短 GET"（视频/资源分片形态），**伪装面总体改善**；代价是"无 Manifest 的纯 seq 递增"在强检测下仍可被归为代理特征。
- **缓解措施（进 wire_audit 门禁）**：段大小在 S±10% 抖动（`biasedRangeRand` 复用）；W 在 6-10 抖动；段间间隔 0-50ms 随机（复用 `packetUploadLaunchIntervalMs` 思路，`packet_upload.go:229-237`）；段响应固定 Content-Type（如 `application/octet-stream`）避免 Go 的 sniff 在文本载荷时变 `text/plain`（`hub.go:672-681` 的教训）。

---

## 4. 风险

1. **协议复杂度（最大的工程风险）**：服务端双模式共存（legacy 单流 + 段流）意味着下行写路径两套；段队列/组装器是新状态机（EOF、半段、窗口滑动、驱逐）；模式定型发生在首个请求。估：服务端 ~300-400 行 + 客户端 ~400-500 行（不含测试）。**缓解**：downloadQueue 照抄 uploadQueue（已有 256 行成熟实现 + 单测），客户端拉取器照抄上传循环骨架，新增状态机只有"段产出定时器"和"EOF 标记"两处。
2. **服务端内存**：N 会话 × K × S 的乘积（§3.1），加上现有 uploadQueue 最坏 64 包 × 1M = 64MB/会话的既有问题，段缓存必须带全局上限 + 驱逐 + 会话关闭即释放。**继承 DoS pin 原则**（非阻塞 Push、上限内 404/410，`upload_queue.go:84-92`）。
3. **时序/挂起堆积**：慢目标 + 快客户端时，"等未产出段的阻塞 GET"会挂起堆积 → 需要等待上限（2s）+ 每会话挂起段 GET 并发上限（= W）+ 挂起超时即 404。与 `maxSeqGapWait`（`upload_queue.go:37`）同一哲学：**有界等待，绝不无限阻塞**。
4. **伪装面冲突**：无 Manifest 的 seq 递增参数序列是新的可归因特征；H1 路径每段一次 TCP+TLS 握手（`dialer.go:747` `DisableKeepAlives:true`）既是性能灾难也是连接风暴特征。**缓解**：段 GET 只走 H2/H3（H3 原生多路复用，`dialer.go:566-682,937-991` 已就绪）；H1 回退 legacy 单流。
5. **升级不对称**：新客户端 vs 老服务端会静默拿到空段 → 靠 §2.2 的探测回退兜底；但探测本身多一次请求。**缓解**：按用户既有发布惯例标注"两端同步升级"，探测只作保险丝。
6. **内层应用视角**：段组装器引入客户端缓冲（峰值 ≈ W×S ≈ 3MB，与现状 H2 4MiB 窗口同量级）与至多"段 hold 时间 + 重拉 RTT×3 + 退避"的头部阻塞延迟——相比现状"一次丢包 = 整连接重连"，是净改善。
7. **与既有功能交互**：mode_degrade 级联、sticky mode（Wave-4，`dialer.go:890-901`）只按模式记忆、不受下行腿实现影响；wire_audit 门禁（padding 池、分布审计）需扩展覆盖段 GET 形态（§5.4）。

---

## 5. 落地建议

### 5.1 最小可行范围（M1-core）

- **只动 packet-up 下行腿**；stream-one（无 session、无分段基础）与 stream-up（可后做同构扩展）不动。
- 服务端**双模式共存**：纯 GET = legacy（一行不改）；GET+seq+`x-bray-dseg:1` = 段拉取。
- 默认参数：S=256KB（RTT 分档同上行）、K=24、W=12（`packetUploadWindow` 复用）、全局 256MB、hold 10-50ms、段 GET 头部超时 3s、等待未产出段 ≤2s、重试 3 次 25/50ms。
- 不做（M1.5 再议）：CDN 段缓存头、Range 变体、stream-up 分段、H1 段拉取。

### 5.2 改动文件清单

| 文件 | 改动 |
|---|---|
| `transport/internet/splithttp/hub.go` | GET 分支分派（段模式 vs legacy）；httpSession 加 `segMode`/`downloadQueue` 字段；段响应 handler（查缓存/阻塞等待/410/404/EOF 头）；session.close 释放段缓存 |
| `transport/internet/splithttp/upload_queue.go` | 泛化出 `downloadQueue`（或新文件 `download_queue.go` 照抄 + 按 seq 随机访问语义 + 窗口滑动/驱逐） |
| `transport/internet/splithttp/connection.go` | splitConn writer 侧接 segmentWriter（或 httpSC 加 segment 模式：flush-on-waiter、hold 定时器、EOF 发布） |
| `transport/internet/splithttp/config.go` + `config.proto` + `config.pb.go` | 新配置项（§2.4）；proto 需重新生成 pb |
| `transport/internet/splithttp/dialer.go` | packet-up 分支：`conn.reader` 换 segmentAssembler；首段 GET 探测回退；下载连接配额豁免/摊销 |
| `transport/internet/splithttp/client.go` | OpenStream 支持带 seq+标记头的段 GET（或新 `GetSegment` 方法，复用 `FillPacketRequest` 的元通道） |
| 新 `transport/internet/splithttp/download_segment.go` | 客户端拉取器：窗口/重试/重排/组装/EOF |
| `transport/internet/splithttp/packet_upload.go` | 重试循环与 durable 逻辑提取共享（只读复用，尽量不挪动现有上行热路径） |

### 5.3 双端升级策略

1. **服务端先发**（超集解析）：老客户端（纯 GET）零影响，可直接上生产。
2. **新客户端后发**：首段 GET 探测（空响应即回退 legacy），对老服务端仍可用（降级为现状行为）。
3. 发布说明按既有惯例标注"两端同步升级后启用下行分段"，Release notes 列默认参数与配置项。
4. 若 M1 出问题：客户端配置关 `scMaxEachDownloadBytes`（=0 即回 legacy），服务端删 `x-bray-dseg` 解析即回滚，无需双端同时回滚。

### 5.4 验证方法（对齐仓库既有门禁）

- **单测（download_queue）**：乱序注入重排、gap 超时断流、重复段幂等、窗口满驱逐、410/404/EOF 语义——镜像 `upload_queue_test.go` 全套。
- **wire_audit 扩展**（`wire_audit_test.go` 模式）：段 GET 的 path/参数/标记头形态审计（40 会话池分布）、段大小 ±10% 抖动分布均值检查、段响应头固定性（Content-Type/无 Cache-Control 泄漏）——复用 `TestWireAuditPaddingNamePool`（`:15-53`）与 `TestWireAuditSkewedDistributions`（`:55+`）的套路。
- **场景/端到端**：`go test ./testing/scenarios/ -run 'TestVlessXtls'` 同型测试跑大文件下行（skill 铁律 7c：wire 改动必跑）；新增 httptest 包装器注入"随机丢段响应"验证重试与重排；老服务端模拟（纯 GET 空响应）验证回退路径。
- **性能门禁**：`scripts/bench_compare.sh`（benchstat，count=6）对比下行吞吐与 TTFB——注意 `BenchmarkXHTTP_TTFB` 的口径缺陷（测的是完整连接生命周期，见 `references/xhttp-ttfb-latency.md`），需新增"段模式下行吞吐"bench 且构造方式必须走生产路径（skill 铁律 7a）；内存用 `go test -memprofile` + `go tool pprof -top -alloc_space` 验证段缓存峰值与池复用（铁律 7b）。
- **伪装面实测**：抓包对比新旧 GET 序列（请求间隔分布、段大小分布、Content-Length 分布、并发度），与浏览器视频段拉流样本对照。

---

## 附：方案对比速查

| 方案 | 线格式 | 服务端缓存 | 重试 | CDN 友好 | 工程量 | 结论 |
|---|---|---|---|---|---|---|
| **A. seq 参数段拉取（推荐）** | `GET ?session&seq&x-bray-dseg` | 滑动窗口 + 全局上限 | 同段 3 次幂等重拉 | 中（需去 no-store） | 中 | **首版** |
| B. Range 定长段 | `GET + Range: bytes=` | 同上 | 同上 | 高（浏览器视频同款） | 中高（Range 语义 + 活流叠加） | M1.5 |
| C. 拉取式无缓存 | `GET ?seq`（服务端阻塞生产、不发缓存） | 无 | 无（重拉无源） | 低 | 低 | 仅作 `scMaxBufferedSegments=0` 退化项 |
