# B1/M1 下行分段 — 生产通道设计点（续接指引）

状态：B1 前置（ref-count）+ 段缓存结构已完成；本文件记录"下行生产通道"这一关键未决设计，供续接会话直接推进。

## 现状（已确认的事实）

- splithttp 服务端**自身不 dispatch**：`ListenXH` 产出 `stat.Connection`，由上层（proxy/vless inbound）转成连接并发起 `dispatcher.Dispatch`。
- **下行（目标响应 → 客户端）写入口 = GET 下载腿创建的 `httpServerConn`**：客户端保持一条"长 GET 下载腿"，其响应体就是下行推流的载体（`httpServerConn.Write` → ResponseWriter → 客户端）。改造成 `conn.reader = currentSession.uploadQueue`（上行读），`conn.writer = httpServerConn`（下行写）。
- **没有长 GET 腿就没有下行写目标** → 下行数据无出口。

## 因此段模式必须有一条"下行生产通道"，两条路线

### 路线 A：建连即生产（推荐，伪装最佳）
- 客户端**建连时**（POST 上传腿之后 / 首个包）协商段模式；服务端**在 dispatch 时刻**把 downlink writer 直接接到 `session.downseg`（而非等 GET 腿建 `httpServerConn`）。
- 客户端不再发长 GET，只发 `GET+seq` 段拉取缓存。
- 伪装形态 = 多段短 GET（HLS/DASH），无长 GET 暴露。
- **改动深**：需改上层 inbound 如何拿到 session 的 downlink writer（splithttp 需要暴露"段模式写目标"，vless inbound 在 dispatch 时选择）。双端都要新客户端。

### 路线 B：保留一条"生产长 GET"（改动小，伪装次优）
- 客户端保持一条标记 `x-bray-dseg-prod` 的长 GET 作为生产通道；`httpServerConn.Write` 在段模式下转存 downseg（响应体占位/空），段 GET 拉缓存。
- **代价**：仍有一条长 GET（伪装改善有限）；但改动集中在 splithttp 内、双端协同简单。

### 判定
- 若目标是明确的"抗GFW提升"（多段短 GET 形态）→ **路线 A**，需要连上层 inbound 一起改（一个完整会话级项目）。
- 路线 B 作为阶段性中间态（先让段拉取跑通、可回归），后升级 A。

## 续接任务清单（下次会话起步）

1. downseg 已就绪（append/get/finalize/滑动，单测绿）——直接接生产。
2. 生产通道选路（A 或 B），先 B 打通端到端（splithttp 内改动），再评估是否上 A。
3. httpServerConn 加 `sess *httpSession` + 段模式 CAS；`Write` 分流：段模式 → `sess.downseg.append`；`Close` → `finalize`。
4. ServeHTTP GET 分支：`GET + seq != "" + 标记头` → 段拉取 handler（读缓存：命中返回；未产出轮询 ≤2s → 404；Gone → 410；EOF → 结束）。
5. 客户端段拉取器：并发窗口 W、≤3 次重试（复用 packet_upload 机制）、按 seq 重排（复用 uploadQueue 堆）。
6. 兼容：老客户端长 GET 走原路径（超集）；新客户端对老服务端"首段空响应"-回退长流。
7. 测试：段 handler 单测 → 客户端段拉取单测 → 双端场景 → 弱网耐力。

## 已完成（2026-08-18，onlyBray 分支 12 commits 领先 main）

- 服务端：downseg 段缓存 + 生产路由（httpServerConn.Write 转缓存 + Close finalize）+ GET+seq 段拉取 handler（200/410/404/EOF-空200）—— commit 895ba80d / d6567c43。
- 客户端：`DownSegPuller`（**顺序版**，窗口=1） + `PullSegment` + `FillStreamRequest` 尊重 seq + dialer 接线（`x-bray-dseg` 控制头门控，默认关）。commit b6e18aa6 / ae37ff33。
- 测试/基准：段缓存单测、服务端拉取闭环、真实 httptest 双端集成（3 段重组字节对字节）、吞吐基准。
- **吞吐现状（本机）**：顺序拉取 **81 MB/s** vs legacy 长 GET **348 MB/s**（~23%）——受顺序窗口限制。

## P1 并发段拉取窗口（下一步，明确价值 + 已有卡点记录）

目标：81 → 数百 MB/s（窗口 W=6 并发）。

**已在尾段尝试并回滚**（保全绿的顺序版）。卡点与教训（供续接直接修复，勿重蹈）：
1. **EOF 前置误判（已定位并修过）**：worker 预拉超前到流尾（seq ≥ produced）时，服务端 over 回**空 200**——不能立即 return，需 `eofAt=seq` + **补完 eofAt 之前所有缺失段**再判 EOF。否则早期段没拉就 EOF → Read 等挂。
2. **window 判定用有符号** `int64(produced)-int64(consumed)`（`produced--`(404退位) 可致 `produced<consumed`，uint64 下溢 → 误判 window 满 → worker 永等）。
3. **未收敛的卡点**：worker 阻塞在某 HTTP 请求未返回（`persistConn.Read` 阻塞，但 dump 无 `ServeHTTP` goroutine）——疑似 http keep-alive 连接池/服务端并发交互，**需在 fresh 会话带 http 并发上下文专项调试**（加上请求级日志/超时，逐步收敛）。
4. 设计骨架：单 worker 滑动窗口（produced/consumed + buf map + skip map + eofAt + wake channel），`DownSegWindowSize=6`；接口/测试与顺序版一致（同 `NewDownSegPuller` 签名）。
5. 实现纪律：once 超长会话尾段做并发正确性改造易出隐蔽 bug（本会话多次手滑破坏性 patch）——**P1 必须独立 commit + 乱序/race/EOF 断言测试 + 双端联调后再上默认关门控**。

## 外部审计（LLM-as-verifier，双 verifier）处置记录（2026-08-19）

两次独立审计逐步验证并修掉了 **M1 端到端死路**（Verifier-1 HIGH#1 = Verifier-2 headline）。最终逐条裁决：

| 发现 | 级别 | 处置 | commit |
|---|---|---|---|
| 生产腿未接线 = M1 端到端死路 | HIGH | 镜像 legacy 腿建 splitConn+addConn | 726fe7e7 |
| 404 重试丢弃保留 seq = 流死锁 | HIGH | 内层循环重试同一 seq | 4e9a305f |
| trigger 拉取 +2s TTFB | MED | 新会话首拉立即 404（fast-path） | cf83e132 |
| 410 静默跳 = 字节流损坏 | MED | 410 → 协议错误（不静默 skip） | cf83e132 |
| s.downseg 数据竞争 | MED | → atomic.Pointer | 4e9a305f |
| 默认 ON 对 legacy 黑盒 | MED | 仅双端 Bray 前提，文档标注；回退探测后项 | 保留默认 ON |
| 生产腿僵尸 8MiB+goroutine | MED | 生产腿自身 idle 兜底回收（30min 无 pull/produce 即关） | 94877f1b |
| 固定 1MiB/W/节律 = 聚类特征 | LOW | 段大小右偏 ±10% 抖动 + 404 backoff 抖动（客户端盲拉 seq，协议零影响） | 94877f1b |
| 无 Content-Type | LOW | **决策：不加**（固定 media type 反成新固定指纹；默认 octet-stream 更像普通下载） | — |
| 全局 cache 无 aggregate cap | MED | 单会话 8MiB bounded 已够；每客户端仅 1 个 dseg 会话，接受 | — |
| LeftRequests underrun 无下限 | LOW | 无害 unbalanced（永久生产腿不 idle 排水），跳过 | — |

**违反的教训（记录在案）**：Verifier-1 ANY 测试全绿却功能全死——**单包内造 production leg 的测试绿了不代表 transport 可用**。真实生产腿必须 VLESS inbound 参与才算数，故 flaky 的单包"真实生产腿"测试（downseg_protocol_test.go / downseg_prodconn_test.go）已删除，改由**双端（VLESS）集成**验证——这必须发生在 push 之后的双端联调阶段。

