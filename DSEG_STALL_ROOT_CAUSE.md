# XHTTP + RA + packet-up + dseg 断流根因定位报告

POC：`testing/scenarios/dseg_stall_poc_test.go`（`go test ./testing/scenarios -run TestXHTTPDsegStall_POC -v -count 1`）

## 结论

**根因：dseg 生产腿（production-leg GET）与全部 segment 拉取请求游离于 XMUX 连接会计之外（不持 Borrow、不计 activeStreams、不消耗 LeftRequests）。当该连接的复用预算（CMaxReuseTimes/leftUsage）或生命周期（HMaxReusableSecs/UnreusableAt、maxConnectionAge）耗尽时，XMUX 健康检查在"上传侧换持释放了最后一个 Borrow"之后的任意时刻把该连接判定为"空闲且耗尽"，drain 并硬关闭底层 TLS/H2 连接——而活跃会话的生产腿与 6 个 puller 就在这条连接上。服务端随之删除会话，客户端 puller 陷入 404 重试，下载永久停滞。**

短会话在预算耗尽前已结束，因此表现为"短时间正常、长时间必现"（生产默认 CMaxReuseTimes 64-128 次上传、HMaxReusableSecs 600-1200s、maxConnectionAge 20min）。纯下载流（上传循环不发新 chunk）不会换持、Borrow 永远持有，故不受影响——需要上传型流量（视频通话/SSH/网页交互）才会触发。

## 证据链（POC run4 时间线，实测）

| 时刻 | 事件 | 证据 |
|---|---|---|
| t=0 | 主连接 setup，Borrow client A（act=1，上传侧唯一持槽） | `[DBGBR] borrow OK p=0x...a6e000 act=1`（dialer.go L1065） |
| t≈0.2s | CMaxReuseTimes=2 被主连接+首个短连接耗尽 | drain 时 `leftUsage=0`（mux.go GetXmuxClient 每次 reuse `leftUsage--`） |
| t=22.0s | 上传重试触发 rescue 换持：Borrow B + `prev.Release()` 释放 A 的最后 Borrow | 同一毫秒 `borrow OK p=0x...4f2000` + `release p=0x...a6e000 act=0`（dialer.go rescueClient L1374-1380） |
| ≤5s 后 | healthCheckTick：`leftUsage==0 && activeStreams==0` → maybeDrain + RemoveAt；Draining 态下 tryClose 立即 closeConn() **硬关闭 A 的 TLS/H2 socket** | `XMUX: health-check draining exhausted xmuxClient`（mux.go L979-987；run3 插桩实测 `act= 0 leftUsage= 0`） |
| 同刻 | A 上在途 POST 全部 `use of closed network connection` → Fast Eviction 风暴 | `XMUX: Fast Eviction triggered, marking client dead: Post ...:98: read tcp ...: use of closed network connection` |
| 同刻 | 服务端生产腿 ctx 取消 → defer `downloadLegs.Add(-1)==0` → `session.close()` + `deleteSession()` | 服务端 `[DBGPROD] prodLeg exit: ctx done` + `Push 404 seq= 98 closed= true dllegs= 0 err= packet queue closed` |
| +30s 内 | 客户端 puller 对 404 无限重试（downseg_puller.go worker 无退出条件），下载无进展 | 测试断言：`XHTTP dseg stream closed by peer after 168034304 bytes — mid-download disconnect (断流)` |

## 代码定位

1. **会计洞** — `transport/internet/splithttp/dialer.go`（packet-up dseg 分支，L1147-1174）：
   - 触发拉取 `dc.PullSegment(ctx, &requestURL2, sessionId, "0")`；
   - 生产腿 `dc.OpenStreamAsync(ctx, &requestURL2, sessionId, ...)`；
   - 全部 segment 拉取 `DownSegPuller` worker 的 `PullSegment`。
   三者均直接使用 `httpClient2`（= 主 client），既不 `Borrow()` 也不 `LeftRequests.Add(-1)`。XMUX 的 `activeStreams` 因此恒为 0（除上传侧的 1 个 setup Borrow）。
2. **换持释放** — `dialer.go` rescueClient（L1363-1386）与上传块循环 renew（L1470-1510）：`ownedUploadXmux = newXmux; prev.Release()`。释放后旧 client 的 `activeStreams==0`，但其 H2 连接上仍有活跃的生产腿 + puller。
3. **空闲判定 + 硬关闭** — `mux.go` `healthCheckTick`（L969-987）：`activeStreams > 0` 跳过一切回收；否则 `leftUsage==0 || LeftRequests<=0 || now>UnreusableAt` → `maybeDrain()` → `tryClose()`（activeStreams==0 即 CAS 到 Closed 并 `closeConn()`，force-close 全部 tracked socket）。
4. **会话级联销毁** — `hub.go` 生产腿 handler 的 `defer`：ctx 取消 → `downloadLegs.Add(-1)==0` → `session.close()` + `deleteSession()`；`downSegCache` 随之 finalize/shutdown，后续 pull 全部 404。

## 对照实验

| 实验 | 流量形态 | 结果 |
|---|---|---|
| run1 | 纯下载 4MiB/s，100s（默认上传侧 Borrow 永不释放） | PASS，无断流（418MB 连续） |
| run2/3/4 | 双向：下载 4MiB/s + 上传 320KB/s | **FAIL ~40s**，168MB 处 mid-download disconnect |

同一份客户端 XMUX 预算（缩小版 CMaxReuseTimes=2 等），唯一变量是上传侧是否触发换持——决定性证实因果。

## 修复（已实施，见本仓库工作树）

**设计：下载腿生命周期 pin（downloadPins）** —— 最小、正交、优雅：

- `mux.go`：`XmuxClient` 新增 `downloadPins atomic.Int32`；`tryClose()` 首行加守卫 `downloadPins > 0 → return false`（单一咽喉点覆盖所有卫生关闭路径：预算/生命周期耗尽 drain、空闲/老化/质量 drain、GetXmuxClient CAS 耗尽、Release 触发关闭）。`PinDownload()/UnpinDownload()` 成对 API：最后一个 pin 释放时自动补上被搁置的关闭。
- `dialer.go`：下载腿建立成功后 `xmuxClient2.PinDownload()`（dseg 与 legacy long-GET 共用该分支）；pin 引用记录于 `pinnedDownloadXmux`，由 `splitConn.onClose` 恰好释放一次。pin **不跟随**上传轮换（新 client 按定义不承载既有下载），不存在泄漏路径。

关键正交性：pin ≠ Borrow——不进 `activeStreams`，不影响选择评分/准入/`xmuxClientReusable`（耗尽的 pinned 连接只服务既有下载腿、不接新活，处于 StateDraining 且已被移出池，不重复参与回收）；真实故障路径 `MarkDead`（Fast Eviction）绕过 `tryClose`，坏连接仍被立即强杀。

### 验证（A/B 对照，同一 POC：4MiB/s 下载 + 320KB/s 上传 + 生命周期 HMaxReusableSecs=2s）

| 版本 | 结果 | 失败签名 |
|---|---|---|
| 基线（无修复） | **FAIL @ 27.4s** | `STALLED: no progress for 20s — 断流 reproduced`（连接未关、数据停滞 = 生产症状） |
| 修复后 | **PASS 100s 满速 4.0MiB/s** | — （期间多次 `health-check draining exhausted xmuxClient`，drain 被 pin 搁置，下载无感知） |

回归：`TestVLESSXHTTP_LongIdleConnectionReuse`、`TestVlessXHTTPRealityPacketUpDseg`、`TestVLESSXHTTP_SessionChurnNo404`、`TestVLESSXHTTP_UploadBackpressureScenario` 全部 PASS；splithttp XMUX 单测全绿。`TestVlessTLSPacketUpDsegPlain` 与 `TestXHTTP_NoDrop_Continuous` 在未修改基线上同样失败/挂起，为分支存量问题，与本修复无关。
