# XHTTP dseg 静默截断修复报告（2026-08-30）

## 1. 症状

大文件下载在 dseg（下行分段 M1）开启时**静默丢字节**：不报错、连接正常关闭，
但交付的字节数少于实际值。

复现（`testing/scenarios/dseg_slow_reader_poc_test.go`，64 MiB，客户端以
256 KiB/5ms 的节奏慢速读取）：

```
download truncated at 37842012/67108864 bytes: unexpected EOF
```

同一条链路上把 `dsegDisable: true` 切到 legacy 长 GET 路径，完整交付
67108864 字节 —— 确认是 dseg 特有问题，不是测试装置的问题。

之所以长期未被发现：现有 dseg 场景用例要么 `io.Copy(io.Discard, conn)`
（尽可能快地排空），要么做逐字节 echo（读取节奏跟得上生产节奏）。两者都不会
让"生产快于消费"的窗口真正张开。

## 2. 根因

两个独立缺陷叠加。缺任何一个都不会造成这么大的丢失。

### 2.1 服务端：生产腿在 finalize 后立刻拆除会话

`hub.go` 生产腿 handler 的退出条件之一是 `httpSC.Wait()`，即生产者调用了
`httpServerConn.Close()`（→ `downsegFinalize()`）。它的 `defer` 会把
`downloadLegs` 减到 0，从而 `session.close()` + `deleteSession` →
`downSegCache.shutdown()`。

问题在于生产者和消费者被段缓存解耦，而缓存允许服务端领先客户端最多
`downsegAdaptiveSegs = 64` 段（≈64 MiB）。一个快源站会在**交付还差几十 MiB**
的时候就走到 EOF。此时：

* 已产出但客户端还没拉的段随会话一起消失，之后所有拉取都 404；
* 客户端无法重试恢复 —— 数据在上游已经没有了。

### 2.2 客户端：`Read()` 先判 `fatal` 后查 `buf`

`DownSegPuller.Read()` 的检查顺序是 fatal → buf，注释说明是为了"上传侧不要在
已失效的 sessionId 上继续 POST"。而 `monitorProductionLeg` 对**任何**生产腿 EOF
都调 `failProductionLeg` 设 fatal —— 包括传输正常结束时那个良性的 EOF。

于是生产腿一关，worker 已经预取、应用还没读的段（最多
`prefetchAheadSegs = DownSegWindowSize*4 = 24` 段 ≈24 MiB）被整批丢弃。

### 2.3 探针实测

在旧代码 `Read()` 的三个终态分支上插临时日志，得到决定性证据：

```
[1.908s] [DBGFIN]  finalize produced: 70              ← 服务端 1.9s 产完全部 70 段
[1.908s] [DBGPROD] prodLeg exit: httpSC.Wait          ← 生产腿立刻退出
[1.607s] [DBGRD]   FATAL branch c= 40 buffered= 25 err= XHTTP dseg production leg closed: EOF
```

客户端消费到第 40 段时，`p.buf` 里还躺着 **25 段（≈25 MiB）**被 fatal 分支
丢弃。注意 `err` 是干净的 `io.EOF`，证实这是良性收尾而非异常拆除。

补充一个诊断陷阱：测试看到的是 `unexpected EOF`，这是 `io.ReadFull` 把
"读到部分字节后遇到 EOF"包装后的结果；代理把真实错误（`XHTTP dseg production
leg closed: ...`）在链路上报后就拆了连接，应用侧 socket 只看到 EOF。按表面错误
文本去排查会误判成"过早的干净 EOF"。

## 3. 修复

### 3.1 服务端：排空保持（`hub.go` + `downseg.go`）

生产腿在生产者结束后**不再立刻返回**，而是等到客户端真正收完为止：

* `downSegCache` 新增 `eofServed`，由 `handleDownSegment` 在应答"空 200 EOF 标记"
  时置位 —— 这是客户端"已看到流末尾"的凭据。
* 新增 `drained()`：`final && eofServed && len(segs) == 0`。三个条件缺一不可，
  尤其"EOF 已应答但仍有未交付段"必须判定为**未排空**（那正是会截断的情形）。
* 新增 `holdDrainLeg()`：轮询 `drained()` 直到成立，或请求 ctx 取消，或客户端
  彻底停止拉取超过 `downsegDrainGrace = 2min`（`idleFor()` 由每次 `get()` 刷新，
  所以只要是还在慢慢拉的客户端就不会被误伤）。

### 3.2 客户端：先排空、延后判死（`downseg_puller.go`）

* `Read()` 顺序改为 **buf → skip → eofAt → fatal → prodErr**：客户端已经拥有的字节
  绝不因为另一条腿上发生的事被丢弃。已知流末尾（`eofAt`）优先于任何待处理错误，
  所以正常收尾仍然返回干净的 `io.EOF`。
* `failProductionLeg` 从"立即 fatal + cancel"改为**延后**：只记录 `prodErr` 并
  刷新 `lastProgress`，不取消 ctx（取消会立刻杀掉还在拉取的 worker）。
* 用**停摆检测**取代盲目的快速失败：`lastProgress` 在任何推进（拉取到段、发现
  EOF 标记、消费掉一段）时刷新；停摆超过 `downsegStallGrace = 30s` 才把
  `prodErr` 升级为错误返回，并 `cancel()` 收掉 worker。
* worker 在 `prodErr` 已设且 404 停摆超时后主动退出，避免在已删除的会话上
  形成拉取风暴（同时把 404 退避拉长 4 倍）。

### 3.3 为什么延后判死不会让上传侧 POST 到失效会话

原注释担心的是排空期间上传侧继续 POST。排空期间服务端会话仍然存在（这正是
`holdDrainLeg` 的保证），所以 POST 是有效的；排空结束后下行已经完整，上行的
语义与旧的"生产腿一关就拆"完全一致。

## 4. 验证

### 4.1 2×2 分量验证（证明两处修复都是必需的）

| 服务端 | 客户端 | 结果 |
|---|---|---|
| 旧 | 旧 | FAIL @ 40.2 MiB（丢 26.9 MiB） |
| 新 | 旧 | FAIL @ 43.1 MiB（丢 24.0 MiB） |
| 旧 | 新 | FAIL @ 66.8 MiB（丢 0.28 MiB，且多花 30s 停摆等待） |
| **新** | **新** | **PASS @ 67.1 MiB 完整交付** |

第三行尤其关键：只修客户端能救回 24 MiB 的本地缓冲，但服务端会话一删，
最后 0.28 MiB 永远拿不回来。

### 4.2 新增 POC 用例

`transport/internet/splithttp/downseg_drain_poc_test.go`（5 个，全部毫秒级）：

* `TestPOCProductionLegDeathDoesNotDiscardPrefetchedSegments` — 客户端顺序
* `TestPOCProductionLegDeathStillFailsOnAStalledStream` — 停摆检测安全栏
* `TestPOCDownSegCacheDrainedRequiresEofAndEmptyCache` — `drained()` 三条件
* `TestPOCHoldDrainLegWaitsForClientDrain` — 排空保持的持有/释放
* `TestPOCHoldDrainLegHonoursContextCancel` — ctx 取消可中断保持

`testing/scenarios/dseg_slow_reader_poc_test.go` — 端到端 64 MiB 慢速消费者门。

### 4.3 现有用例调整

`TestDownSegPullerProductionFailurePreemptsBufferedSegments` 断言的正是"终态条件
抢在缓冲区前面"，即被修掉的 bug 本身。已替换为
`TestDownSegPullerHardFailureSurfacesAfterBufferedPrefix`：硬错误仍然是终态的，
但当前位置已有的字节要先交付（那些字节位于错误之前，是有效数据）。

### 4.4 回归

* `./testing/scenarios -run 'TestDseg|TestVLESSXHTTP'`：**14/14 PASS**
  （含 `TestVLESSXHTTP_WeakNetScenario`，它之前的日志里就有那条
  `XHTTP dseg production leg closed: unexpected EOF`，是本次调查的起点）
* `./transport/internet/splithttp -race -run 'TestPOC|TestDownSeg|TestDownseg'`：
  **27/27 PASS，无竞态告警**
* `scripts/bench_dseg.sh both 6`：见 §5

## 5. 性能影响

排空保持会让会话在生产者结束后多存活一小段时间。实测 `drain complete` 与
`producer finished` 之间只差 ~200ms（客户端 worker 在 6 并发下很快就能把剩余段
拉完），因此对稳态吞吐无影响。唯一新增的开销是每个会话结束时最多
`downsegDrainPoll = 100ms` 的轮询延迟，且发生在客户端已经收完之后。

风险与权衡：客户端彻底停止拉取时（进程消失 / TCP 半开 / 放弃下载），会话连同
其最多 ~64 MiB 段缓存会被多持有 `downsegDrainGrace = 2min`。这比修复前"立即拆除"
更占内存，但有界，且覆盖的人群与 finalize 前的僵尸会话清扫器完全一致。
