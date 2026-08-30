# 上游提交合并报告（2026-08-30）

分支：`merge-upstream-2026-08-30`　基线：`3833cb40`　上游：`XTLS/Xray-core` @ `c1958dba`

---

## 1. 合并结果

按拓扑序 cherry-pick，7/7 成功：

| # | 提交 | 标题 | 冲突 | 处理方式 |
|---|------|------|------|----------|
| 1 | `6bd9e448` | WireGuard outbound: Add `remoteDNS` & honor TTL (#6620) | 无 | 直接应用 |
| 2 | `805d011f` | XHTTP client: Define `Request.GetBody()` for packet-up (#6632) | **有** | 手工适配 fork 池化实现 |
| 3 | `d4f17ec2` | TUN: `Wait()` blocks via kqueue instead of busy-spinning (Darwin) (#6580) | 无 | 直接应用 |
| 4 | `e213cfbb` | Config: Fix some issues (#6640) | 无 | 自动合并（**安全修复**） |
| 5 | `93550e14` | Hysteria: Upgrade to official v2.12.2 (#6565) | **复杂** | 手工移植配置层 + go.mod |
| 6 | `62f45580` | Transport: Bind UDP outbound socket in destination family (#6688) | 无 | 直接应用 |
| 7 | `cf330b6d` | TUN: FreeBSD `autoSystemRoutingTable` / `autoOutboundsInterface` (#6691) | 无 | 直接应用 |

工作区与基线 `3833cb40` 对比：新增 3 个文件、修改 35 个、**删除 0 个**。

---

## 2. ⚠️ 破坏性变更（必须知会用户）

`e213cfbb` 修复了一个此前**完全失效**的安全校验。

**修复前**：`validateOutboundTransportSecurity()` 读取 `vlessCfg.Address` / `tjCfg.Address`，
但 VLESS/Trojan 出站的真实地址在 `vnext[0].address` / `servers[0].address`，顶层字段恒为 `nil`，
`requiresTransportSecurity(nil)` 返回 `false` → **"禁止公网明文 VLESS/Trojan" 规则从未生效**。

**修复后**：地址源改为 `Vnext[0].Address` / `Servers[0].Address`，规则真正生效。

> **升级后，任何指向公网地址且未启用 TLS 的 VLESS / Trojan 出站配置将无法启动。**
> 报错：`vless without TLS or other encryption is prohibited unless the server address is a private IP or domain`

已确认 `Vnext[0]` / `Servers[0]` 无越界风险：两个 `Build()` 都在调用校验前强制
`len(Vnext) != 1` / `len(Servers) != 1` 报错。

**域名公私判定边界**（写配置/测试时易踩坑）：
`geodata.GetPrivateDomainMatcher()` 按**子域**匹配 RFC 2606 保留 TLD 与局域网后缀：

| 域名 | 判定 |
|------|------|
| `host.example`、`box.lan`、`localhost`、`a.internal`、`x.invalid` | 私有（放行） |
| `example.com`、`cdn.cloudflare.com` | **公网**（拒绝） |

`example.com` 是 IANA 持有的真实公网注册域，**不是** `example` 保留 TLD 的子域。
仓库内 `infra/conf/bray_session_seed_test.go` 已据此修正（`example.com` → `host.example`）。

---

## 3. 冲突解决要点（供后续合并参考）

### 3.1 `805d011f` — XHTTP `GetBody` 与 fork 池化实现的冲突

上游把 `payload` 的释放**提前**，会破坏 fork 的池化路径（`packetBody.Close()` 负责把
`MultiBufferContainer` 归还 `sync.Pool`）。采用的方案：

- **非热路径 `FillPacketRequest`**：保留 fork 的池化 `acquirePacketBody(payload)`，
  仅在此时**急切**拷贝一份 `replay` 快照供 `GetBody` 使用。
  急切拷贝是必需的：首次尝试就会消费掉 `payload`，且 `Close()` 会把 buffer 归还池子，
  之后可能被其他 goroutine 复用 —— 重放会读到脏数据。
- **热路径 `FillPacketRequestBytes`**：调用方本来就持有持久 `[]byte` 快照，
  用零拷贝 `acquireDurableBody(data)` 重放，不额外拷贝。

### 3.2 `93550e14` — Hysteria v2.12.2 与 fork 未做的配置层拆分

fork **没有**做上游 `fb548f54` 的 `infra/conf/transport_internet.go` 文件拆分。
上游改在 `transport_method.go` / `transport_finalmask.go` 的内容，已手工平移回未拆分的文件：

- `QuicParamsConfig` 新增 `BrutalDisableLossCompensation`、`DisableChromeParrot`、
  `DisableGSO`、`DisableStatelessReset`
- `Realm` 新增 `IPMode`、`PortMapping`
- `Masquerade` 新增 `XForwarded`

`go.mod`：`klauspost/compress` 保留 fork 的 v1.19.0；新增 `koron/go-ssdp v0.0.4`；
`apernet/quic-go` 被 Hysteria v2.12.2 顶到 `v0.61.1-0.20260806010916-184d081eef3e`；
`Maolaohei/REALITY v0.3.0` + replace 本地路径保持不变。

---

## 4. POC 验证

共新建 13 个 POC 用例，**全部通过**。

### 4.1 安全修复 `e213cfbb`

`infra/conf/outbound_transport_security_poc_test.go`（`package conf`）

- `TestPOC_VlessPublicAddrWithoutTLS_Rejected` — 公网 IPv4 + 无 TLS → 拒绝
- `TestPOC_TrojanPublicAddrWithoutTLS_Rejected` — 同上，Trojan
- `TestPOC_PublicDomainWithoutTLS_Rejected` — 公网域名 → 拒绝
- `TestPOC_PrivateAddrWithoutTLS_Allowed` — 私网 IP（3 例）→ 放行
- `TestPOC_DomainPrivateBoundary` — 域名公私边界矩阵（7 例）
- `TestPOC_TLSSenderShortCircuits` — 已启用 TLS 直接放行
- `TestPOC_AddressSourceIsVnext` — **回归保护**：防止有人把地址源改回顶层 `Address`

### 4.2 XHTTP `GetBody` `805d011f`

`transport/internet/splithttp/xhttp_getbody_poc_test.go`（`package splithttp`）

上游自带的 `Test_FillPacketRequest_GetBody` 覆盖较弱 —— 它在**首 body 尚未关闭时**就重放，
从未走到释放路径。本 POC 采用**真实 GOAWAY 时序**：消费 → `Close()` → `GetBody()`。

- `TestPOC_HotPathReplayAfterClose` — 热路径关闭后重放 + 零拷贝不变量
- `TestPOC_HotPathRepeatedReplays` — 连续 3 次重放各自从 offset 0 读
- `TestPOC_NonHotPathReplaySurvivesBufferRelease` — **核心回归**：关闭首 body 释放
  MultiBuffer 后，用 64 次 `buf.New()` 搅动池子制造复用，再重放必须仍是原数据
- `TestPOC_ReplayBodiesAreIndependent` — 两次 `GetBody()` 必须返回不同 reader
- `TestPOC_PlacementVariantsDefineGetBody` — 4 种 placement 全覆盖

**负向对照（关键）**：临时移除 3 处 `GetBody` 赋值后，**5/5 全部 FAIL**，
确认 POC 有真实检出能力而非恒真。恢复后 `git diff --stat HEAD` 零残留。

### 4.3 UDP 地址族 `62f45580`

`transport/internet/udp_addr_family_poc_test.go`（`package internet_test`）

- `TestPOC_UDPWildcardBindFollowsDestFamily` — IPv4/IPv6 目标分别绑定对应地址族 + 回环收发
- `TestPOC_UDPSrcStillOverridesWildcard` — 显式 `sendThrough` 仍然优先

> **平台陷阱**：Windows 上 `ListenPacket("udp","0.0.0.0:0")` 的 `LocalAddr()` 返回
> `[::]:port`（双栈套接口），**不能**用来断言请求的地址族。已实测确认，
> 因此 IPv4 方向只做功能断言，仅 IPv6 方向做严格族断言。

---

## 5. 回归测试

| 范围 | 结果 |
|------|------|
| `go build ./...` | 通过 |
| `GOOS=darwin go build ./proxy/tun/...` | 通过 |
| `GOOS=freebsd go build ./proxy/tun/...` | 通过 |
| `go vet ./transport/internet/... ./infra/conf/... ./proxy/wireguard/...` | 通过 |
| `./infra/conf/...` | 通过（修 `bray_session_seed_test.go` 后） |
| `./transport/internet/` | 通过 |
| `./transport/internet/splithttp/` | 通过（453s） |
| `./transport/internet/hysteria/...` | 通过 |
| `./transport/internet/finalmask/...` | 通过 |
| `./proxy/wireguard/...` | 无测试文件 |
| `./transport/... ./proxy/... ./testing/... ./app/... ./infra/... ./features/...` | **61 个包全部通过，0 失败**（17 分钟） |

---

## 6. 合并过程中的事故与修复

1. **`git rm -f` 误删整个 `infra/`（72 文件）**：路径解析异常导致删除范围扩大。
   立即 `git checkout HEAD -- infra/` 全量恢复，随后重新手工应用配置改动。
2. **带斜杠分支名 `merge/upstream-2026-08-30` 产生 unborn 分支**：该仓库 git 无法在
   `refs/heads/` 下建子目录。用 `git symbolic-ref` + `git reset --mixed` 完整恢复（0 丢失），
   改用扁平分支名。
3. **备份文件路径混乱**：Git Bash 的 `/tmp` 与仓库相对路径混用导致恢复一度找不到备份。
   后续统一使用工作区内临时目录（如 `.pocbak/`）并及时清理。

---

## 7. 后续：XHTTP 专项（性能 + 稳定性）

已有研究基础：

- `docs/xhttp-tcp-over-tcp-report.md` — 双层 TCP 的 RTT 耦合与缓冲耦合诊断、优化分档
- `docs/xhttp-downlink-segmentation-m1-report.md` — 下行分段 M1 设计
- 下行分段 **M1 已落地**：`downseg.go`（582 行）、`downseg_puller.go`（433 行）
- 端到端基准已就绪：`testing/scenarios/downlink_bench_test.go`
  （`BenchmarkXHTTPModes_Downlink` / `BenchmarkLegacyLongGETDownlink` / `BenchmarkDsegRealDownlink`）

### 7.1 dseg 下行自拆除 —— 已验证修复

用户报告的问题：64MB 下载 2-4 秒后中断，curl(18)，`downseg_puller` EOF → fatal。
级联路径为：生产腿提前退出 → 会话关闭 → 兄弟段拉取 EOF。

复现用例 `testing/scenarios/dseg_downlink_stall_test.go`（此前未纳入版本控制），
合并后实测：

| 用例 | 结果 |
|------|------|
| `TestDsegLargeSustainedDownload` | PASS (36.4s) |
| `TestDsegDownlinkStallBeyondDownlinkOnly`（源站中途 stall 2s） | PASS (16.0s) |
| `TestDsegLargeFileEcho64MiB`（16×4MiB 逐字节校验） | PASS (1.6s) |

**结论：该问题在当前代码上已不复现。** 建议把这份复现用例纳入版本控制作为长期回归门。

### 7.2 优化路线图落地审计（对照 `xhttp-tcp-over-tcp-report.md` §5）

| 档位 | 方案 | 状态 | 证据 |
|------|------|------|------|
| L1 | lossy 反向 AIMD（升连接数、降每连接并发流） | ✅ 已落地 | `mux.go:1252-1253` Reverse AIMD on lossy/saturated |
| L2 | packet-up 窗口/重试联动 | ✅ 已落地 | `packet_upload.go:21` MaxAttempts=4；`:130` 窗口按 RTT 自适应 |
| L3 | stream 上行管道有界容量 | ✅ 已落地 | `dialer.go:1264` `pipe.WithSizeLimit(...)` |
| M1 | 下行多段 GET | ✅ 已落地 | `downseg.go` / `downseg_puller.go` |
| M3 | 外层 socket 参数（BBR + 缓冲） | ✅ 已落地（仅 Linux） | `tune_socket_linux.go:26-30` BBR + 2MiB SND/RCVBUF |
| L4 | 会话失败粒度收敛（断连只影响本连接） | ❓待审计 | `dialer.go` 上传循环错误处理 |
| M2 | XMUX 参数配置化 + lossy 联动 | ❓待审计 | `xmux_adaptive.go` / `xmux_control.go` |
| M4 | H2 客户端窗口瘦身（需 fork x/net） | ❓待审计 | `dialer.go` H2 窗口参数 |
| B1 | XHTTP/3 转正为主通道 | ❓待审计 | `h3_fallback.go` 目前 H3 仍为 fallback |

### 7.3 性能基准设施

`testing/scenarios/downlink_bench_test.go` 已具备端到端下行基准：

- `BenchmarkXHTTPModes_Downlink` — 跨模式（stream-one / stream-up / packet-up）下行对比
- `BenchmarkLegacyLongGETDownlink` — dseg 关闭的 A/B 基线
- `BenchmarkDsegRealDownlink` — 真实 dseg 路径（VLESS inbound → 生产腿 → 段缓存 → 拉取器）

按报告 §6 的纪律：改动走 `scripts/bench_compare.sh` + ABAB 交替验证；
涉及 wire 形态的改动跑 `wire_audit_test.go`。
