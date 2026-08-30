# 上游同步审查报告（Bray-Core `onlyBray` ← XTLS/Xray-core `main`）

- 审查日期：2026-08-30
- 当前分支：`onlyBray` @ `3833cb40`（与 `origin/main` 一致，2026-08-27）
- 上游比对点：`upstream/main` @ `c1958dba`（2026-08-27 22:32:45 UTC）
- 共同祖先（merge-base）：`be8009c6` Geodata: Cleanup unneeded matchers (#6342)，2026-06-19
- 分叉后：上游新增 80 个提交，本地新增 499 个提交

## 分析方法

1. `git fetch upstream --unshallow` 解除浅克隆（原历史仅 28 个提交，无法求 merge-base）
2. `git cherry -v HEAD <upstream>` 用 patch-id 剔除已以等价补丁形式合入的提交（18 个）
3. 对剩余 62 个提交用 `git merge-tree --write-tree --merge-base=<parent> HEAD <commit>` 逐个做只读三方合并，判断是否冲突
4. 由于 fork 对大量文件做了重写（导致 patch-id 失效），另用「新增行在当前 HEAD 同路径文件中的命中率」做语义重合度校验，剔除 fork 已独立实现的提交

**结论：62 个候选提交中，仅 8 个真正值得处理；其余 54 个为已包含、平台无关或无实质价值。**

---

## 一、强烈建议合并（3 个）

### 1. `65458e919fcb3548d44481ea7929031a14bf117e` — 安全修复（最高优先级）

| 项 | 值 |
|---|---|
| 提交信息 | Config: Fix some issues (#6640) |
| 作者 | 风扇滑翔翼 |
| 日期 | 2026-08-27 20:03:59 +0800 |
| 规模 | 6 files, +19 / -12 |
| cherry-pick | **无冲突，可直接应用** |

**理由：** 修复了一处安全校验被完全绕过的缺陷。当前 fork 的 `infra/conf/xray.go` 中：

```go
if requiresTransportSecurity(vlessCfg.Address) {   // fork 现状
if requiresTransportSecurity(tjCfg.Address) {
```

VLESS / Trojan 出站配置的服务端地址实际位于 `vnext[0].address` / `servers[0].address`，顶层 `address` 字段不会被填充，因此 `vlessCfg.Address` 恒为 `nil`，而 `requiresTransportSecurity()` 在 `address == nil` 时直接 `return false`。这意味着 `d7fa2076`（禁止在公网上使用未加密的 VLESS/Trojan 出站）引入的保护在本 fork 中**形同虚设**。上游改为 `vlessCfg.Vnext[0].Address` / `tjCfg.Servers[0].Address` 才真正生效。

同提交还包含：
- `(*Address).Build()` 增加 nil 接收者保护（`infra/conf/common.go`），避免 nil 指针 panic
- 入站 `ListenOn` 增加空字符串判断（`len(c.ListenOn.String()) == 0`），避免空地址通过校验

### 2. `540b9070f5bc0e67a04341f11bde598153957b91` — UDP 出站绑定地址族错误

| 项 | 值 |
|---|---|
| 提交信息 | Transport: Bind the UDP outbound socket in the destination family (#6688) |
| 作者 | Maksim Varentsov |
| 日期 | 2026-08-27 21:46:26 +0000 |
| 规模 | 1 file, +11 / -5 |
| cherry-pick | **无冲突，可直接应用** |

**理由：** `transport/internet/system_dialer.go` 中 UDP 出站 socket 的源地址被硬编码为 IPv4 通配地址：

```go
srcAddr = &net.UDPAddr{ IP: []byte{0, 0, 0, 0}, Port: 0 }   // fork 现状（第 51 行）
```

部分系统不支持 IPv4-mapped 双栈 socket，向 IPv6 目标发包会失败。上游改为按目标地址族选择 `0.0.0.0` 或 `[::]`。fork 此处代码与上游分叉前完全一致，确认未修复。

### 3. `dffc7ada5eef8a8b3df7da8928536ce57135a119` — XHTTP packet-up 无法重放

| 项 | 值 |
|---|---|
| 提交信息 | XHTTP client: Define Request.GetBody() for packet-up so h2 can replay after GOAWAY (#6632) |
| 作者 | FunLay123 |
| 日期 | 2026-08-26 20:41:43 +0000 |
| 规模 | 2 files, +46 / -5 |
| cherry-pick | **有冲突**：`transport/internet/splithttp/config.go`、`config_test.go` |

**理由：** packet-up 请求体未设置 `GetBody`，HTTP/2 连接在收到 GOAWAY 后无法重放请求，导致上传中断。fork 的 `FillPacketRequest`（`transport/internet/splithttp/config.go:817`）使用自研的池化 body（`acquirePacketBody`），同样没有 `GetBody`，缺陷同样存在。

与本分支的 XHTTP/DSEG 工作高度相关。**合并时需手工适配**：保留 fork 的池化优化，同时缓存一份可重放数据供 `GetBody` 返回（不能简单照搬上游的 `bytes.NewReader`，否则会丢掉池化收益）。

---

## 二、建议合并（3 个）

### 4. `aa3d6589da5e28fc3b0303572e4330dfeb7a383c` — macOS TUN 忙等占满 CPU

| 项 | 值 |
|---|---|
| 提交信息 | TUN inbound: Wait() blocks via kqueue instead of busy-spinning on Darwin (#6580) |
| 作者 | Jidos |
| 日期 | 2026-08-26 21:29:58 +0000 |
| 规模 | 2 files, +320 / -5 |
| cherry-pick | **无冲突，可直接应用** |

**理由：** fork 的 `proxy/tun/tun_darwin.go:246` 仍为：

```go
func (t *DarwinTun) Wait() { procyield(1) }
```

即在 macOS 上持续忙等自旋，会长期占用一个 CPU 核。上游改用 kqueue 阻塞等待。若本 fork 有 macOS TUN 用户，修复收益明显。

### 5. `c7e569b0377724600af1ea2a05eb8f4c7c3e0609` — WireGuard 出站 remoteDNS

| 项 | 值 |
|---|---|
| 提交信息 | WireGuard outbound: Add `remoteDNS` & honor TTL (#6620) |
| 作者 | LjhAUMEM |
| 日期 | 2026-08-25 14:28:36 +0000 |
| 规模 | 5 files, +82 / -28 |
| cherry-pick | **无冲突，可直接应用** |

**理由：** 为 WireGuard 出站新增 `remoteDNS` 配置项并正确处理 DNS TTL。全仓检索确认 fork 尚无 `remoteDNS` / `RemoteDNS` 实现，属真实功能缺失。

### 6. `c1958dba04ba065cd82a05b65bfe877e2323f0cc` — FreeBSD TUN 路由特性

| 项 | 值 |
|---|---|
| 提交信息 | TUN inbound: Support `autoSystemRoutingTable` and `autoOutboundsInterface` on FreeBSD as well (#6691) |
| 作者 | brookwko |
| 日期 | 2026-08-27 22:32:45 +0000（上游 main 当前 HEAD） |
| 规模 | 2 files, +877 / -23 |
| cherry-pick | **无冲突，可直接应用** |

**理由：** fork 已有 `proxy/tun/tun_freebsd.go`（163 行），但不支持 `autoSystemRoutingTable` / `autoOutboundsInterface` 两个自动路由特性（macOS/Linux 版本已在本地实现，见下）。若目标部署环境包含 FreeBSD，值得合并补齐。

---

## 三、谨慎评估（2 个，改动大、风险高）

### 7. `ada99a4eb00f169b0e2d650990d77fb7930967bb` — Hysteria 升级至官方 v2.12.2

| 项 | 值 |
|---|---|
| 提交信息 | Hysteria: Upgrade to official v2.12.2 (#6565) |
| 作者 | LjhAUMEM |
| 日期 | 2026-08-27 17:25:56 +0000 |
| 规模 | 22 files, +830 / -138 |
| cherry-pick | 冲突：`go.mod`、`go.sum`、`infra/conf/transport_finalmask.go`、`infra/conf/transport_method.go` |

**理由与风险：** 除 Hysteria 协议栈升级外，还带来了 finalmask realm 的 portmap 新特性、brutal 拥塞控制修正，并顺带改动 `splithttp/dialer.go` 与 `splithttp/hub.go`。风险在于：（a）依赖 fork 尚不存在的 `infra/conf/transport_method.go`；（b）触及 fork 改动较大的 splithttp。**建议单独开分支验证，或仅挑选其中的 brutal 拥塞修正。**

### 8. `fb548f54d22856446e883c6d13b32d60f0dda9bd` — infra/conf 配置层重构

| 项 | 值 |
|---|---|
| 提交信息 | infra/conf/transport_internet.go: Split into multiple files; Rename `network` to `method` (#6426) |
| 作者 | Meow |
| 日期 | 2026-07-07 01:26:15 +0000 |
| 规模 | 7 files, +2304 / -2268 |
| cherry-pick | 冲突：`infra/conf/transport_internet.go` |

**理由与风险：** 纯结构性重构，本身无功能收益，但它是 `18e28390`、`af7eb680`、`e78d8ef1`、`ada99a4e` 等多个上游后续提交的前置依赖。当前 fork 的 `infra/conf/` 下只有 `transport_internet.go`（未拆分）+ 自研的 `transport_authenticators.go` + `transport_finalmask_xmc_test.go`，与上游结构已明显分叉。

**建议：** 如果计划长期跟踪上游，值得做这次对齐以降低后续合并成本；如果只想挑 bugfix，可跳过，改为手工平移后续修复。

---

## 四、不建议合并（54 个）

### 4.1 fork 已包含等价实现（patch-id 不同但语义相同，30 个）

这些提交的代码已在本 fork 中，无需重复合并：

| 提交 | 说明 | fork 中的对应位置 |
|---|---|---|
| `77f98eba` | XHTTP client 竞态与数据竞争 (#6665) | `client.go:76` 已改 `closed atomic.Bool`；`dialer.go` 已在 handoff 前取长度 |
| `8f15190c` | QUIC sniffer 畸形包 panic (#6471) | `common/protocol/quic/sniff.go:146` 已有长度校验 |
| `0bafca94` | Stats GetOrRegister*() 竞态 (#6468) | 96% 行命中 |
| `e7e92546` | TUN nil RemoteAddr panic (#6365) | 100% 行命中 |
| `64fada32` | TLS pinning 需 serverName (#6472) | 100% 行命中 |
| `e4e7614c` | TLS ECH 解析健壮性 (#6441) | 100% 行命中 |
| `c18b39ed` | unsafe fingerprint 更多 cipherSuites (#6450) | 100% 行命中 |
| `d7fa2076` | 禁止公网无加密出站 (#6303) | 100% 行命中（但校验失效，见 #1） |
| `4aba687d` | XHTTP/gRPC 服务端精确 localAddr (#6526) | 100% 行命中 |
| `ac04c445` | DNS TTL 意外截断 (#6363) | `app/dns/dnscommon.go:339` 已有等价逻辑 |
| `583bb4a6` | xPaddingObfsMode 下 scStreamUpServerSecs (#6343) | `hub.go:756` 已有 `obfsPaddingAccepted` 判定 |
| `af7eb680` | REALITY 默认 minClientVer 26.3.27 | `reality_warning_test.go` 已断言该默认值 |
| `e78d8ef1` | REALITY 配置警告完善 (#6508) | 同上，已含 Microsoft/.cn 等风险域名 |
| `18e28390` | XHTTP maxConnections 默认 6→3 | fork 用 `xmuxRangeOrNil(..., 1, 8)` 实现，测试断言默认值 3 |
| `35387572` | Finalmask XMC (TCP, Minecraft) (#6210) | `transport/internet/finalmask/xmc/` 已存在 |
| `6ab123bf` | XMC 方向性 padding / keep-alive (#6487) | 97% 行命中 |
| `345c76f9` | WireGuard 入站动态 peer 管理 (#6360) | 99% 行命中 |
| `452b7195` | Hysteria 入站 vlessRoute (#6375) | 84% 行命中 |
| `3dc8bf3d` | Hysteria 入站动态 UUID (#6395) | 100% 行命中 |
| `d5bc58dc` | Root config `env` 配置 (#6400) | 86% 行命中 |
| `241aa38a` | TUN autoSystemRoutingTable（macOS/Linux）(#6366) | 93% 行命中 |
| `65f6f0a4` | TUN gateway 精化（Linux）(#6398) | 94% 行命中 |
| `3263ae92` | TUN autoOutboundsInterface 修复（Linux）(#6413) | 92% 行命中 |
| `6ce924ad` | TUN 默认名 utunN (#6485) | 100% 行命中 |
| `1d8eb81d` | TUN Windows `desc` 配置 (#6486) | 92% 行命中 |
| `9cd9382e` | TUN Linux `XRAY_TUN_FD` (#6338) | 100% 行命中 |
| `bc6e966a`/`7021606a`/`d3f1a242`/`0604ffa9`/`5fe6d621`/`f02a3578`/`dfdbcf86` | circl 1.6.5、protobuf 1.36.12、stun 3.1.7、miekg/dns 1.1.73、grpc 1.83.1、testify 1.12.1、cpuid 2.4.0 | fork 的 `go.mod` 版本已与上游完全一致（由 `26bf1bfb` 批量同步） |

### 4.2 平台不相关（2 个）

- `a000371b` Tunnel inbound: TPROXY on OpenBSD (#6546)
- `25c11e2d` Tunnel inbound: SO_REUSEPORT fix + IPv6 for OpenBSD transparent proxy (#6624)

fork 全仓无任何 OpenBSD 平台文件（`find proxy transport -name "*openbsd*"` 为空），不具备移植基础。

### 4.3 无实质价值（8 个）

- `45cf2898` / `50231eaf` / `5ca6f4b7` — 版本号标记（v26.6.27 / v26.7.11 / v26.7.28）
- `b12bc504` — README 增加 Magisk 安装项
- `2b329b36` / `fc5620de` — docker/login-action 版本
- `f9eb1597` / `6e3322d2` — GitHub Actions 版本（actions/cache、setup-go）
- `1f74c480` — Docker `pre-release` tag

### 4.4 与 fork 自研实现冲突过大（1 个）

- `f496437b` XHTTP server: Refactor upload_queue.go (#6372) — 该文件上游 143 行，fork 已扩展至 440 行（DSEG 相关），重构会覆盖本地实现，不建议合并。

---

## 五、建议的操作顺序

```bash
# 1. 安全修复 + 两个小修复（均无冲突）
git cherry-pick 65458e919fcb3548d44481ea7929031a14bf117e
git cherry-pick 540b9070f5bc0e67a04341f11bde598153957b91

# 2. 功能补齐（无冲突）
git cherry-pick aa3d6589da5e28fc3b0303572e4330dfeb7a383c
git cherry-pick c7e569b0377724600af1ea2a05eb8f4c7c3e0609
git cherry-pick c1958dba04ba065cd82a05b65bfe877e2323f0cc

# 3. 需手工解决冲突（建议先建临时分支）
git checkout -b merge/xhttp-getbody
git cherry-pick dffc7ada5eef8a8b3df7da8928536ce57135a119
#   手工合并 splithttp/config.go：保留 acquirePacketBody 池化，
#   额外缓存一份 []byte 供 request.GetBody 返回

# 4. 每步之后
go build ./... && go test ./transport/internet/splithttp/... ./infra/conf/... ./app/dns/...
```

## 附：本次分析中遇到的仓库状态问题

- 该克隆为**浅克隆**（`.git/shallow` 边界为 `5ca6f4b7` Xray-core v26.7.28），直接 `git merge-base` 会失败，需先 `git fetch upstream --unshallow`
- `git fetch upstream` 创建的 `refs/remotes/upstream/*` 在本环境未能持久化，`git for-each-ref refs/remotes/upstream/` 返回空；但对象库写入有效，可直接使用完整 SHA 操作
- 由于 fork 对大量文件做过重写，**patch-id 去重存在假阳性**，必须配合语义重合度校验才能得出准确结论
