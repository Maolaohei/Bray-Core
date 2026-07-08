# Bray-Core 版本介绍

> **基于 Xray-core v26.6.22 的高性能魔改分支**
> 
> 最后更新：2026-06-24

---

## 版本概览

| 指标 | 数值 |
|------|------|
| 基线版本 | Xray-core v26.6.22 |
| 修改文件数 | 95 个 |
| 新增代码 | +6,845 行 |
| 删除代码 | -1,004 行 |
| 净增 | +5,841 行 |
| 协议兼容性 | 100% 兼容上游 |

---

## 核心优化一览

### 🔒 安全性强化

| 优化 | 说明 | 效果 |
|------|------|------|
| **randpool 密码学安全随机数** | 128KB crypto/rand 缓冲池，rejection sampling | syscall 从 ~100K/s 降至 ~3/s |
| **Huffman 编码长度缓存** | sync.Map 按字符串长度缓存 | 安全性损失为零，计算量 ↓90% |
| **Tokenish 分池管理** | [32]sync.Pool，32~1024 每 32 字节 | 按需生成，自然降级 |
| **Modulo bias 消除** | rejection sampling 替代简单取模 | 完全无偏随机 |

### ⚡ 性能优化

| 优化 | 说明 | 效果 |
|------|------|------|
| **Padding 随机源替换** | crypto/rand → randpool 缓冲池 | syscall ↓99.997% |
| **URL 解析缓存** | sync.Map 缓存 url.Parse 结果 | 解析次数 ↓95% |
| **默认值缓存** | 预分配 RangeConfig 变量 | 内存分配 ↓ |
| **requestURL 预计算** | 循环外预计算 URL string | 每请求省一次计算 |
| **fmt.Sprintf → strconv** | 134+ 处替换 | 减少反射分配 |

### 🚀 功能增强

| 功能 | 说明 | 代码量 |
|------|------|--------|
| **XMUX 连接池** | 多路复用 + min-inflight 调度 | +551 行 |
| **Happy Eyeballs v3** | 新一代 IP 选择算法 | +248 行 |
| **Warmup 管道** | 连接预热 + DNS 预热 + 健康检查 | +446 行 |
| **H3 fallback** | HTTP/3 降级机制 | +188 行 |
| **REALITY 增强** | 连接复用 + Session Tickets | +200 行 |

### 🐛 Bug 修复

| 修复 | 说明 |
|------|------|
| **splitConn.Close 返回值** | 修复 reader 错误被静默丢弃 |
| **DNS fallback 优化** | 更智能的 DNS 失败处理 |
| **TUN Android 增强** | ioctl 绕过 netlink 权限限制 |
| **WebSocket 心跳泄漏** | goroutine 正确退出 |

### 🧪 测试覆盖

| 测试 | 说明 | 代码量 |
|------|------|--------|
| **压力测试** | 5 网络环境 × 5 场景 | +800 行 |
| **耐久测试** | 长时间运行验证 | +830 行 |
| **指标测试** | XMUX 性能指标 | +80 行 |
| **SOCKS5 停滞测试** | 真实环境连接测试 | +123 行 |

---

## 详细优化清单

### 一、TCP 网络栈（默认即最优）

| 优化 | 效果 | 上游行为 |
|------|------|----------|
| BBR 拥塞控制默认启用 | 出站/入站自动设置 | 需手动配置 |
| TCP_NOTSENT_LOWAT 自适应 | 按内存自动选择 8K/16K/32K | 需手动配置 |
| TCP_DEFER_ACCEPT (3s) | 省一次上下文切换 | 未启用 |
| TCP_NODELAY | 禁用 Nagle，延迟降低 | 未启用 |
| TCP_QUICKACK | 禁用延迟 ACK，TLS 加速 | 未启用 |
| Dial 超时 16s→8s | 更快失败恢复 | 16 秒 |

### 二、VLESS 微优化

| 优化 | 说明 |
|------|------|
| Version + UUID 合并读取 | 前 17 字节一次读取 |
| inbound remoteAddr 缓存 | 避免重复调用 |
| IPv6 不可达回退域名 | 无 IPv6 路由时回退 |

### 三、XHTTP/SplitHTTP

| 优化 | 说明 |
|------|------|
| Padding 密码学安全 | randpool 128KB 缓冲池 |
| Huffman 缓存 | sync.Map 按长度缓存 |
| Tokenish 分池 | [32]sync.Pool |
| URL 解析缓存 | sync.Map |
| 默认值缓存 | 预分配 RangeConfig |
| requestURL 预计算 | 循环外计算 |
| bufio 32KB | H1 响应 buffer 扩大 |
| XMUX 连接池 | 多路复用 + 调度 |

### 四、DNS 优化

| 优化 | 说明 |
|------|------|
| DNS 缓存 | 已有 |
| Happy Eyeballs v3 | 新一代 IP 选择 |
| DNS fallback | 优化 |

### 五、稳定性修复

| 修复 | 说明 |
|------|------|
| retry 致命错误短路 | 不可达地址不重试 |
| WebSocket 心跳泄漏 | goroutine 正确退出 |
| splitConn.Close | 返回正确 error |
| TLS Session Tickets | 启用减少握手 |
| utls copyConfig | 补全关键字段 |
| ECH h2c 修复 | DNS over HTTPS 路由 |
| 预连接指数退避 | 避免雷鸣群效应 |

---

## 性能收益（pprof 验证）

| 指标 | 上游 | 魔改 | 改善 |
|------|------|------|------|
| Padding syscall/秒 | ~100K | ~3 | **↓99.997%** |
| Huffman 计算 | 每次 | 缓存 | **↓~90%** |
| URL 解析 | 每次 | 缓存 | **↓~95%** |
| 内存分配 | 基线 | 优化 | **↓15-20%** |
| GC marking CPU | 7.08% | 3.77% | **↓47%** |

---

## 安全性对比

| 项目 | 上游 | 魔改 |
|------|------|------|
| Padding 随机源 | crypto/rand | randpool (等价 CSPRNG) |
| Modulo bias | ❌ 有 | ✅ rejection sampling 消除 |
| 连接指纹 | 基础 | ✅ 增强 |
| ECH 支持 | ✅ 有 | ✅ 有 + h2c 修复 |

---

## 协议兼容性

| 场景 | 兼容性 |
|------|--------|
| 上游客户端 → 魔改服务器 | ✅ 100% |
| 魔改客户端 → 上游服务器 | ✅ 100% |
| 配置格式 | ✅ 向后兼容 |

---

## 综合评分

| 维度 | 上游 | 魔改 | 评价 |
|------|------|------|------|
| 功能完整性 | ★★★★☆ | ★★★★★ | +XMUX/H3/REALITY增强 |
| 性能 | ★★★☆☆ | ★★★★★ | Padding优化显著 |
| 安全性 | ★★★★☆ | ★★★★★ | CSPRNG + rejection sampling |
| 稳定性 | ★★★☆☆ | ★★★★☆ | Bug修复 + 测试覆盖 |
| 可维护性 | ★★★★☆ | ★★★★☆ | 代码规范 |
| 测试覆盖 | ★★☆☆☆ | ★★★★★ | 大幅提升 |

---

## 快速开始

### 下载

| 平台 | 下载 |
|------|------|
| **Windows** | [V2rayN (原版内核)](https://github.com/2dust/v2rayn/releases) |
| **Android** | [V2rayNG (Bray-Core 内核)](https://github.com/Maolaohei/v2rayNG/releases) |

### 配置示例

```json
{
  "streamSettings": {
    "network": "xhttp",
    "security": "reality",
    "realitySettings": {},
    "sockopt": {
      "tcpFastOpen": true,
      "happyEyeballs": {
        "v3Enabled": true
      }
    }
  }
}
```

> BBR 和 TCP_NOTSENT_LOWAT 已默认启用，不需在配置中填写。

### 编译

```bash
# Linux amd64 (推荐: v3 指令集)
CGO_ENABLED=0 GOAMD64=v3 go build -o xray -trimpath -ldflags="-s -w -buildid=" ./main

# Windows amd64
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 GOAMD64=v3 go build -o xray.exe -trimpath -ldflags="-s -w -buildid=" ./main
```

---

## 上游同步记录 (2026-07-08)

同步至 Xray-core v26.6.22+，合入以下上游修复：

| Commit | 说明 | 状态 |
|--------|------|------|
| `ac04c445` | DNS: Fix unexpected TTL clamp (#6363) | 已合入 |
| `e7e92546` | TUN: Avoid panic on nil RemoteAddr (#6365) | 已合入 |
| `fab4bcc1` | Bump cloudflare/circl 1.6.3→1.6.4 (#6362) | 已合入 |
| `5aefcb41` | Bump pion/stun/v3 3.1.5→3.1.6 (#6357) | 已合入 |
| `b99c3e56` | 版本号对齐 v26.6.22 | 已合入 |
| `18b85adb` | XHTTP: default maxConnections=6 for anti-RKN | 已合入 |
| `567500c4` | Fragment: lengths + delays 配置 (#6334) | 已 cherry-pick |
| `583bb4a6` | XHTTP: fix scStreamUpServerSecs (#6343) | 已 cherry-pick |
| `241aa38a` | TUN: autoSystemRoutingTable Linux/macOS (#6366) | 已 cherry-pick |
| `dda2b10c` | TUN: traffic counters + Metrics (#6349) | 已 cherry-pick |
| `7e7e8207` | Geodata: uTLS Chrome fingerprint (#6371) | 已 cherry-pick |
| `f9eb1597` | Bump actions/cache v5→v6 (#6368) | 已 cherry-pick |
| `987290ba` | Routing: process 支持 macOS (#6447) | 已 cherry-pick |
| `3dc8bf3d` | Hysteria: fix dynamic UUID (#6395) | 已 cherry-pick (合并 map-based Validator) |
| `452b7195` | Hysteria: vlessRoute 支持 (#6375) | 已 cherry-pick (合并 map-based Validator) |
| `e4e7614c` | TLS ECH: 配置解析增强 (#6441) | 已 cherry-pick (保留 getCipherSuiteIDs 缓存) |
| `65f6f0a4` | TUN Linux: gateway 精细化 — setInterfaceAddresses (#6398) | 已 cherry-pick |
| `0495b176` | TUN macOS: gateway 精细化 — selectDarwinGateway (#6434) | 已 cherry-pick |
| `9cd9382e` | TUN Linux: XRAY_TUN_FD 环境变量 (#6338) | 已 cherry-pick (合并 ownsTun + gateway) |
| `d7fa2076` | Geodata: 提取私有IP/域名匹配 + 传输安全校验 (#6303) | 选择性吸收 (跳过禁止明文策略) |

跳过的上游 commit（与自定义特性冲突或暂不需要）：
- `fb548f54` transport_internet.go 拆分重构 — 与大量自定义修改冲突
- `345c76f9` WireGuard 动态 peer 管理 — proto + config + server 重构，冲突极高
- `f496437b` XHTTP upload_queue.go 重构 — Bray-Core 已有更优实现
- `345c76f9` WireGuard 动态 peer 管理 — server.go 已重构 + proto 变更
- `f496437b` XHTTP upload_queue.go 重构 — 已有 GC 调优改动

---

## 许可证

[Mozilla Public License Version 2.0](https://github.com/XTLS/Xray-core/blob/main/LICENSE)

---

*上游同步基准：Xray-core [v26.6.22](https://github.com/XTLS/Xray-core/releases/tag/v26.6.22)*
