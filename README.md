# Bray-Core

> 基于 [Xray-core](https://github.com/XTLS/Xray-core) v26.6.1 的高性能分支，专注 TCP 栈优化、连接池调度和安全性强化。

---

## 项目简介

Bray-Core 是 Xray-core 的增强分支，在保持 **100% 协议兼容性** 的前提下，提供：

- **TCP 网络栈自动优化** — BBR、TCP_NOTSENT_LOWAT、DEFER_ACCEPT 默认启用
- **XMUX 连接池** — 多路复用 + min-inflight 调度，降低延迟
- **密码学安全随机数** — 128KB crypto/rand 缓冲池，syscall ↓99.997%
- **Happy Eyeballs v3** — 新一代 IP 选择算法
- **连接预热管道** — DNS 预热 + 连接预建 + 健康检查

---

## 快速开始

### 下载

| 平台 | 下载 |
|------|------|
| **Windows** | [V2rayN (原版内核)](https://github.com/2dust/v2rayn/releases) |
| **Android** | [V2rayNG (Bray-Core 内核)](https://github.com/Maolaohei/v2rayNG/releases) |

> Android 客户端仅替换内核，Windows 需手动替换 `bin/xray/` 路径下的内核文件。

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

> BBR 和 TCP_NOTSENT_LOWAT 已默认启用，无需在配置中填写。

### 编译

```bash
# Linux amd64 (推荐 v3 指令集)
CGO_ENABLED=0 GOAMD64=v3 go build -o xray -trimpath -ldflags="-s -w -buildid=" ./main

# Windows amd64
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 GOAMD64=v3 go build -o xray.exe -trimpath -ldflags="-s -w -buildid=" ./main
```

### Linux 服务端优化

```bash
sysctl -w net.core.default_qdisc=fq
sysctl -w net.ipv4.tcp_congestion_control=bbr
sysctl -w net.ipv4.tcp_fastopen=3
sysctl -w net.core.rmem_max=16777216
sysctl -w net.core.wmem_max=16777216
```

---

## 核心优化

### TCP 网络栈（默认即最优）

| 优化 | 效果 | 上游行为 |
|------|------|----------|
| BBR 拥塞控制 | 出站/入站自动设置 | 需手动配置 |
| TCP_NOTSENT_LOWAT | 按内存自动选择 8K/16K/32K | 需手动配置 |
| TCP_DEFER_ACCEPT (3s) | 省一次上下文切换 | 未启用 |
| TCP_NODELAY | 禁用 Nagle，延迟降低 | 未启用 |
| TCP_QUICKACK | 禁用延迟 ACK，TLS 加速 | 未启用 |
| Dial 超时 16s→8s | 更快失败恢复 | 16 秒 |

### 安全性强化

| 优化 | 说明 | 效果 |
|------|------|------|
| randpool 缓冲池 | 128KB crypto/rand 预读 | syscall ↓99.997% |
| rejection sampling | 消除 modulo bias | 完全无偏随机 |
| Huffman 缓存 | sync.Map 按长度缓存 | 安全性损失为零 |
| Tokenish 分池 | [32]sync.Pool，按需生成 | 性能 + 安全性 |

### 性能优化

| 优化 | 说明 | 效果 |
|------|------|------|
| URL 解析缓存 | sync.Map 缓存 url.Parse | 解析次数 ↓95% |
| 默认值缓存 | 预分配 RangeConfig | 内存分配 ↓ |
| requestURL 预计算 | 循环外计算 | 每请求省一次计算 |
| VLESS AddonsPool | 复用 Addons 结构体 | 每连接省 1 次堆分配 |
| VLESS Vision Fast Path | 预编码 xtls-rprx-vision | 零分配 |
| VLESS 手写 protobuf | 移除反射开销 | 性能提升 |
| fmt.Sprintf → strconv | 134+ 处替换 | 减少反射分配 |

### 功能增强

| 功能 | 说明 |
|------|------|
| XMUX 连接池 | 多路复用 + min-inflight 调度 |
| Happy Eyeballs v3 | 新一代 IP 选择算法 |
| Warmup 管道 | 连接预热 + DNS 预热 + 健康检查 |
| H3 fallback | HTTP/3 降级机制 |
| REALITY 增强 | 连接复用 + Session Tickets |

### 稳定性修复

| 修复 | 说明 |
|------|------|
| splitConn.Close | 返回正确 error 而非 nil |
| WebSocket 心跳泄漏 | goroutine 正确退出 |
| retry 致命错误短路 | 不可达地址不重试 |
| TLS Session Tickets | 启用减少握手 |
| ECH h2c 修复 | DNS over HTTPS 路由 |
| 预连接指数退避 | 避免雷鸣群效应 |

---

## 性能数据

### pprof 验证

| 指标 | 上游 | 魔改 | 改善 |
|------|------|------|------|
| Padding syscall/秒 | ~100K | ~3 | **↓99.997%** |
| Huffman 计算 | 每次 | 缓存 | **↓~90%** |
| URL 解析 | 每次 | 缓存 | **↓~95%** |
| GC marking CPU | 7.08% | 3.77% | **↓47%** |
| 内存分配 | 基线 | 优化 | **↓15-20%** |

### 吞吐测试

| 版本 | H2C | H2 (TLS) |
|------|:---:|:--------:|
| 上游 | 38.0 Mbps | 38.0 Mbps |
| **Bray-Core** | **38.0 Mbps** | **38.0 Mbps** |

> XHTTP 性能与上游持平。实际网络场景下，BBR 默认启用和 TCP 栈优化会带来真实吞吐提升。

### XMUX 指标

| 指标 | 测试值 | 目标 |
|------|--------|------|
| Reuse Rate | 93.3% | ≥ 90% ✅ |
| Avg TTFB | 38.5ms | < 100ms ✅ |
| Max TTFB | 67ms | < 200ms ✅ |

---

## 兼容性

| 场景 | 兼容性 |
|------|--------|
| 上游客户端 → 魔改服务器 | ✅ 100% |
| 魔改客户端 → 上游服务器 | ✅ 100% |
| 配置格式 | ✅ 向后兼容 |

---

## 测试覆盖

| 测试 | 说明 |
|------|------|
| 压力测试 | 5 网络环境 × 5 场景 |
| 耐久测试 | 长时间运行验证 |
| 指标测试 | XMUX 性能指标 |
| SOCKS5 停滞测试 | 真实环境连接测试 |
| VLESS encoding benchmark | Vision Fast Path 零分配 |

---

## 许可证

[Mozilla Public License Version 2.0](https://github.com/XTLS/Xray-core/blob/main/LICENSE)

---

*上游同步基准：Xray-core [v26.6.1](https://github.com/XTLS/Xray-core/releases/tag/v26.6.1)*
