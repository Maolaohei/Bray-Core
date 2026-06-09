# Bray-Core

基于 [Xray-core](https://github.com/XTLS/Xray-core) 的高性能定制分支。

---

## 设计原则

- **默认即最优** — TCP 网络栈参数自动优化，无须额外配置
- **前向兼容** — 不改协议帧格式与握手流程
- **零配置迁移** — 直接替换上游二进制即可

---

## 相比上游 Xray-core 的优势

### 一、TCP 网络栈（默认即最优）

| 优化 | 效果 | 上游行为 |
|------|------|----------|
| **BBR 拥塞控制默认启用** | 出站/入站连接自动设置，无需在配置中填写 | 需手动配置 `tcpCongestion: "bbr"` |
| **TCP_NOTSENT_LOWAT 自适应** | 按系统内存大小自动选择 8K/16K/32K，防 Bufferbloat | 需手动配置精确值 |
| **入站 TCP_DEFER_ACCEPT (3s)** | ClientHello 到达后才唤醒 `accept()`，省一次上下文切换 | 未启用 |
| **入站 TCP_NODELAY** | 禁用 Nagle 算法，SOCKS 小请求延迟降低 | 未启用 |
| **入站 TCP_QUICKACK** | 禁用延迟 ACK，TLS 握手加速 | 未启用 |
| **Dial 超时 16s→8s** | 更快失败恢复，移动端更友好 | 16 秒 |

### 二、VLESS 微优化

| 优化 | 说明 |
|------|------|
| **Version + UUID 合并读取** | 前 17 字节一次读取，少一次 ReadFullFrom 系统调用 |
| **inbound remoteAddr 提前缓存** | 避免在错误处理路径中重复调 `connection.RemoteAddr()` |
| **IPv6 不可达时回退域名** | 无 IPv6 路由时回退到域名，由服务端自行解析 |

### 三、稳定性修复

| 修复 | 涉及文件 | 场景 |
|------|---------|------|
| **retry 致命错误短路** | `common/retry/retry.go` | 不可达地址不重试，减少无意义等待 |
| **WebSocket 心跳 goroutine 泄漏** | `transport/internet/websocket/connection.go` | 长时间运行不推积 goroutine |
| **splitConn.Close 返回值** | `transport/internet/splithttp/connection.go` | reader 关闭失败不再被 writer 成功掩盖 |
| **TLS Session Tickets 启用** | `transport/internet/tls/config.go` | 上游禁用，本分支启用（减少 TLS 握手） |
| **utls copyConfig 补全** | `transport/internet/tls/tls.go` | 补全 Time/MinVersion/MaxVersion/CurvePreferences |
| **ECH h2c/H2C 查询修复** | `transport/internet/tls/ech.go` | DNS over HTTPS 正确路由 |
| **预连接指数退避 + 抖动** | `proxy/vless/outbound/outbound.go` | 连接失败时指数退避，避免雷鸣群效应 |

### 四、已合并的上游 PR

| PR | 内容 |
|----|------|
| [#6261](https://github.com/XTLS/Xray-core/pull/6261) | ECH H2C 查询修复 |
| [#6258](https://github.com/XTLS/Xray-core/pull/6258) | XHTTP Custom sessionID、sessionIDTable |
| [#6058](https://github.com/XTLS/Xray-core/pull/6058) | Freedom 兼容性改进 |
| [#6254](https://github.com/XTLS/Xray-core/pull/6254) | Brutal TCP 加速器 |
| [#4231](https://github.com/XTLS/Xray-core/pull/4231) | Mux maxReuseTimes |
| [#6276](https://github.com/XTLS/Xray-core/pull/6276) | TUN autoOutboundsInterface |
| [#6275](https://github.com/XTLS/Xray-core/pull/6275) | TUN 由 AlwaysOnInboundHandler 启动 |
| [#6272](https://github.com/XTLS/Xray-core/pull/6272) | XICMP Linux 服务端优化 |
| [#6228](https://github.com/XTLS/Xray-core/pull/6228) | Salamander crypto/rand 替代 math/rand |

---

## 性能测试

### 测试环境

| 项目 | 值 |
|------|-----|
| CPU | 13th Gen Intel Core i5-13600KF (20 核) |
| RAM | 32 GB DDR5 |
| OS | Windows 11 |
| Go 版本 | go1.26.4 windows/amd64 |
| 编译参数 | `CGO_ENABLED=0 GOAMD64=v3 -trimpath -ldflags="-s -w -buildid="` |
| 测试模式 | 本地环回 (127.0.0.1)，TLS H2 |
| 传输协议 | XHTTP (splithttp)，与上游实现一致 |

### XHTTP 吞吐（H2 TLS）

XHTTP 传输层实现与上游一致，吞吐无差异。以下为本机环批复制测试（128KB × 10 回显）：

| 版本 | H2C | H2 (TLS) |
|------|:---:|:--------:|
| **上游 (fdb9b616)** | 38.0 Mbps | 38.0 Mbps |
| **Bray-Core** | **38.0 Mbps** | **38.0 Mbps** |

> XHTTP 性能与上游持平。实际网络场景下，BBR 默认启用和 TCP 栈优化会带来真实吞吐提升（取决于链路质量）。

---

## 配置示例

```json
"streamSettings": {
    "network": "xhttp",
    "security": "reality",
    "realitySettings": {},
    "sockopt": {
        "tcpFastOpen": true
    }
}
```

> BBR 和 TCP_NOTSENT_LOWAT 已默认启用，不需在配置中填写。

### Linux 服务端额外优化

```bash
sysctl -w net.core.default_qdisc=fq
sysctl -w net.ipv4.tcp_congestion_control=bbr
sysctl -w net.ipv4.tcp_fastopen=3
sysctl -w net.core.rmem_max=16777216
sysctl -w net.core.wmem_max=16777216
```

---

## 编译

```bash
# Linux amd64 (推荐: v3 指令集)
CGO_ENABLED=0 GOAMD64=v3 go build -o xray -trimpath -ldflags="-s -w -buildid=" ./main

# Windows amd64
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 GOAMD64=v3 go build -o xray.exe -trimpath -ldflags="-s -w -buildid=" ./main
```

---

## 许可证

[Mozilla Public License Version 2.0](https://github.com/XTLS/Xray-core/blob/main/LICENSE)

---

*上游同步基准：Xray-core [v26.6.1](https://github.com/XTLS/Xray-core/releases/tag/v26.6.1)*
