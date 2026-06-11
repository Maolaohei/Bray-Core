# Bray-Core

> **⚠️ 警告：这是一个个人实验性 Fork**
> 
> 本项目是基于 [Xray-core](https://github.com/XTLS/Xray-core) 的**个人实验性分支**，用于测试和验证各种网络优化方案。
> 
> - **非官方版本**：与 Xray-core 官方团队无关
> - **实验性质**：部分功能未经大规模生产验证
> - **仅供学习**：建议在生产环境使用官方版本
> - **风险自负**：使用本分支产生的任何问题，作者不承担责任

---

## 快速下载

### 客户端

| 平台 | 下载 |
|------|------|
| **Windows** | [V2rayN (Bray-Core 内核)](https://github.com/Maolaohei/v2rayN/releases) |
| **Android** | [V2rayNG (Bray-Core 内核)](https://github.com/Maolaohei/v2rayNG/releases) |

> 客户端除内核外原汁原味，仅替换为 Bray-Core 内核。

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

### 四、性能优化（GC 压力降低 47%）

通过 pprof 分析定位热点，针对性优化内存分配路径：

| 优化 | 涉及文件 | 效果 |
|------|---------|------|
| **VLESS AddonsPool** | `proxy/vless/encoding/addons.go` | 复用 Addons 结构体，每连接省 1 次堆分配 |
| **VLESS Vision Fast Path** | `proxy/vless/encoding/addons.go` | 预编码 xtls-rprx-vision，零分配 |
| **VLESS 手写 protobuf** | `proxy/vless/encoding/addons.go` | 移除 proto.Marshal/Unmarshal 反射开销 |
| **AEAD Nonce IsMax 缓存** | `proxy/vless/encryption/common.go` | 避免每 chunk bytes.Equal 12 字节比较 |
| **mux writer mbPool** | `common/mux/writer.go` | 复用 mux 帧 MultiBuffer slice |
| **XHTTP 默认值缓存** | `transport/internet/splithttp/config.go` | 6 个默认 RangeConfig 包级变量缓存 |
| **XHTTP requestURL 预计算** | `transport/internet/splithttp/dialer.go` | 上传循环外预计算 URL string |
| **XHTTP mux rand 优化** | `transport/internet/splithttp/mux.go` | crypto/rand → math/rand/v2 |
| **XHTTP bufio 32KB** | `transport/internet/splithttp/h1_conn.go` | H1 响应读取 buffer 4KB → 32KB |
| **XHTTP Padding randBufPool** | `transport/internet/splithttp/xpadding.go` | 复用 256 字节随机 buffer |
| **fmt.Sprintf → strconv** | `config.go` + `hub.go` | 减少每请求 fmt 反射分配 |

**pprof 验证结果：**
- GC marking CPU 占比：7.08% → 3.77%（**降低 47%**）
- requestHandler 内存分配：164MB → 141MB（**降低 14%**）
- 网络吞吐：42.8 Mbps，0 错误，无回归

### 五、错误处理修复

| 修复 | 涉及文件 | 说明 |
|------|---------|------|
| **context 泄漏** | `proxy/shadowsocks_2022/outbound.go` | `context.WithCancel` → `context.WithoutCancel` |
| **ECDH 错误处理** | `transport/internet/reality/reality.go` | ECDH 和 HTTP 请求错误不再静默忽略 |
| **证书解析错误** | `transport/internet/tls/config.go` | ParseCertificate 错误不再跳过 |
| **panic → error** | `splithttp/dialer.go`, `hysteria/dialer.go` 等 | 13 处 panic 改为返回 error |
| **strconv 错误** | `sockopt_linux.go`, `sockopt_windows.go` | Atoi 转换错误不再忽略 |
| **splitConn.Close** | `transport/internet/splithttp/connection.go` | 返回正确 error 而非 nil |

### 六、CI/测试

| 改进 | 说明 |
|------|------|
| **Go 版本更新** | CI 从 1.22 更新至 1.26，与 go.mod 一致 |
| **golangci-lint** | 添加静态分析配置和 CI 步骤 |
| **断流耐久测试** | 5 网络环境 × 5 测试场景，验证 0 断流 |
| **VLESS encoding benchmark** | 验证 Vision Fast Path 零分配 |

---

## 七、用户体验升级（P0-P3）

重点强化用户感知的流畅性、稳定性和首次访问体验。

### P0: XMUX min-inflight 调度

将随机选择改为最小活跃流调度，均匀分担负载：

```go
// 改动前：随机选择
i := rand.IntN(len(xmuxClients))

// 改动后：选择最少活跃流的连接
best := xmuxClients[0]
bestUsage := best.OpenUsage.Load()
for _, c := range xmuxClients[1:] {
    if usage := c.OpenUsage.Load(); usage < bestUsage {
        best = c
        bestUsage = usage
    }
}
```

**用户感知**：视频/直播卡顿率降低，负载更均匀

### P1: XMUX 预连接

后台预建连接，降低首次访问延迟：

```go
// 每 5s 检查，空池时预建连接
func (m *XmuxManager) preConnectLoop() {
    ticker := time.NewTicker(5 * time.Second)
    for {
        select {
        case <-m.stopCh:
            return
        case <-ticker.C:
            if len(m.xmuxClients) == 0 {
                m.newXmuxClient()
            }
        }
    }
}
```

**用户感知**：网页/视频冷启动更快

### P2: 快速故障检测

5 秒间隔主动检查连接健康：

```go
func (m *XmuxManager) healthCheckLoop() {
    ticker := time.NewTicker(5 * time.Second)
    for {
        select {
        case <-m.stopCh:
            return
        case <-ticker.C:
            // 主动移除关闭的连接
            for i := 0; i < len(m.xmuxClients); {
                if m.xmuxClients[i].XmuxConn.IsClosed() {
                    m.xmuxClients = append(m.xmuxClients[:i], m.xmuxClients[i+1:]...)
                } else {
                    i++
                }
            }
            // 空池时自动恢复
            if len(m.xmuxClients) == 0 {
                m.newXmuxClient()
            }
        }
    }
}
```

**用户感知**：断流恢复更快，移动网络切换更平滑

### P3: RTT 感知调度

在 min-inflight 基础上加入 RTT 权重：

```go
// 评分公式：inflight * 1000 + rtt_ms
func scoreClient(c *XmuxClient) int64 {
    inflight := int64(c.OpenUsage.Load())
    rttMs := c.GetRTT().Milliseconds()
    return inflight*1000 + rttMs
}
```

**用户感知**：P99 延迟降低

---

## 八、Happy Eyeballs v3 + DNS Warmup

### Happy Eyeballs v3 升级

从 RFC 8305 升级到 v3 草案规范，**默认启用**：

| 特性 | RFC 8305 (v2) | Happy Eyeballs v3 |
|------|---------------|-------------------|
| IPv4/IPv6 交错 | ✔ | ✔ |
| tryDelay | 固定 | 动态调整 |
| maxConcurrentTry | 固定 | 自适应并发 |
| SVCB/HTTPS 优先 | ✘ | ✔ |
| RTT 学习与历史记录 | ✘ | ✔ |
| 协议无关（QUIC/HTTP3） | 部分 | ✔ |

**新增功能：**
- IP 评分模型：`score = priority*1e9 + rtt*(1+failRate*10)`
- 自适应并发控制：根据 RTT 动态调整 delay
- 历史学习：EWMA RTT 追踪 + 成功/失败计数
- RTT 钳位：默认 100ms，上限 999ms 防止评分反转

### DNS Warmup 完整流水线

```
启动
  ↓
ExtractWarmupDomains(obm)
  ├── 节点域名 (VLESS/VMess address)
  ├── REALITY serverName/dest
  └── 用户配置的 warmupDomains
  ↓
DNS 缓存预热
  ↓
XMUX preConnectLoop
  ↓
用户请求时直接复用热连接 (0~10ms)
```

**关键改进：**
- **删除硬编码公网域名**：不再预热 google.com/youtube.com
- **智能域名提取**：从出站配置自动提取节点域名、REALITY 域名
- **追加模式**：用户配置的 `warmupDomains` 追加而非覆盖
- **网络变化检测**：Wi-Fi ↔ 4G 切换时自动预热

### XMUX 动态预热队列

- `WarmupTarget` 优先级队列，支持多域名并发预热
- `networkWatchLoop`：每 10 秒检测网络接口变化
- 网络切换后自动清理旧连接并触发重新预热
- 信号量控制并发预热数（最多 2 个）

### 可量化指标追踪

```go
type XmuxMetrics struct {
    ReuseRate    float64       // 连接复用率（目标 ≥ 90%）
    AvgTTFB      time.Duration // 平均首字节时间
    MaxTTFB      time.Duration // 最大首字节时间
    WarmupHit    int64         // 预热命中次数
    WarmupMiss   int64         // 预热未命中次数
    NetRecovery  int64         // 网络切换恢复次数
}
```

**验收标准：**
- Reuse Rate ≥ 90%
- TTFB 下降 ≥ 20%
- 网络恢复时间 < 2s

### 配置示例

```json
{
  "streamSettings": {
    "happyEyeballs": {
      "v3Enabled": true,
      "rttWeight": 0.7,
      "failPenalty": 10.0,
      "adaptiveConcurrency": true
    }
  },
  "dns": {
    "warmupDomains": ["custom.cdn.com"]
  }
}
```

---

## 九、已合并的上游 PR

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

### XMUX 指标测试

| 指标 | 测试值 | 目标 |
|------|--------|------|
| **Reuse Rate** | 93.3% | ≥ 90% ✅ |
| **Avg TTFB** | 38.5ms | < 100ms ✅ |
| **Max TTFB** | 67ms | < 200ms ✅ |
| **Warmup Enqueue** | 2 | - |
| **NetRecovery** | 1 | - |

> 测试环境：本地单元测试，模拟 15 次请求。

---

## 配置示例

```json
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
