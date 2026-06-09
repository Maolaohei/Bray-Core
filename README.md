# Bray-Core

基于 [Xray-core](https://github.com/XTLS/Xray-core) 的高性能定制分支，专注于传输层优化与实际体验。

---

## 设计原则

- **默认即最优** — 所有性能特性默认启用，无须额外配置
- **改动可追溯** — 每个优化点对应上游 Issue/PR，清晰可回退
- **前向兼容** — 不改协议帧格式与握手流程

---

## 性能测试报告

### 测试环境

| 项目 | 值 |
|------|-----|
| **CPU** | 13th Gen Intel Core i5-13600KF (20 核) |
| **RAM** | 32 GB DDR5 |
| **OS** | Windows 11 |
| **Go 版本** | go1.26.4 windows/amd64 |
| **编译参数** | `CGO_ENABLED=0 GOAMD64=v3 -trimpath -ldflags="-s -w -buildid="` |
| **测试模式** | 本地环回 (127.0.0.1)，TLS H2 / H2C |
| **传输协议** | XHTTP (splithttp) |
| **测试时间** | 2026-06-06 00:23 UTC+8 |

### 对比版本

| 版本 | Commit | 说明 |
|------|--------|------|
| **上游 (Upstream)** | `fdb9b616` | Xray-core v26.6.1 基准 |
| **Bray-Core** | `076ba968` | 本分支最新 |

### 测试方法

服务端：XHTTP echo server（`io.Copy(c, c)`），读取所有数据后原样写回。
客户端：Dial 连接后写入固定大小 payload，读取完整 echo，记录耗时。
每组合 3 轮取均值，网络栈为本机 TCP 环回。

### H2 (TLS) 性能对比

| 场景 | 负载 | 上游 (fdb9b616) | Bray-Core | 提升 |
|------|:----:|:---------------:|:---------:|:----:|
| **packet-up** | 128 KB | 83.7 MB/s | **128.4 MB/s** | **+53.3%** |
| **packet-up** | 1 MB | 16.8 MB/s | **17.1 MB/s** | +2.0% |
| **packet-up** | 2 MB | — | **17.0 MB/s** | — |
| **stream-up** | 128 KB | — | **258.8 MB/s** | — |
| **stream-up** | 1 MB | 171.8 MB/s | **257.7 MB/s** | **+50.0%** |
| **stream-up** | 2 MB | — | **272.7 MB/s** | — |
| **stream-one** | 128 KB | — | **129.5 MB/s** | — |
| **stream-one** | 1 MB | — | **295.5 MB/s** | — |
| **stream-one** | 2 MB | — | **255.4 MB/s** | — |

### 结论

- **stream-up / stream-one** 模式吞吐量达 **250-295 MB/s**，远超任何实际网络带宽瓶颈
- **packet-up 小包 (128KB)** 在 Bray-Core 上比上游快 **53%**，得益于 Flush 节流与服务端批量写入优化
- **packet-up 大包 (1MB+)** 受限于协议本身的顺序分片上行机制，吞吐量稳定在 17 MB/s 水平
- **VLESS 头解码** 性能约为 815ns/op (31.87 MB/s)，对整体吞吐影响可忽略

---

## 优化清单

### 传输层 (XHTTP) 已回退

| 优化 | 涉及文件 | 效果评估 |
|------|---------|:--------:|
| **H2/H3 PostPacket fire-and-forget** | `client.go` | 上行不阻塞等响应，匹配上游规范 🔥 |
| **服务端下行 Flush 节流 (1460/10ms)** | `hub.go` | 聚合小写入，减少 TCP 中断，**下行 +50%** 🔥 |
| **XmuxManager RWMutex 双路径** | `mux.go` | 读锁替代写锁，高并发锁争用消除 |
| **fastCandidates 扫描分离** | `mux.go` | 读路径不阻塞写路径 |
| **DefaultDialerClient 内联 POST** | `client.go`, `dialer.go` | 每分块省去 goroutine + channel 分配 |
| **uploadQueue heap 指针化** | `upload_queue.go` | `[]Packet` → `[]*Packet`，避免 48B struct 复制 |
| **Happy Eyeballs (H3→H2 竞速)** | `h3_fallback.go` (新增 188 行) | H3 超时无缝降级 H2 |
| **H2 MaxConcurrentStreams:256** | `hub.go` | 显式设置 H2 并发上限 |
| **QUIC Allow0RTT + Transport GSO/GRO** | `hub.go` | H3 0-RTT 握手 + 批处理 I/O |
| **服务端 uploadSem (256)** | `hub.go` | 限制并发上行处理，抗攻击 |
| **write404 随机 body** | `hub.go` | 抗指纹 |
| **CDN keepalive** | `hub.go` | GET 下行空闲时写 padding，防止 CF 100s 断连 |
| **h1ReqBufPool sync.Pool** | `client.go` | 复用 HTTP/1.1 请求序列化 buffer |
| **Config 缓存 (sync.Map)** | `config.go` | Path/Query/Header 预计算，每连接省 map 构建 |
| **Bytespool (128KB 阈值池)** | `config.go` | header/cookie 上行数据分配池化 |
| **XPadding 零长度短路** | `config.go` | padding=0 时跳过 URL.String() 分配 |
| **SessionID 自定义 (base62/hex)** | `config.go` | 可配置 sessionID 生成方式 |

### TCP 网络栈

| 优化 | 涉及文件 | 效果评估 |
|------|---------|:--------:|
| **BBR 不填默认启用** | `sockopt_linux.go` | Linux 服务端空字符串自动设为 BBR 🔥 |
| **TCP Fast Open 不填默认开启** | `sockopt.go` | 减少 1 RTT，新连接响应更快 🔥 |
| **tcpNoDelayListener** | `system_listener.go` | 入站连接禁用 Nagle，SOCKS 响应提速 |
| **TCP_QUICKACK 入站连接** | `sockopt_linux.go` | TLS 握手加速 |
| **TCP_NOTSENT_LOWAT 自适应** | `sockopt_linux.go` | 自动设置，防 Bufferbloat |
| **setQuickAck** | `sockopt.go` | 首数据包不延迟 ACK |
| **Dial 超时 16s → 8s** | `system_dialer.go` | 更快失败恢复，移动端友好 |

### 缓存与内存

| 优化 | 说明 | 效果 |
|------|------|:----:|
| **TLS Session 缓存 128→1024** | 更多并发会话复用 | 减少 TLS 握手 |
| **getCipherSuiteIDs OnceValue** | 懒加载密套件表 | 省每次重新构建 map |
| **VisionReader MultiBuffer 复用** | unpadding 阶段复用底层数组 | 减少分配 |
| **`first` buffer 池化** | `buf.FromBytes(make(...))` → `buf.New()` | 减少大内存分配 |
| **Spider path Builder 池化** | `sync.Pool` 复用 strings.Builder | 减少每连接分配 |

### 修复与稳定性

| 修复 | 涉及文件 | 说明 |
|------|---------|------|
| **XMUX MaxConcurrency 默认值回退** | `infra/conf/transport_internet.go` | `{16,32}` → `{1,1}`，避免 H2 队头阻塞 |
| **retry 致命错误短路** | `common/retry/retry.go` | 不可达地址不重试 |
| **VLESS IPv6 不可达回退域名** | `proxy/vless/outbound/outbound.go` | 无 IPv6 路由时回退域名 |
| **freedom 重试修复** | `proxy/freedom/freedom.go` | 减少 CANCEL 风暴 |
| **WebSocket 心跳 goroutine 泄露** | `transport/internet/websocket/connection.go` | `done.Instance` 正确退出 |
| **splitConn.Close 返回值修复** | `transport/internet/splithttp/connection.go` | 不吞 reader 关闭错误 |
| **TLS Session Tickets 不禁用** | `transport/internet/tls/config.go` | 启用会话恢复 |
| **utls copyConfig 补全** | `transport/internet/tls/tls.go` | 补全 Time/MinVersion/MaxVersion/CurvePreferences |
| **ECH h2c 修复** | `transport/internet/tls/ech.go` | DNS 查询走 HTTPS |
| **ECH UDP buffer 512→4096** | `transport/internet/tls/ech.go` | 大 UDP 响应不截断 |
| **Moonlight 端口旁路** | `proxy/vless/outbound/outbound.go` | 47984-48010 端口不经过 XUDP |
| **预连接指数退避 + 抖动** | `proxy/vless/outbound/outbound.go` | 失败时指数退避，成功时 ±50% 抖动 |
| **应用层 keepalive** | `common/net/net.go` | H2 保活间隔 10s |

### VLESS

| 优化 | 说明 | 效果 |
|------|------|:----:|
| **Version + UUID 一次读取** | 合并 2 次 ReadFullFrom 为 1 次 | 少 1 次 syscall |
| **去除 switch case 嵌套** | `switch v { case 0: }` → 直接处理 | 少一次分支 |

---

## 配置示例

### 性能优化配置

```json
"sockopt": {
    "tcpCongestion": "bbr",
    "tcpFastOpen": true,
    "tcpNotsentLowat": 16384
}
```

> BBR + TFO 已默认启用，填与不填效果相同。

### 网络栈加速（Linux 服务端）

```bash
# BBR 拥塞控制 + TCP Fast Open + 大窗口
net.core.default_qdisc = fq
net.ipv4.tcp_congestion_control = bbr
net.ipv4.tcp_fastopen = 3
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = "4096 131072 16777216"
net.ipv4.tcp_wmem = "4096 65536 16777216"
```

---

## 编译

```bash
# Linux amd64 (推荐: v3 指令集 + strip)
CGO_ENABLED=0 GOAMD64=v3 go build -o xray -trimpath -ldflags="-s -w -buildid=" ./main

# Windows amd64
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 GOAMD64=v3 go build -o xray.exe -trimpath -ldflags="-s -w -buildid=" ./main
```

---

## 上游 PR 合并

| PR | 内容 |
|----|------|
| [#6261](https://github.com/XTLS/Xray-core/pull/6261) | TLS: ECH H2C 查询修复 |
| [#6254](https://github.com/XTLS/Xray-core/pull/6254) | Finalmask: brutal (tcp-brutal 加速) |
| [#6058](https://github.com/XTLS/Xray-core/pull/6058) | Direct/Freedom: 兼容性改进 |
| [#6258](https://github.com/XTLS/Xray-core/pull/6258) | XHTTP: Custom sessionID (base62/hex) |
| [#4231](https://github.com/XTLS/Xray-core/pull/4231) | Mux: maxReuseTimes |
| [#6226](https://github.com/XTLS/Xray-core/pull/6226) | TLS config: 移除弃用字段 |
| [#6222](https://github.com/XTLS/Xray-core/pull/6222) | CI: Builders 在相同 PR 中只运行一次 |
| [#6244](https://github.com/XTLS/Xray-core/pull/6244) | README: HarmonyOS GUI 客户端 |

---

## 许可证

[Mozilla Public License Version 2.0](https://github.com/XTLS/Xray-core/blob/main/LICENSE)

---

*上游同步基准：Xray-core [v26.6.1](https://github.com/XTLS/Xray-core/releases/tag/v26.6.1)*
