# Bray-Core 连接生命周期架构文档

> 基于真实代码的架构图，用于专家审计。
> 代码位置: `transport/internet/splithttp/`, `proxy/http/client.go`, `transport/internet/tls/`

---

## 1. XMUX 内部连接生命周期

### 1.1 三态状态机 (XmuxClient)

源码: `mux.go:26-29` 定义状态常量, `mux.go:81-110` 定义 XmuxClient 结构体

```
┌─────────────────────────────────────────────────────────────────────┐
│                    XmuxClient 三态状态机                             │
│                                                                     │
│  ┌──────────┐    maybeDrain()     ┌────────────┐                   │
│  │  Active   │ ──────────────────▶│  Draining  │                   │
│  │  (state=0)│    (CAS 0→1)       │  (state=1) │                   │
│  └────┬─────┘                     └─────┬──────┘                   │
│       │                                 │                           │
│       │ MarkDead()                      │ tryClose()                │
│       │ (state=2, immediate)            │ (CAS 1→2, when            │
│       │                                 │  activeStreams==0)        │
│       ▼                                 ▼                           │
│  ┌──────────┐                    ┌──────────┐                       │
│  │  Closed  │◀───────────────────│  Closed  │                       │
│  │  (state=2)│                    │  (state=2)│                       │
│  └──────────┘                    └──────────┘                       │
│                                                                     │
│  触发条件:                                                           │
│  Active → Draining:  maybeDrain()                                   │
│    - Health check: RTT > 5s, idle > 120s, quality drops ≥5         │
│    - GetXmuxClient: leftUsage=0, LeftRequests≤0, UnreusableAt过期    │
│    - Network change: clearStaleConnections()                        │
│  Active → Closed:   MarkDead() (Fast Eviction)                     │
│    - Fatal errors: EOF, broken pipe, GOAWAY, TLS errors            │
│    - Probe failure during connection setup                          │
│  Draining → Closed: tryClose()                                     │
│    - activeStreams 减至 0 后自动关闭 TCP                             │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.2 XmuxManager 整体架构

源码: `mux.go:300-345` 定义 XmuxManager, `dialer.go:133-250` 定义 getHTTPClient

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        XmuxManager 架构                                   │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐     │
│  │                     全局连接池 globalDialerMap                    │     │
│  │                   map[MuxKey]*XmuxManager                        │     │
│  │  MuxKey = {dest, tlsServerName, realityServerName,              │     │
│  │            protocol, security, configHash(SHA256)}              │     │
│  └───────────────────────────┬─────────────────────────────────────┘     │
│                              │                                           │
│  ┌───────────────────────────▼─────────────────────────────────────┐     │
│  │                      XmuxManager                                │     │
│  │                                                                  │     │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐   │     │
│  │  │ preConnectLoop│  │healthCheckLoop│  │ networkWatchLoop   │   │     │
│  │  │ (goroutine)   │  │ (goroutine)   │  │ (goroutine)        │   │     │
│  │  │              │  │  每5秒tick     │  │  每10秒tick         │   │     │
│  │  │ 启动时创建    │  │              │  │                    │   │     │
│  │  │ 1-2个连接     │  │  清理不健康   │  │  检测网络变化       │   │     │
│  │  └──────────────┘  │  补充新连接   │  │  清理+重建连接     │   │     │
│  │                     └──────────────┘  └────────────────────┘   │     │
│  │                                                                  │     │
│  │  ┌──────────────────────────────────────────────────────┐      │     │
│  │  │              XmuxClientPool (RWMutex)                 │      │     │
│  │  │  clients []*XmuxClient                                │      │     │
│  │  │                                                       │      │     │
│  │  │  读操作: Len(), Snapshot()  → RLock                    │      │     │
│  │  │  写操作: RemoveAt(), Append(), CloseAll() → Lock       │      │     │
│  │  └──────────────────────────────────────────────────────┘      │     │
│  │                                                                  │     │
│  │  ┌──────────────────────────────────────────────────────┐      │     │
│  │  │           动态连接缩放 (V2.1 AIMD)                    │      │     │
│  │  │  _dynamicConns: atomic.Int32 (锁无关读)               │      │     │
│  │  │  _dynamicConc:  atomic.Int32 (锁无关读)               │      │     │
│  │  │                                                       │      │     │
│  │  │  Behavior → 目标值:                                   │      │     │
│  │  │    LowLatency:  conns×1.5, conc×1.5                  │      │     │
│  │  │    Normal:      conns×1,   conc×1                    │      │     │
│  │  │    Aggressive:  conns×0.67,conc×0.75                  │      │     │
│  │  │    Lossy:       conns×0.5, conc×0.5                   │      │     │
│  │  │    Saturated:   conns×0.5, conc×0.5                   │      │     │
│  │  │                                                       │      │     │
│  │  │  AIMD: 改进→+1, 恶化→×0.5, clamp [1, base×2]       │      │     │
│  │  └──────────────────────────────────────────────────────┘      │     │
│  └──────────────────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────────────┘
```

### 1.3 GetXmuxClient 获取流程

源码: `mux.go:1026-1204`

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    GetXmuxClient(ctx) 获取流程                            │
│                                                                          │
│  ┌─────────────────┐                                                     │
│  │ forceNewConnection? │──Yes──▶ 创建新连接, WaitForReady, 返回           │
│  └────────┬────────┘                                                     │
│           │ No                                                           │
│  ┌────────▼────────────────────────────────────────────────────────┐     │
│  │ Phase 1: RLock — 扫描池 + 寻找最佳候选                           │     │
│  │                                                                  │     │
│  │  遍历 pool.clients:                                             │     │
│  │    ├─ state ≠ Active → 升级Lock, RemoveAt, 继续                 │     │
│  │    ├─ IsClosed() || leftUsage==0 || LeftRequests≤0             │     │
│  │    │   || UnreusableAt过期 → 升级Lock, RemoveAt, 继续           │     │
│  │    └─ ok → i++                                                   │     │
│  │                                                                  │     │
│  │  effectiveConns = AIMD缩放后的连接数                              │     │
│  │  poolLen = 清理后的池大小                                        │     │
│  │  needNew = (poolLen==0) || (poolLen < effectiveConns)           │     │
│  └────────┬───────────────────────────────────────────────────────┘     │
│           │                                                              │
│           │ !needNew                                                     │
│  ┌────────▼────────────────────────────────────────────────────────┐     │
│  │ Snapshot-based 选择:                                            │     │
│  │                                                                  │     │
│  │  effectiveConc = AIMD缩放后的并发数                              │     │
│  │  snap = pool.Snapshot()  (RLock下的浅拷贝)                      │     │
│  │                                                                  │     │
│  │  遍历 snap:                                                     │     │
│  │    ├─ state ≠ Active → skip                                     │     │
│  │    ├─ 空闲超时 (idle > 120s) → skip                             │     │
│  │    ├─ activeStreams ≥ effectiveConc → skip                       │     │
│  │    └─ cachedScore < bestScore → best = c                        │     │
│  │                                                                  │     │
│  │  best ≠ nil:                                                    │     │
│  │    ├─ leftUsage==0 → maybeDrain, 重新snapshot                   │     │
│  │    ├─ leftUsage<0 (无限) → acquired=true                        │     │
│  │    └─ CAS(leftUsage, old, old-1) → acquired=true               │     │
│  │                                                                  │     │
│  │  acquired: WaitForReady → 返回 best                             │     │
│  │  !acquired: needNew=true                                         │     │
│  └────────┬───────────────────────────────────────────────────────┘     │
│           │ needNew                                                      │
│  ┌────────▼────────────────────────────────────────────────────────┐     │
│  │ Phase 2: 无锁 — 创建新连接                                      │     │
│  │                                                                  │     │
│  │  conn = newConnFunc()  (创建 DefaultDialerClient)               │     │
│  │  addToPool(conn)  (Lock下追加到池)                               │     │
│  │  WaitForReady(ctx)  (等待 probe HEAD 完成)                       │     │
│  │  返回 client                                                     │     │
│  └──────────────────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────────────┘
```

### 1.4 XmuxClient 调度评分

源码: `mux.go:1219-1283`

```
┌──────────────────────────────────────────────────────────────────┐
│                 scoreClient 评分公式 (V2.1)                      │
│                                                                  │
│  score = inflight × 10000 + rttMs × 10                         │
│        + retrans × 50 × combinedFixed / 10000                   │
│        + lossRate × combinedFixed / (20 × 10000)                │
│                                                                  │
│  其中:                                                           │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ confidenceFixed:                                         │   │
│  │   conf ≥ 80  → 100                                      │   │
│  │   conf ≥ 30  → 20 + (conf-30)×2                         │   │
│  │   conf < 30  → 20                                        │   │
│  │                                                           │   │
│  │ behaviorFixed:                                           │   │
│  │   LowLatency  → 50  (0.5×)  惩罚减半                     │   │
│  │   Normal       → 100 (1.0×)                              │   │
│  │   Aggressive   → 120 (1.2×)                              │   │
│  │   Lossy        → 150 (1.5×)                              │   │
│  │   Saturated    → 150 (1.5×)                              │   │
│  │                                                           │   │
│  │ combinedFixed = confidenceFixed × behaviorFixed / 100    │   │
│  │ max = 100 × 150 / 100 = 150                              │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  定点数: confidence×100, behavior×100, combined×10000           │
│  溢出安全: 最大中间值 = 100 × 50 × 15000 = 75,000,000 < 2^63  │
└──────────────────────────────────────────────────────────────────┘
```

### 1.5 Borrow/Release 原子操作

源码: `mux.go:126-179`

```
┌──────────────────────────────────────────────────────────────────────┐
│                     Borrow() CAS 循环                                 │
│                                                                      │
│  for {                                                               │
│    ① state.Load() ≠ Active → return false                           │
│    ② old = activeStreams.Load()                                     │
│    ③ CAS(activeStreams, old, old+1) 失败 → retry                    │
│    ④ CAS成功后再次检查 state.Load() ≠ Active                        │
│       → 回滚 activeStreams.Add(-1), return false                     │
│    ⑤ recomputeScore(), return true                                  │
│  }                                                                   │
│                                                                      │
│  防止竞态: GetXmuxClient返回client → Draining触发 → 流加入已退役连接  │
│                                                                      │
│  ────────────────────────────────────────────────────────────────── │
│                                                                      │
│  Release():                                                          │
│    ① activeStreams.Add(-1)                                          │
│    ② recomputeScore()                                               │
│    ③ tryClose()                                                     │
│       ├─ state ≠ Draining → return                                 │
│       ├─ activeStreams > 0 → return                                 │
│       └─ CAS(Draining, Closed) → StopProfiling, close(XmuxConn)   │
└──────────────────────────────────────────────────────────────────────┘
```

### 1.6 后台goroutine 协作

```
┌──────────────────────────────────────────────────────────────────────────┐
│                   XmuxManager 后台goroutine协作                          │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ preConnectLoop (启动时)                                          │   │
│  │   pool.Len()==0 → newXmuxClient() (async, 100ms pause)          │   │
│  │   pool.Len()<2  → newXmuxClient() (async)                        │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ healthCheckLoop (每5秒)                                          │   │
│  │                                                                  │   │
│  │  Phase 1: Lock — 扫描池                                          │   │
│  │    ├─ Draining → tryClose(), 若Closed则RemoveAt                 │   │
│  │    ├─ Closed → RemoveAt (防御性)                                 │   │
│  │    ├─ Active + IsClosed() → maybeDrain, RemoveAt                │   │
│  │    ├─ Active + 无活跃流 + idle>120s → maybeDrain, RemoveAt      │   │
│  │    ├─ Active + 新建<10s → skip (冷启动保护)                      │   │
│  │    ├─ Active + ShouldDrain(≥5连续质量下降) → maybeDrain, RemoveAt│   │
│  │    └─ Active + RTT≥5s → maybeDrain, RemoveAt                    │   │
│  │                                                                  │   │
│  │  needNew = effectiveConns - poolLen                              │   │
│  │  Unlock                                                          │   │
│  │                                                                  │   │
│  │  Phase 2: 无锁 — dial新连接补充池                                │   │
│  │                                                                  │   │
│  │  V2.1: 聚合所有client的behavior → UpdatePoolBehavior             │   │
│  │    badRatio > 0.4 → 强制标记Lossy/Saturated                     │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ networkWatchLoop (每10秒)                                        │   │
│  │                                                                  │   │
│  │  checkNetworkChange() (每30秒最多一次)                           │   │
│  │    ├─ getNetworkHash(): 活跃非回环接口的name+addr拼接           │   │
│  │    ├─ hash变化 → pendingNetChangeCount++                         │   │
│  │    ├─ 连续2次相同新hash → 确认网络变化                           │   │
│  │    │                                                             │   │
│  │    │ 确认后:                                                     │   │
│  │    │   ├─ internet.ClearDNSCache()                               │   │
│  │    │   ├─ go internet.TriggerDNSWarmup()                         │   │
│  │    │   └─ clearStaleConnections()                                │   │
│  │    │       ├─ Lock: 移除所有stale连接                            │   │
│  │    │       └─ 无锁: 创建effectiveConns个替换连接                 │   │
│  │    └─ 不同hash → 重置计数器                                      │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 2. HTTP/2 ClientConn 状态

### 2.1 proxy/http 代理的HTTP/2连接管理

源码: `proxy/http/client.go:40-350`

```
┌──────────────────────────────────────────────────────────────────────────┐
│              proxy/http Client — HTTP/2 ClientConn 状态                   │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                    h2Conn 缓存 (全局)                             │   │
│  │                                                                  │   │
│  │  var cachedH2Conns map[net.Destination]h2Conn                    │   │
│  │  var cachedH2Mutex sync.Mutex                                    │   │
│  │                                                                  │   │
│  │  h2Conn {                                                        │   │
│  │    rawConn  net.Conn        ← 底层TCP+TLS连接                    │   │
│  │    h2Conn   *http2.ClientConn ← HTTP/2客户端连接                 │   │
│  │  }                                                               │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │              setUpHTTPTunnel 连接建立流程                         │   │
│  │                                                                  │   │
│  │  ┌──────────────┐                                               │   │
│  │  │检查cachedH2Conns│                                            │   │
│  │  │  (Mutex保护)   │                                              │   │
│  │  └──────┬───────┘                                               │   │
│  │         │                                                         │   │
│  │    found?                                                         │   │
│  │    ├─ Yes: CanTakeNewRequest()?                                  │   │
│  │    │   ├─ Yes: connectHTTP2(rawConn, cachedH2Conn) → 复用       │   │
│  │    │   └─ No: 走下面的dial流程                                   │   │
│  │    │                                                             │   │
│  │    └─ No / expired:                                              │   │
│  │       ┌──────────────────────────────────────────────┐           │   │
│  │       │ rawConn = dialer.Dial(ctx, dest)              │           │   │
│  │       └──────────────┬───────────────────────────────┘           │   │
│  │                      │                                            │   │
│  │            ┌─────────▼──────────┐                                │   │
│  │            │ iConn = UnwrapStats │                                │   │
│  │            └─────────┬──────────┘                                │   │
│  │                      │                                            │   │
│  │            ┌─────────▼──────────────────────────┐                │   │
│  │            │ TLS握手 + 检查NegotiatedProtocol    │                │   │
│  │            └──────┬──────────┬──────────┬───────┘                │   │
│  │                   │          │          │                          │   │
│  │            "http/1.1"    "h2"       "" (默认)                     │   │
│  │                   │          │          │                          │   │
│  │            ┌──────▼──┐  ┌───▼────────┐ │                         │   │
│  │            │connectHTTP1│ │Transport{} │ │                         │   │
│  │            │写CONNECT │  │.NewClient  │ │                         │   │
│  │            │读200 OK  │  │Conn(raw)   │ │                         │   │
│  │            └─────────┘  └───┬────────┘ │                         │   │
│  │                             │          │                          │   │
│  │                    ┌────────▼────────┐ │                         │   │
│  │                    │connectHTTP2(     │ │                         │   │
│  │                    │ rawConn, h2CC)   │ │                         │   │
│  │                    │ io.Pipe → body   │ │                         │   │
│  │                    └────────┬────────┘ │                         │   │
│  │                             │          │                          │   │
│  │                    ┌────────▼────────────────────┐              │   │
│  │                    │ 缓存到 cachedH2Conns[dest]  │              │   │
│  │                    │ (下次复用)                    │              │   │
│  │                    └─────────────────────────────┘              │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────┘
```

### 2.2 http2Conn 双向隧道

源码: `proxy/http/client.go:352-374`

```
┌──────────────────────────────────────────────────────────────────┐
│                    http2Conn 结构体                               │
│                                                                  │
│  type http2Conn struct {                                         │
│    net.Conn                  ← 底层连接 (用于RemoteAddr等)       │
│    in  *io.PipeWriter        ← 写入端 (POST body)               │
│    out io.ReadCloser         ← 读取端 (RESPONSE body)           │
│  }                                                               │
│                                                                  │
│  Read(p)  → out.Read(p)     ← 从HTTP/2响应体读取                │
│  Write(p) → in.Write(p)     ← 写入HTTP/2请求的PipeWriter       │
│  Close()  → in.Close() + out.Close()                             │
│                                                                  │
│  数据流:                                                         │
│  ┌──────────┐    Write     ┌──────────┐   HTTP/2    ┌────────┐ │
│  │ 协议处理器│ ───────────▶│ PipeWriter│ ──POST──▶ │ 代理服务器│ │
│  └──────────┘              └──────────┘             └────────┘ │
│  ┌──────────┐    Read      ┌──────────┐   HTTP/2    ┌────────┐ │
│  │ 协议处理器│ ◀───────────│ReadCloser│ ◀─200 body─│ 代理服务器│ │
│  └──────────┘              └──────────┘             └────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

### 2.3 splithttp 的 HTTP/2 Transport 选择

源码: `dialer.go:252-532`

```
┌──────────────────────────────────────────────────────────────────────────┐
│                  HTTP版本决策 + Transport创建                             │
│                                                                          │
│  decideHTTPVersion():                                                   │
│    realityConfig ≠ nil → "2"                                            │
│    tlsConfig == nil   → "1.1"                                           │
│    len(NextProtocol)==1 && NextProtocol[0]=="http/1.1" → "1.1"          │
│    len(NextProtocol)==1 && NextProtocol[0]=="h3" → "3"                   │
│    其他 → "2"                                                           │
│                                                                          │
│  Transport创建:                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ HTTP/3:                                                           │   │
│  │   h3Transport = &http3.Transport{QUICConfig, Dial: ...}          │   │
│  │   h2Transport = &http2.Transport{DialTLSContext: dialContext}    │   │
│  │   transport = newHappyEyeballsTransport(h3, h2)                  │   │
│  │   (QUIC优先, TCP fallback)                                       │   │
│  │                                                                   │   │
│  │ HTTP/2:                                                           │   │
│  │   transport = &http2.Transport{                                   │   │
│  │     DialTLSContext: dialContext,                                  │   │
│  │     IdleConnTimeout: ConnIdleTimeout,                            │   │
│  │     ReadIdleTimeout: keepAlivePeriod (默认ChromeH2KeepAlive),   │   │
│  │   }                                                              │   │
│  │                                                                   │   │
│  │ HTTP/1.1:                                                         │   │
│  │   transport = &http.Transport{                                    │   │
│  │     DialTLSContext / DialContext: dialContext,                    │   │
│  │     DisableKeepAlives: true,  // 避免chunked+keepalive bug      │   │
│  │   }                                                              │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  dialContext流程 (所有HTTP版本共用):                                      │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ 1. internet.DialSystem(ctx, dest, socketSettings)               │   │
│  │    → rawConn (TCP socket)                                        │   │
│  │                                                                   │   │
│  │ 2. onNewConn(rawConn)  → 触发TransportProfile开始采样           │   │
│  │                                                                   │   │
│  │ 3. TcpmaskManager.WrapConnClient(conn) (如果配置了)             │   │
│  │                                                                   │   │
│  │ 4. 安全层包装:                                                   │   │
│  │    ├─ REALITY: reality.UClient(conn, config, ctx, dest)         │   │
│  │    └─ TLS:                                                       │   │
│  │       ├─ 有fingerprint: tls.UClient(conn, config, fingerprint)  │   │
│  │       │              → uTLS握手 (Chrome/Firefox指纹)            │   │
│  │       └─ 无fingerprint: tls.Client(conn, config)                │   │
│  │                    → 标准Go TLS握手                              │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 3. TLS Tunnel 状态

### 3.1 TLS/UConn 连接包装层

源码: `tls/tls.go:30-176`

```
┌──────────────────────────────────────────────────────────────────────────┐
│                     TLS Tunnel 包装层次                                   │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  Interface: tls.Interface                                        │   │
│  │    ├─ net.Conn                                                   │   │
│  │    ├─ HandshakeContext(ctx) error                                │   │
│  │    ├─ VerifyHostname(host) error                                 │   │
│  │    ├─ HandshakeContextServerName(ctx) string                     │   │
│  │    └─ NegotiatedProtocol() string                                │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  Conn (标准TLS)                                                   │   │
│  │    struct { *tls.Conn }                                          │   │
│  │                                                                   │   │
│  │  Close():                                                        │   │
│  │    timer = AfterFunc(250ms, c.Conn.NetConn().Close())            │   │
│  │    defer timer.Stop()                                             │   │
│  │    return c.Conn.Close()                                          │   │
│  │    → 优雅关闭: 先尝试TLS close_notify, 250ms后强制关闭           │   │
│  │                                                                   │   │
│  │  Client(conn, config) → &Conn{tls.Client(conn, config)}          │   │
│  │  Server(conn, config) → &Conn{tls.Server(conn, config)}          │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  UConn (uTLS指纹伪装)                                            │   │
│  │    struct { *utls.UConn }                                         │   │
│  │                                                                   │   │
│  │  Close(): 同Conn的250ms优雅关闭                                   │   │
│  │                                                                   │   │
│  │  UClient(conn, config, fingerprint) → &UConn{utls.UClient(...)} │   │
│  │                                                                   │   │
│  │  WebsocketHandshakeContext(ctx):                                  │   │
│  │    ① BuildHandshakeState()                                        │   │
│  │    ② 修改ALPN:                                                   │   │
│  │       ├─ 非 h2+http/1.1 → 强制 http/1.1                         │   │
│  │       └─ 有ECH → 保持outer ALPN                                  │   │
│  │    ③ 遍历Extensions, 找到ALPNExtension, 替换AlpnProtocols       │   │
│  │    ④ 重新BuildHandshakeState()                                    │   │
│  │    ⑤ HandshakeContext(ctx)                                        │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────┘
```

### 3.2 完整的TLS Tunnel建立流程

```
┌──────────────────────────────────────────────────────────────────────────┐
│              TLS Tunnel 完整建立流程 (从Dial到Ready)                     │
│                                                                          │
│  ┌─────────────────────┐                                                │
│  │ getHTTPClient()      │                                                │
│  │  (dialer.go:133)    │                                                │
│  └──────────┬──────────┘                                                │
│             │                                                            │
│  ┌──────────▼──────────────────────────────────────────────────────┐    │
│  │ 1. globalDialerAccess.Lock()                                    │    │
│  │    key = newMuxKey(dest, streamSettings)                        │    │
│  │    xmuxManager = globalDialerMap[key]  (查找或创建)             │    │
│  │    globalDialerAccess.Unlock()                                   │    │
│  └──────────┬──────────────────────────────────────────────────────┘    │
│             │                                                            │
│  ┌──────────▼──────────────────────────────────────────────────────┐    │
│  │ 2. xmuxClient = xmuxManager.GetXmuxClient(ctx)                 │    │
│  │    → 从池中选择 或 创建新连接                                    │    │
│  └──────────┬──────────────────────────────────────────────────────┘    │
│             │                                                            │
│  ┌──────────▼──────────────────────────────────────────────────────┐    │
│  │ 3. createHTTPClient(dest, streamSettings)                       │    │
│  │    (dialer.go:271)                                              │    │
│  │                                                                  │    │
│  │    ┌──────────────────────────────────────────────────────┐     │    │
│  │    │ dialContext = func(ctx) (net.Conn, error) {          │     │    │
│  │    │                                                        │     │    │
│  │    │  ┌────────────────────────────────────────────┐       │     │    │
│  │    │  │ A. TCP拨号                                    │       │     │    │
│  │    │  │    rawConn = internet.DialSystem(ctx,dest)  │       │     │    │
│  │    │  │    → TCP socket建立                           │       │     │    │
│  │    │  └────────────────┬───────────────────────────┘       │     │    │
│  │    │                   │                                    │     │    │
│  │    │  ┌────────────────▼───────────────────────────┐       │     │    │
│  │    │  │ B. 通知TransportProfile (raw TCP socket)    │       │     │    │
│  │    │  │    onNewConn(rawConn)                       │       │     │    │
│  │    │  │    → tcpinfo.NewProfile(rawConn)            │       │     │    │
│  │    │  │    → profile.Start() (后台TCP_INFO采样)     │       │     │    │
│  │    │  │    → profile.OnUpdate → xmuxClient.         │       │     │    │
│  │    │  │              UpdateQuality()                │       │     │    │
│  │    │  └────────────────┬───────────────────────────┘       │     │    │
│  │    │                   │                                    │     │    │
│  │    │  ┌────────────────▼───────────────────────────┐       │     │    │
│  │    │  │ C. TCP Mask (可选)                          │       │     │    │
│  │    │  │    TcpmaskManager.WrapConnClient(conn)     │       │     │    │
│  │    │  └────────────────┬───────────────────────────┘       │     │    │
│  │    │                   │                                    │     │    │
│  │    │  ┌────────────────▼───────────────────────────┐       │     │    │
│  │    │  │ D. 安全层握手                               │       │     │    │
│  │    │  │                                            │       │     │    │
│  │    │  │  REALITY路径:                              │       │     │    │
│  │    │  │    reality.UClient(conn, config, ctx, dest)│       │     │    │
│  │    │  │    → REALITY握手 (伪装为目标网站)          │       │     │    │
│  │    │  │                                            │       │     │    │
│  │    │  │  uTLS路径 (有fingerprint):                 │       │     │    │
│  │    │  │    tls.UClient(conn, config, fingerprint)  │       │     │    │
│  │    │  │    → uTLS握手 (Chrome/Firefox指纹)         │       │     │    │
│  │    │  │                                            │       │     │    │
│  │    │  │  标准TLS路径:                              │       │     │    │
│  │    │  │    tls.Client(conn, config)                │       │     │    │
│  │    │  │    → 标准Go TLS握手                        │       │     │    │
│  │    │  │                                            │       │     │    │
│  │    │  │  → 返回 wrappedConn                        │       │     │    │
│  │    │  └────────────────────────────────────────────┘       │     │    │
│  │    │                                                        │     │    │
│  │    │  return wrappedConn                                    │     │    │
│  │    │ }                                                      │     │    │
│  │    └──────────────────────────────────────────────────────┘     │    │
│  └──────────┬──────────────────────────────────────────────────────┘    │
│             │                                                            │
│  ┌──────────▼──────────────────────────────────────────────────────┐    │
│  │ 4. probeConnection(conn, xmuxClient)                            │    │
│  │    (mux.go:944)                                                │    │
│  │                                                                  │    │
│  │    HEAD请求到 probeURL → 触发真实的TCP/TLS连接                  │    │
│  │    success → close(xmuxClient.ready)  (解除阻塞)               │    │
│  │    failure → xmuxClient.MarkDead() + close(ready)              │    │
│  └──────────┬──────────────────────────────────────────────────────┘    │
│             │                                                            │
│  ┌──────────▼──────────────────────────────────────────────────────┐    │
│  │ 5. WaitForReady(ctx)                                            │    │
│  │    select {                                                     │    │
│  │      case <-ready: return probeErr                              │    │
│  │      case <-ctx.Done(): return ctx.Err()                        │    │
│  │    }                                                            │    │
│  └──────────┬──────────────────────────────────────────────────────┘    │
│             │                                                            │
│  ┌──────────▼──────────────────────────────────────────────────────┐    │
│  │ 6. 客户端连接就绪, 开始数据传输                                 │    │
│  │    SetOnRTT → xmuxClient.UpdateRTT() (EWMA平滑)               │    │
│  │    SetOnFatalError → xmuxClient.MarkDead() (Fast Eviction)     │    │
│  └──────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────┘
```

### 3.3 Fast Eviction 错误分类

源码: `client.go:92-131`

```
┌──────────────────────────────────────────────────────────────────┐
│              isFatalConnError 错误分类                            │
│                                                                  │
│  触发 Fast Eviction (MarkDead) 的错误:                          │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ 快速路径 (branch-predictor friendly):                     │   │
│  │   io.EOF                                                  │   │
│  │   io.ErrUnexpectedEOF                                     │   │
│  │   net.ErrClosed                                           │   │
│  │   syscall.EPIPE                                           │   │
│  │   syscall.ECONNRESET                                      │   │
│  │                                                           │   │
│  │ 慢速路径 (string matching):                               │   │
│  │   tls.RecordHeaderError                                   │   │
│  │   "tls:"                                                  │   │
│  │   "x509:"                                                 │   │
│  │   "cipher suite"                                          │   │
│  │   "SSL_VERSION_OR_CIPHER_MISMATCH"                       │   │
│  │   "RemoteCertificateNameMismatch"                        │   │
│  │   "GOAWAY"                                                │   │
│  │   "connection shutdown"                                   │   │
│  │   "transport closed"                                      │   │
│  │   "server sent disconnect"                                │   │
│  │   "client connection force closed"                        │   │
│  │   "http2: Transport closing idle connection"              │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  不触发 Fast Eviction 的错误 (由各自的retry逻辑处理):            │
│    dial失败, stream-level错误, context取消                       │
└──────────────────────────────────────────────────────────────────┘
```

### 3.4 NetworkLearner 行为学习

源码: `quality/learning.go`, `quality/behavior.go`

```
┌──────────────────────────────────────────────────────────────────────────┐
│                   NetworkLearner 行为学习                                │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  输入: quality.Snapshot (来自tcpinfo.Profile)                   │   │
│  │                                                                  │   │
│  │  ClassifyBehavior(snap) → Behavior:                             │   │
│  │    confidence < 10 || !hasRTT → Unknown                         │   │
│  │    RTT<15ms, jitter<5%, loss<0.5%, retrans<3 → Aggressive     │   │
│  │    RTT<30ms, jitter<10%, loss<1%, retrans<5 → LowLatency      │   │
│  │    loss>1% || retrans>10 → Lossy                                │   │
│  │    RTT>200ms, unacked>50 → Saturated                            │   │
│  │    其他 → Normal                                                │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  NetworkLearner 状态:                                           │   │
│  │                                                                  │   │
│  │  recent[32]: 环形缓冲区 (最近32次分类)                         │   │
│  │  head: 写指针                                                    │   │
│  │  count: 已填充数量 (max 32)                                     │   │
│  │  behaviorCounts: 各行为累计次数                                 │   │
│  │  dominant: 最近窗口内最频繁的行为                               │   │
│  │  transitions: 行为变化次数 (用于检测不稳定性)                   │   │
│  │                                                                  │   │
│  │  Record(snap) → Behavior:                                       │   │
│  │    ① b = ClassifyBehavior(snap)                                 │   │
│  │    ② recent[head] = b, head = (head+1) % 32                    │   │
│  │    ③ behaviorCounts[b]++                                         │   │
│  │    ④ if b ≠ lastBehavior → transitions++                        │   │
│  │    ⑤ dominant = computeDominant() (从recent窗口统计)            │   │
│  │    ⑥ return dominant                                            │   │
│  │                                                                  │   │
│  │  TransitionRate:                                                 │   │
│  │    transitions / (totalSamples - 1)                              │   │
│  │    0.0 = 稳定, 1.0 = 混乱                                      │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  数据流闭环:                                                     │   │
│  │                                                                  │   │
│  │  raw TCP socket                                                 │   │
│  │       │                                                          │   │
│  │       ▼                                                          │   │
│  │  tcpinfo.Profile (后台goroutine, 定期采样TCP_INFO)              │   │
│  │       │ OnUpdate(snap)                                          │   │
│  │       ▼                                                          │   │
│  │  XmuxClient.UpdateQuality(score, conf, retrans, loss)          │   │
│  │       │                                                          │   │
│  │       ├──▶ NetworkLearner.Record(snap) → dominant behavior      │   │
│  │       │                                                          │   │
│  │       ├──▶ recomputeScore() → cachedScore (调度用)              │   │
│  │       │                                                          │   │
│  │       └──▶ consecDrops 追踪 (≥5则ShouldDrain)                  │   │
│  │                                                                  │   │
│  │  XmuxManager.healthCheckTick():                                 │   │
│  │    聚合所有client的dominant → UpdatePoolBehavior(debounce=3)    │   │
│  │    → applyAIMD → 调整 _dynamicConns/_dynamicConc               │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 4. 全局生命周期总览

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         连接生命周期总览                                      │
│                                                                              │
│  ┌────────────┐    ┌─────────────┐    ┌──────────────┐    ┌──────────────┐ │
│  │  Dial()     │───▶│ getHTTPClient│───▶│ GetXmuxClient│───▶│ openStream/  │ │
│  │  (splithttp)│    │  (dialer)    │    │  (pool)       │    │ postPacket   │ │
│  └────────────┘    └─────────────┘    └──────────────┘    └──────────────┘ │
│                                                                              │
│  Phase 1: 连接池管理                                                        │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  globalDialerMap[MuxKey] → XmuxManager                              │   │
│  │    └─ XmuxClientPool → []*XmuxClient                               │   │
│  │       └─ XmuxClient {                                              │   │
│  │            XmuxConn: DefaultDialerClient                           │   │
│  │            state: Active→Draining→Closed                           │   │
│  │            activeStreams: 原子计数                                   │   │
│  │            leftUsage: 连接复用次数 (-1=无限)                       │   │
│  │            LeftRequests: 请求次数限制                               │   │
│  │            cachedScore: 调度评分                                   │   │
│  │            qualityScore/confidence/retrans/loss: 链路质量          │   │
│  │            learner: NetworkLearner (行为学习)                      │   │
│  │            profile: tcpinfo.Profile (TCP_INFO采样)                 │   │
│  │          }                                                          │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  Phase 2: 底层连接                                                          │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  DefaultDialerClient {                                              │   │
│  │    client: *http.Client (封装http2/1.1/3 Transport)                │   │
│  │    uploadRawPool: sync.Pool (HTTP/1.1上传连接池)                   │   │
│  │    dialUploadConn: func (底层拨号)                                  │   │
│  │  }                                                                  │   │
│  │                                                                      │   │
│  │  dialContext:                                                       │   │
│  │    TCP → [TcpMask] → [REALITY/uTLS/标准TLS] → wrappedConn          │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  Phase 3: 数据传输                                                          │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  splitConn {                                                        │   │
│  │    writer: pipe.Writer / uploadWriter                               │   │
│  │    reader: pipe.Reader / resp.Body / uploadQueue                    │   │
│  │    onClose: func() { xmuxClient.Release() }                       │   │
│  │  }                                                                  │   │
│  │                                                                      │   │
│  │  Modes:                                                             │   │
│  │    stream-one:  单个GET请求, body=请求数据, response=响应           │   │
│  │    stream-up:   上行=POST body, 下行=GET response                   │   │
│  │    stream-down: 下行=GET response, 上行=多个POST                    │   │
│  │    packet-up:   多个POST (带seq), 每个POST一个包                   │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. 关键并发安全设计

```
┌──────────────────────────────────────────────────────────────────────────┐
│                      并发安全设计要点                                     │
│                                                                          │
│  1. XmuxClientPool: RWMutex                                             │
│     读 (Len/Snapshot): RLock — 允许并发调度                             │
│     写 (Remove/Append): Lock — 独占修改                                │
│     升级: RLock→释放→Lock→re-check index (防止ABA)                     │
│                                                                          │
│  2. XmuxClient原子操作:                                                  │
│     state: atomic.Int32 (CAS循环保证状态转换原子性)                      │
│     activeStreams: atomic.Int32 (Borrow用CAS+验证防止竞态)              │
│     cachedScore: atomic.Int64 (锁无关读)                                 │
│     leftUsage/LeftRequests: atomic.Int32 (CAS递减)                       │
│                                                                          │
│  3. GetXmuxClient:                                                       │
│     Phase 1 (RLock) → 释放 → Phase 2 (无锁) → dial新连接              │
│     RLock→Lock升级: 释放RLock后重新检查index                            │
│                                                                          │
│  4. globalDialerMap: sync.Mutex保护全局map                              │
│     全局cleanup goroutine: 定时扫描+删除idle manager                    │
│                                                                          │
│  5. DefaultDialerClient:                                                │
│     onRTT/onNewConn/onFatalError: 回调函数, 由xmuxManager设置          │
│     closed: atomic.Bool                                                  │
│                                                                          │
│  6. http2Conn:                                                           │
│     in (PipeWriter) + out (ReadCloser): 无锁, 管道语义保证顺序         │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 6. 审计关注点

```
┌──────────────────────────────────────────────────────────────────────────┐
│                      专家审计关注点                                       │
│                                                                          │
│  [XMUX]                                                                 │
│  ① CAS循环的活性保证: Borrow()中的无限循环是否可能死锁?               │
│  ② state验证窗口: CAS成功后再次检查state是否足够防止TOCTOU?          │
│  ③ 锁升级: RLock→Lock路径中index是否可能被其他goroutine修改?          │
│  ④ AIMD clamping: 是否所有边界case (base=0, base=1) 都正确处理?      │
│  ⑤ probe超时: WaitForReady是否可能永远阻塞? (ctx超时)                │
│                                                                          │
│  [HTTP/2]                                                               │
│  ⑥ cachedH2Conns生命周期: 无LRU/超时机制, 内存泄漏风险?             │
│  ⑦ CanTakeNewRequest(): 何时返回false? GOAWAY处理是否正确?           │
│  ⑧ io.Pipe的背压: http2Conn.Write是否可能阻塞?                       │
│                                                                          │
│  [TLS]                                                                  │
│  ⑨ Close()的250ms超时: 是否足够覆盖TLS close_notify?                 │
│  ⑩ ECH + ALPN: WebsocketHandshakeContext中ECH分支的ALPN处理         │
│  ⑪ uTLS指纹一致性: HandshakeState重建是否可能改变指纹?               │
│                                                                          │
│  [全局]                                                                 │
│  ⑫ globalDialerCleanup: 动态timeout计算是否正确? (pool=11时)         │
│  ⑬ Network change debounce: 2次确认是否足够防止误判?                  │
│  ⑭ Fast Eviction string matching: 是否可能误判?                       │
│  ⑮ scoreClient定点数: 是否存在精度损失或溢出?                         │
│  ⑯ leftUsage=-1 (无限复用): 是否可能导致连接永不退役?                 │
└──────────────────────────────────────────────────────────────────────────┘
```

---

*文档生成时间: 2026-07-10*
*代码版本: Bray-Core HEAD*
*审计目的: 内部连接生命周期、HTTP/2 ClientConn状态、TLS tunnel状态的架构审查*
