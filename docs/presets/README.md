# Bray-V2 Wave-1 Transport Presets

Zero-config defaults remain compatible with Xray configs. These presets are
**recommended starting points** for operators; copy fields into your existing
`streamSettings` rather than requiring new protocol fields.

## 1) Direct REALITY + XHTTP (RA)

Use when the landing IP is stable enough and TLS fingerprint consistency matters.

```json
{
  "network": "xhttp",
  "security": "reality",
  "realitySettings": {
    "show": false,
    "fingerprint": "chrome",
    "serverName": "www.example-target.com",
    "publicKey": "<server-public-key>",
    "shortId": "<short-id>",
    "spiderX": "/"
  },
  "xhttpSettings": {
    "path": "/api/v1/connect",
    "mode": "stream-one",
    "xPaddingBytes": "100-1000",
    "xmux": {
      "maxConcurrency": { "from": 8, "to": 16 },
      "maxConnections": { "from": 2, "to": 4 },
      "cMaxReuseTimes": { "from": 64, "to": 128 },
      "hMaxRequestTimes": { "from": 400, "to": 800 },
      "hMaxReusableSecs": { "from": 600, "to": 1200 }
    }
  }
}
```

Notes:
- Server REALITY amortize defaults to L2 with hardened eligibility (shape + age).
- Omit `xmux` to use Bray-V2 browser-like defaults (same ranges as above).

## 2) CDN front (Cloudflare-like)

Prefer H2 + stream-one/stream-up. Keep path looking like a normal API/static route.

```json
{
  "network": "xhttp",
  "security": "tls",
  "tlsSettings": {
    "serverName": "cdn.example.com",
    "fingerprint": "chrome",
    "alpn": ["h2", "http/1.1"]
  },
  "xhttpSettings": {
    "host": "cdn.example.com",
    "path": "/assets/app/channel",
    "mode": "stream-one",
    "xPaddingBytes": "100-500",
    "xPaddingObfsMode": true,
    "xPaddingMethod": "tokenish",
    "xPaddingPlacement": "query",
    "xPaddingKey": "x_pad",
    "xmux": {
      "maxConcurrency": { "from": 4, "to": 8 },
      "maxConnections": { "from": 1, "to": 2 },
      "hMaxReusableSecs": { "from": 300, "to": 600 }
    }
  }
}
```

## 3) CDN / reverse-proxy generic (packet-up fallback)

When edge rejects long bidirectional streams.

```json
{
  "network": "xhttp",
  "security": "tls",
  "tlsSettings": {
    "serverName": "edge.example.com",
    "fingerprint": "chrome"
  },
  "xhttpSettings": {
    "host": "edge.example.com",
    "path": "/upload",
    "mode": "packet-up",
    "xPaddingBytes": "50-300",
    "scMaxEachPostBytes": "500000-1000000",
    "scMinPostsIntervalMs": "20-40",
    "xmux": {
      "maxConcurrency": { "from": 2, "to": 4 },
      "maxConnections": { "from": 1, "to": 2 },
      "hMaxReusableSecs": { "from": 180, "to": 420 }
    }
  }
}
```

## Compatibility

| Field omitted | Bray-V2 default behavior |
|---------------|--------------------------|
| `xmux` entirely | concurrency 8–16, connections 2–4, reuse 64–128, reusable 600–1200s |
| explicit `xmux.*` | user values win |
| REALITY server | L2 when eligible; shape/age gated; fail → quarantine → calibrate |

## Observability

- REALITY: `CacheReport()` includes L1/L2 hits, L2 fails, quarantines, calibrations
- XHTTP H3 race: `GetH3Metrics()` / `H3MetricsReport()`
- XMUX: existing `XmuxManager.GetMetrics()` / `LogMetrics()`
