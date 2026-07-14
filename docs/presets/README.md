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

## Wave-2 / Wave-3 recovery / CDN cascade (opt-in)

| Feature | How to enable | Behavior |
|---------|---------------|----------|
| Mode degrade (runtime) | `mode: "auto"` or header `x-bray-mode-degrade: "true"` | On open fail: stream-one -> stream-up -> packet-up |
| Multi-endpoint race (runtime) | header `x-bray-multi-endpoint: "true"` + `x-bray-endpoints: "a:443,b:443"` | TCP dial races extras; first success wins |
| REALITY soft demotion | server default | L2 fail -> next handshake L1 (Suspect), not L0 |
| Control header isolation | automatic | `x-bray-*` never sent on wire |
| Sticky last-good mode | default on when cascade allowed; `x-bray-sticky-mode: "false"` to opt out | Prefer last successful mode (TTL) |
| Sticky multi-endpoint | default on when multi-endpoint on; `x-bray-sticky-endpoint: "false"` to opt out | Prefer last winning IP/host first (TTL) |
| Bray stats publish | automatic when `stats` app enabled (30s mirror); or manual Publish | Mirror atomics into `bray-v2>>>...` counters |
| Sticky TTL A/B | headers `x-bray-sticky-mode-ttl` / `x-bray-sticky-endpoint-ttl` | Override default 10m (max 24h) |
| Rates report | `GetBrayV2Rates()` / `BrayV2RatesReport()` | Field A/B ratios (read-only) |

CDN stream-one with explicit degrade opt-in example (headers are client-local):

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
    "headers": {
      "x-bray-mode-degrade": "true",
      "x-bray-multi-endpoint": "true",
      "x-bray-endpoints": "1.2.3.4:443,5.6.7.8:443",
      "x-bray-sticky-mode": "true",
      "x-bray-sticky-endpoint": "true"
    },
    "xPaddingBytes": "100-500",
    "xmux": {
      "maxConcurrency": { "from": 4, "to": 8 },
      "maxConnections": { "from": 1, "to": 2 },
      "hMaxReusableSecs": { "from": 300, "to": 600 }
    }
  }
}
```

See `docs/bray-v2-wave2.md` ... `docs/bray-v2-wave6.md`, and full-body `docs/bray-v2-full.md`.

## Compatibility

| Field omitted | Bray-V2 default behavior |
|---------------|--------------------------|
| `xmux` entirely | concurrency 8-16, connections 2-4, reuse 64-128, reusable 600-1200s |
| explicit `xmux.*` | user values win |
| REALITY server | L2 when eligible; shape/age gated; fail -> quarantine -> calibrate |

## Observability

- REALITY: `CacheReport()` includes L1/L2 hits, L2 fails, L1 fails, L2 soft demotions, quarantines, calibrations
- XHTTP H3 race: `GetH3Metrics()` / `H3MetricsReport()`
- XMUX: existing `XmuxManager.GetMetrics()` / `LogMetrics()`
- Bray-V2 cascade/sticky/multi: `GetBrayV2Metrics()` / `BrayV2MetricsReport()`
