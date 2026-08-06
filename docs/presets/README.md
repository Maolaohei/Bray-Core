# Bray-V2 Wave-1 Transport Presets

These presets are **recommended starting points** for Bray client **and** Bray
server. JSON shape still looks like Xray `streamSettings`, but **`main` is
Bray-only**: both ends should run Bray-Core. Session MAC is UUID-derived by
default (no extra `x-bray-session-secret` required for VLESS).

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

CDN example: on hostile edges prefer `mode: "packet-up"` or `"stream-up"` first.
Use `stream-one` + `x-bray-mode-degrade` only if you accept the cascade ladder fingerprint
(headers are client-local and never leave the process):

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

Wave-by-wave delivery history (wave2–6) and the full-body overview are archived in [`../archive/README.md`](../archive/README.md).

## Green-zone hardening (default-safe)

Hard caps and fail-invalidate rules that keep defaults safe (compat + perf first):

| Item | Behavior | Perf / compat |
|------|----------|---------------|
| `x-bray-*` strip | Client-local only; never on wire | zero wire cost; locked by tests |
| Cascade step jitter | On **failed** mode steps only, 0–200ms | happy path zero |
| XMUX default jitter | Unconfigured `xmux` fields: process-stable ±10% in browser band | explicit ranges unchanged |
| CDN first mode | Docs/preset: prefer `packet-up`/`stream-up` on hostile edges | **no** global auto→packet-up change |
| Multi-endpoint | hard cap `MaxMultiEndpoints=4` (primary+extras); no IP scan ranges; sticky on | opt-in headers |
| Sticky EP fail-invalidate | race/dial fail of preferred sticky EP clears affinity (mirror mode sticky) | default on with multi |
| LeftRequests half-open | stream-up decrements download quota only after upload also succeeds | cascade-safe |
| Fatal open typed | `net.Error` / `OpError` / `ErrClosed` / EOF first, then tight string needles | XMUX rotate |

Multi-endpoint operator constraints:
- Keep the list short (2–3 hosts/IPs). Do not put large scan ranges in `x-bray-endpoints`.
- Prefer sticky endpoint so the race does not re-scan every dial.
- Control headers stay process-local (stripped by `GetRequestHeader`).

## Compatibility

| Field omitted | Bray-V2 default behavior |
|---------------|--------------------------|
| `xmux` entirely | browser-like bases (8-16 / 2-4 / 64-128 / 600-1200s) with process-stable ±10% jitter, clamped to browser band |
| explicit `xmux.*` | user values win |
| REALITY server | L2 when eligible; shape/age gated; fail -> quarantine -> calibrate |

## Observability

- REALITY: `CacheReport()` includes L1/L2 hits, L2 fails, L1 fails, L2 soft demotions, quarantines, calibrations
- XHTTP H3 race: `GetH3Metrics()` / `H3MetricsReport()`
- XMUX: existing `XmuxManager.GetMetrics()` / `LogMetrics()`
- Bray-V2 cascade/sticky/multi: `GetBrayV2Metrics()` / `BrayV2MetricsReport()`


## VLESS + Vision under XHTTP (expectations)

Bray default stack is **VLESS over XHTTP + REALITY/TLS**. Application-layer notes:

| Setup | Vision (`xtls-rprx-vision`) | Notes |
|-------|------------------------------|-------|
| VLESS + REALITY/TLS **direct** (no XHTTP) | splice may apply (`CanSpliceCopy=2`) | best latency/CPU |
| **VLESS + XHTTP** (+ REALITY/TLS) | treat as **copy path** (`CanSpliceCopy=3` in practice) | XHTTP/XMUX owns the transport; do not expect zero-copy Vision |
| VLESS Encryption (`decryption` / client encryption) | opt-in only | extra AEAD CPU; keep off unless threat model needs it |

Green-zone VLESS hardening (default-safe):
- Encoding invalid-user errors no longer echo full UUIDs into logs.
- Encryption header/AEAD/XorConn covered by unit tests (no wire-format change).
- `OutBytesCapacity = 5+8192+16` documented next to the write chunk limit.

## Bray-only auth notes

- Prefer **no** manual x-bray-session-secret when using VLESS: MAC key is derived from the account UUID on both ends.
- All x-bray-* keys are **local control** and are stripped before the request hits the wire.
- Packet-up defaults (window/chunk) are automatic; tune scMaxEachPostBytes / scMaxBufferedPosts only if you know the edge limits.

