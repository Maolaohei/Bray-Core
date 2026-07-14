# Bray-V2 Full Body

Branch: `Bray-V2`
Audience: operators + core maintainers
Principle: **compatibility first, performance second, recovery always opt-in or auto-safe**.

## What "full body" means

Bray-V2 is not a new protocol. It is a **hardened stack** on XHTTP + REALITY Amortize (RA):

1. **Fast happy path** when the path is clean (stream-one / L2 amortize / browser-like XMUX).
2. **Graceful degradation** when CDN/edge/GFW breaks long streams or L2.
3. **Observability** so failures are countable, not tribal knowledge.
4. **No wire fingerprint** from Bray control headers (`x-bray-*` client-local only).

## Stack map

```
Client Dial
  |- Multi-endpoint race (opt-in) ---- TCP candidates
  |- REALITY UClient ------------------ L2 amortize / Suspect soft L1 / quarantine
  |- HTTP/2 or H3 Happy-Eyeballs ------ H3 prefer, H2 fallback, cooldown metrics
  |- XMUX pool ------------------------ browser defaults, probe, fast eviction
  |- Mode open cascade ---------------- sticky last-good + stream-one->up->packet-up
  '- Upload path ---------------------- stream body or packet-up POST loop
```

## Waves delivered

| Wave | Theme | Key outcomes |
|------|-------|--------------|
| **1** | Defaults + L2 harden + H3 metrics | Browser-like XMUX defaults; REALITY L2 eligibility gates; H3 race metrics |
| **2** | Recovery helpers | REALITY Suspect soft demotion (L2 fail -> next HS L1); mode ladder helpers; multi-endpoint race helper; H3 cooldown metric fix |
| **3** | Runtime wiring | Dial mode cascade; multi-endpoint in TCP dial; strip `x-bray-*` |
| **4** | Intelligence | Sticky last-good mode; Bray-V2 metrics; adaptive XMUX open eviction; this overview |

## Optimization matrix (compat / perf / risk)

| Lever | Compat | Perf (good path) | Recovery value | Default |
|-------|--------|------------------|----------------|---------|
| XMUX browser defaults | high | high reuse | medium | on (overridable) |
| REALITY L2 amortize | high | high HS save | high | on (gated) |
| Suspect soft demotion | high | avoids L0 thrash | high | on |
| Mode cascade (auto) | high | slight open retry cost | high | on for auto |
| Mode cascade (explicit) | high | zero unless header | high | opt-in header |
| Sticky mode | high | fewer failed opens | high | on when cascade |
| Multi-endpoint race | high | first-win latency | medium | opt-in headers |
| H3 Happy Eyeballs | high | H3 win latency | medium | when ALPN h3 |
| XMUX fatal open evict | high | faster rotate | medium | on |
| Control header strip | critical | n/a | fingerprint | always |

## Operator presets

See `docs/presets/README.md`.

| Scenario | Recommend |
|----------|-----------|
| Direct IP + REALITY | `mode: stream-one` or `auto`, RA L2 server |
| CDN H2 front | `stream-one` + `x-bray-mode-degrade: true` |
| Hostile edge / RP | start `packet-up` or rely on sticky after first cascade |
| Multi landing IPs | `x-bray-multi-endpoint` + endpoints list |
| Disable sticky re-probe | `x-bray-sticky-mode: false` |

## Control headers (client-local)

| Header | Wave | Effect |
|--------|------|--------|
| `x-bray-mode-degrade` | 2/3 | Allow cascade for explicit modes |
| `x-bray-multi-endpoint` | 2/3 | Enable endpoint race |
| `x-bray-endpoints` | 2/3 | Extra `host:port` list |
| `x-bray-sticky-mode` | 4 | Opt-out sticky (`false`) |

Never appear on the wire (`GetRequestHeader` strips `x-bray-*`).

## Observability APIs

| API | Source |
|-----|--------|
| `reality.CacheReport()` | L1/L2 hits, fails, soft demotions, quarantine, calibrate |
| `GetH3Metrics()` / `H3MetricsReport()` | H3 wins, H2 fallback, cooldown, races |
| `XmuxManager.GetMetrics()` | reuse, TTFB, net recovery |
| `GetBrayV2Metrics()` / `BrayV2MetricsReport()` | cascade, sticky, multi, xmux evict |

## Threat model notes (GFW / CDN)

| Pressure | Bray-V2 response |
|----------|------------------|
| Active probe / bad session | XMUX probe + fast eviction + open-fail MarkDead |
| Edge kills bidirectional stream | mode cascade -> stream-up -> packet-up |
| Edge stays packet-only | sticky remembers packet-up (TTL) |
| L2 fingerprint / mismatch | Suspect soft demote to L1 next HS |
| Repeated L2 fails | quarantine + calibrate ladder (Wave-1/2) |
| H3 blocked | Happy Eyeballs -> H2 + cooldown |
| Single IP scrubbed | multi-endpoint race (opt-in) |

## What we deliberately do NOT do

- Same-connection L2->L1 after bytes written (transcript mix).
- Silent protocol changes for explicit operator modes without opt-in.
- Send Bray control headers to CDN (fingerprint / WAF risk).
- Unbounded sticky memory (max entries + TTL).
- Evict XMUX on every HTTP 4xx (pool thrash).

## Build / test

```
go build ./transport/internet/splithttp/ ./transport/internet/reality/ ./REALITY/
go test -count=1 -timeout 120s ./transport/internet/splithttp/ -run "TestH3|TestResolve|TestNext|TestMode|TestMulti|TestRace|TestBuild|TestIsDegrade|TestOpenStream|TestDestination|TestBray|TestXmux|TestApply|TestSticky|TestIsFatal"
```

## Future (post full-body)

1. Dual-stack / multi-SNI policy OS (uses existing HappyEyeballsConfig)
2. Sticky multi-endpoint winner (IP affinity with TTL)
3. Export Bray metrics via stats API / gRPC
4. Client REALITY path hint -> XHTTP initial mode
5. Field A/B: sticky TTL and cascade success rates

## Doc index

- `docs/bray-v2-wave1.md`
- `docs/bray-v2-wave2.md`
- `docs/bray-v2-wave3.md`
- `docs/bray-v2-wave4.md`
- `docs/bray-v2-full.md` (this file)
- `docs/presets/README.md`
