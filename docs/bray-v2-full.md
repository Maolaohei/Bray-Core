# Bray-V2 Full Body

Branch: `main` (formerly feature branch `Bray-V2`; legacy line is `v1`)
Audience: operators + core maintainers
Principle: **compatibility first, performance second, recovery always opt-in or auto-safe**.

## Branch policy (2026-07)

| Branch | Role |
|--------|------|
| **`main`** | Current product line = Bray fully-hardened XHTTP + RA stack (Waves 1–7). Default clone/build/release target. |
| **`v1`** | Frozen pre-V2 line (former `main` @ pre-promotion tip). Compatibility / rollback reference only. |
| `Bray-V2` | Historical feature-branch name; tip was merged into `main`. Prefer `main`. |

Release CI continues to build from `main`.


## What "full body" means

Bray-V2 is not a new protocol. It is a **hardened stack** on XHTTP + REALITY Amortize (RA):

1. **Fast happy path** when the path is clean (stream-one / L2 amortize / browser-like XMUX).
2. **Graceful degradation** when CDN/edge/GFW breaks long streams or L2.
3. **Observability** so failures are countable, not tribal knowledge.
4. **No wire fingerprint** from Bray control headers (`x-bray-*` client-local only).

## Stack map

```
Client Dial
  |- Multi-endpoint race + sticky EP - TCP candidates
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
| **5** | Affinity + export | Sticky multi-endpoint winner; stats.Manager mirror; ep sticky metrics |
| **6** | Safe ops glue | Auto stats mirror; sticky TTL headers; A/B rate helpers |
| **7** | Review fixes | Cascade re-getHTTPClient after fatal open; LeftRequests only on success; sticky TTL per-entry (no globals); sticky fail invalidate; IPv6 multi-EP parse; empty multi-EP dedicated errors |

## Wave-7 review fixes

Post-review hardening on the mode cascade / sticky / multi-endpoint path (default behavior still safe):

1. **P1 XMUX cascade**: on fatal open with more modes remaining, `MarkDead` + re-`getHTTPClient` before next mode (avoids Borrow-dead).
2. **P1 LeftRequests**: decrement only after successful OpenStream / packet-up arming (failed cascade steps no longer burn quota).
3. **P2 Sticky TTL**: `x-bray-sticky-*-ttl` apply per-entry at remember time; `ApplyStickyTTLFromHeaders` is a process-global no-op.
4. **P2 Sticky fail**: `NoteStickyModeFailure` clears sticky when the sticky mode itself fails.
5. **P2 IPv6 endpoints**: `destinationFromEndpoint` uses SplitHostPort; bare IPv6 inherits primary port.
6. **P3 Multi-EP errors**: empty race list / nil dialFn return dedicated errors (not `context.Canceled`).
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
| Sticky endpoint | high | fewer race staggers | medium | on when multi |
| Stats metrics publish | high | zero unless polled | low | opt-in API |
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
| `x-bray-sticky-mode` | 4 | Opt-out mode sticky (`false`) |
| `x-bray-sticky-endpoint` | 5 | Opt-out endpoint sticky (`false`) when multi-endpoint on |
| `x-bray-sticky-mode-ttl` | 6 | Optional mode sticky TTL (`10m` / `30s` / minutes int) |
| `x-bray-sticky-endpoint-ttl` | 6 | Optional endpoint sticky TTL |

Never appear on the wire (`GetRequestHeader` strips `x-bray-*`).

## Observability APIs

| API | Source |
|-----|--------|
| `reality.CacheReport()` | L1/L2 hits, fails, soft demotions, quarantine, calibrate |
| `GetH3Metrics()` / `H3MetricsReport()` | H3 wins, H2 fallback, cooldown, races |
| `XmuxManager.GetMetrics()` | reuse, TTFB, net recovery |
| `GetBrayV2Metrics()` / `BrayV2MetricsReport()` | cascade, sticky mode/endpoint, multi, xmux evict |
| `PublishBrayV2MetricsToStats(m)` | optional stats.Manager absolute mirror (`bray-v2>>>...`) |
| Auto mirror on stats Start | Wave-6: 30s background publish when real stats app present |
| `GetBrayV2Rates()` / `BrayV2RatesReport()` | field A/B ratios from atomics |

## Threat model notes (GFW / CDN)

| Pressure | Bray-V2 response |
|----------|------------------|
| Active probe / bad session | XMUX probe + fast eviction + open-fail MarkDead |
| Edge kills bidirectional stream | mode cascade -> stream-up -> packet-up |
| Edge stays packet-only | sticky remembers packet-up (TTL) |
| L2 fingerprint / mismatch | Suspect soft demote to L1 next HS |
| Repeated L2 fails | quarantine + calibrate ladder (Wave-1/2) |
| H3 blocked | Happy Eyeballs -> H2 + cooldown |
| Single IP scrubbed | multi-endpoint race + sticky winner (opt-in) |

## Green-zone hardening

Low-cost anti-fingerprint defaults (compat + happy-path perf preserved):

1. **Strip lock tests** — `x-bray-*` never on wire via `GetRequestHeader`.
2. **Cascade step jitter** — only between failed mode steps, 0–200ms; first-mode success is free.
3. **XMUX default jitter** — nil `xmux` fields get process-stable ±10% browser-band ranges; explicit config wins.
4. **CDN presets** — recommend `packet-up`/`stream-up` first on hostile edges; no global auto policy change.
5. **Multi-EP** — hard cap `MaxMultiEndpoints=4` (primary+extras), no scan ranges; sticky preferred; sticky EP cleared on preferred-race fail.
6. **LeftRequests half-open** — stream-up counts download+upload quota only after both opens succeed.
7. **Fatal open typed** — prefer `net.OpError`/`ErrClosed`/EOF before string needles for XMUX eviction.
8. **Cascade cancel** — jitter wait cancel returns `errors.Join(openErr, werr)` so root cause is preserved.

## What we deliberately do NOT do

- Same-connection L2->L1 after bytes written (transcript mix).
- Silent protocol changes for explicit operator modes without opt-in.
- Send Bray control headers to CDN (fingerprint / WAF risk).
- Unbounded sticky memory (max entries + TTL).
- Evict XMUX on every HTTP 4xx (pool thrash).

## Build / test

```
go build ./transport/internet/splithttp/ ./transport/internet/reality/ ./REALITY/
go test -count=1 -timeout 120s ./transport/internet/splithttp/ -run "TestH3|TestResolve|TestNext|TestMode|TestMulti|TestRace|TestBuild|TestIsDegrade|TestOpenStream|TestDestination|TestBray|TestXmux|TestApply|TestSticky|TestIsFatal|TestPublish|TestParse|TestCompute"
```

## Future (intentionally deferred)

These change dial policy or need more client hooks; not zero-risk:

1. Dual-stack / multi-SNI policy OS (HappyEyeballs already operator-configurable)
2. Client REALITY path hint -> XHTTP initial mode

## Doc index

- `docs/bray-v2-wave1.md`
- `docs/bray-v2-wave2.md`
- `docs/bray-v2-wave3.md`
- `docs/bray-v2-wave4.md`
- `docs/bray-v2-wave5.md`
- `docs/bray-v2-wave6.md`
- `docs/bray-v2-full.md` (this file)
- `docs/presets/README.md`
