# Bray-V2 Wave-6

Branch: `Bray-V2`

## Scope (safe post-full-body only)

Only items that are **compatibility-neutral** and **no dial hot-path cost** by default.

| ID | Item | Status |
|----|------|--------|
| O6 | Auto stats mirror on real stats.Manager Start | done |
| T6 | Optional sticky TTL override headers | done |
| R6 | Read-only A/B rate helpers | done |
| D6 | Docs + presets | done |

**Explicitly NOT in Wave-6** (risk / protocol surface):

- multi-SNI policy OS
- Client REALITY path hint -> XHTTP mode
- Changing HappyEyeballs defaults (already exists; leave operator-configured)

## Design rules

1. No wire protocol changes; no new required config fields.
2. Default dial path unchanged when headers omitted and stats app absent.
3. Stats mirror: only when real `stats` app starts (not NoopManager); 30s timer; can disable via `BrayV2StatsAutoMirror=false`.
4. Sticky TTL headers are client-local (`x-bray-*` stripped); invalid values leave previous TTL.
5. Rates are pure functions over existing atomics (field A/B).

## Auto stats mirror

```
stats.Manager.Start()
  -> features/stats.InvokeManagerStartHooks
  -> splithttp BindBrayV2StatsManager(m)
  -> ticker PublishBound every BrayV2StatsMirrorInterval (default 30s)
stats.Manager.Close()
  -> unbind + stop ticker
```

Manual API still works: `PublishBrayV2MetricsToStats`, `Bind`+`PublishBound`.

## Sticky TTL headers (optional)

| Header | Effect |
|--------|--------|
| `x-bray-sticky-mode-ttl` | Override mode sticky TTL (`10m`, `30s`, or minutes int `15`) |
| `x-bray-sticky-endpoint-ttl` | Override endpoint sticky TTL |

Clamp max 24h. Process-wide vars (same as defaults); set once per Dial setup.

## A/B rates

`GetBrayV2Rates()` / `BrayV2RatesReport()`:

| Field | Formula |
|-------|---------|
| ModeSuccessRate | successes / attempts |
| CascadeWinRate | cascade_wins / successes |
| StickyHitRate | sticky_hits / attempts |
| MultiAltWinRate | multi_alt / multi_races |
| EndpointStickyHitRate | ep_sticky_hits / multi_races |

## Files

- `features/stats/hooks.go`
- `app/stats/stats.go` (Start/Close hooks)
- `transport/internet/splithttp/bray_stats.go`
- `transport/internet/splithttp/sticky_ttl.go`
- `transport/internet/splithttp/bray_rates.go`
- `transport/internet/splithttp/dialer.go` (ApplyStickyTTLFromHeaders)
- `transport/internet/splithttp/wave6_test.go`
- `docs/bray-v2-wave6.md`

## Rollback

- Auto mirror: `BrayV2StatsAutoMirror = false` or remove OnManagerStart registration.
- TTL headers: omit headers (defaults 10m).
- Rates: API-only, no runtime effect.
