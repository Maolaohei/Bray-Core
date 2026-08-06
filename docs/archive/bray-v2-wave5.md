# Bray-V2 Wave-5

Branch: `main` (landed from historical `Bray-V2`; legacy pre-V2 is `v1`)

## Scope (post full-body, compatibility-first)

| ID | Item | Status |
|----|------|--------|
| E5 | Sticky multi-endpoint winner (IP affinity, TTL) | done |
| R5 | Reorder race list with sticky first (head-start) | done |
| S5 | Optional stats.Manager mirror for Bray metrics | done |
| D5 | Wave-5 docs + presets | done |

## Design rules

1. Endpoint sticky applies **only when multi-endpoint is enabled** (opt-in headers). Single-dest dials unchanged.
2. Sticky reorders the race list so last-good endpoint dials first; backups still race with stagger.
3. Default **on** when multi-endpoint is active; opt-out `x-bray-sticky-endpoint=false|0|off|no`.
4. TTL + max entries bound memory (same pattern as mode sticky).
5. Stats export is **pull-based**: `PublishBrayV2MetricsToStats` / `BindBrayV2StatsManager` + `PublishBoundBrayV2Metrics`. No hot-path cost if never published.
6. Control headers stay client-local (`x-bray-*` stripped on wire).

## Sticky endpoint

```
list = BuildEndpointList(primary, extras)
if multi && stickyEndpointEnabled:
  if ep = LookupStickyEndpoint(dest|host): list = ApplyStickyEndpoints(list, ep)
conn, winner = RaceDialEndpoints(list, ...)
on success: RememberStickyEndpoint(dest|host, winner)
```

| Header | Meaning |
|--------|---------|
| `x-bray-sticky-endpoint` | default on; `false`/`0`/`off`/`no` disables |

## Stats export

`PublishBrayV2MetricsToStats(m)` mirrors absolute counter values into:

| Stats name |
|------------|
| `bray-v2>>>mode_attempts` |
| `bray-v2>>>mode_successes` |
| `bray-v2>>>mode_cascade_steps` |
| `bray-v2>>>mode_cascade_wins` |
| `bray-v2>>>sticky_hits` |
| `bray-v2>>>sticky_remembers` |
| `bray-v2>>>multi_endpoint_races` |
| `bray-v2>>>multi_endpoint_alt_wins` |
| `bray-v2>>>endpoint_sticky_hits` |
| `bray-v2>>>endpoint_sticky_remembers` |
| `bray-v2>>>xmux_open_evicts` |

Also: `GetBrayV2Metrics()` / `BrayV2MetricsReport()` (in-process atomics).

## Files

- `transport/internet/splithttp/sticky_endpoint.go`
- `transport/internet/splithttp/bray_metrics.go`
- `transport/internet/splithttp/bray_stats.go`
- `transport/internet/splithttp/dialer.go`
- `transport/internet/splithttp/wave5_test.go`
- `docs/bray-v2-wave5.md`
- `docs/bray-v2-full.md`
- `docs/presets/README.md`

## Rollback

- Sticky endpoint: `x-bray-sticky-endpoint=false` or remove Apply/Remember in dialer.
- Stats: leave unused (API only).

## Out of Wave-5 (still future)

- Dual-stack / multi-SNI policy OS (HappyEyeballs already exists)
- Client REALITY path hint -> XHTTP initial mode
- Field A/B: sticky TTL and cascade success rates
