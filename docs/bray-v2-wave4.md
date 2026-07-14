# Bray-V2 Wave-4

Branch: `Bray-V2`

## Scope (full-body recovery intelligence, compatibility-first)

| ID | Item | Status |
|----|------|--------|
| S4 | Sticky last-good mode per destination (TTL) | done |
| O4 | Bray-V2 metrics: cascade / sticky / multi-endpoint / xmux evict | done |
| X4 | Adaptive XMUX lite: MarkDead on fatal open transport errors | done |
| D4 | Full-body overview doc (`docs/bray-v2-full.md`) | done |

## Design rules

1. Sticky applies **only when cascade is allowed** (`auto`/empty or `x-bray-mode-degrade`). Explicit single-mode dials stay sticky-free unless degrade is opt-in.
2. Sticky **skips higher modes** that already failed for this dest until TTL expires (re-probe window).
3. Sticky default **on**; opt-out header `x-bray-sticky-mode=false|0|off|no`.
4. XMUX eviction only on **fatal transport** strings (EOF/reset/GOAWAY/timeout). CDN HTTP 403/404 stream rejects do **not** thrash the pool.
5. Metrics are process-wide atomics; no hot-path locks.

## Sticky mode

```
cascade = BuildModeCascade(initial, allow)
if allow && stickyEnabled:
  if m = LookupSticky(dest|host): cascade = ApplyStickyMode(cascade, m)
on success: RememberSticky(dest|host, mode)  // TTL default 10m
```

| Header | Meaning |
|--------|---------|
| `x-bray-sticky-mode` | default on; `false`/`0`/`off`/`no` disables |

## Metrics

`GetBrayV2Metrics()` / `BrayV2MetricsReport()`:

| Counter | Meaning |
|---------|---------|
| ModeAttempts | Dial entered cascade loop |
| ModeSuccesses | Open success |
| ModeCascadeSteps | Failed mode -> next |
| ModeCascadeWins | Success after cascade step |
| StickyHits | Cascade reordered by sticky |
| StickyRemembers | Successful remembers |
| MultiEndpointRaces | Race dials completed |
| MultiEndpointAltWins | Non-primary endpoint won |
| XmuxOpenEvicts | MarkDead after fatal open |

## Adaptive XMUX lite

`MaybeEvictXmuxAfterOpenFailure` after mode open fail. Rotates broken H2/H3 sessions without same-conn protocol fallthrough.

## Files

- `transport/internet/splithttp/sticky_mode.go`
- `transport/internet/splithttp/bray_metrics.go`
- `transport/internet/splithttp/xmux_adaptive.go`
- `transport/internet/splithttp/dialer.go`
- `transport/internet/splithttp/wave4_test.go`
- `docs/bray-v2-wave4.md`
- `docs/bray-v2-full.md`
- `docs/presets/README.md`

## Rollback

- Sticky: `x-bray-sticky-mode=false` or revert ApplyStickyMode call.
- Metrics: leave (observability only).
- XMUX adaptive: no-op path if MaybeEvict removed.

## Out of Wave-4 (future)

- Dual-stack/multi-SNI balancer OS policy layer
- Sticky multi-endpoint winner (IP affinity)
- Per-dest cascade metrics export API
- REALITY client-side soft path hints to XHTTP mode
