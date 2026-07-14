# Bray-V2 Wave-2

Branch: `main` (landed from historical `Bray-V2`; legacy pre-V2 is `v1`)

## Scope (recovery + multi-path lite, compatibility-first)

| ID | Item | Status |
|----|------|--------|
| S1-lite | REALITY Suspect soft-demotion: L2 fail → next HS uses L1 (not L0) | done |
| S1-metrics | L1Fails + L2SoftDemotions in CacheReport | done |
| X3-fix | H3 cooldown path no longer double-counts H2Fallbacks | done |
| C2 | CDN mode degrade ladder helpers (`stream-one → stream-up → packet-up`) | done |
| M1 | Opt-in multi-endpoint dual-path probe race helper | done |

## Design rules

1. **No same-conn L2→L1 fallthrough** after bytes are written (transcript would mix). Soft recovery is **next connection** via `ProfileSuspect`.
2. **Quarantine still hard-blocks** L1/L2 (Wave-1 ladder unchanged).
3. **Explicit user modes / xmux win**. Degrade and multi-endpoint are opt-in or auto-only.
4. Prefer helpers + headers over proto changes for Wave-2.

## REALITY S1-lite

| Event | Next `LookupAmortize` | Counters |
|-------|------------------------|----------|
| L2 success | PathL2 | L2Hits |
| L2 handshake fail (below quarantine threshold) | **PathL1** (Suspect blocks L2 only) | L2Fails, L2SoftDemotions |
| L1 handshake fail | L1/Suspect then quarantine ladder | L1Fails |
| FailCount ≥ MaxL2FailWindow | PathL0 (Quarantined) | Quarantines |

## XHTTP CDN mode cascade

Helpers in `transport/internet/splithttp/mode_degrade.go`:

- `ResolveInitialMode` — used by dialer for `""`/`auto`
- `NextDegradedMode` / `CanDegradeMode`
- `ShouldAttemptModeDegrade` — `auto` always; explicit modes require header `x-bray-mode-degrade=true`

Ladder: **stream-one → stream-up → packet-up**.

Runtime full dial-time cascade loop is intentionally staged: helpers + tests land first so operators/presets can document the ladder without changing explicit-mode success path behavior.

## Multi-endpoint (opt-in)

Helpers in `transport/internet/splithttp/multi_endpoint.go`:

| Header | Meaning |
|--------|---------|
| `x-bray-multi-endpoint` | `true`/`1`/`on` enables feature advertising |
| `x-bray-endpoints` | comma/space list of extra host:port candidates |

`RaceDialEndpoints` races N dials with short stagger (`MultiEndpointRaceWindow`), first success wins, losers closed. Default single-destination path unchanged.

## Observability

- REALITY `CacheReport()`: + L1 fails, + L2 soft demotions
- H3 `GetH3Metrics()`: cooldown increments only `H3Cooldowns`, not `H2Fallbacks`

## Files touched

- `REALITY/amortize_cache.go`
- `REALITY/cache_manager.go`
- `REALITY/amortize_test.go`
- `transport/internet/splithttp/h3_fallback.go`
- `transport/internet/splithttp/h3_metrics_test.go`
- `transport/internet/splithttp/mode_degrade.go`
- `transport/internet/splithttp/mode_degrade_test.go`
- `transport/internet/splithttp/multi_endpoint.go`
- `transport/internet/splithttp/multi_endpoint_test.go`
- `transport/internet/splithttp/dialer.go`
- `docs/bray-v2-wave2.md`
- `docs/presets/README.md`

## Rollback

- REALITY: revert submodule / keep Suspect hard-skip (Wave-1 behavior).
- XHTTP: remove helpers; dialer falls back to inline auto mode assignment.
- Opt-in headers default off → no behavior change when headers absent.

## Wave-3 candidates (not this wave)

- Full dialer cascade retry loop with cleanup on mode fail
- Wire multi-endpoint into TCP dial pre-REALITY
- Dual-stack / multi-SNI balancer OS
- Adaptive XMUX under GFW active probing
