# Bray-V2 Wave-1

Branch: `Bray-V2`

## Scope (first wave, compatibility-first)

| ID | Item | Status |
|----|------|--------|
| R1 | REALITY L2 structure + age gates | done |
| R2 | L2 fail quarantine + calibration ladder | done |
| X1 | XMUX browser-like defaults + 20m max age | done |
| C1 | CDN / RA JSON presets | done |
| S2+X3 | REALITY CacheReport amortize metrics + H3 race metrics | done |

## Default parameter changes

### XMUX (`transport/internet/splithttp`)

| Getter (nil config) | Before | After |
|---------------------|--------|-------|
| MaxConcurrency | 0 (unlimited-ish) | 8–16 |
| MaxConnections | 0 | 2–4 |
| CMaxReuseTimes | 0 (unlimited) | 64–128 |
| HMaxRequestTimes | 0 (unlimited) | 400–800 |
| HMaxReusableSecs | 0 (unlimited) | 600–1200 |
| maxConnectionAge (internal) | 30m | 20m |

Rollback: set explicit `xmux` ranges in config (including `0` where supported by range semantics).

### REALITY amortize

| Gate | Before | After |
|------|--------|-------|
| Evidence | ≥2 | ≥2 (unchanged) |
| ShapeHash | not required | required non-zero for L2 |
| CapturedAt age | only profile TTL | L2 requires ≤10m (`MaxL2ProfileAge`) |
| Quarantine recovery | blocked until invalidate | after 5m cooldown, live obs calibrates with Evidence=1 |

## Files touched

- `REALITY/amortize.go`
- `REALITY/amortize_cache.go`
- `REALITY/cache_manager.go`
- `REALITY/amortize_test.go`
- `transport/internet/splithttp/config.go`
- `transport/internet/splithttp/mux.go`
- `transport/internet/splithttp/h3_fallback.go`
- `docs/presets/README.md`
- `docs/bray-v2-wave1.md`
