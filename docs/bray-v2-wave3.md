# Bray-V2 Wave-3

Branch: `Bray-V2`

## Scope (runtime wiring, compatibility-first)

| ID | Item | Status |
|----|------|--------|
| C2-runtime | Full dialer mode cascade on open failure (`stream-one -> stream-up -> packet-up`) | done |
| M1-runtime | Opt-in multi-endpoint race wired into TCP dial path (pre-REALITY/TLS wrap) | done |
| H1 | Strip `x-bray-*` control headers from wire request headers | done |
| T3 | Unit tests: cascade builders, endpoint parse, header leak | done |

## Design rules

1. **Default path unchanged**: without opt-in headers / `mode: auto`, Dial behavior matches pre-Wave-3 (single mode, single dest).
2. **Cascade only on open failure**: partial stream bytes never cross modes; each attempt gets a fresh pipe + XMUX borrow.
3. **Context cancel/deadline never cascade** (`IsDegradeEligibleError`).
4. **Control headers are client-local**: `x-bray-*` never leave the process via `GetRequestHeader`.
5. **tryMode keeps historical semantics**: download-first for non-`stream-one` (even when downloadSettings is nil -> same primary client/URL).

## Mode cascade (runtime)

Dial flow:

```
initial = ResolveInitialMode(mode, hasREALITY, hasDownload)
allow   = ShouldAttemptModeDegrade(mode, headers)  // auto/empty always; explicit needs x-bray-mode-degrade
for mode in BuildModeCascade(initial, allow):
  borrow XMUX -> open tryMode(mode)
  on openErr: cleanup; if more modes && eligible -> continue
  on established / packet-up success -> return conn
```

| Configured mode | Cascade? |
|-----------------|----------|
| `""` / `auto` | yes (full ladder from resolved initial) |
| explicit (`stream-one` etc.) | only if header `x-bray-mode-degrade=true|1|on|yes` |
| `packet-up` | terminal (no further steps) |

## Multi-endpoint (runtime)

| Header | Meaning |
|--------|---------|
| `x-bray-multi-endpoint` | `true`/`1`/`on`/`yes` enables race |
| `x-bray-endpoints` | comma/space/`;` list of extra `host:port` |

When enabled and `len(endpoints)>1`, `createHTTPClient.dialContext` uses `RaceDialEndpoints` + `destinationFromEndpoint`. Single endpoint or feature off -> original `DialSystem(dest)`.

Network inheritance: bare host:port keeps primary network (TCP or UDP for H3).

## Header isolation

`isBrayControlHeader` -> any key with prefix `x-bray-` (case-insensitive) is skipped in `GetRequestHeader` (and thus all stream/packet builders).

## Files touched

- `transport/internet/splithttp/dialer.go` - cascade loop, multi-endpoint dial, `destinationFromEndpoint`
- `transport/internet/splithttp/mode_degrade.go` - `BuildModeCascade`, `IsDegradeEligibleError`
- `transport/internet/splithttp/multi_endpoint.go` - `BuildEndpointList`
- `transport/internet/splithttp/config.go` - strip `x-bray-*`
- `transport/internet/splithttp/mode_degrade_test.go`
- `transport/internet/splithttp/multi_endpoint_test.go`
- `transport/internet/splithttp/wave3_test.go`
- `docs/bray-v2-wave3.md`
- `docs/presets/README.md`

## Observability

- Log: `XHTTP mode <m> open failed; cascading to <next>` (info)
- No new metrics counters in Wave-3 (Wave-4 candidate: cascade attempt / success counts)

## Rollback

1. Revert Dial cascade loop to single-mode open; keep helpers.
2. Remove multi-endpoint branch in `dialContext` (always `dialRawTCP(dest)`).
3. Control-header strip can remain (pure safety, no behavior change for clean configs).

## Wave-4 candidates (not this wave)

- Dual-stack / multi-SNI balancer OS
- Adaptive XMUX under GFW active probing
- Cascade / multi-endpoint metrics counters
- Remember last-good mode per destination (sticky degrade)
