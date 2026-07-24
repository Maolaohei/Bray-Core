# optN9 residual — 2026-07-25

## Goal
Close H2+TLS packet-up residual under stability-first (no probe/MarkDead/over-admit/H1 ordered/session IP/P0 pace cuts).
Answer "200 commits but looks slower" with serial evidence + honest ceilings.

## Code (this wave)
- **common/buf**: `FromBytes` reuses `*Buffer` shells via `bufferShellPool` (unmanaged only).
- **packet-up body**:
  - `durableBodyPool` + `pooledDurableBody` + `acquireDurableBody`
  - `FillPacketRequestBytes` (body Close never frees durable bytes)
  - `DefaultDialerClient.PostPacketBytes` + optional `packetBytesPoster`
  - `postPacketReliable` prefers `PostPacketBytes`; MultiBuffer/FromBytes is fallback only
- **request shells**:
  - `urlURLPool` + `cloneURL` / `releaseURL` for request-local Path mutation
  - H1 path closes body after `req.Write` before pipeline snapshot
- **tests**: `TestFromBytesShellPool`, `TestAcquireDurableBody`, `TestFillPacketRequestBytesBody` + existing H1/H2 packet tests

## Verify
- `go test ./common/buf/ -count=1` **PASS**
- `go test ./transport/internet/splithttp/ -run 'TestAcquire|TestFillPacketRequestBytes|TestH1PostPacket|TestPostPacket_H2' -count=1` **PASS**
- rebuilt `bench_results/run_20260725_optN9/splithttp_optN9.test.exe` **after** code change

## Official short serial (800ms×3 H2C/H2; 500ms stream/modes) — **alloc signal only**

| Bench | pre-change (same dir, old binary) | **optN9 post** | Delta |
|------|----------------------------------:|---------------:|------:|
| H2C packet-up alloc | 112 | **109–110** (med **110**) | 🟢 **−2** |
| H2+TLS packet-up alloc | 201 | **196–197** (med **197**) | 🟢 **−4** |
| H2C B/op | 91189 | **~88.0–88.4k** | 🟢 |
| H2 B/op | 90162 | **~86.5–87.7k** | 🟢 |
| Modes packet-up | — | **110 alloc** | 🟢 matches H2C |
| Modes stream-up / stream-one | — | **18 / 18 alloc** | ⚪ same as optN8 |

Short thruput (do **not** overwrite product headlines):
- H2C med ~**138.6** MB/s (window soft; one sample 198.6)
- H2 med ~**68.3** MB/s (samples 70.2 / 68.3 / 61.7)
- StreamUp_Throughput (TLS bench) ~117–161 MB/s · **78 alloc** (different from Modes H2C stream-up)

Product thruput headlines remain **quiet2/optN3**: H2C ~224–226 · H2 ~84 · stream-up ~262 · stream-one ~213.

## pprof notes
- `PostPacketBytes` is on the hot path (cum ~12% alloc_objects on H2 profile).
- `FillPacketRequestBytes` itself is small (~0.55% cum); residual is not MultiBuffer wrap.
- Top flat still polluted by package `init` `generateTokenishPaddingBase62Raw` (pre-cache fill). Steady path is **cached** tokenish/strict tables.
- Remaining mass is `net/http` / `http2` / `crypto/tls` / uTLS / header shallow clone / `Request.WithContext` — **not** a missing FromBytes shell.

## Residual next (ROI, stability-first)
1. **P1 H2+TLS ceiling (~197 vs H2C ~110)**: mostly stack (TLS record + http2 frames + Client.Do). XHTTP-owned leftovers: header map clone, path string (`appendToPath2`), request shell/`WithContext`. Do not expect to close the full ~90 gap inside XHTTP alone.
2. **P2 XMUX large-pool O(n) scan** (keep score/probe/over-admit).
3. **P2 HE SortIPs/LargeList** only after quiet reconfirm vs optN7d.
4. **P2 stream-one residual** vs stream-up peak.
5. **P3 common/buf.Copy** vs upstream micro — not XHTTP thruput mainline.

## Do not
- Claim XMUX should match old 17ns simple pool
- Overwrite product headlines with 500ms soft windows
- Frame “200 commits = full regression”
- Cut probe / MarkDead / over-admit / H1 ordered / session IP / P0 pace for ns
- Treat init padding generation as steady-state packet cost

## Artifacts
- pre: `h2c_prof.txt`, `h2_prof.txt`, `h2c.mem.prof`, `h2.mem.prof`
- post: `h2c_post.txt`, `h2_post.txt`, `stream_post.txt`, `modes_post.txt`, `*_post.mem.prof`
- binary: `splithttp_optN9.test.exe` (2026-07-25 05:53)
