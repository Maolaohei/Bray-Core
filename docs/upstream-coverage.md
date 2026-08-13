# 上游覆盖清单（Upstream Coverage Ledger）

本表登记"功能已由 fork 实现、无需重复合并"的上游 commit。登记后运行
`scripts/upstream_cover.sh <sha> [reason]` 创建 git replace ref，使该 commit
从 `git log HEAD..upstream/main`（triage 视图）中消失。

- 登记：`scripts/upstream_cover.sh <sha> [reason]`
- 重建（新 clone/worktree 后）：`scripts/upstream_triage.sh` 会自动调用 rebuild
- 撤销：`git replace -d <sha>`（先确认 ledger 中的原因已不成立）
- 换机器后 replace refs 不随分支走——用 triage 或 rebuild 脚本从本表重建

| 上游 commit | 标题 | 覆盖原因 | 登记日期 |
|---|---|---|---|
| `5b1b4105` | Routing: Exclude iOS from Darwin for `process` | fork 的 8b419d83 合并时已含等价逻辑 | 2026-08-14 |
| `412898fe` | WireGuard outbound: Fix a race condition | cherry-pick 为空（fork 已有等价修复） | 2026-08-14 |
| `0495b176` | TUN inbound: Refine gateway/autoSystemRoutingTable macOS | cherry-pick 为空（fork 已有） | 2026-08-14 |
| `7e7e8207` | Geodata: Apply uTLS Chrome fingerprint when downloading | cherry-pick 为空（fork 已有） | 2026-08-14 |
| `9cd9382e` | TUN inbound: Support env XRAY_TUN_FD on Linux | fork 已显式 cherry-pick（`8ef7ba17`） | 2026-08-14 |
| `0bafca94` | Stats: Fix GetOrRegister*() races | fork 移植 `66567499`（等价，12 文件） | 2026-08-14 |
| `ac04c445` | DNS: Fix unexpected TTL clamp | fork 自研 DNS 加固 `9cc5e95f`（RFC5452 + TTL 语义）更全面 | 2026-08-14 |
| `452b7195` | Hysteria inbound: Support vlessRoute | fork 已显式 cherry-pick（`1be31b1c`，适配 map-based Validator） | 2026-08-14 |
| `64fada32` | TLS client: Pinning CA must have serverName | fork 移植 `be5f4347`（tls/config.go + grpc dialer 均已覆盖） | 2026-08-14 |
| `65f6f0a4` | TUN inbound: Refine gateway/autoSystemRoutingTable Linux | cherry-pick 为空（fork 已有） | 2026-08-14 |
| `987290ba` | Xray-core: Forbid unencrypted outbounds on public Internet for VLESS and Trojan; Remove "none/zero/plain" for VMess and Shadowsocks (#6303) | 已合并到 fork (2026.08.13 批次) | 2026-08-14 |
| `a12801c1` | Tunnel inbound: Support TPROXY on OpenBSD as well (#6546) | 已合并到 fork (2026.08.13 批次) | 2026-08-14 |
| `8f15190c` | Root config: Add `env` config (#6400) | 已合并到 fork (2026.08.13 批次) | 2026-08-14 |
| `3dc8bf3d` | Bump github.com/pires/go-proxyproto from 0.12.0 to 0.14.0 (#6420) | 已合并到 fork (2026.08.13 批次) | 2026-08-14 |
| `7d214f8b` | Tunnel inbound: Support TPROXY on OpenBSD as well (#6546) | 已合并到 fork (2026.08.13 批次) | 2026-08-14 |
| `d7fa2076` | infra/conf/transport_internet.go: Split into multiple files; Rename `network` to `method` (#6426) | 已合并到 fork (2026.08.13 批次) | 2026-08-14 |
| `1d8eb81d` | REALITY server: Refine warnings in parsing configuration (#6508) | 已合并到 fork (2026.08.13 批次) | 2026-08-14 |
| `6ce924ad` | Bump google.golang.org/grpc from 1.82.0 to 1.82.1 (#6502) | 已合并到 fork (2026.08.13 批次) | 2026-08-14 |
| `3263ae92` | Bump github.com/pires/go-proxyproto from 0.12.0 to 0.14.0 (#6420) | 已合并到 fork (2026.08.13 批次) | 2026-08-14 |
| `241aa38a` | Bump actions/cache from 5 to 6 (#6368) | 已合并到 fork (2026.08.13 批次) | 2026-08-14 |
| `dda2b10c` | Bump actions/cache from 5 to 6 (#6368) | 已合并到 fork (2026.08.13 批次) | 2026-08-14 |
| `345c76f9` | XHTTP server: Refactor upload_queue.go (#6372) | 已合并到 fork (2026.08.13 批次) | 2026-08-14 |
| `d5bc58dc` | TLS client: Support more `cipherSuites` for "unsafe" (golang) `fingerprint` for anti-NIN (#6450) | 已合并到 fork (2026.08.13 批次) | 2026-08-14 |
| `35387572` |  | 已合并到 fork (2026.08.13 批次) | 2026-08-14 |
| `6ab123bf` | XHTTP & gRPC servers: Get accurate localAddr (#6526) | 已合并到 fork (2026.08.13 批次) | 2026-08-14 |
| `8b419d83` | Routing: Fix `process` for macOS IPv4-mapped sockets (#6557) | 已合并到 fork (2026.08.13 批次) | 2026-08-14 |
