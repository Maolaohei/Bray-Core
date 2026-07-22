# 文档索引

| 文档 | 说明 |
|------|------|
| [../README.md](../README.md) | 项目说明、构建与 **Bray-only** 兼容策略 |
| [../CHANGELOG.md](../CHANGELOG.md) | 变更记录 |
| [architecture-connection-lifecycle.md](architecture-connection-lifecycle.md) | XMUX / 连接生命周期（基于源码） |
| [bray-v2-full.md](bray-v2-full.md) | Bray 完全体总览（现已并入 `main`） |
| [presets/README.md](presets/README.md) | 传输预设与 `x-bray-*` 本地控制头 |
| [bray-v2-wave1.md](bray-v2-wave1.md) … [wave6](bray-v2-wave6.md) | 分波交付说明（历史） |
| [../SECURITY.md](../SECURITY.md) | 安全策略 |
| [../DEFECT_REPORT.md](../DEFECT_REPORT.md) | 2026-06-27 历史审计快照（非实时） |

## 分支策略

| 分支 | 说明 |
|------|------|
| `main` | 当前默认主干（Bray 完全体 + Bray-only 安全/性能） |
| `v1` | 升级前的旧主干快照，用于回滚/对比 |

## 兼容提示

`main` **只保证 Bray 客户端 ↔ Bray 服务端**。Session MAC、packet-up 窗口/分片、OpenStream 超时驱逐、padding 指纹加固等与上游 Xray 可能不互通。

REALITY 子模块文档见 [`../REALITY/README.md`](../REALITY/README.md) 与 [上游发布页](https://github.com/Maolaohei/REALITY/releases)。
