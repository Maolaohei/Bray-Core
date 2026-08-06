# 文档索引

| 文档 | 说明 |
|------|------|
| [../README.md](../README.md) | 项目说明、快速开始、构建与 **Bray-only** 兼容策略（含分支策略） |
| [../CHANGELOG.md](../CHANGELOG.md) | 变更记录 |
| [../SECURITY.md](../SECURITY.md) | 安全策略 |
| [presets/README.md](presets/README.md) | 传输预设与 `x-bray-*` 本地控制头 |
| [architecture-connection-lifecycle.md](architecture-connection-lifecycle.md) | XMUX / 连接生命周期（基于源码） |
| [server-tuning.md](server-tuning.md) | Linux 服务端 TCP 调优（可选） |
| [archive/README.md](archive/README.md) | 历史 / 一次性文档归档（Bray-V2 分波交付说明、旧审计快照等） |

## 兼容提示

`main` **只保证 Bray 客户端 ↔ Bray 服务端**。Session MAC、packet-up 窗口/分片、OpenStream 超时驱逐、padding 指纹加固等与上游 Xray 可能不互通（详见 [../README.md](../README.md)「兼容性（Bray-only）」）。

REALITY 子模块文档见 [../REALITY/README.md](../REALITY/README.md) 与 [上游发布页](https://github.com/Maolaohei/REALITY/releases)。
