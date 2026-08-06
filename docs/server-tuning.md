# Linux 服务端调优（可选）

> 原 README「Linux 服务端参考」章节迁出，仅服务端部署时参考。

以下参数消除 **TCP 慢启动冷启动**（新建连接首段吞吐爬升），对高码率流（4K 视频、大文件下载）首段卡顿改善明显；下行由服务端发送，故调优在服务端生效。

**方式一：当前生效（重启后失效）**

```bash
sysctl -w net.ipv4.tcp_initcwnd=60
sysctl -w net.ipv4.tcp_slow_start_after_idle=0
sysctl -w net.ipv4.tcp_fastopen=3
```

**方式二：永久生效（写入配置文件并加载）**

```bash
cat > /etc/sysctl.d/99-bray.conf <<'EOF'
net.ipv4.tcp_initcwnd=60
net.ipv4.tcp_slow_start_after_idle=0
net.ipv4.tcp_fastopen=3
EOF
sysctl --system
```

说明：`tcp_initcwnd=60` 将初始拥塞窗口提到约 60 段（~85KB，MSS 1460B），首 RTT 即可发送更多数据；`tcp_slow_start_after_idle=0` 空闲后不再重置 cwnd（连接保活期间吞吐不回退）；`tcp_fastopen=3` 启用 TFO 握手减 1 RTT。需 Linux 4.9+（默认内核即可）。

> 安全权衡：`tcp_fastopen=3` 存在 RFC 7413 提示的反射放大理论面（内核 TFO cookie 有缓解，Xray 应用数据仅在手握成功后发送，实际放大有限）；对公网/不可信环境可改用 `tcp_fastopen=1`（仅客户端）或保持关闭，并按部署环境调整 `tcp_initcwnd`（60 段亦高于 RFC 6928 建议的 10，共享链路/低端 VPS 可调低以利拥塞公平）。
