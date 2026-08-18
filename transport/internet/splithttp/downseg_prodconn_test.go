package splithttp_test

// 真实连接回归测试（audit Finding-1）：生产腿 addConn 注册的 splitConn
// 必须真正把下行写入 downseg 段缓存。用真实 ListenXH + Dial 驱动：服务端
// addConn 收到生产腿连接后把 payload 写入（模拟 VLESS 下行），随后 EOF；
// 客户端 Dial 走 packet-up + downseg（默认开）。若生产腿未接线
// （Finding-1 bug），下行不会进 cache → 客户端得不到数据/挂起。

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport/internet"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/stat"
)

func TestDownsegProductionLegRealConn(t *testing.T) {
	cfg := &Config{
		Path:            "/sh",
		SessionIDLength: &RangeConfig{From: 16, To: 16},
		Headers: map[string]string{
			BraySessionSecretHeader: "s",
		},
		Mode:          "packet-up",
		XPaddingBytes: &RangeConfig{From: 16, To: 64},
	}
	pid := tcp.PickPort()
	payload := bytes.Repeat([]byte{0xA5}, 64<<10) // 64 KiB downlink
	var wrote atomic.Bool

	listen, err := ListenXH(context.Background(), net.LocalHostIP, pid, &internet.MemoryStreamConfig{
		ProtocolName: "splithttp", ProtocolSettings: cfg,
	}, func(conn stat.Connection) {
		// Production leg connection: simulate proxied downlink.
		go func(c stat.Connection) {
			defer c.Close()
			if _, werr := c.Write(payload); werr != nil && werr != io.ErrClosedPipe {
				return
			}
			wrote.Store(true)
		}(conn)
	})
	common.Must(err)
	defer listen.Close()

	conn, err := Dial(context.Background(), net.TCPDestination(net.DomainAddress("localhost"), pid), &internet.MemoryStreamConfig{
		ProtocolName: "splithttp", ProtocolSettings: cfg,
	})
	common.Must(err)
	defer conn.Close()

	// Read downlink; expect payload back (echo through the real conn).
	var b [4096]byte
	total := 0
	conn.SetReadDeadline(time.Now().Add(8 * time.Second))
	deadline := time.Now().Add(8 * time.Second)
	for total < len(payload) && time.Now().Before(deadline) {
		n, rerr := conn.Read(b[:])
		total += n
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			break
		}
	}
	if total != len(payload) {
		t.Fatalf("production-leg real conn: read %d bytes, want %d (wrote=%v). Finding-1 regression: downlink not reaching segment cache", total, len(payload), wrote.Load())
	}
}
