package splithttp

// 协议级真实服务端验证：用真实 ListenXH（其内部 requestHandler 走我们的
// marker-free 判定），客户端以已知 sid 手动序列 trigger -> production leg
// (addConn handler 写 payload) -> puller 拉段。若通，则服务端链路正确，
// 之前跨包"Real Dial 读 0"问题是 dialer 内部/时序而非服务端。

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
)

func newTestHTTPClient() *http.Client { return &http.Client{Timeout: 30 * time.Second} }
func netAddr(p net.Port) string       { return "127.0.0.1:" + strconv.Itoa(int(p)) }

func TestDownsegProtocolViaRealServer(t *testing.T) {
	cfg := &Config{
		Path:            "/sh",
		SessionIDLength: &RangeConfig{From: 16, To: 16},
		Headers:         map[string]string{BraySessionSecretHeader: "s"},
		Mode:            "packet-up",
		XPaddingBytes:   &RangeConfig{From: 16, To: 64},
	}
	pid := tcp.PickPort()
	payload := bytes.Repeat([]byte{0x5A}, downsegSize*2+500)

	listen, err := ListenXH(context.Background(), net.LocalHostIP, pid, &internet.MemoryStreamConfig{
		ProtocolName: "splithttp", ProtocolSettings: cfg,
	}, func(conn stat.Connection) {
		// Production-leg connection: handle.Write routes to downseg.
		go func(c stat.Connection) {
			defer c.Close()
			if _, werr := c.Write(payload); werr != nil && werr != io.ErrClosedPipe {
				return
			}
		}(conn)
	})
	common.Must(err)
	defer listen.Close()

	dc := &DefaultDialerClient{transportConfig: cfg, client: newTestHTTPClient()}
	base := &url.URL{Scheme: "http", Host: netAddr(pid), Path: cfg.GetNormalizedPath()}
	sid := cfg.GenerateSessionID()

	// 1) trigger segment pull (enter segment mode server-side).
	_, _ = dc.PullSegment(context.Background(), base, sid, "0")

	// 2) open production leg (server recognises once session is segment mode).
	prodCtx, prodCancel := context.WithCancel(context.Background())
	prodLeg, oerr := dc.OpenStreamAsync(prodCtx, base, sid, nil, false, nil)
	if oerr != nil {
		t.Fatalf("production leg open: %v", oerr)
	}
	defer func() { prodCancel(); _ = prodLeg.Close() }()

	// 3) pull the downlink via puller.
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	pr := NewDownSegPuller(ctx, dc, base, sid, prodLeg)
	defer pr.Close()

	var got bytes.Buffer
	buf := make([]byte, 64<<10)
	for {
		n, rerr := pr.Read(buf)
		got.Write(buf[:n])
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			break
		}
	}
	if !bytes.Equal(got.Bytes(), payload) {
		t.Fatalf("protocol test: got %d want %d", got.Len(), len(payload))
	}
}
