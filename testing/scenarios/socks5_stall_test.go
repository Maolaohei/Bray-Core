package scenarios

import (
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// TestSOCKS5_RapidNewPages 模拟真实环境：通过 SOCKS5 代理快速创建新连接
// 用于复现"新网页卡一下"的问题
//
// 使用方法：
// 1. 启动 v2rayN，确保 SOCKS5 代理在 127.0.0.1:9996 运行
// 2. 运行: go test -v -run TestSOCKS5_RapidNewPages -count=1 ./testing/scenarios/
func TestSOCKS5_RapidNewPages(t *testing.T) {
	proxyAddr := "127.0.0.1:9996"
	// 测试目标：访问常见网站
	targets := []string{
		"www.google.com:443",
		"www.youtube.com:443",
		"github.com:443",
		"www.baidu.com:443",
		"www.cloudflare.com:443",
	}

	// 阶段 1：快速创建新连接（模拟新网页）
	t.Log("Phase 1: 快速创建新连接...")
	var (
		totalConns   int
		totalStalls  int
		totalLatency time.Duration
	)

	for i := 0; i < 20; i++ {
		target := targets[i%len(targets)]
		start := time.Now()

		conn, err := net.DialTimeout("tcp", proxyAddr, 5*time.Second)
		if err != nil {
			t.Logf("  [conn %d] dial proxy failed: %v", i, err)
			totalStalls++
			continue
		}

		// SOCKS5 握手
		conn.Write([]byte{0x05, 0x01, 0x00})
		buf := make([]byte, 2)
		conn.Read(buf)

		// SOCKS5 连接请求
		req := buildSocks5Connect(target)
		conn.Write(req)
		resp := make([]byte, 10)
		conn.Read(resp)

		latency := time.Since(start)
		totalLatency += latency
		totalConns++

		if latency > 500*time.Millisecond {
			t.Logf("  [conn %d] STALL: %v to %s", i, latency, target)
			totalStalls++
		}

		conn.Close()
	}

	avgLatency := totalLatency / time.Duration(totalConns)
	t.Logf("=== 快速新连接测试 ===")
	t.Logf("  连接数: %d, 失败: %d", totalConns, totalStalls)
	t.Logf("  平均延迟: %v", avgLatency)

	// 阶段 2：复用连接测试
	t.Log("Phase 2: 测试连接复用...")
	conn, err := net.DialTimeout("tcp", proxyAddr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// SOCKS5 握手
	conn.Write([]byte{0x05, 0x01, 0x00})
	buf := make([]byte, 2)
	conn.Read(buf)

	// 连接目标
	req := buildSocks5Connect("www.google.com:443")
	conn.Write(req)
	resp := make([]byte, 10)
	conn.Read(resp)

	// 复用连接发送多次请求
	reuseStart := time.Now()
	for i := 0; i < 10; i++ {
		req := buildSocks5Connect(targets[i%len(targets)])
		conn.Write(req)
		resp := make([]byte, 10)
		conn.Read(resp)
	}
	reuseTime := time.Since(reuseStart)
	t.Logf("  复用 10 次: %v (平均 %v/次)", reuseTime, reuseTime/10)

	// 对比
	if totalConns > 0 {
		ratio := float64(totalLatency/time.Duration(totalConns)) / float64(reuseTime/10)
		t.Logf("  新建/复用比: %.1fx", ratio)
	}
}

func buildSocks5Connect(target string) []byte {
	host, portStr, _ := net.SplitHostPort(target)
	port := 0
	fmt.Sscanf(portStr, "%d", &port)

	// SOCKS5 CONNECT request
	// VER CMD RSV ATYP DST.ADDR DST.PORT
	req := []byte{0x05, 0x01, 0x00, 0x03}
	req = append(req, byte(len(host)))
	req = append(req, []byte(host)...)
	req = append(req, byte(port>>8), byte(port))
	return req
}
