package scenarios

import (
	"testing"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/testing/servers/tcp"
)

// TestVlessTLSXHTTPServerAutoModeMatrix proves that a single auto-mode XHTTP
// server accepts every wire shape emitted by an explicit client mode. It uses
// real child Xray server/client processes, VLESS, TLS/H2, and a TCP XOR echo
// target; no request handler or transport is mocked.
func TestVlessTLSXHTTPServerAutoModeMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("real dual-end XHTTP mode matrix is skipped in short mode")
	}

	for _, clientMode := range []string{"stream-one", "stream-up", "packet-up", "auto"} {
		clientMode := clientMode
		t.Run(clientMode, func(t *testing.T) {
			ep := &dsegEndpoints{
				serverPort:  tcp.PickPort(),
				serverMode:  "auto",
				clientMode:  clientMode,
				dsegDisable: true,
			}
			startServerOnly(t, ep)
			startClientOnly(t, ep, "127.0.0.1", ep.serverPort)

			conn, err := dialClient(ep.clientPort)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			for _, size := range []int{32 << 10, 2 << 20} {
				if err := echoOnce(conn, size); err != nil {
					t.Fatalf("%s %d-byte echo: %v", clientMode, size, err)
				}
			}
		})
	}
}

// TestVlessTLSXHTTPServerAutoPacketUpTrafficShapes drives the same server:auto
// / client:packet-up pair through the high-fanout shapes seen in browser and
// video workloads. This is the local regression line for packet-up POST 404
// storms: a correct run must complete every byte without retry exhaustion.
func TestVlessTLSXHTTPServerAutoPacketUpTrafficShapes(t *testing.T) {
	if testing.Short() {
		t.Skip("real dual-end packet-up traffic shapes are skipped in short mode")
	}

	ep := &dsegEndpoints{
		serverPort: tcp.PickPort(),
		serverMode: "auto",
		clientMode: "packet-up",
	}
	startServerOnly(t, ep)
	startClientOnly(t, ep, "127.0.0.1", ep.serverPort)

	for _, workload := range []struct {
		name string
		fn   func(*testing.T, net.Port) error
	}{
		{name: "web", fn: workloadWeb},
		{name: "video", fn: workloadVideo},
		{name: "multi-thread-file", fn: workloadMultiThreadFile},
	} {
		workload := workload
		t.Run(workload.name, func(t *testing.T) {
			if err := workload.fn(t, ep.clientPort); err != nil {
				t.Fatal(err)
			}
		})
	}
}
