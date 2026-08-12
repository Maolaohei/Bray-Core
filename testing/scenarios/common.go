package scenarios

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/retry"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/units"
	core "github.com/xtls/xray-core/core"
	"google.golang.org/protobuf/proto"
)

func xor(b []byte) []byte {
	r := make([]byte, len(b))
	for i, v := range b {
		r[i] = v ^ 'c'
	}
	return r
}

func readFrom2(conn net.Conn, timeout time.Duration, length int) ([]byte, error) {
	b := make([]byte, length)
	deadline := time.Now().Add(timeout)
	conn.SetReadDeadline(deadline)
	n, err := io.ReadFull(conn, b[:length])
	if err != nil {
		return nil, err
	}
	return b[:n], nil
}

func InitializeServerConfigs(configs ...*core.Config) ([]*exec.Cmd, error) {
	servers := make([]*exec.Cmd, 0, 10)

	for _, config := range configs {
		server, err := InitializeServerConfig(config)
		if err != nil {
			CloseAllServers(servers)
			return nil, err
		}
		servers = append(servers, server)
	}

	time.Sleep(time.Second * 2)

	return servers, nil
}

func InitializeServerConfig(config *core.Config) (*exec.Cmd, error) {
	return InitializeServerConfigWithEnv(config, nil)
}

func InitializeServerConfigWithEnv(config *core.Config, env []string) (*exec.Cmd, error) {
	err := BuildXray()
	if err != nil {
		return nil, err
	}

	config = withDefaultApps(config)
	configBytes, err := proto.Marshal(config)
	if err != nil {
		return nil, err
	}
	proc := RunXrayProtobufWithEnv(configBytes, env)

	if err := proc.Start(); err != nil {
		return nil, err
	}

	return proc, nil
}

var (
	testBinaryPath    string
	testBinaryCleanFn func()
	testBinaryPathGen sync.Once
)

func genTestBinaryPath() {
	testBinaryPathGen.Do(func() {
		var tempDir string
		common.Must(retry.Timed(5, 100).On(func() error {
			dir, err := os.MkdirTemp("", "xray")
			if err != nil {
				return err
			}
			tempDir = dir
			testBinaryCleanFn = func() { os.RemoveAll(dir) }
			return nil
		}))
		file := filepath.Join(tempDir, "xray.test")
		if runtime.GOOS == "windows" {
			file += ".exe"
		}
		testBinaryPath = file
		fmt.Printf("Generated binary path: %s\n", file)
	})
}

func GetSourcePath() string {
	return filepath.Join("github.com", "xtls", "xray-core", "main")
}

func CloseAllServers(servers []*exec.Cmd) {
	log.Record(&log.GeneralMessage{
		Severity: log.Severity_Info,
		Content:  "Closing all servers.",
	})
	for _, server := range servers {
		if runtime.GOOS == "windows" {
			server.Process.Kill()
		} else {
			server.Process.Signal(syscall.SIGTERM)
		}
	}
	for _, server := range servers {
		server.Process.Wait()
	}
	log.Record(&log.GeneralMessage{
		Severity: log.Severity_Info,
		Content:  "All server closed.",
	})
}

func CloseServer(server *exec.Cmd) {
	log.Record(&log.GeneralMessage{
		Severity: log.Severity_Info,
		Content:  "Closing server.",
	})
	if runtime.GOOS == "windows" {
		server.Process.Kill()
	} else {
		server.Process.Signal(syscall.SIGTERM)
	}
	server.Process.Wait()
	log.Record(&log.GeneralMessage{
		Severity: log.Severity_Info,
		Content:  "Server closed.",
	})
}

func withDefaultApps(config *core.Config) *core.Config {
	config.App = append(config.App, serial.ToTypedMessage(&dispatcher.Config{}))
	config.App = append(config.App, serial.ToTypedMessage(&proxyman.InboundConfig{}))
	config.App = append(config.App, serial.ToTypedMessage(&proxyman.OutboundConfig{}))
	return config
}

func testTCPConn(port net.Port, payloadSize int, timeout time.Duration) func() error {
	return func() error {
		conn, err := net.DialTCP("tcp", nil, &net.TCPAddr{
			IP:   []byte{127, 0, 0, 1},
			Port: int(port),
		})
		if err != nil {
			return err
		}
		defer conn.Close()

		return testTCPConn2(conn, payloadSize, timeout)()
	}
}

func testUDPConn(port net.Port, payloadSize int, timeout time.Duration) func() error {
	return func() error {
		conn, err := net.DialUDP("udp", nil, &net.UDPAddr{
			IP:   []byte{127, 0, 0, 1},
			Port: int(port),
		})
		if err != nil {
			return err
		}
		defer conn.Close()

		return testTCPConn2(conn, payloadSize, timeout)()
	}
}

func testTCPConn2(conn net.Conn, payloadSize int, timeout time.Duration) func() error {
	return func() (err1 error) {
		start := time.Now()
		defer func() {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			// For info on each, see: https://golang.org/pkg/runtime/#MemStats
			fmt.Println("testConn finishes:", time.Since(start).Milliseconds(), "ms\t",
				err1, "\tAlloc =", units.ByteSize(m.Alloc).String(),
				"\tTotalAlloc =", units.ByteSize(m.TotalAlloc).String(),
				"\tSys =", units.ByteSize(m.Sys).String(),
				"\tNumGC =", m.NumGC)
		}()
		singleWrite := func(length int) error {
			payload := make([]byte, length)
			common.Must2(rand.Read(payload))

			nBytes, err := conn.Write(payload)
			if err != nil {
				return err
			}
			if nBytes != len(payload) {
				return errors.New("expect ", len(payload), " written, but actually ", nBytes)
			}

			response, err := readFrom2(conn, timeout, length)
			if err != nil {
				return err
			}
			_ = response

			if r := bytes.Compare(response, xor(payload)); r != 0 {
				return errors.New(r)
			}

			return nil
		}
		for payloadSize > 0 {
			sizeToWrite := 1024
			if payloadSize < 1024 {
				sizeToWrite = payloadSize
			}
			if err := singleWrite(sizeToWrite); err != nil {
				return err
			}
			payloadSize -= sizeToWrite
		}
		return nil
	}
}

func WaitConnAvailableWithTest(t *testing.T, testFunc func() error) bool {
	// Windows CI runners are slower to bind/accept after process start, and
	// Hyper-V excluded port ranges make first attempts fail more often.
	// Linux/macOS GitHub runners also need headroom under parallel packages.
	maxAttempts := 20
	sleep := 25 * time.Millisecond
	if runtime.GOOS == "windows" {
		maxAttempts = 30
		sleep = 50 * time.Millisecond
	}
	for i := 1; ; i++ {
		if i > maxAttempts {
			t.Log("All attempts failed to test tcp conn")
			return false
		}
		time.Sleep(sleep)
		if err := testFunc(); err != nil {
			t.Log("err ", err)
		} else {
			t.Log("success with", i, "attempts")
			break
		}
	}
	return true
}

// waitTCPListening returns true once a TCP dial to 127.0.0.1:port succeeds.
// Used by scenario tests that must not race process startup.
func waitTCPListening(port net.Port, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	addr := &net.TCPAddr{IP: []byte{127, 0, 0, 1}, Port: int(port)}
	for time.Now().Before(deadline) {
		conn, err := net.DialTCP("tcp", nil, addr)
		if err == nil {
			_ = conn.Close()
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// isBenignPreAuthDialError reports dial outcomes that are acceptable before a
// VMess user has been added: peer closed (EOF), read timeout, or the Windows
// connection-refused race when the inbound is not yet fully ready.
func isBenignPreAuthDialError(err error) bool {
	if err == nil || err == io.EOF {
		return true
	}
	s := err.Error()
	if strings.HasSuffix(s, "i/o timeout") {
		return true
	}
	// Windows: "connectex: No connection could be made because the target
	// machine actively refused it." / Unix: "connection refused".
	return strings.Contains(s, "actively refused") ||
		strings.Contains(s, "connection refused")
}

// pickFreeTCPPortRange returns a contiguous block of free TCP ports on
// 127.0.0.1 of length count (inclusive From..To = From+count-1). A single
// PickPort() only proves one free port; dokodemo multi-port inbounds need the
// whole range free or the xray subprocess binds partially and probes refuse.
func pickFreeTCPPortRange(count int) (net.Port, error) {
	if count < 1 {
		count = 1
	}
	const attempts = 64
	for i := 0; i < attempts; i++ {
		base := tcpPickPort()
		from := int(base)
		listeners := make([]net.Listener, 0, count)
		ok := true
		for off := 0; off < count; off++ {
			ln, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", from+off))
			if err != nil {
				ok = false
				break
			}
			listeners = append(listeners, ln)
		}
		for _, ln := range listeners {
			_ = ln.Close()
		}
		if ok {
			return net.Port(from), nil
		}
	}
	return 0, errors.New("unable to pick free TCP port range of size ", count)
}

// pickFreeUDPPortRange is the UDP counterpart of pickFreeTCPPortRange.
func pickFreeUDPPortRange(count int) (net.Port, error) {
	if count < 1 {
		count = 1
	}
	const attempts = 64
	for i := 0; i < attempts; i++ {
		base := udpPickPort()
		from := int(base)
		conns := make([]*net.UDPConn, 0, count)
		ok := true
		for off := 0; off < count; off++ {
			c, err := net.ListenUDP("udp4", &net.UDPAddr{
				IP:   []byte{127, 0, 0, 1},
				Port: from + off,
			})
			if err != nil {
				ok = false
				break
			}
			conns = append(conns, c)
		}
		for _, c := range conns {
			_ = c.Close()
		}
		if ok {
			return net.Port(from), nil
		}
	}
	return 0, errors.New("unable to pick free UDP port range of size ", count)
}

func tcpPickPort() net.Port {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	common.Must(err)
	defer listener.Close()
	return net.Port(listener.Addr().(*net.TCPAddr).Port)
}

func udpPickPort() net.Port {
	c, err := net.ListenUDP("udp4", &net.UDPAddr{IP: []byte{127, 0, 0, 1}, Port: 0})
	common.Must(err)
	defer c.Close()
	return net.Port(c.LocalAddr().(*net.UDPAddr).Port)
}
