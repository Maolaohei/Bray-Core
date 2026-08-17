package mux

// POC for a suspected HIGH-severity issue: an unroutable/failing UDP
// target in xudpEstablish() returns an error that propagates up through
// handleFrame -> run() -> done.Close(), tearing down the ENTIRE Mux
// connection (all TCP streams + UDP sessions) — a single datagram DoS.

import (
	"context"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

type failingDispatcher struct{}

func (failingDispatcher) Type() interface{} { return routing.DispatcherType() }
func (failingDispatcher) Start() error      { return nil }
func (failingDispatcher) Close() error      { return nil }
func (failingDispatcher) Dispatch(ctx context.Context, dest net.Destination) (*transport.Link, error) {
	return nil, errors.New("dial failed: unroutable target " + dest.String())
}
func (failingDispatcher) DispatchLink(ctx context.Context, dest net.Destination, link *transport.Link) error {
	return errors.New("dial failed")
}

type okayDispatcher struct{}

func (okayDispatcher) Type() interface{} { return routing.DispatcherType() }
func (okayDispatcher) Start() error      { return nil }
func (okayDispatcher) Close() error      { return nil }
func (okayDispatcher) Dispatch(ctx context.Context, dest net.Destination) (*transport.Link, error) {
	// Emulate a working UDP downstream: a closed pipe pair acts as an
	// instant authoritative endpoint.
	r, w := pipe.New(pipe.WithSizeLimit(4096))
	return &transport.Link{Reader: r, Writer: w}, nil
}
func (okayDispatcher) DispatchLink(ctx context.Context, dest net.Destination, link *transport.Link) error {
	return nil
}

func encodeXUDPFrame(t *testing.T, target net.Destination) *buf.Buffer {
	t.Helper()
	frame := buf.New()
	// b.UDP must be set so WriteTo emits the 8-byte GlobalID into the meta
	// payload (frame.go WriteTo line ~100).
	frame.UDP = &net.Destination{Network: net.Network_UDP, Address: target.Address, Port: target.Port}
	meta := FrameMetadata{
		SessionStatus: SessionStatusNew,
		Option:        OptionData,
		Target:        target,
		GlobalID:      [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
	}
	if err := meta.WriteTo(frame); err != nil {
		t.Fatal(err)
	}
	// XUDP datagram body: 2B length + payload.
	datagram := []byte("do")
	frame.WriteByte(byte(len(datagram) >> 8))
	frame.WriteByte(byte(len(datagram)))
	frame.Write(datagram)
	return frame
}

// TestXUDPUnroutableTargetKillsMuxConn reproduces the suspected DoS.
func TestXUDPUnroutableTargetKillsMuxConn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// serverReader = what the server reads (client frames), serverWriter = server responses.
	_, serverWriter := pipe.New()
	serverReader, clientWriter := pipe.New()
	link := &transport.Link{Reader: serverReader, Writer: serverWriter}

	worker, err := NewServerWorker(ctx, failingDispatcher{}, link)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()

	target := net.UDPDestination(net.DomainAddress("unroutable.invalid"), 53)
	if err := clientWriter.WriteMultiBuffer(buf.MultiBuffer{encodeXUDPFrame(t, target)}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-worker.WaitClosed():
		t.Fatalf("REGENERATED/HIGH: single unroutable UDP datagram killed the whole Mux connection (worker closed)")
	case <-time.After(3 * time.Second):
		// Connection survives: expected behavior for a fix (log & drop only).
	}
}

// TestXUDPReachableTargetKeepsMux: control case — a non-failing target
// must NOT close the connection (guards against a false positive).
func TestXUDPReachableTargetKeepsMux(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, serverWriter := pipe.New()
	serverReader, clientWriter := pipe.New()
	link := &transport.Link{Reader: serverReader, Writer: serverWriter}

	worker, err := NewServerWorker(ctx, okayDispatcher{}, link)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()

	target := net.UDPDestination(net.LocalHostIP, 9999)
	if err := clientWriter.WriteMultiBuffer(buf.MultiBuffer{encodeXUDPFrame(t, target)}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-worker.WaitClosed():
		t.Fatal("control: connection closed unexpectedly")
	case <-time.After(500 * time.Millisecond):
		// Expected: alive.
	}
}
