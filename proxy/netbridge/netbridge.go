package netbridge

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet/stat"
)

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		n := new(NetBridge)
		err := core.RequireFeatures(ctx, func(pm policy.Manager) error {
			return n.Init(config.(*Config), pm)
		})
		return n, err
	}))
}

// NetBridge is the inbound handler for NetBridge protocol.
type NetBridge struct {
	config *Config
	policy policy.Manager
	token  uint32
}

// Init initializes the NetBridge inbound.
func (n *NetBridge) Init(config *Config, pm policy.Manager) error {
	if config == nil {
		return errors.New("netbridge: config is nil")
	}
	if err := config.validateListenAddress(); err != nil {
		return err
	}
	n.config = config
	n.token = config.Token
	n.policy = pm
	return nil
}

// Network implements proxy.Inbound.
func (n *NetBridge) Network() []net.Network {
	return []net.Network{net.Network_TCP, net.Network_UDP}
}

// Process implements proxy.Inbound.
func (n *NetBridge) Process(ctx context.Context, network net.Network, conn stat.Connection, dispatcher routing.Dispatcher) error {
	errors.LogDebug(ctx, "netbridge: processing connection from ", conn.RemoteAddr())

	if network == net.Network_TCP {
		return n.processTCP(ctx, conn, dispatcher)
	}
	return n.processUDP(ctx, conn, dispatcher)
}

// processTCP handles a TCP connection with NetBridge Header.
func (n *NetBridge) processTCP(ctx context.Context, conn stat.Connection, dispatcher routing.Dispatcher) error {
	hdr, err := ParseTcpHeader(conn)
	if err != nil {
		errors.LogDebug(ctx, "netbridge: failed to parse TCP header: ", err)
		return err
	}

	if hdr.Magic != NbMagic {
		errors.LogDebug(ctx, "netbridge: invalid magic")
		conn.Close()
		return errors.New("invalid magic")
	}

	if hdr.Version != NbVersion {
		errors.LogDebug(ctx, "netbridge: version mismatch: got ", hdr.Version, " want ", NbVersion)
		SendNbError(conn, NbErrVersion)
		conn.Close()
		return errors.New("version mismatch")
	}

	if n.token != 0 && hdr.Token != n.token {
		errors.LogDebug(ctx, "netbridge: invalid token from pid=", hdr.Pid, " proc=", hdr.ProcName)
		SendNbError(conn, NbErrToken)
		conn.Close()
		return errors.New("invalid token")
	}

	dest := net.Destination{
		Network: net.Network_TCP,
		Address: hdr.DstNetAddress(),
		Port:    net.Port(hdr.DstPort),
	}

	if !dest.IsValid() || dest.Address == nil {
		conn.Close()
		return errors.New("netbridge: invalid destination")
	}

	inbound := session.InboundFromContext(ctx)
	inbound.Name = "netbridge"
	inbound.CanSpliceCopy = 1
	inbound.User = &protocol.MemoryUser{
		Level: n.config.UserLevel,
	}

	ctx = log.ContextWithAccessMessage(ctx, &log.AccessMessage{
		From:   conn.RemoteAddr(),
		To:     dest,
		Status: log.AccessAccepted,
		Reason: "",
	})
	errors.LogInfo(ctx, "netbridge: TCP pid=", hdr.Pid,
		" proc=", hdr.ProcName, " -> ", dest)

	if err := dispatcher.DispatchLink(
		ctx, dest, &transport.Link{
			Reader: buf.NewReader(conn),
			Writer: buf.NewWriter(conn),
		},
	); err != nil {
		return errors.New("netbridge: failed to dispatch TCP request").Base(err)
	}
	return nil
}

// processUDP handles a UDP session with NetBridge Header.
func (n *NetBridge) processUDP(ctx context.Context, conn stat.Connection, dispatcher routing.Dispatcher) error {
	packetConn, ok := conn.(net.PacketConn)
	if !ok {
		return errors.New("netbridge: connection is not a PacketConn")
	}

	recvBuf := make([]byte, 65535)
	for {
		numRead, remoteAddr, err := packetConn.ReadFrom(recvBuf)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return errors.New("netbridge: UDP read error").Base(err)
		}

		if numRead < NbUdpReqHeaderSize {
			continue
		}

		hdr, payload, err := ParseUdpReqHeader(recvBuf[:numRead])
		if err != nil {
			errors.LogDebug(ctx, "netbridge: UDP parse error: ", err)
			continue
		}

		if hdr.Magic != NbMagic {
			continue
		}
		if hdr.Version != NbVersion {
			continue
		}
		if n.token != 0 && hdr.Token != n.token {
			errors.LogDebug(ctx, "netbridge: UDP invalid token from pid=", hdr.Pid)
			continue
		}

		var dstIP net.IP
		if hdr.AddrType == NbAddrIPv4 {
			dstIP = net.IP(append([]byte(nil), hdr.DstAddr[:4]...))
		} else {
			dstIP = net.IP(append([]byte(nil), hdr.DstAddr[:]...))
		}
		dest := net.Destination{
			Network: net.Network_UDP,
			Address: net.IPAddress(dstIP),
			Port:    net.Port(hdr.DstPort),
		}

		if !dest.IsValid() || dest.Address == nil {
			continue
		}

		errors.LogDebug(ctx, "netbridge: UDP pid=", hdr.Pid, " -> ", dest)

		link := &transport.Link{
			Reader: buf.NewReader(bytes.NewReader(payload)),
			Writer: &udpResponseWriter{
				conn:       packetConn,
				remoteAddr: remoteAddr,
				hdr:        hdr,
			},
		}

		_ = dispatcher.DispatchLink(ctx, dest, link)
	}
}

// udpResponseWriter wraps responses back into NetBridge UDP format.
type udpResponseWriter struct {
	conn       net.PacketConn
	remoteAddr net.Addr
	hdr        *NbUdpReqHeader
}

func (w *udpResponseWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	for {
		mb2, b := buf.SplitFirst(mb)
		mb = mb2
		if b == nil {
			break
		}

		var srcAddr [16]byte
		addrType := uint8(NbAddrIPv4)
		if w.hdr.AddrType == NbAddrIPv6 {
			addrType = NbAddrIPv6
			copy(srcAddr[:], w.hdr.DstAddr[:])
		} else {
			copy(srcAddr[:4], w.hdr.DstAddr[:4])
		}

		respHdr := BuildNbUdpRespHeader(addrType, w.hdr.DstPort, srcAddr, uint16(b.Len()))
		data := b.Bytes()

		pkt := make([]byte, len(respHdr)+len(data))
		copy(pkt, respHdr)
		copy(pkt[len(respHdr):], data)

		w.conn.WriteTo(pkt, w.remoteAddr)
		b.Release()
	}
	return nil
}

func (w *udpResponseWriter) Close() error {
	return nil
}

func hex32(v uint32) string {
	return fmt.Sprintf("%08X", v)
}
