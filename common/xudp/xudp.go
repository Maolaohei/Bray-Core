package xudp

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/platform"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"lukechampine.com/blake3"
)

var AddrParser = protocol.NewAddressParser(
	protocol.AddressFamilyByte(byte(protocol.AddressTypeIPv4), net.AddressFamilyIPv4),
	protocol.AddressFamilyByte(byte(protocol.AddressTypeDomain), net.AddressFamilyDomain),
	protocol.AddressFamilyByte(byte(protocol.AddressTypeIPv6), net.AddressFamilyIPv6),
	protocol.PortThenAddress(),
)

const (
	maxMetaLen = 2048 // upper bound for meta-data length to prevent DoS
)

var (
	Show    bool
	BaseKey []byte

	// Pre-allocated, read-only header prefixes to avoid per-packet
	// allocations on the hot path. Never modify these at runtime.
	newPacketPrefix  = [7]byte{0, 0, 0, 0, 1, 1, 2} // meta length (2) + Mux Session ID (2) + New + Opt + UDP
	keepPacketPrefix = [6]byte{0, 0, 0, 0, 2, 1}    // meta length (2) + Mux Session ID (2) + Keep + Opt
)

func init() {
	if strings.ToLower(platform.NewEnvFlag(platform.XUDPLog).GetValue(func() string { return "" })) == "true" {
		Show = true
	}
	BaseKey = make([]byte, 32)
	if _, err := rand.Read(BaseKey); err != nil {
		panic("xudp: crypto/rand.Read failed: " + err.Error())
	}
	go func() {
		time.Sleep(100 * time.Millisecond) // this is not nice, but need to give some time for Android to setup ENV
		if raw := platform.NewEnvFlag(platform.XUDPBaseKey).GetValue(func() string { return "" }); raw != "" {
			if BaseKey, _ = base64.RawURLEncoding.DecodeString(raw); len(BaseKey) == 32 {
				return
			}
			panic(platform.XUDPBaseKey + ": invalid value (BaseKey must be 32 bytes): " + raw + " len " + strconv.Itoa(len(BaseKey)))
		}
	}()
}

func GetGlobalID(ctx context.Context) (globalID [8]byte) {
	cone, ok := ctx.Value("cone").(bool)
	if !ok || !cone {
		return
	}
	if inbound := session.InboundFromContext(ctx); inbound != nil && inbound.Source.Network == net.Network_UDP &&
		(inbound.Name == "dokodemo-door" || inbound.Name == "socks" || inbound.Name == "shadowsocks" || inbound.Name == "tun") {
		h := blake3.New(8, BaseKey)
		h.Write([]byte(inbound.Source.String()))
		copy(globalID[:], h.Sum(nil))
		if Show {
			errors.LogInfo(ctx, fmt.Sprintf("XUDP inbound.Source.String(): %v\tglobalID: %v\n", inbound.Source.String(), globalID))
		}
	}
	return
}

func NewPacketWriter(writer buf.Writer, dest net.Destination, globalID [8]byte) *PacketWriter {
	return &PacketWriter{
		Writer:   writer,
		Dest:     dest,
		GlobalID: globalID,
	}
}

type PacketWriter struct {
	Writer   buf.Writer
	Dest     net.Destination
	GlobalID [8]byte
}

func (w *PacketWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	defer buf.ReleaseMulti(mb)

	// L2a batching: same-target datagrams arriving in one write are
	// serialized as a single frame (one header + 1B count + N×[2B len +
	// payload]), amortizing the frame header and cutting wire bytes.
	// Off by default (XUDPBatch=true): new clients must not talk batch
	// to old servers. N=1 / mixed targets / oversized fall back to the
	// single-frame path below.
	if batchWriteEnabled() && len(mb) > 1 {
		if ok, err := w.tryWriteBatch(mb); ok {
			return err
		}
	}

	mb2Write := make(buf.MultiBuffer, 0, len(mb))
	for _, b := range mb {
		length := b.Len()
		if length == 0 {
			continue
		}
		// Exact per-frame overhead instead of the old flat +666 reserve
		// (~650B over-reserved, which silently skipped packets on the
		// 64870-65500B boundary). Reject loudly like the VLESS layer's
		// MultiLengthPacketWriter does, never drop silently.
		head := 9 // 7B mux prefix + 2B payload length
		if w.Dest.Network == net.Network_UDP {
			head += addrWireLen(w.Dest.Address)
			if b.UDP != nil {
				head += 8 // GlobalID
			}
		} else if b.UDP != nil {
			head += 1 + addrWireLen(b.UDP.Address) // UDP flag + address
		}
		if length > buf.Size-int32(head) {
			return errors.New("UDP datagram too large for XUDP frame: ", length, " > ", buf.Size-int32(head))
		}

		eb := buf.New()
		if w.Dest.Network == net.Network_UDP {
			eb.Write(newPacketPrefix[:]) // metaHeader + New + Opt + UDP
			AddrParser.WriteAddressPort(eb, w.Dest.Address, w.Dest.Port)
			if b.UDP != nil { // make sure it's user's proxy request
				eb.Write(w.GlobalID[:]) // no need to check whether it's empty
			}
			w.Dest.Network = net.Network_Unknown
		} else {
			eb.Write(keepPacketPrefix[:]) // metaHeader + Keep + Opt
			if b.UDP != nil {
				eb.WriteByte(2) // UDP
				AddrParser.WriteAddressPort(eb, b.UDP.Address, b.UDP.Port)
			}
		}
		l := eb.Len() - 2
		eb.SetByte(0, byte(l>>8))
		eb.SetByte(1, byte(l))
		eb.WriteByte(byte(length >> 8))
		eb.WriteByte(byte(length))
		eb.Write(b.Bytes())

		mb2Write = append(mb2Write, eb)
	}
	if mb2Write.IsEmpty() {
		return nil
	}
	return w.Writer.WriteMultiBuffer(mb2Write)
}

// addrWireLen returns the encoded wire size (type byte + payload) of an
// address as written by AddrParser.
func addrWireLen(a net.Address) int {
	switch a.Family() {
	case net.AddressFamilyIPv4:
		return 5
	case net.AddressFamilyIPv6:
		return 17
	default:
		return 1 + len(a.Domain())
	}
}

// batchFrameMaxCount caps sub-frames per batch frame: with buf.Size 8KB
// and a minimum 4B sub-frame the hard bound is ~180; 64 keeps the count
// byte distribution tighter and limits a single frame's blast radius.
const (
	// batchOpt mirrors mux.OptionBatch (0x04) without importing mux
	// (import cycle: mux/client.go imports xudp).
	batchOpt = 0x04
	// batchFrameMaxCount caps sub-frames per batch frame: with buf.Size 8KB
	// and a minimum 4B sub-frame the hard bound is ~180; 64 keeps the count
	// byte distribution tighter and limits a single frame blast radius.
	batchFrameMaxCount = 64
)

// batchWriteEnabled gates L2a batch frames. Default ON: the server-side
// batch expansion ships in the same release and always accepts legacy
// single frames, so batch frames are safe with any current server. The
// only risk window is a NEW client talking to a PRE-batch server (the
// meta length would misparse and kill the stream) — disable with
// XUDPBatch=false during staggered upgrades.
func batchWriteEnabled() bool {
	return !strings.EqualFold(platform.NewEnvFlag("XUDPBatch").GetValue(func() string { return "true" }), "false")
}

// sameDestination reports whether two datagram destinations (address +
// port) are identical — required for sharing one batch frame header.
func sameDestination(a, b *net.Destination) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Port != b.Port || a.Address.Family() != b.Address.Family() {
		return false
	}
	if a.Address.Family().IsIP() {
		return a.Address.IP().Equal(b.Address.IP())
	}
	return a.Address.Domain() == b.Address.Domain()
}

// tryWriteBatch writes all non-empty datagrams of mb as one batch frame
// when possible. Returns (true, err) when the batch was written (err may
// be non-nil from the underlying writer), (false, nil) when the batch
// does not apply (mixed targets, too large, single packet) and the caller
// must fall back to the single-frame path.
func (w *PacketWriter) tryWriteBatch(mb buf.MultiBuffer) (bool, error) {
	firstFrame := w.Dest.Network == net.Network_UDP
	// Collect non-empty packets; verify target consistency for keep frames.
	var firstDest *net.Destination
	var sum int32
	n := 0
	for _, b := range mb {
		if b.Len() == 0 {
			continue
		}
		if !firstFrame {
			if b.UDP == nil {
				return false, nil
			}
			if firstDest == nil {
				firstDest = b.UDP
			} else if !sameDestination(firstDest, b.UDP) {
				return false, nil
			}
		}
		sum += b.Len()
		n++
	}
	if n < 2 || n > batchFrameMaxCount {
		return false, nil
	}

	head := 0
	if firstFrame {
		head = 7 + 2 + addrWireLen(w.Dest.Address) + 8 // prefix + port + addr + GlobalID
	} else {
		head = 6 + 1 + 2 + addrWireLen(firstDest.Address) // keep prefix + UDP flag + port + addr
	}
	head += 1 + 2*n // count + sub-frame length fields
	if head+int(sum) > buf.Size {
		return false, nil
	}

	eb := buf.New()
	if firstFrame {
		eb.Write(newPacketPrefix[:])
		AddrParser.WriteAddressPort(eb, w.Dest.Address, w.Dest.Port)
		eb.Write(w.GlobalID[:])
		w.Dest.Network = net.Network_Unknown
	} else {
		eb.Write(keepPacketPrefix[:])
		eb.WriteByte(2) // UDP
		AddrParser.WriteAddressPort(eb, firstDest.Address, firstDest.Port)
	}
	eb.WriteByte(byte(n))
	for _, b := range mb {
		if b.Len() == 0 {
			continue
		}
		eb.WriteByte(byte(b.Len() >> 8))
		eb.WriteByte(byte(b.Len()))
		eb.Write(b.Bytes())
	}
	// meta length prefix (2B at offset 0) + Batch option bit (offset 5).
	l := eb.Len() - 2
	eb.SetByte(0, byte(l>>8))
	eb.SetByte(1, byte(l))
	eb.SetByte(5, eb.Byte(5)|batchOpt)
	return true, w.Writer.WriteMultiBuffer(buf.MultiBuffer{eb})
}

func NewPacketReader(reader io.Reader) *PacketReader {
	return &PacketReader{
		Reader: reader,
		cache:  make([]byte, 2),
	}
}

type PacketReader struct {
	Reader io.Reader
	cache  []byte
}

func (r *PacketReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	for {
		if _, err := io.ReadFull(r.Reader, r.cache); err != nil {
			return nil, err
		}
		l := int32(r.cache[0])<<8 | int32(r.cache[1])
		if l < 4 || l > maxMetaLen {
			return nil, io.EOF
		}
		b := buf.New()
		if _, err := b.ReadFullFrom(r.Reader, l); err != nil {
			b.Release()
			return nil, err
		}
		discard := false
		switch b.Byte(2) {
		case 2:
			if l > 4 && b.Byte(4) == 2 { // MUST check the flag first
				b.Advance(5)
				// b.Clear() will be called automatically if all data had been read.
				addr, port, err := AddrParser.ReadAddressPort(nil, b)
				if err != nil {
					b.Release()
					return nil, err
				}
				b.UDP = &net.Destination{
					Network: net.Network_UDP,
					Address: addr,
					Port:    port,
				}
			}
		case 4:
			discard = true
		default:
			b.Release()
			return nil, io.EOF
		}
		b.Clear() // in case there is padding (empty bytes) attached
		if b.Byte(3) == 1 {
			if _, err := io.ReadFull(r.Reader, r.cache); err != nil {
				b.Release()
				return nil, err
			}
			length := int32(r.cache[0])<<8 | int32(r.cache[1])
			if length <= 0 || length > int32(buf.Size) {
				b.Release()
				continue
			}
			if length > 0 {
				if _, err := b.ReadFullFrom(r.Reader, length); err != nil {
					b.Release()
					return nil, err
				}
				if !discard {
					return buf.MultiBuffer{b}, nil
				}
			}
		}
		b.Release()
	}
}
