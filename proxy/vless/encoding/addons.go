package encoding

import (
	"context"
	"io"
	"net"
	"sync"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/proxy"
	"github.com/xtls/xray-core/proxy/vless"
)

// Sentinel errors for zero-allocation error returns on hot paths.
var (
	errUnexpectedWireType    = errors.New("unexpected wire type in addons")
	errAddonsDataTruncated   = errors.New("addons data truncated")
	errAddonsProtobufLength  = errors.New("failed to read addons protobuf length")
	errAddonsProtobufValue   = errors.New("failed to read addons protobuf value")
	errUnmarshalAddonsFailed = errors.New("failed to unmarshal addons protobuf value")
	errVarintTooLong         = errors.New("varint too long")
	errUnexpectedEndOfVarint = errors.New("unexpected end of varint")
)

// AddonsPool reuses Addons structs to reduce GC pressure.
var AddonsPool = sync.Pool{
	New: func() any {
		return &Addons{}
	},
}

func GetAddons() *Addons {
	a := AddonsPool.Get().(*Addons)
	a.Flow = ""
	a.Seed = nil
	return a
}

func PutAddons(a *Addons) {
	if a != nil {
		if a.Seed != nil {
			PutSeed(a.Seed)
		}
		a.Flow = ""
		a.Seed = nil
		AddonsPool.Put(a)
	}
}

// marshalAddons is a hand-optimized protobuf encoder for Addons.
// Addons has two fields: Flow (string, field 1) and Seed (bytes, field 2).
// Wire format: field_tag varint_length bytes
func marshalAddons(addons *Addons) []byte {
	flow := addons.Flow
	flowLen := len(flow)
	seed := addons.Seed
	seedLen := len(seed)

	if flowLen == 0 && seedLen == 0 {
		return nil
	}

	// Fast path for common case: Flow = "xtls-rprx-vision", Seed = nil
	// Pre-computed: 0x0a 0x10 "xtls-rprx-vision" = 18 bytes
	if flowLen == 16 && seedLen == 0 && flow == "xtls-rprx-vision" {
		return []byte{0x0a, 0x10,
			'x', 't', 'l', 's', '-', 'r', 'p', 'r', 'x', '-', 'v', 'i', 's', 'i', 'o', 'n'}
	}

	// Calculate total size:
	// For each field: 1 byte tag + varint(length) + bytes
	totalLen := 0
	// Field 1: Flow
	if flowLen > 0 {
		totalLen++ // tag
		totalLen += lenVarint(uint32(flowLen))
		totalLen += flowLen
	}
	// Field 2: Seed
	if seedLen > 0 {
		totalLen++ // tag
		totalLen += lenVarint(uint32(seedLen))
		totalLen += seedLen
	}

	result := make([]byte, totalLen)
	pos := 0

	// Encode Flow (field 1, wire type 2 = 0x0a)
	if flowLen > 0 {
		result[pos] = 0x0a // (1 << 3) | 2
		pos++
		pos += putVarint(result[pos:], uint32(flowLen))
		copy(result[pos:], flow)
		pos += flowLen
	}

	// Encode Seed (field 2, wire type 2 = 0x12)
	if seedLen > 0 {
		result[pos] = 0x12 // (2 << 3) | 2
		pos++
		pos += putVarint(result[pos:], uint32(seedLen))
		copy(result[pos:], seed)
		pos += seedLen
	}

	return result
}

// MarshalAddons is an exported wrapper for benchmark testing.
func lenVarint(x uint32) int {
	n := 0
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n + 1
}

func putVarint(buf []byte, x uint32) int {
	n := 0
	for x >= 0x80 {
		buf[n] = byte(x) | 0x80
		x >>= 7
		n++
	}
	buf[n] = byte(x)
	return n + 1
}

// unmarshalAddons is a hand-optimized protobuf decoder for Addons.
// knownFlows contains pre-allocated Flow string constants to avoid per-request allocation.
var knownFlows = []struct {
	flow string
}{
	{vless.XRV},
}

// flowString converts fieldData to a string, using pre-allocated constants for known flows.
func flowString(fieldData []byte) string {
	for _, kf := range knownFlows {
		if len(fieldData) == len(kf.flow) && string(fieldData) == kf.flow {
			return kf.flow
		}
	}
	return string(fieldData)
}

// seedPool reuses Seed byte slices to reduce allocations.
var seedPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 64)
		return &b
	},
}

// copySeed copies fieldData into a pooled buffer, growing if needed.
func copySeed(fieldData []byte) []byte {
	bp := seedPool.Get().(*[]byte)
	b := *bp
	if cap(b) < len(fieldData) {
		b = make([]byte, len(fieldData))
	} else {
		b = b[:len(fieldData)]
	}
	copy(b, fieldData)
	*bp = b
	return b
}

// PutSeed returns a Seed buffer to the pool for reuse.
func PutSeed(seed []byte) {
	if seed == nil || cap(seed) < 32 {
		return
	}
	bp := seed[:0]
	seedPool.Put(&bp)
}

func unmarshalAddons(data []byte, addons *Addons) error {
	pos := 0
	for pos < len(data) {
		if pos >= len(data) {
			break
		}
		// Read tag
		tag := data[pos]
		pos++
		fieldNumber := int(tag >> 3)
		wireType := tag & 0x07

		if wireType != 2 { // length-delimited
			return errUnexpectedWireType
		}

		// Read length
		length, n, err := decodeVarint(data[pos:])
		if err != nil {
			return err
		}
		pos += n

		if pos+int(length) > len(data) {
			return errAddonsDataTruncated
		}

		fieldData := data[pos : pos+int(length)]
		pos += int(length)

		switch fieldNumber {
		case 1: // Flow — use pre-allocated constants for known values
			addons.Flow = flowString(fieldData)
		case 2: // Seed — reuse pooled buffer
			addons.Seed = copySeed(fieldData)
		}
	}
	return nil
}

func decodeVarint(data []byte) (uint32, int, error) {
	var x uint32
	for i, b := range data {
		x |= uint32(b&0x7f) << (7 * uint(i))
		if b < 0x80 {
			return x, i + 1, nil
		}
		if i >= 4 {
			return 0, 0, errVarintTooLong
		}
	}
	return 0, 0, errUnexpectedEndOfVarint
}

// visionFlowWire is the pre-encoded protobuf bytes for the common
// Flow="xtls-rprx-vision" addons (0x0a 0x10 + 16 ASCII chars). Package-level
// and read-only so the per-connection header encode does not allocate a fresh
// slice every call.
var visionFlowWire = []byte{0x0a, 0x10,
	'x', 't', 'l', 's', '-', 'r', 'p', 'r', 'x', '-', 'v', 'i', 's', 'i', 'o', 'n'}

func EncodeHeaderAddons(buffer *buf.Buffer, addons *Addons) error {
	switch addons.Flow {
	case vless.XRV:
		if len(addons.Seed) == seedLength {
			// Fast path: XRV flow + 8-byte seed has a fixed 28-byte wire
			// layout (0x0a 0x10 <16B flow> 0x12 0x08 <8B seed>) — write it
			// directly instead of marshalAddons' temporary buffer.
			if err := buffer.WriteByte(28); err != nil {
				return errors.New("failed to write addons protobuf length").Base(err)
			}
			if _, err := buffer.Write(visionFlowWire); err != nil {
				return errors.New("failed to write addons protobuf value").Base(err)
			}
			if err := buffer.WriteByte(0x12); err != nil {
				return errors.New("failed to write addons protobuf value").Base(err)
			}
			if err := buffer.WriteByte(seedLength); err != nil {
				return errors.New("failed to write addons protobuf value").Base(err)
			}
			if _, err := buffer.Write(addons.Seed); err != nil {
				return errors.New("failed to write addons protobuf value").Base(err)
			}
			return nil
		}
		if len(addons.Seed) == 0 {
			// Fast path: constant wire bytes, no marshal allocation. The
			// payload is 18 bytes, always within the 255-byte length prefix.
			if err := buffer.WriteByte(byte(len(visionFlowWire))); err != nil {
				return errors.New("failed to write addons protobuf length").Base(err)
			}
			if _, err := buffer.Write(visionFlowWire); err != nil {
				return errors.New("failed to write addons protobuf value").Base(err)
			}
			return nil
		}
		// XRV flow with a seed: fall through to the full marshal (Flow+Seed).
		fallthrough
	default:
		// Serialize non-empty addons for non-XRV flows.
		if addons.Flow != "" {
			bytes := marshalAddons(addons)
			if len(bytes) > 255 {
				return errors.New("addons payload too large: ", len(bytes), " bytes (max 255)")
			}
			if err := buffer.WriteByte(byte(len(bytes))); err != nil {
				return errors.New("failed to write addons protobuf length").Base(err)
			}
			if _, err := buffer.Write(bytes); err != nil {
				return errors.New("failed to write addons protobuf value").Base(err)
			}
		} else {
			if err := buffer.WriteByte(0); err != nil {
				return errors.New("failed to write addons protobuf length").Base(err)
			}
		}
	}

	return nil
}

func DecodeHeaderAddons(buffer *buf.Buffer, reader io.Reader, addons *Addons) error {
	buffer.Clear()
	if _, err := buffer.ReadFullFrom(reader, 1); err != nil {
		return errAddonsProtobufLength
	}

	if length := int32(buffer.Byte(0)); length != 0 {
		buffer.Clear()
		if _, err := buffer.ReadFullFrom(reader, length); err != nil {
			return errAddonsProtobufValue
		}

		if err := unmarshalAddons(buffer.Bytes(), addons); err != nil {
			return errUnmarshalAddonsFailed
		}
	}

	return nil
}

// EncodeBodyAddons returns a Writer that auto-encrypt content written by caller.
func EncodeBodyAddons(writer buf.Writer, request *protocol.RequestHeader, requestAddons *Addons, state *proxy.TrafficState, isUplink bool, context context.Context, conn net.Conn, ob *session.Outbound) buf.Writer {
	if request.Command == protocol.RequestCommandUDP {
		return NewMultiLengthPacketWriter(writer)
	}
	if requestAddons.Flow == vless.XRV {
		account, ok := request.User.Account.(*vless.MemoryAccount)
		if !ok {
			return writer
		}
		return proxy.NewVisionWriter(writer, state, isUplink, context, conn, ob, account.Testseed)
	}
	return writer
}

// DecodeBodyAddons returns a Reader from which caller can fetch decrypted body.
func DecodeBodyAddons(reader io.Reader, request *protocol.RequestHeader, addons *Addons) buf.Reader {
	switch addons.Flow {
	default:
		if request.Command == protocol.RequestCommandUDP {
			return NewLengthPacketReader(reader)
		}
	}
	return buf.NewReader(reader)
}

func NewMultiLengthPacketWriter(writer buf.Writer) *MultiLengthPacketWriter {
	return &MultiLengthPacketWriter{
		Writer: writer,
	}
}

type MultiLengthPacketWriter struct {
	buf.Writer
}

func (w *MultiLengthPacketWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	defer buf.ReleaseMulti(mb)
	// Fresh MultiBuffer ownership transfer: do not recycle a pooled slice after
	// WriteMultiBuffer, or a retaining writer/pipe can observe corrupted frames.
	mb2Write := make(buf.MultiBuffer, 0, len(mb))
	for _, b := range mb {
		length := b.Len()
		if length == 0 {
			continue
		}
		if length+2 > buf.Size {
			// A single UDP datagram larger than the 2-byte length prefix can
			// frame is silently dropped by upstream Xray; surface it instead so
			// the caller can react instead of losing traffic without notice.
			return errors.New("UDP datagram too large for VLESS length-prefix framing: ", length, " > ", buf.Size-2)
		}
		eb := buf.New()
		if err := eb.WriteByte(byte(length >> 8)); err != nil {
			eb.Release()
			continue
		}
		if err := eb.WriteByte(byte(length)); err != nil {
			eb.Release()
			continue
		}
		if _, err := eb.Write(b.Bytes()); err != nil {
			eb.Release()
			continue
		}
		mb2Write = append(mb2Write, eb)
	}
	if len(mb2Write) == 0 {
		return nil
	}
	return w.Writer.WriteMultiBuffer(mb2Write)
}

func NewLengthPacketReader(reader io.Reader) *LengthPacketReader {
	return &LengthPacketReader{
		Reader: reader,
		cache:  make([]byte, 2),
	}
}

type LengthPacketReader struct {
	io.Reader
	cache []byte
}

func (r *LengthPacketReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	if _, err := io.ReadFull(r.Reader, r.cache); err != nil { // maybe EOF
		return nil, errors.New("failed to read packet length").Base(err)
	}
	length := int32(r.cache[0])<<8 | int32(r.cache[1])
	mb := make(buf.MultiBuffer, 0, length/buf.Size+1)
	for length > 0 {
		size := length
		if size > buf.Size {
			size = buf.Size
		}
		length -= size
		b := buf.New()
		if _, err := b.ReadFullFrom(r.Reader, size); err != nil {
			return nil, errors.New("failed to read packet payload").Base(err)
		}
		mb = append(mb, b)
	}
	return mb, nil
}
