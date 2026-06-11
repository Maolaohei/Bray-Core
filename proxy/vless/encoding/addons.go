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
		a.Flow = ""
		a.Seed = nil
		AddonsPool.Put(a)
	}
}

// mbSlicePool reuses MultiBuffer slices for UDP packet encoding/decoding.
// buf.MultiBuffer is []*buf.Buffer, so we pool []*buf.Buffer slices.
var mbSlicePool = sync.Pool{
	New: func() any {
		s := make([]*buf.Buffer, 0, 8)
		return &s
	},
}

func getMBSlice() *[]*buf.Buffer {
	return mbSlicePool.Get().(*[]*buf.Buffer)
}

func putMBSlice(s *[]*buf.Buffer) {
	*s = (*s)[:0]
	mbSlicePool.Put(s)
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
	// Pre-computed: 0x0a 0x11 "xtls-rprx-vision" = 19 bytes
	if flowLen == 17 && seedLen == 0 && flow == "xtls-rprx-vision" {
		return []byte{0x0a, 0x11,
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
		pos = putVarint(result[pos:], uint32(flowLen))
		copy(result[pos:], flow)
		pos += flowLen
	}

	// Encode Seed (field 2, wire type 2 = 0x12)
	if seedLen > 0 {
		result[pos] = 0x12 // (2 << 3) | 2
		pos++
		pos = putVarint(result[pos:], uint32(seedLen))
		copy(result[pos:], seed)
		pos += seedLen
	}

	return result
}

// MarshalAddons is an exported wrapper for benchmark testing.
func MarshalAddons(addons *Addons) []byte {
	return marshalAddons(addons)
}

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
			return errors.New("unexpected wire type in addons")
		}

		// Read length
		length, n, err := decodeVarint(data[pos:])
		if err != nil {
			return err
		}
		pos += n

		if pos+int(length) > len(data) {
			return errors.New("addons data truncated")
		}

		fieldData := data[pos : pos+int(length)]
		pos += int(length)

		switch fieldNumber {
		case 1: // Flow
			addons.Flow = string(fieldData)
		case 2: // Seed
			seed := make([]byte, len(fieldData))
			copy(seed, fieldData)
			addons.Seed = seed
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
			return 0, 0, errors.New("varint too long")
		}
	}
	return 0, 0, errors.New("unexpected end of varint")
}

func EncodeHeaderAddons(buffer *buf.Buffer, addons *Addons) error {
	switch addons.Flow {
	case vless.XRV:
		bytes := marshalAddons(addons)
		if err := buffer.WriteByte(byte(len(bytes))); err != nil {
			return errors.New("failed to write addons protobuf length").Base(err)
		}
		if len(bytes) > 0 {
			if _, err := buffer.Write(bytes); err != nil {
				return errors.New("failed to write addons protobuf value").Base(err)
			}
		}
	default:
		// Serialize non-empty addons for non-XRV flows.
		if addons.Flow != "" {
			bytes := marshalAddons(addons)
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

func DecodeHeaderAddons(buffer *buf.Buffer, reader io.Reader) (*Addons, error) {
	addons := GetAddons()
	buffer.Clear()
	if _, err := buffer.ReadFullFrom(reader, 1); err != nil {
		PutAddons(addons)
		return nil, errors.New("failed to read addons protobuf length").Base(err)
	}

	if length := int32(buffer.Byte(0)); length != 0 {
		buffer.Clear()
		if _, err := buffer.ReadFullFrom(reader, length); err != nil {
			PutAddons(addons)
			return nil, errors.New("failed to read addons protobuf value").Base(err)
		}

		if err := unmarshalAddons(buffer.Bytes(), addons); err != nil {
			PutAddons(addons)
			return nil, errors.New("failed to unmarshal addons protobuf value").Base(err)
		}

		// Verification.
		switch addons.Flow {
		default:
		}
	}

	return addons, nil
}

// EncodeBodyAddons returns a Writer that auto-encrypt content written by caller.
func EncodeBodyAddons(writer buf.Writer, request *protocol.RequestHeader, requestAddons *Addons, state *proxy.TrafficState, isUplink bool, context context.Context, conn net.Conn, ob *session.Outbound) buf.Writer {
	if request.Command == protocol.RequestCommandUDP {
		return NewMultiLengthPacketWriter(writer)
	}
	if requestAddons.Flow == vless.XRV {
		return proxy.NewVisionWriter(writer, state, isUplink, context, conn, ob, request.User.Account.(*vless.MemoryAccount).Testseed)
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
	sp := getMBSlice()
	mb2Write := (*sp)[:0]
	for _, b := range mb {
		length := b.Len()
		if length == 0 || length+2 > buf.Size {
			continue
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
		putMBSlice(sp)
		return nil
	}
	err := w.Writer.WriteMultiBuffer(mb2Write)
	putMBSlice(sp)
	return err
}

func NewLengthPacketWriter(writer io.Writer) *LengthPacketWriter {
	return &LengthPacketWriter{
		Writer: writer,
		cache:  make([]byte, 0, 65536),
	}
}

type LengthPacketWriter struct {
	io.Writer
	cache []byte
}

func (w *LengthPacketWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	length := mb.Len() // none of mb is nil
	// fmt.Println("Write", length)
	if length == 0 {
		return nil
	}
	defer func() {
		w.cache = w.cache[:0]
	}()
	w.cache = append(w.cache, byte(length>>8), byte(length))
	for i, b := range mb {
		w.cache = append(w.cache, b.Bytes()...)
		b.Release()
		mb[i] = nil
	}
	if _, err := w.Write(w.cache); err != nil {
		return errors.New("failed to write a packet").Base(err)
	}
	return nil
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
