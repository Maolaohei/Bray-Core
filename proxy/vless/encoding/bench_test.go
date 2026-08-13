package encoding_test

import (
	"io"
	"testing"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/uuid"
	"github.com/xtls/xray-core/proxy/vless"
	. "github.com/xtls/xray-core/proxy/vless/encoding"
)

// BenchmarkEncodeRequestHeader measures the full outbound header path the
// way outbound.go builds it: pooled Addons + pooled anti-replay seed per
// request. Remaining allocations are framework-level: the BufferedWriter
// lazily creates its buffer and the header's StackNew buffer escapes via
// the &buffer callee calls — both also present on the real path. The
// outbound-side Addons/Seed allocations were removed by pooling; the
// marshal temporary and the parser interface allocations were removed by
// the fixed-layout fast path and writeAddressPortFast.
func BenchmarkEncodeRequestHeader(b *testing.B) {
	user := &protocol.MemoryUser{
		Account: &vless.MemoryAccount{ID: protocol.NewID(uuid.New())},
	}
	request := &protocol.RequestHeader{
		Version: Version,
		User:    user,
		Command: protocol.RequestCommandTCP,
		Address: net.DomainAddress("example.com"),
		Port:    net.Port(443),
	}
	writer := buf.NewBufferedWriter(buf.NewWriter(io.Discard))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Mirrors outbound.go: pooled Addons, fresh NewSeed (its 8-byte
		// slice has cap < 32, so PutSeed short-circuits without boxing),
		// returned once the header is marshalled.
		addons := GetAddons()
		addons.Flow = "xtls-rprx-vision"
		addons.Seed = NewSeed()
		if err := EncodeRequestHeader(writer, request, addons); err != nil {
			b.Fatal(err)
		}
		PutAddons(addons)
		if err := writer.Flush(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeHeaderAddons(b *testing.B) {
	buffer := buf.StackNew()
	defer buffer.Release()
	addons := &Addons{Flow: "xtls-rprx-vision"}
	_ = EncodeHeaderAddons(&buffer, addons)
	encoded := make([]byte, len(buffer.Bytes()))
	copy(encoded, buffer.Bytes())

	reuseBuf := buf.New()
	defer reuseBuf.Release()
	reuseAddons := &Addons{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := &bufferReader{data: encoded}
		reuseBuf.Clear()
		_ = DecodeHeaderAddons(reuseBuf, reader, reuseAddons)
	}
}

func BenchmarkDecodeHeaderAddonsParallel(b *testing.B) {
	buffer := buf.StackNew()
	defer buffer.Release()
	addons := &Addons{Flow: "xtls-rprx-vision"}
	_ = EncodeHeaderAddons(&buffer, addons)
	encoded := make([]byte, len(buffer.Bytes()))
	copy(encoded, buffer.Bytes())

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			reader := &bufferReader{data: encoded}
			out := buf.New()
			a := &Addons{}
			_ = DecodeHeaderAddons(out, reader, a)
			out.Release()
		}
	})
}

type bufferReader struct {
	data []byte
	pos  int
}

func (r *bufferReader) Read(p []byte) (int, error) {
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
