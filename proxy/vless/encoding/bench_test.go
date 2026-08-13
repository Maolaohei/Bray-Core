package encoding_test

import (
	"testing"

	"github.com/xtls/xray-core/common/buf"
	. "github.com/xtls/xray-core/proxy/vless/encoding"
)

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
