package encoding_test

import (
	"testing"

	"github.com/xtls/xray-core/common/buf"
	. "github.com/xtls/xray-core/proxy/vless/encoding"
)

func BenchmarkMarshalAddons_Vision(b *testing.B) {
	addons := &Addons{Flow: "xtls-rprx-vision"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = MarshalAddons(addons)
	}
}

func BenchmarkMarshalAddons_Empty(b *testing.B) {
	addons := &Addons{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = MarshalAddons(addons)
	}
}

func BenchmarkMarshalAddons_WithSeed(b *testing.B) {
	addons := &Addons{Flow: "xtls-rprx-vision", Seed: make([]byte, 32)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = MarshalAddons(addons)
	}
}

func BenchmarkDecodeHeaderAddons(b *testing.B) {
	buffer := buf.StackNew()
	defer buffer.Release()
	addons := &Addons{Flow: "xtls-rprx-vision"}
	_ = EncodeHeaderAddons(&buffer, addons)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := &bufferReader{data: buffer.Bytes()}
		_, _ = DecodeHeaderAddons(&buf.Buffer{}, reader)
	}
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
