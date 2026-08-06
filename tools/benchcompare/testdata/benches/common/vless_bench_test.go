// Copyright 2026 Bray-Core. Harness-owned benchmark source.
//
// Injected by tools/benchcompare into every target worktree. Only uses VLESS
// encoding APIs whose signatures are identical in Bray-Core and upstream
// Xray-core (EncodeRequestHeader / EncodeResponseHeader / DecodeResponseHeader),
// so the same code runs on both sides. Bray-specific addons marshal paths are
// covered by the repo's own bench_test.go (bray-only scenarios).
package encoding_test

import (
	"bytes"
	"testing"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/proxy/vless"
	. "github.com/xtls/xray-core/proxy/vless/encoding"
)

func benchcmpVlessRequest() *protocol.RequestHeader {
	return &protocol.RequestHeader{
		Version: Version, // encoding.Version (same in both forks)
		User: &protocol.MemoryUser{
			Account: &vless.MemoryAccount{ID: &protocol.ID{}},
		},
		Command: protocol.RequestCommandTCP,
		Address: net.LocalHostIP,
		Port:    net.Port(443),
	}
}

func BenchmarkVless_EncodeRequestHeader(b *testing.B) {
	req := benchcmpVlessRequest()
	addons := &Addons{Flow: vless.XRV}
	var out bytes.Buffer

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out.Reset()
		if err := EncodeRequestHeader(&out, req, addons); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVless_EncodeResponseHeader(b *testing.B) {
	req := benchcmpVlessRequest()
	addons := &Addons{Flow: vless.XRV}
	var out bytes.Buffer

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out.Reset()
		if err := EncodeResponseHeader(&out, req, addons); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVless_DecodeResponseHeader(b *testing.B) {
	req := benchcmpVlessRequest()
	var encoded bytes.Buffer
	if err := EncodeResponseHeader(&encoded, req, &Addons{Flow: vless.XRV}); err != nil {
		b.Fatal(err)
	}
	data := encoded.Bytes()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(data)
		if _, err := DecodeResponseHeader(reader, req); err != nil {
			b.Fatal(err)
		}
	}
}
