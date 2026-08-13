package encoding_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/uuid"
	"github.com/xtls/xray-core/proxy/vless"
	. "github.com/xtls/xray-core/proxy/vless/encoding"
)

func toAccount(a *vless.Account) protocol.Account {
	account, err := a.AsAccount()
	common.Must(err)
	return account
}

func TestRequestSerialization(t *testing.T) {
	user := &protocol.MemoryUser{
		Level: 0,
		Email: "test@example.com",
	}
	id := uuid.New()
	account := &vless.Account{
		Id: id.String(),
	}
	user.Account = toAccount(account)

	expectedRequest := &protocol.RequestHeader{
		Version: Version,
		User:    user,
		Command: protocol.RequestCommandTCP,
		Address: net.DomainAddress("www.example.com"),
		Port:    net.Port(443),
	}
	expectedAddons := &Addons{}

	buffer := buf.StackNew()
	common.Must(EncodeRequestHeader(&buffer, expectedRequest, expectedAddons))

	Validator := new(vless.MemoryValidator)
	Validator.Add(user)

	_, actualRequest, actualAddons, _, err := DecodeRequestHeader(context.Background(), false, nil, &buffer, Validator)
	common.Must(err)

	if r := cmp.Diff(actualRequest, expectedRequest, cmp.AllowUnexported(protocol.ID{})); r != "" {
		t.Error(r)
	}

	addonsComparer := func(x, y *Addons) bool {
		return (x.Flow == y.Flow) && cmp.Equal(x.Seed, y.Seed)
	}
	if r := cmp.Diff(actualAddons, expectedAddons, cmp.Comparer(addonsComparer)); r != "" {
		t.Error(r)
	}
}

// TestAddonsSeedRoundTrip pins the XRV+8-byte-seed fixed-layout fast path
// (28-byte wire, 0x0a 0x10 <flow16> 0x12 0x08 <seed8>) against the decoder.
// This was the regression that slipped through the header round-trips
// above, which only exercise empty addons: the length prefix was written
// as 26 and the decoder truncated the seed block.
func TestAddonsSeedRoundTrip(t *testing.T) {
	id := uuid.New()
	user := &protocol.MemoryUser{Account: toAccount(&vless.Account{Id: id.String()})}
	validator := new(vless.MemoryValidator)
	validator.Add(user)

	seed := make([]byte, 8)
	for i := range seed {
		seed[i] = byte(i + 1)
	}
	expected := &protocol.RequestHeader{
		Version: Version,
		User:    user,
		Command: protocol.RequestCommandTCP,
		Address: net.DomainAddress("www.example.com"),
		Port:    net.Port(443),
	}
	addons := &Addons{Flow: "xtls-rprx-vision", Seed: seed}

	buffer := buf.StackNew()
	common.Must(EncodeRequestHeader(&buffer, expected, addons))
	_, actual, actualAddons, _, err := DecodeRequestHeader(context.Background(), false, nil, &buffer, validator)
	common.Must(err)
	if r := cmp.Diff(actual, expected, cmp.AllowUnexported(protocol.ID{})); r != "" {
		t.Error(r)
	}
	if actualAddons.Flow != addons.Flow || !cmp.Equal(actualAddons.Seed, seed) {
		t.Errorf("addons mismatch: flow=%q seed=%x want flow=%q seed=%x",
			actualAddons.Flow, actualAddons.Seed, addons.Flow, seed)
	}
}

// TestAddressSerializationRoundTrip pins the hand-rolled
// writeAddressPortFast encoder against the parser-based decoder for all
// three address families, so a wire-format drift cannot slip in silently.
func TestAddressSerializationRoundTrip(t *testing.T) {
	id := uuid.New()
	user := &protocol.MemoryUser{Account: toAccount(&vless.Account{Id: id.String()})}
	validator := new(vless.MemoryValidator)
	validator.Add(user)

	cases := []struct {
		name string
		addr net.Address
	}{
		{"domain", net.DomainAddress("www.example.com")},
		{"ipv4", net.ParseAddress("192.168.1.1")},
		{"ipv6", net.ParseAddress("2001:4860:4860::8888")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expected := &protocol.RequestHeader{
				Version: Version,
				User:    user,
				Command: protocol.RequestCommandTCP,
				Address: tc.addr,
				Port:    net.Port(8443),
			}
			buffer := buf.StackNew()
			common.Must(EncodeRequestHeader(&buffer, expected, &Addons{}))
			_, actual, _, _, err := DecodeRequestHeader(context.Background(), false, nil, &buffer, validator)
			common.Must(err)
			if r := cmp.Diff(actual, expected, cmp.AllowUnexported(protocol.ID{})); r != "" {
				t.Error(r)
			}
		})
	}
}

func TestInvalidRequest(t *testing.T) {
	user := &protocol.MemoryUser{
		Level: 0,
		Email: "test@example.com",
	}
	id := uuid.New()
	account := &vless.Account{
		Id: id.String(),
	}
	user.Account = toAccount(account)

	expectedRequest := &protocol.RequestHeader{
		Version: Version,
		User:    user,
		Command: protocol.RequestCommand(100),
		Address: net.DomainAddress("www.example.com"),
		Port:    net.Port(443),
	}
	expectedAddons := &Addons{}

	buffer := buf.StackNew()
	common.Must(EncodeRequestHeader(&buffer, expectedRequest, expectedAddons))

	Validator := new(vless.MemoryValidator)
	Validator.Add(user)

	_, _, _, _, err := DecodeRequestHeader(context.Background(), false, nil, &buffer, Validator)
	if err == nil {
		t.Error("nil error")
	}
}

func TestMuxRequest(t *testing.T) {
	user := &protocol.MemoryUser{
		Level: 0,
		Email: "test@example.com",
	}
	id := uuid.New()
	account := &vless.Account{
		Id: id.String(),
	}
	user.Account = toAccount(account)

	expectedRequest := &protocol.RequestHeader{
		Version: Version,
		User:    user,
		Command: protocol.RequestCommandMux,
		Address: net.DomainAddress("v1.mux.cool"),
	}
	expectedAddons := &Addons{}

	buffer := buf.StackNew()
	common.Must(EncodeRequestHeader(&buffer, expectedRequest, expectedAddons))

	Validator := new(vless.MemoryValidator)
	Validator.Add(user)

	_, actualRequest, actualAddons, _, err := DecodeRequestHeader(context.Background(), false, nil, &buffer, Validator)
	common.Must(err)

	if r := cmp.Diff(actualRequest, expectedRequest, cmp.AllowUnexported(protocol.ID{})); r != "" {
		t.Error(r)
	}

	addonsComparer := func(x, y *Addons) bool {
		return (x.Flow == y.Flow) && cmp.Equal(x.Seed, y.Seed)
	}
	if r := cmp.Diff(actualAddons, expectedAddons, cmp.Comparer(addonsComparer)); r != "" {
		t.Error(r)
	}
}

// TestDecodeResponseHeaderLargeAddons guards the stack-buffer fast path in
// DecodeResponseHeader: addons blobs longer than the 64-byte stack array must
// fall back to heap allocation, not slice out of range (regression: CI
// TestVless EOF after a length>64 response addons panicked the server).
func TestDecodeResponseHeaderLargeAddons(t *testing.T) {
	req := &protocol.RequestHeader{Version: Version}
	var encoded bytes.Buffer
	addons := &Addons{Flow: vless.XRV, Seed: make([]byte, 80)}
	common.Must(EncodeResponseHeader(&encoded, req, addons))

	decoded, err := DecodeResponseHeader(bytes.NewReader(encoded.Bytes()), req)
	common.Must(err)
	if !bytes.Equal(decoded.Seed, addons.Seed) {
		t.Fatalf("seed mismatch: got %d bytes, want 80", len(decoded.Seed))
	}
	PutAddons(decoded)
}
