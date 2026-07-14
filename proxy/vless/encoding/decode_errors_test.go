package encoding_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/uuid"
	"github.com/xtls/xray-core/proxy/vless"
	. "github.com/xtls/xray-core/proxy/vless/encoding"
)

func testUser() *protocol.MemoryUser {
	id := uuid.New()
	user := &protocol.MemoryUser{
		Level: 0,
		Email: "green@example.com",
	}
	acc, err := (&vless.Account{Id: id.String()}).AsAccount()
	common.Must(err)
	user.Account = acc
	return user
}

func assertNoUUIDEcho(t *testing.T, err error, candidate uuid.UUID) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "invalid request user id") {
		t.Fatalf("err=%v", err)
	}
	// Green-zone: must not use the old "invalid request user id: <uuid>" form.
	if strings.Contains(msg, "invalid request user id:") {
		t.Fatalf("error still embeds user id after colon: %v", err)
	}
	if strings.Contains(msg, candidate.String()) {
		t.Fatalf("error embeds candidate UUID: %v", err)
	}
}

func TestDecodeRequestHeader_InvalidVersion_FailFast(t *testing.T) {
	v := new(vless.MemoryValidator)
	// Only one byte version=1; decoder must fail without requiring more data.
	r := bytes.NewReader([]byte{1})
	_, _, _, _, err := DecodeRequestHeader(context.Background(), false, nil, r, v)
	if err == nil {
		t.Fatal("expected invalid version")
	}
	if !strings.Contains(err.Error(), "invalid request version") {
		t.Fatalf("err=%v", err)
	}
}

func TestDecodeRequestHeader_InvalidUser_NoUUIDEcho(t *testing.T) {
	v := new(vless.MemoryValidator)
	// version 0 + fixed 16-byte id not in validator
	var id uuid.UUID
	for i := 0; i < 16; i++ {
		id[i] = byte(i + 1)
	}
	payload := make([]byte, 1+16)
	payload[0] = 0
	copy(payload[1:], id.Bytes())

	_, _, _, _, err := DecodeRequestHeader(context.Background(), false, nil, bytes.NewReader(payload), v)
	assertNoUUIDEcho(t, err, id)
}

func TestDecodeRequestHeader_InvalidUser_NoUUIDEcho_Isfb(t *testing.T) {
	v := new(vless.MemoryValidator)
	var id uuid.UUID
	for i := 0; i < 16; i++ {
		id[i] = byte(0xa0 + i)
	}
	first := buf.New()
	defer first.Release()
	common.Must(first.WriteByte(0))
	common.Must2(first.Write(id.Bytes()))

	_, _, _, _, err := DecodeRequestHeader(context.Background(), true, first, bytes.NewReader(nil), v)
	assertNoUUIDEcho(t, err, id)
}

func TestDecodeRequestHeader_InvalidCommandAddress(t *testing.T) {
	user := testUser()
	v := new(vless.MemoryValidator)
	common.Must(v.Add(user))

	// Craft: ver + uuid + addons(0) + command(100) without address
	id := user.Account.(*vless.MemoryAccount).ID.Bytes()
	var b bytes.Buffer
	b.WriteByte(0)
	b.Write(id)
	b.WriteByte(0)   // empty addons
	b.WriteByte(100) // invalid command
	_, _, _, _, err := DecodeRequestHeader(context.Background(), false, nil, &b, v)
	if err == nil {
		t.Fatal("expected invalid address/command")
	}
	if !strings.Contains(err.Error(), "invalid request address") {
		t.Fatalf("err=%v", err)
	}
}

func TestDecodeRequestHeader_TCPRoundTrip(t *testing.T) {
	user := testUser()
	v := new(vless.MemoryValidator)
	common.Must(v.Add(user))

	req := &protocol.RequestHeader{
		Version: Version,
		User:    user,
		Command: protocol.RequestCommandTCP,
		Address: net.DomainAddress("www.example.com"),
		Port:    net.Port(443),
	}
	addons := &Addons{}
	buffer := buf.StackNew()
	defer buffer.Release()
	common.Must(EncodeRequestHeader(&buffer, req, addons))

	_, got, gotAddons, _, err := DecodeRequestHeader(context.Background(), false, nil, &buffer, v)
	common.Must(err)
	if got.Address.String() != "www.example.com" || got.Port != 443 {
		t.Fatalf("got %v:%v", got.Address, got.Port)
	}
	if gotAddons == nil {
		t.Fatal("nil addons")
	}
}
