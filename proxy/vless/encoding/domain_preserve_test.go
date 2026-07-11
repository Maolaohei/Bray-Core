package encoding_test

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/uuid"
	"github.com/xtls/xray-core/proxy/vless"
	. "github.com/xtls/xray-core/proxy/vless/encoding"
)

// TestEncodeRequestPreservesDomainName verifies that a domain target is written
// as a domain address in the VLESS header, not as a bare IP. This is critical
// for avoiding cross-host cert mismatches when CDN domains share an IP.
func TestEncodeRequestPreservesDomainName(t *testing.T) {
	id := uuid.New()
	user := &protocol.MemoryUser{
		Level:   0,
		Email:   "test@example.com",
		Account: common.Must2((&vless.Account{Id: id.String()}).AsAccount()),
	}

	// Simulate client path: resolved IP + preserved domain.
	domain := "github.com"
	request := &protocol.RequestHeader{
		Version: Version,
		User:    user,
		Command: protocol.RequestCommandTCP,
		Address: net.DomainAddress(domain),
		Port:    net.Port(443),
	}

	buffer := buf.StackNew()
	defer buffer.Release()
	common.Must(EncodeRequestHeader(&buffer, request, &Addons{}))

	validator := new(vless.MemoryValidator)
	common.Must(validator.Add(user))

	_, actual, _, _, err := DecodeRequestHeader(context.Background(), false, nil, &buffer, validator)
	common.Must(err)

	if !actual.Address.Family().IsDomain() {
		t.Fatalf("expected domain address, got family=%v addr=%v", actual.Address.Family(), actual.Address)
	}
	if actual.Address.Domain() != domain {
		t.Fatalf("expected domain %q, got %q", domain, actual.Address.Domain())
	}
	if actual.Port != 443 {
		t.Fatalf("expected port 443, got %d", actual.Port)
	}
}

// TestEncodeRequestIPWithoutDomainKeepsIP verifies bare IP destinations still
// encode as IP (compatibility for IP-literal destinations).
func TestEncodeRequestIPWithoutDomainKeepsIP(t *testing.T) {
	id := uuid.New()
	user := &protocol.MemoryUser{
		Level:   0,
		Email:   "test@example.com",
		Account: common.Must2((&vless.Account{Id: id.String()}).AsAccount()),
	}

	ip := net.IPAddress([]byte{20, 205, 243, 166})
	request := &protocol.RequestHeader{
		Version: Version,
		User:    user,
		Command: protocol.RequestCommandTCP,
		Address: ip,
		Port:    net.Port(443),
	}

	buffer := buf.StackNew()
	defer buffer.Release()
	common.Must(EncodeRequestHeader(&buffer, request, &Addons{}))

	validator := new(vless.MemoryValidator)
	common.Must(validator.Add(user))

	_, actual, _, _, err := DecodeRequestHeader(context.Background(), false, nil, &buffer, validator)
	common.Must(err)

	if !actual.Address.Family().IsIP() {
		t.Fatalf("expected IP address, got family=%v addr=%v", actual.Address.Family(), actual.Address)
	}
}
