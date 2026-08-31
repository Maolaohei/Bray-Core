package outbound

import (
	"testing"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
)

// TestProcess_InvalidTargetNoPanic is a POC for B8: a malformed outbound session
// whose Target Destination is invalid AND has a nil Address used to be
// dereferenced by `ob.Target.Address.String()` (the `&&` short-circuits into the
// right operand only when !IsValid()) and panicked with a nil-pointer
// dereference instead of returning the "target not specified" error.
//
// It also guards the two legitimate cases that must keep working:
//   - the reverse-proxy sentinel target (Address "v1.rvs.cool", Network_Unknown
//     by design) must be allowed through;
//   - a normal valid target must pass untouched.
func TestProcess_InvalidTargetNoPanic(t *testing.T) {
	// Malformed: Network_Unknown + nil Address. Pre-fix this dereferenced
	// ob.Target.Address.String() and panicked; post-fix it returns the error.
	bad := &session.Outbound{Target: net.Destination{}}
	if err := validateOutboundTarget(bad); err == nil {
		t.Fatal("expected 'target not specified' error for invalid nil-address target")
	}

	// Reverse-proxy sentinel: invalid by IsValid() (Network_Unknown) but a
	// valid Address; must be allowed through (no error).
	rvs := &session.Outbound{Target: net.Destination{Address: net.DomainAddress("v1.rvs.cool")}}
	if err := validateOutboundTarget(rvs); err != nil {
		t.Fatalf("reverse sentinel target wrongly rejected: %v", err)
	}

	// Normal valid target: must pass.
	valid := &session.Outbound{Target: net.TCPDestination(net.DomainAddress("example.com"), 443)}
	if err := validateOutboundTarget(valid); err != nil {
		t.Fatalf("valid target wrongly rejected: %v", err)
	}
}
