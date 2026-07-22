package reality

import (
	"crypto/x509"
	"reflect"
	"testing"

	utls "github.com/refraction-networking/utls"
)

// VerifyPeerCertificate reads the peer certificate chain out of the embedded
// utls.Conn via reflect + unsafe pointer arithmetic (see reality.go). That trick
// silently breaks if utls renames/retypes/reorders the unexported
// `peerCertificates` field: the offset would point at the wrong bytes and the
// HMAC/ML-DSA auth path would read garbage. This test pins the field's presence
// and type so a utls bump fails here first, loudly, instead of in production.
//
// The production code reflects on c.Conn, which is the *utls.Conn embedded
// through utls.UConn, so we assert against that exact type.
func TestUTLSConnPeerCertificatesFieldStable(t *testing.T) {
	typ := reflect.TypeOf((*utls.Conn)(nil)).Elem()

	f, ok := typ.FieldByName("peerCertificates")
	if !ok {
		t.Fatal("utls.Conn.peerCertificates field not found: " +
			"REALITY VerifyPeerCertificate's reflect/unsafe read is broken; " +
			"update the field access in reality.go for the new utls layout")
	}

	want := reflect.TypeOf([]*x509.Certificate(nil))
	if f.Type != want {
		t.Fatalf("utls.Conn.peerCertificates type=%v want=%v: "+
			"the unsafe cast in VerifyPeerCertificate assumes []*x509.Certificate", f.Type, want)
	}
}
