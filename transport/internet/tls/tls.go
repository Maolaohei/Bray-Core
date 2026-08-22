package tls

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"math/big"
	"slices"
	"time"

	utls "github.com/refraction-networking/utls"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/utils"
)

type Interface interface {
	net.Conn
	HandshakeContext(ctx context.Context) error
	VerifyHostname(host string) error
	HandshakeContextServerName(ctx context.Context) string
	NegotiatedProtocol() string
}

var (
	_ buf.Writer = (*Conn)(nil)
	_ Interface  = (*Conn)(nil)
)

type Conn struct {
	*tls.Conn
}

const tlsCloseTimeout = 250 * time.Millisecond

func (c *Conn) Close() error {
	timer := time.AfterFunc(tlsCloseTimeout, func() {
		c.Conn.NetConn().Close()
	})
	defer timer.Stop()
	return c.Conn.Close()
}

func (c *Conn) WriteMultiBuffer(mb buf.MultiBuffer) error {
	mb = buf.Compact(mb)
	mb, err := buf.WriteMultiBuffer(c, mb)
	buf.ReleaseMulti(mb)
	return err
}

func (c *Conn) HandshakeContextServerName(ctx context.Context) string {
	if err := c.HandshakeContext(ctx); err != nil {
		return ""
	}
	return c.ConnectionState().ServerName
}

func (c *Conn) NegotiatedProtocol() string {
	state := c.ConnectionState()
	return state.NegotiatedProtocol
}

// Client initiates a TLS client handshake on the given connection.
func Client(c net.Conn, config *tls.Config) net.Conn {
	tlsConn := tls.Client(c, config)
	return &Conn{Conn: tlsConn}
}

// Server initiates a TLS server handshake on the given connection.
func Server(c net.Conn, config *tls.Config) net.Conn {
	tlsConn := tls.Server(c, config)
	return &Conn{Conn: tlsConn}
}

type UConn struct {
	*utls.UConn
}

var _ Interface = (*UConn)(nil)

func (c *UConn) Close() error {
	timer := time.AfterFunc(tlsCloseTimeout, func() {
		c.Conn.NetConn().Close()
	})
	defer timer.Stop()
	return c.Conn.Close()
}

func (c *UConn) HandshakeContextServerName(ctx context.Context) string {
	if err := c.HandshakeContext(ctx); err != nil {
		return ""
	}
	return c.ConnectionState().ServerName
}

// WebsocketHandshakeContext basically calls UConn.Handshake inside it but it will try
// to build outer ALPN to `http/1.1` or `h2 http/1.1` (if manually specified for camouflage)
func (c *UConn) WebsocketHandshakeContext(ctx context.Context) error {
	config := *utils.AccessField[*utls.Config](c, "config")
	ALPN := slices.Clone(config.NextProtos)
	// set other kinds of ALPN to http/1.1
	if !slices.Equal(ALPN, []string{"h2", "http/1.1"}) {
		ALPN = []string{"http/1.1"}
	}
	// Build the handshake state. This will apply every variable of the TLS of the
	// fingerprint in the UConn
	if err := c.BuildHandshakeState(); err != nil {
		return err
	}
	// Do not modify outer ALPN if ECH is used
	// Outer ALPN will be h2,http/1.1, and real http/1.1 in config will be hidden in ECH
	if config.EncryptedClientHelloConfigList != nil {
		config.NextProtos = []string{"http/1.1"}
		return c.HandshakeContext(ctx)
	}
	// Iterate over extensions and check for utls.ALPNExtension
	hasALPNExtension := false
	for _, extension := range c.Extensions {
		if alpn, ok := extension.(*utls.ALPNExtension); ok {
			hasALPNExtension = true
			alpn.AlpnProtocols = ALPN
			break
		}
	}
	if !hasALPNExtension { // Append extension if doesn't exists
		c.Extensions = append(c.Extensions, &utls.ALPNExtension{AlpnProtocols: ALPN})
	}
	// Rebuild the client hello and do the handshake
	if err := c.BuildHandshakeState(); err != nil {
		return err
	}
	return c.HandshakeContext(ctx)
}

func (c *UConn) NegotiatedProtocol() string {
	state := c.ConnectionState()
	return state.NegotiatedProtocol
}

// enableUTLSResumption re-expresses a fingerprinted ClientHelloID as an
// equivalent custom spec plus a trailing placeholder PSK extension. Browser
// presets like HelloChrome_133 ship &SessionTicketExtension{} but NO
// PreSharedKeyExtension, yet TLS 1.3 resumption requires one: uLoadSession ->
// initPskExt finds pskExtension == nil and silently skips loading the cached
// session (skipResumptionOnNilExtension defaults to true for preset IDs), so
// every dial paid a full handshake even with a ClientSessionCache configured.
//
// The injection must happen at the SPEC level: uconn.Extensions is rebuilt
// from the spec on every BuildHandshakeState (ApplyPreset copies p.Extensions),
// so appending to it before the handshake is wiped. With HelloCustom + the
// patched spec the marshaled ClientHello is identical to the stock fingerprint
// for full handshakes — an uninitialized UtlsPreSharedKeyExtension has
// Len()==0 and emits no bytes — while resumed handshakes carry PSK
// identity+binder exactly like a real browser.
func enableUTLSResumption(id *utls.ClientHelloID) (*utls.ClientHelloSpec, bool) {
	if id == nil {
		return nil, false
	}
	spec, err := utls.UTLSIdToSpec(*id)
	if err != nil {
		return nil, false
	}
	// A utls invariant requires the PSK extension to be the LAST entry.
	spec.Extensions = append(spec.Extensions, &utls.UtlsPreSharedKeyExtension{})
	return &spec, true
}

func newUConn(c net.Conn, config *tls.Config, fingerprint *utls.ClientHelloID) *utls.UConn {
	cfg := copyConfig(config)
	if cfg.ClientSessionCache == nil || fingerprint == nil {
		return utls.UClient(c, cfg, *fingerprint)
	}
	spec, ok := enableUTLSResumption(fingerprint)
	if !ok {
		return utls.UClient(c, cfg, *fingerprint)
	}
	uconn := utls.UClient(c, cfg, utls.HelloCustom)
	// ApplyPreset errors only if the spec itself is malformed; we built it
	// from a valid fingerprint, so ignore the error and keep the conn.
	_ = uconn.ApplyPreset(spec)
	return uconn
}

func UClient(c net.Conn, config *tls.Config, fingerprint *utls.ClientHelloID) net.Conn {
	return &UConn{UConn: newUConn(c, config, fingerprint)}
}

func GeneraticUClient(c net.Conn, config *tls.Config) *utls.UConn {
	return newUConn(c, config, &utls.HelloChrome_Auto)
}

func convertCurvePreferences(curves []tls.CurveID) []utls.CurveID {
	if curves == nil {
		return nil
	}
	out := make([]utls.CurveID, len(curves))
	for i, v := range curves {
		out[i] = utls.CurveID(v)
	}
	return out
}

// uGlobalSessionCache backs TLS session resumption for uTLS (fingerprinted)
// connections. The stdlib path already resumes via globalSessionCache in
// tls/config.go; without an equivalent here, every uTLS dial paid a full
// handshake — both a latency cost (extra RTT + cert-chain work on XMUX pool
// rotation / pre-connect dials) and a fingerprint anomaly: real browsers
// resume sessions aggressively, so never-resuming uTLS conns deviate from
// the Chrome baseline they imitate. VerifyPeerCertificate is still invoked
// by utls on resumed connections (with the session's peer certificates), so
// pinned-CA / verifyPeerCertByName checks keep working.
var uGlobalSessionCache = utls.NewLRUClientSessionCache(1024)

func copyConfig(c *tls.Config) *utls.Config {
	config := &utls.Config{
		Rand:                           c.Rand,
		Time:                           c.Time,
		RootCAs:                        c.RootCAs,
		ServerName:                     c.ServerName,
		InsecureSkipVerify:             c.InsecureSkipVerify,
		VerifyPeerCertificate:          c.VerifyPeerCertificate,
		KeyLogWriter:                   c.KeyLogWriter,
		EncryptedClientHelloConfigList: c.EncryptedClientHelloConfigList,
		NextProtos:                     c.NextProtos,
		MinVersion:                     c.MinVersion,
		MaxVersion:                     c.MaxVersion,
		SessionTicketsDisabled:         c.SessionTicketsDisabled,
		// Conceal the (still-empty) placeholder PSK extension on full
		// handshakes; utls rejects an empty PSK outright without this.
		OmitEmptyPsk: true,
		// CipherSuites: used by HelloGolang / custom specs; TLS 1.3 suites remain non-configurable in utls.
		// Enables anti-NIN / restricted cipher lists when fingerprint is not pure browser presets.
		CipherSuites:     append([]uint16(nil), c.CipherSuites...),
		CurvePreferences: convertCurvePreferences(c.CurvePreferences),
	}
	if c.ClientSessionCache != nil {
		config.ClientSessionCache = uGlobalSessionCache
	}
	return config
}

func init() {
	weights := utls.DefaultWeights
	weights.TLSVersMax_Set_VersionTLS13 = 1
	weights.FirstKeyShare_Set_CurveP256 = 0
	randomized := utls.HelloRandomizedALPN
	randomized.Seed, _ = utls.NewPRNGSeed()
	randomized.Weights = &weights
	randomizednoalpn := utls.HelloRandomizedNoALPN
	randomizednoalpn.Seed, _ = utls.NewPRNGSeed()
	randomizednoalpn.Weights = &weights
	PresetFingerprints["randomized"] = &randomized
	PresetFingerprints["randomizednoalpn"] = &randomizednoalpn
}

func GetFingerprint(name string) (fingerprint *utls.ClientHelloID) {
	if name == "" {
		return &utls.HelloChrome_Auto
	}
	if name == "random" {
		keys := make([]string, 0, len(ModernFingerprints))
		for k := range ModernFingerprints {
			keys = append(keys, k)
		}
		bigInt, _ := rand.Int(rand.Reader, big.NewInt(int64(len(keys))))
		return ModernFingerprints[keys[bigInt.Int64()]]
	}
	if fingerprint = PresetFingerprints[name]; fingerprint != nil {
		return
	}
	if fingerprint = ModernFingerprints[name]; fingerprint != nil {
		return
	}
	if fingerprint = OtherFingerprints[name]; fingerprint != nil {
		return
	}
	return
}

var PresetFingerprints = map[string]*utls.ClientHelloID{
	// Recommended preset options in GUI clients
	"chrome":           &utls.HelloChrome_Auto,
	"firefox":          &utls.HelloFirefox_Auto,
	"safari":           &utls.HelloSafari_Auto,
	"ios":              &utls.HelloIOS_Auto,
	"android":          &utls.HelloAndroid_11_OkHttp,
	"edge":             &utls.HelloEdge_Auto,
	"360":              &utls.Hello360_Auto,
	"qq":               &utls.HelloQQ_Auto,
	"random":           nil,
	"randomized":       nil,
	"randomizednoalpn": nil,
	"unsafe":           nil,
}

var ModernFingerprints = map[string]*utls.ClientHelloID{
	// One of these will be chosen as `random` at startup
	"hellofirefox_120": &utls.HelloFirefox_120,
	"hellofirefox_148": &utls.HelloFirefox_148,
	"hellochrome_120":  &utls.HelloChrome_120,
	"hellochrome_131":  &utls.HelloChrome_131,
	"hellochrome_133":  &utls.HelloChrome_133,
	"helloios_13":      &utls.HelloIOS_13,
	"helloios_14":      &utls.HelloIOS_14,
	"helloedge_106":    &utls.HelloEdge_106,
	"hellosafari_26_3": &utls.HelloSafari_26_3,
	"hello360_11_0":    &utls.Hello360_11_0,
	"helloqq_11_1":     &utls.HelloQQ_11_1,
}

var OtherFingerprints = map[string]*utls.ClientHelloID{
	// Golang, randomized, auto, and fingerprints that are too old
	"hellogolang":             &utls.HelloGolang,
	"hellorandomized":         &utls.HelloRandomized,
	"hellorandomizedalpn":     &utls.HelloRandomizedALPN,
	"hellorandomizednoalpn":   &utls.HelloRandomizedNoALPN,
	"hellofirefox_auto":       &utls.HelloFirefox_Auto,
	"hellofirefox_55":         &utls.HelloFirefox_55,
	"hellofirefox_56":         &utls.HelloFirefox_56,
	"hellofirefox_63":         &utls.HelloFirefox_63,
	"hellofirefox_65":         &utls.HelloFirefox_65,
	"hellofirefox_99":         &utls.HelloFirefox_99,
	"hellofirefox_102":        &utls.HelloFirefox_102,
	"hellofirefox_105":        &utls.HelloFirefox_105,
	"hellochrome_auto":        &utls.HelloChrome_Auto,
	"hellochrome_58":          &utls.HelloChrome_58,
	"hellochrome_62":          &utls.HelloChrome_62,
	"hellochrome_70":          &utls.HelloChrome_70,
	"hellochrome_72":          &utls.HelloChrome_72,
	"hellochrome_83":          &utls.HelloChrome_83,
	"hellochrome_87":          &utls.HelloChrome_87,
	"hellochrome_96":          &utls.HelloChrome_96,
	"hellochrome_100":         &utls.HelloChrome_100,
	"hellochrome_102":         &utls.HelloChrome_102,
	"hellochrome_106_shuffle": &utls.HelloChrome_106_Shuffle,
	"helloios_auto":           &utls.HelloIOS_Auto,
	"helloios_11_1":           &utls.HelloIOS_11_1,
	"helloios_12_1":           &utls.HelloIOS_12_1,
	"helloandroid_11_okhttp":  &utls.HelloAndroid_11_OkHttp,
	"helloedge_85":            &utls.HelloEdge_85,
	"helloedge_auto":          &utls.HelloEdge_Auto,
	"hellosafari_16_0":        &utls.HelloSafari_16_0,
	"hellosafari_auto":        &utls.HelloSafari_Auto,
	"hello360_auto":           &utls.Hello360_Auto,
	"hello360_7_5":            &utls.Hello360_7_5,
	"helloqq_auto":            &utls.HelloQQ_Auto,

	// Chrome betas
	"hellochrome_100_psk":              &utls.HelloChrome_100_PSK,
	"hellochrome_112_psk_shuf":         &utls.HelloChrome_112_PSK_Shuf,
	"hellochrome_114_padding_psk_shuf": &utls.HelloChrome_114_Padding_PSK_Shuf,
	"hellochrome_115_pq":               &utls.HelloChrome_115_PQ,
	"hellochrome_115_pq_psk":           &utls.HelloChrome_115_PQ_PSK,
	"hellochrome_120_pq":               &utls.HelloChrome_120_PQ,
}
