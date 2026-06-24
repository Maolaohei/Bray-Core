package reality

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	gotls "crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	mrand "math/rand"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/cloudflare/circl/sign/mldsa/mldsa65"
	utls "github.com/refraction-networking/utls"
	"github.com/Maolaohei/REALITY"
	"github.com/xtls/xray-core/common/crypto"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/utils"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/transport/internet/tls"
	"golang.org/x/crypto/hkdf"
	"golang.org/x/net/http2"
)

type Conn struct {
	*reality.Conn
}

func (c *Conn) HandshakeAddress() net.Address {
	if err := c.Handshake(); err != nil {
		return nil
	}
	state := c.ConnectionState()
	if state.ServerName == "" {
		return nil
	}
	return net.ParseAddress(state.ServerName)
}

func Server(c net.Conn, config *reality.Config) (net.Conn, error) {
	realityConn, err := reality.Server(context.Background(), c, config)
	return &Conn{Conn: realityConn}, err
}

type UConn struct {
	*utls.UConn
	Config     *Config
	ServerName string
	AuthKey    []byte
	Verified   bool
}

func (c *UConn) HandshakeAddress() net.Address {
	if err := c.Handshake(); err != nil {
		return nil
	}
	state := c.ConnectionState()
	if state.ServerName == "" {
		return nil
	}
	return net.ParseAddress(state.ServerName)
}

func (c *UConn) VerifyPeerCertificate(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	if c.Config.Show {
		localAddr := c.LocalAddr().String()
		fmt.Printf("REALITY localAddr: %v\tis using X25519MLKEM768 for TLS' communication: %v\n", localAddr, c.HandshakeState.ServerHello.ServerShare.Group == utls.X25519MLKEM768)
		fmt.Printf("REALITY localAddr: %v\tis using ML-DSA-65 for cert's extra verification: %v\n", localAddr, len(c.Config.Mldsa65Verify) > 0)
	}
	p, ok := reflect.TypeOf(c.Conn).Elem().FieldByName("peerCertificates")
	if !ok {
		return errors.New("REALITY: peerCertificates field not found via reflect")
	}
	certs := *(*([]*x509.Certificate))(unsafe.Pointer(uintptr(unsafe.Pointer(c.Conn)) + p.Offset))
	if len(certs) == 0 {
		return errors.New("REALITY: no peer certificates")
	}
	if pub, ok := certs[0].PublicKey.(ed25519.PublicKey); ok {
		h := hmac.New(sha512.New, c.AuthKey)
		h.Write(pub)
		if hmac.Equal(h.Sum(nil), certs[0].Signature) {
			if len(c.Config.Mldsa65Verify) > 0 {
				if len(certs[0].Extensions) > 0 {
					h.Write(c.HandshakeState.Hello.Raw)
					h.Write(c.HandshakeState.ServerHello.Raw)
					verify, err := mldsa65.Scheme().UnmarshalBinaryPublicKey(c.Config.Mldsa65Verify)
					if err != nil {
						return errors.New("REALITY: failed to unmarshal ML-DSA-65 public key")
					}
					pubKey, ok := verify.(*mldsa65.PublicKey)
					if !ok {
						return errors.New("REALITY: unexpected ML-DSA-65 public key type")
					}
					if mldsa65.Verify(pubKey, h.Sum(nil), nil, certs[0].Extensions[0].Value) {
						c.Verified = true
						return nil
					}
				}
			} else {
				c.Verified = true
				return nil
			}
		}
	}
	opts := x509.VerifyOptions{
		DNSName:       c.ServerName,
		Intermediates: x509.NewCertPool(),
	}
	for _, cert := range certs[1:] {
		opts.Intermediates.AddCert(cert)
	}
	if _, err := certs[0].Verify(opts); err != nil {
		return err
	}
	return nil
}

func UClient(c net.Conn, config *Config, ctx context.Context, dest net.Destination) (net.Conn, error) {
	localAddr := c.LocalAddr().String()
	uConn := &UConn{
		Config: config,
	}
	utlsConfig := &utls.Config{
		VerifyPeerCertificate:  uConn.VerifyPeerCertificate,
		ServerName:             config.ServerName,
		InsecureSkipVerify:     true,
		SessionTicketsDisabled: true,
		KeyLogWriter:           KeyLogWriterFromConfig(config),
	}
	if utlsConfig.ServerName == "" {
		utlsConfig.ServerName = dest.Address.String()
	}
	uConn.ServerName = utlsConfig.ServerName
	fingerprint := tls.GetFingerprint(config.Fingerprint)
	if fingerprint == nil {
		return nil, errors.New("REALITY: failed to get fingerprint").AtError()
	}
	uConn.UConn = utls.UClient(c, utlsConfig, *fingerprint)
	{
		uConn.BuildHandshakeState()
		hello := uConn.HandshakeState.Hello
		hello.SessionId = make([]byte, 32)
		copy(hello.Raw[39:], hello.SessionId) // the fixed location of `Session ID`
		hello.SessionId[0] = core.Version_x
		hello.SessionId[1] = core.Version_y
		hello.SessionId[2] = core.Version_z
		hello.SessionId[3] = 0 // reserved
		binary.BigEndian.PutUint32(hello.SessionId[4:], uint32(time.Now().Unix()))
		copy(hello.SessionId[8:], config.ShortId)
		_, _ = rand.Read(hello.SessionId[16:])
		if config.Show {
			fmt.Printf("REALITY localAddr: %v\thello.SessionId[:16]: %v\n", localAddr, hello.SessionId[:16])
		}
		publicKey, err := ecdh.X25519().NewPublicKey(config.PublicKey)
		if err != nil {
			return nil, errors.New("REALITY: publicKey == nil")
		}
		ecdhe := uConn.HandshakeState.State13.KeyShareKeys.Ecdhe
		if ecdhe == nil {
			ecdhe = uConn.HandshakeState.State13.KeyShareKeys.MlkemEcdhe
		}
		if ecdhe == nil {
			return nil, errors.New("Current fingerprint ", uConn.ClientHelloID.Client, uConn.ClientHelloID.Version, " does not support TLS 1.3, REALITY handshake cannot establish.")
		}
		authKey, err := ecdhe.ECDH(publicKey)
		if err != nil {
			return nil, errors.New("REALITY: ECDH key exchange failed: ", err)
		}
		uConn.AuthKey = authKey
		if _, err := hkdf.New(sha256.New, uConn.AuthKey, hello.Random[:20], []byte("REALITY")).Read(uConn.AuthKey); err != nil {
			return nil, err
		}
		aead := crypto.NewAesGcm(uConn.AuthKey)
		if config.Show {
			fmt.Printf("REALITY localAddr: %v\tuConn.AuthKey[:16]: %v\tAEAD: %T\n", localAddr, uConn.AuthKey[:16], aead)
		}
		sealed := aead.Seal(hello.SessionId[:0], hello.Random[20:], hello.SessionId[:16], hello.Raw)
		if len(sealed) != 16+16 {
			return nil, errors.New("REALITY: unexpected AEAD seal output length")
		}
		copy(hello.Raw[39:], sealed)
	}
	if err := uConn.HandshakeContext(ctx); err != nil {
		return nil, err
	}
	if config.Show {
		fmt.Printf("REALITY localAddr: %v\tuConn.Verified: %v\n", localAddr, uConn.Verified)
	}
	if !uConn.Verified {
		errors.LogError(ctx, "REALITY: standard x509 fallback, serverName=", uConn.ServerName)
		if len(config.SpiderY) < 10 {
			return nil, errors.New("REALITY: SpiderY requires 10 elements, got ", len(config.SpiderY)).AtWarning()
		}
		go func() {
			client := &http.Client{
				Timeout: 30 * time.Second,
				Transport: &http2.Transport{
					DialTLSContext: func(ctx context.Context, network, addr string, cfg *gotls.Config) (net.Conn, error) {
						if config.Show {
							fmt.Printf("REALITY localAddr: %v\tDialTLSContext\n", localAddr)
						}
						return uConn, nil
					},
				},
			}
			prefix := []byte("https://" + uConn.ServerName)
			maps.Lock()
			if maps.maps == nil {
				maps.maps = make(map[string]map[string]pathForms)
			}
			paths := maps.maps[uConn.ServerName]
			if paths == nil {
				paths = make(map[string]pathForms)
				paths[config.SpiderX] = newPathForms(config.SpiderX)
				maps.maps[uConn.ServerName] = paths
			}
			firstURL := string(prefix) + spiderPickPath(paths)
			maps.Unlock()
			get := func(first bool) {
				var (
					req  *http.Request
					resp *http.Response
					err  error
					body []byte
				)
				if first {
					req, err = http.NewRequest("GET", firstURL, nil)
				} else {
					maps.Lock()
					req, err = http.NewRequest("GET", string(prefix)+spiderPickPath(paths), nil)
					maps.Unlock()
				}
				if err != nil || req == nil {
					return
				}
				headerModes := []string{"nav", "chrome", "firefox", "safari"}
				utils.TryDefaultHeadersWith(req.Header, headerModes[crypto.RandBetween(0, int64(len(headerModes)-1))])
				if first && config.Show {
					fmt.Printf("REALITY localAddr: %v\treq.UserAgent(): %v\n", localAddr, req.UserAgent())
				}
				times := 1
				if !first {
					times = int(crypto.RandBetween(config.SpiderY[4], config.SpiderY[5]))
				}
				for j := 0; j < times; j++ {
					if !first && j == 0 {
						req.Header.Set("Referer", firstURL)
					}
					req.AddCookie(&http.Cookie{Name: "padding", Value: strings.Repeat("0", int(crypto.RandBetween(config.SpiderY[0], config.SpiderY[1])))})
					if resp, err = client.Do(req); err != nil {
						break
					}
					body, err = io.ReadAll(resp.Body)
					resp.Body.Close()
					if err != nil {
						break
					}
					req.Header.Set("Referer", req.URL.String())
					maps.Lock()
					for _, m := range href.FindAllSubmatch(body, -1) {
						m[1] = bytes.TrimPrefix(m[1], prefix)
						if !bytes.Contains(m[1], dot) {
							addToPoolLocked(paths, string(m[1]))
						}
					}
					for _, m := range srcRe.FindAllSubmatch(body, -1) {
						m[1] = bytes.TrimPrefix(m[1], prefix)
						if !bytes.Contains(m[1], dot) {
							addToPoolLocked(paths, string(m[1]))
						}
					}
					req.URL.Path = spiderPickPath(paths)
					if config.Show {
						fmt.Printf("REALITY localAddr: %v\treq.Referer(): %v\n", localAddr, req.Referer())
						fmt.Printf("REALITY localAddr: %v\tlen(body): %v\n", localAddr, len(body))
						fmt.Printf("REALITY localAddr: %v\tlen(paths): %v\n", localAddr, len(paths))
					}
					maps.Unlock()
					if !first {
						time.Sleep(time.Duration(crypto.RandBetween(config.SpiderY[6], config.SpiderY[7])) * time.Millisecond) // interval
					}
				}
			}
			get(true)
			concurrency := int(crypto.RandBetween(config.SpiderY[2], config.SpiderY[3]))
			for i := 0; i < concurrency; i++ {
				go get(false)
			}
			// Do not close the connection
		}()
		return nil, errors.New("REALITY: connection rejected").AtWarning()
	}
	return uConn, nil
}

const maxPathsPerHost = 500 // cap on Spider path pool per host

var (
	href  = regexp.MustCompile(`href="([/h].*?)"`)
	srcRe = regexp.MustCompile(`src="([/h].*?)"`)
	dot   = []byte(".")
)

// pathForms caches eight pre-computed URL variants of a Spider path so
// that GetRandomPath can return a pre-computed form without per-request
// encoding or allocation overhead.
type pathForms [8]string

func newPathForms(p string) pathForms {
	plain := p[1:]            // assets/images/logo.png
	name := fileNameOnly(p)   // logo.png
	ext := fileExtOnly(p)     // .png
	dir := dirNameOnly(plain) // assets/images
	return pathForms{
		url.QueryEscape(p),                  // %2Fassets%2Fimages%2Flogo.png
		plain,                               // assets/images/logo.png
		name,                                // logo.png
		url.QueryEscape(plain),              // assets%2Fimages%2Flogo.png
		url.QueryEscape(name),               // logo.png (already safe)
		ext,                                 // .png
		dir + "/" + name,                    // assets/images/logo.png (rebuild)
		strings.ReplaceAll(plain, "/", "-"), // assets-images-logo.png
	}
}

// formsLen is the number of pre-computed variants; used by GetRandomPath.
const formsLen = 8

// spiderPickPath selects a random raw (un-obfuscated) path for the Spider's
// own crawling. Must be called with maps lock held.
func spiderPickPath(paths map[string]pathForms) string {
	if len(paths) == 0 {
		return "/"
	}
	stopAt := mrand.Intn(len(paths))
	i := 0
	for k := range paths {
		if i == stopAt {
			return k
		}
		i++
	}
	return "/"
}

func fileNameOnly(p string) string {
	if idx := strings.LastIndexByte(p, '/'); idx >= 0 {
		return p[idx+1:]
	}
	return p[1:]
}

func fileExtOnly(p string) string {
	if idx := strings.LastIndexByte(p, '.'); idx >= 0 {
		return p[idx:]
	}
	return ""
}

func dirNameOnly(plain string) string {
	if idx := strings.LastIndexByte(plain, '/'); idx >= 0 {
		return plain[:idx]
	}
	return ""
}

var maps struct {
	sync.RWMutex
	maps map[string]map[string]pathForms
}

// addToPoolLocked inserts a path into the Spider pool, capping growth at
// maxPathsPerHost entries. At capacity, new paths are accepted with ~10%
// probability (replacing a random old entry), which slows turnover and
// preserves frequently-used paths. Must be called with maps.Lock held.
func addToPoolLocked(paths map[string]pathForms, path string) {
	// Already present — skip pre-computation.
	if _, ok := paths[path]; ok {
		return
	}
	if len(paths) < maxPathsPerHost {
		paths[path] = newPathForms(path)
		return
	}
	// At capacity: ~10% chance to replace a random old entry.
	if mrand.Intn(10) != 0 {
		return
	}
	for k := range paths {
		delete(paths, k)
		break
	}
	paths[path] = newPathForms(path)
}

// GetRandomPath returns a random URL path from the Spider path pool for the
// given serverName. The returned value is one of four pre-computed encodings
// chosen at random. Returns "/" if the pool is empty. Safe for concurrent use.
func GetRandomPath(serverName string) string {
	maps.RLock()
	paths := maps.maps[serverName]
	if paths == nil || len(paths) == 0 {
		maps.RUnlock()
		return "/"
	}
	stopAt := mrand.Intn(len(paths))
	i := 0
	for _, forms := range paths {
		if i == stopAt {
			result := forms[mrand.Intn(formsLen)]
			maps.RUnlock()
			return result
		}
		i++
	}
	maps.RUnlock()
	return "/"
}
