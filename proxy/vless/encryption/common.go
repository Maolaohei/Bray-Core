package encryption

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	stderrors "errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/xtls/xray-core/common/crypto"
	"github.com/xtls/xray-core/common/errors"
	"golang.org/x/crypto/chacha20poly1305"
	"lukechampine.com/blake3"
)

// OutBytesCapacity is the fixed buffer size for CommonConn.Write:
// 5-byte TLS-like header + max payload 8192 + 16-byte AEAD tag.
// Keep in sync with the 8192 chunk limit in CommonConn.Write.
const (
	MaxAEADPayload   = 8192
	OutBytesCapacity = 5 + MaxAEADPayload + 16
)

// ErrInvalidHeader is returned by DecodeHeader on bad framing.
// Error() still contains "invalid header" for string-match callers.
var ErrInvalidHeader = stderrors.New("invalid header")

// bytesToString maps a byte slice to a string without allocation (read-only use).
func bytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

var OutBytesPool = sync.Pool{
	New: func() any {
		return make([]byte, OutBytesCapacity)
	},
}

// BufferAccessor is implemented by connections that expose their internal
// read buffers for Vision splice-copy optimization. Connections that don't
// implement this interface will fall back to reflect+unsafe.Pointer access.
type BufferAccessor interface {
	Input() *bytes.Reader
	RawInput() *bytes.Buffer
}

type CommonConn struct {
	net.Conn
	UseAES      bool
	Client      *ClientInstance
	UnitedKey   []byte
	PreWrite    []byte
	AEAD        *AEAD
	PeerAEAD    *AEAD
	PeerPadding []byte
	rawInput    bytes.Buffer
	input       bytes.Reader
}

// Input returns a pointer to the internal bytes.Reader buffer.
func (c *CommonConn) Input() *bytes.Reader { return &c.input }

// RawInput returns a pointer to the internal bytes.Buffer.
func (c *CommonConn) RawInput() *bytes.Buffer { return &c.rawInput }

func NewCommonConn(conn net.Conn, useAES bool) *CommonConn {
	return &CommonConn{
		Conn:   conn,
		UseAES: useAES,
	}
}

func (c *CommonConn) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	outBytes := OutBytesPool.Get().([]byte)
	defer OutBytesPool.Put(outBytes)
	for n := 0; n < len(b); {
		b := b[n:]
		if len(b) > MaxAEADPayload {
			b = b[:MaxAEADPayload] // for avoiding another copy() in peer's Read()
		}
		n += len(b)
		headerAndData := outBytes[:5+len(b)+16]
		EncodeHeader(headerAndData, len(b)+16)
		c.AEAD.Seal(headerAndData[:5], nil, b, headerAndData[:5])
		if c.AEAD.IsMax {
			newAEAD, err := NewAEAD(headerAndData, c.UnitedKey, c.UseAES)
			if err != nil {
				return 0, err
			}
			c.AEAD = newAEAD
		}
		if c.PreWrite != nil {
			headerAndData = append(c.PreWrite, headerAndData...)
			c.PreWrite = nil
		}
		if _, err := c.Conn.Write(headerAndData); err != nil {
			return 0, err
		}
	}
	return len(b), nil
}

func (c *CommonConn) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if c.PeerAEAD == nil { // client's 0-RTT
		serverRandom := make([]byte, 16)
		if _, err := io.ReadFull(c.Conn, serverRandom); err != nil {
			return 0, err
		}
		var err error
		c.PeerAEAD, err = NewAEAD(serverRandom, c.UnitedKey, c.UseAES)
		if err != nil {
			return 0, err
		}
		if xorConn, ok := c.Conn.(*XorConn); ok {
			ctr, err := NewCTR(c.UnitedKey, serverRandom)
			if err != nil {
				return 0, err
			}
			xorConn.PeerCTR = ctr
		}
	}
	if c.PeerPadding != nil { // client's 1-RTT
		if _, err := io.ReadFull(c.Conn, c.PeerPadding); err != nil {
			return 0, err
		}
		if _, err := c.PeerAEAD.Open(c.PeerPadding[:0], nil, c.PeerPadding, nil); err != nil {
			return 0, err
		}
		c.PeerPadding = nil
	}
	if c.input.Len() > 0 {
		return c.input.Read(b)
	}
	peerHeader := [5]byte{}
	if _, err := io.ReadFull(c.Conn, peerHeader[:]); err != nil {
		return 0, err
	}
	l, err := DecodeHeader(peerHeader[:]) // l: 17~16640
	if err != nil {
		if c.Client != nil && (stderrors.Is(err, ErrInvalidHeader) || strings.Contains(err.Error(), "invalid header")) { // client's 0-RTT
			c.Client.RWLock.Lock()
			if bytes.HasPrefix(c.UnitedKey, c.Client.PfsKey) {
				c.Client.Expire = time.Now() // expired
			}
			c.Client.RWLock.Unlock()
			return 0, errors.New("new handshake needed")
		}
		return 0, err
	}
	c.Client = nil
	if c.rawInput.Cap() < l {
		c.rawInput.Grow(l) // no need to use sync.Pool, because we are always reading
	}
	peerData := c.rawInput.Bytes()[:l]
	if _, err := io.ReadFull(c.Conn, peerData); err != nil {
		return 0, err
	}
	dst := peerData[:l-16]
	if len(dst) <= len(b) {
		dst = b[:len(dst)] // avoids another copy()
	}
	var newAEAD *AEAD
	if c.PeerAEAD.IsMax {
		var err error
		newAEAD, err = NewAEAD(append(peerHeader[:], peerData...), c.UnitedKey, c.UseAES)
		if err != nil {
			return 0, err
		}
	}
	_, err = c.PeerAEAD.Open(dst[:0], nil, peerData, peerHeader[:])
	if newAEAD != nil {
		c.PeerAEAD = newAEAD
	}
	if err != nil {
		return 0, err
	}
	if len(dst) > len(b) {
		c.input.Reset(dst[copy(b, dst):])
		dst = b // for len(dst)
	}
	return len(dst), nil
}

type AEAD struct {
	cipher.AEAD
	Nonce [12]byte
	IsMax bool // true when Nonce == MaxNonce
}

func NewAEAD(ctx, key []byte, useAES bool) (*AEAD, error) {
	k := make([]byte, 32)
	blake3.DeriveKey(k, bytesToString(ctx), key)
	var aead cipher.AEAD
	if useAES {
		block, err := aes.NewCipher(k)
		if err != nil {
			return nil, errors.New("failed to create AES cipher").Base(err)
		}
		aead, err = cipher.NewGCM(block)
		if err != nil {
			return nil, errors.New("failed to create AES-GCM AEAD").Base(err)
		}
	} else {
		var err error
		aead, err = chacha20poly1305.New(k)
		if err != nil {
			return nil, errors.New("failed to create ChaCha20-Poly1305 AEAD").Base(err)
		}
	}
	return &AEAD{AEAD: aead}, nil
}

func (a *AEAD) Seal(dst, nonce, plaintext, additionalData []byte) []byte {
	if nonce == nil {
		nonce = IncreaseNonce(a.Nonce[:])
		// IsMax when all bytes are 0xff after increment (only full-carry case).
		a.IsMax = a.Nonce[0] == 0xff && a.Nonce[11] == 0xff && nonceIsMax(a.Nonce[:])
	}
	return a.AEAD.Seal(dst, nonce, plaintext, additionalData)
}

func (a *AEAD) Open(dst, nonce, ciphertext, additionalData []byte) ([]byte, error) {
	if nonce == nil {
		nonce = IncreaseNonce(a.Nonce[:])
		a.IsMax = a.Nonce[0] == 0xff && a.Nonce[11] == 0xff && nonceIsMax(a.Nonce[:])
	}
	return a.AEAD.Open(dst, nonce, ciphertext, additionalData)
}

// nonceIsMax is a short-circuiting equality vs MaxNonce (all 0xff).
func nonceIsMax(n []byte) bool {
	for i := range n {
		if n[i] != 0xff {
			return false
		}
	}
	return true
}

func IncreaseNonce(nonce []byte) []byte {
	for i := range 12 {
		nonce[11-i]++
		if nonce[11-i] != 0 {
			break
		}
	}
	return nonce
}

var MaxNonce = bytes.Repeat([]byte{255}, 12)

func EncodeLength(l int) []byte {
	return []byte{byte(l >> 8), byte(l)}
}

func DecodeLength(b []byte) int {
	return int(b[0])<<8 | int(b[1])
}

func EncodeHeader(h []byte, l int) {
	h[0] = 23
	h[1] = 3
	h[2] = 3
	h[3] = byte(l >> 8)
	h[4] = byte(l)
}

func DecodeHeader(h []byte) (l int, err error) {
	l = int(h[3])<<8 | int(h[4])
	if h[0] != 23 || h[1] != 3 || h[2] != 3 {
		l = 0
	}
	if l < 17 || l > 16640 { // TLS 1.3 max record: 16384 + 256 (RFC 8446 §5.2)
		err = fmt.Errorf("%w: %v", ErrInvalidHeader, h[:5]) // Error() contains "invalid header:" for string match
	}
	return
}

func ParsePadding(padding string, paddingLens, paddingGaps *[][3]int) (err error) {
	if padding == "" {
		return
	}
	maxLen := 0
	for i, s := range strings.Split(padding, ".") {
		x := strings.Split(s, "-")
		if len(x) < 3 || x[0] == "" || x[1] == "" || x[2] == "" {
			return errors.New("invalid padding lenth/gap parameter: " + s)
		}
		y := [3]int{}
		if y[0], err = strconv.Atoi(x[0]); err != nil {
			return
		}
		if y[1], err = strconv.Atoi(x[1]); err != nil {
			return
		}
		if y[2], err = strconv.Atoi(x[2]); err != nil {
			return
		}
		if i == 0 && (y[0] < 100 || y[1] < 18+17 || y[2] < 18+17) {
			return errors.New("first padding length must not be smaller than 35")
		}
		if i%2 == 0 {
			*paddingLens = append(*paddingLens, y)
			maxLen += max(y[1], y[2])
		} else {
			*paddingGaps = append(*paddingGaps, y)
		}
	}
	if maxLen > 18+65535 {
		return errors.New("total padding length must not be larger than 65553")
	}
	return
}

func CreatPadding(paddingLens, paddingGaps [][3]int) (length int, lens []int, gaps []time.Duration) {
	if len(paddingLens) == 0 {
		paddingLens = [][3]int{{100, 111, 1111}, {50, 0, 3333}}
		paddingGaps = [][3]int{{75, 0, 111}}
	}
	for _, y := range paddingLens {
		l := 0
		if y[0] >= int(crypto.RandBetween(0, 100)) {
			l = int(crypto.RandBetween(int64(y[1]), int64(y[2])))
		}
		lens = append(lens, l)
		length += l
	}
	for _, y := range paddingGaps {
		g := 0
		if y[0] >= int(crypto.RandBetween(0, 100)) {
			g = int(crypto.RandBetween(int64(y[1]), int64(y[2])))
		}
		gaps = append(gaps, time.Duration(g)*time.Millisecond)
	}
	return
}

// ExtractBuffers safely extracts input and rawInput from a connection.
// It first checks if the connection implements BufferAccessor (safe path).
// Falls back to reflect+unsafe.Pointer for backward compatibility with
// connection types that don't implement the interface yet.
func ExtractBuffers(conn interface{}) (input *bytes.Reader, rawInput *bytes.Buffer) {
	// Fast path: connection implements BufferAccessor
	if ba, ok := conn.(BufferAccessor); ok {
		return ba.Input(), ba.RawInput()
	}

	// Slow path: reflect+unsafe.Pointer (backward compatibility)
	val := reflect.ValueOf(conn)
	if val.Kind() == reflect.Ptr {
		t := val.Type().Elem()
		p := val.Pointer()

		fi, _ := t.FieldByName("input")
		fr, _ := t.FieldByName("rawInput")

		if fi.Name != "" && fr.Name != "" {
			input = (*bytes.Reader)(unsafe.Pointer(p + fi.Offset))
			rawInput = (*bytes.Buffer)(unsafe.Pointer(p + fr.Offset))
		}
	}
	return
}
