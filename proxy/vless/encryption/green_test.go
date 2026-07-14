package encryption

import (
	"bytes"
	"io"
	"net"
	"strings"
	"testing"
)

func TestEncodeDecodeHeader_RoundTrip(t *testing.T) {
	h := make([]byte, 5)
	for _, l := range []int{17, 100, 8192 + 16, 16640} {
		EncodeHeader(h, l)
		got, err := DecodeHeader(h)
		if err != nil {
			t.Fatalf("l=%d: unexpected err %v", l, err)
		}
		if got != l {
			t.Fatalf("l=%d: got %d", l, got)
		}
		if h[0] != 23 || h[1] != 3 || h[2] != 3 {
			t.Fatalf("bad content-type/version: %v", h)
		}
	}
}

func TestDecodeHeader_Bounds(t *testing.T) {
	h := []byte{23, 3, 3, 0, 16} // 16 < 17
	if _, err := DecodeHeader(h); err == nil {
		t.Fatal("expected error for length 16")
	}
	h = []byte{23, 3, 3, 0x41, 0x01} // 16641
	if _, err := DecodeHeader(h); err == nil {
		t.Fatal("expected error for length 16641")
	}
	h = []byte{22, 3, 3, 0, 100}
	if _, err := DecodeHeader(h); err == nil {
		t.Fatal("expected error for wrong content type")
	}
	h = []byte{23, 3, 3, 0, 1}
	_, err := DecodeHeader(h)
	if err == nil || !strings.Contains(err.Error(), "invalid header: ") {
		t.Fatalf("want 'invalid header: ' prefix, got %v", err)
	}
}

func TestOutBytesCapacity(t *testing.T) {
	if OutBytesCapacity != 5+MaxAEADPayload+16 {
		t.Fatalf("OutBytesCapacity=%d", OutBytesCapacity)
	}
	if MaxAEADPayload != 8192 {
		t.Fatalf("MaxAEADPayload=%d", MaxAEADPayload)
	}
	b := OutBytesPool.Get().([]byte)
	defer OutBytesPool.Put(b)
	if len(b) != OutBytesCapacity {
		t.Fatalf("pool buf len=%d want %d", len(b), OutBytesCapacity)
	}
}

func TestAEAD_RoundTrip_AES_and_ChaCha(t *testing.T) {
	key := bytes.Repeat([]byte{0x11}, 32)
	ctx := []byte("test-ctx-vless-aead")
	plain := []byte("hello-vless-green-zone")

	for _, useAES := range []bool{true, false} {
		a, err := NewAEAD(ctx, key, useAES)
		if err != nil {
			t.Fatalf("useAES=%v NewAEAD: %v", useAES, err)
		}
		b, err := NewAEAD(ctx, key, useAES)
		if err != nil {
			t.Fatalf("useAES=%v peer NewAEAD: %v", useAES, err)
		}
		ad := []byte{23, 3, 3, 0, byte(len(plain) + 16)}
		ct := a.Seal(nil, nil, plain, ad)
		if len(ct) != len(plain)+16 {
			t.Fatalf("ct len=%d", len(ct))
		}
		out, err := b.Open(nil, nil, ct, ad)
		if err != nil {
			t.Fatalf("useAES=%v Open: %v", useAES, err)
		}
		if !bytes.Equal(out, plain) {
			t.Fatalf("useAES=%v plaintext mismatch", useAES)
		}
	}
}

func TestIncreaseNonce(t *testing.T) {
	n := make([]byte, 12)
	IncreaseNonce(n)
	if n[11] != 1 {
		t.Fatalf("nonce[11]=%d", n[11])
	}
	n[11] = 255
	IncreaseNonce(n)
	if n[11] != 0 || n[10] != 1 {
		t.Fatalf("carry failed: %v", n)
	}

	a, err := NewAEAD([]byte("ctx"), bytes.Repeat([]byte{1}, 32), true)
	if err != nil {
		t.Fatal(err)
	}
	copy(a.Nonce[:], MaxNonce)
	a.Seal(nil, nil, []byte("x"), nil)
	if a.IsMax {
		t.Fatal("IsMax should be false after increment from MaxNonce")
	}
}

func TestParsePadding_ValidAndInvalid(t *testing.T) {
	var lens, gaps [][3]int
	if err := ParsePadding("100-100-200.75-0-10.50-0-50", &lens, &gaps); err != nil {
		t.Fatalf("valid padding: %v", err)
	}
	if len(lens) != 2 || len(gaps) != 1 {
		t.Fatalf("lens=%d gaps=%d", len(lens), len(gaps))
	}

	lens, gaps = nil, nil
	if err := ParsePadding("50-10-20", &lens, &gaps); err == nil {
		t.Fatal("expected first padding too small")
	}
	if err := ParsePadding("bad", &lens, &gaps); err == nil {
		t.Fatal("expected invalid format")
	}
	if err := ParsePadding("", &lens, &gaps); err != nil {
		t.Fatalf("empty should be ok: %v", err)
	}
}

func TestCreatPadding_Default(t *testing.T) {
	length, lens, gaps := CreatPadding(nil, nil)
	if len(lens) == 0 {
		t.Fatal("default lens empty")
	}
	if length < 0 {
		t.Fatal("negative length")
	}
	_ = gaps
}

func TestXorConn_WriteRead_PartialHeader(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	// Same key/iv => identical keystreams on independent CTR instances.
	// Writer only needs Out CTR; reader only needs PeerCTR that matches the writer keystream.
	key := bytes.Repeat([]byte{0x22}, 32)
	iv := bytes.Repeat([]byte{0x33}, 16)
	writeCTR, err := NewCTR(key, iv)
	if err != nil {
		t.Fatal(err)
	}
	peerReadCTR, err := NewCTR(key, iv)
	if err != nil {
		t.Fatal(err)
	}

	xw := NewXorConn(c1, writeCTR, nil, 0, 0)    // write path uses CTR
	xr := NewXorConn(c2, nil, peerReadCTR, 0, 0) // read path uses PeerCTR

	payload := bytes.Repeat([]byte{'A'}, 20)
	frame := make([]byte, 5+len(payload))
	EncodeHeader(frame[:5], len(payload))
	copy(frame[5:], payload)
	want := append([]byte(nil), frame...)

	errCh := make(chan error, 1)
	go func() {
		off := 0
		for off < len(frame) {
			n := 3 // force partial TLS-like header assembly
			if off+n > len(frame) {
				n = len(frame) - off
			}
			if _, err := xw.Write(frame[off : off+n]); err != nil {
				errCh <- err
				return
			}
			off += n
		}
		errCh <- nil
	}()

	buf := make([]byte, len(want))
	if _, err := io.ReadFull(xr, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(buf, want) {
		t.Fatalf("roundtrip mismatch\ngot  %v\nwant %v", buf, want)
	}
}

func TestXorConn_EmptyWrite(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	ctr, err := NewCTR(bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 16))
	if err != nil {
		t.Fatal(err)
	}
	x := NewXorConn(c1, ctr, ctr, 0, 0)
	n, err := x.Write(nil)
	if n != 0 || err != nil {
		t.Fatalf("empty write n=%d err=%v", n, err)
	}
}

func TestExtractBuffers_BufferAccessor(t *testing.T) {
	c := NewCommonConn(nil, true)
	in, raw := ExtractBuffers(c)
	if in != c.Input() || raw != c.RawInput() {
		t.Fatal("BufferAccessor path failed")
	}
}

func TestNewCTR_Deterministic(t *testing.T) {
	key := bytes.Repeat([]byte{0xab}, 32)
	iv := bytes.Repeat([]byte{0xcd}, 16)
	a, err := NewCTR(key, iv)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewCTR(key, iv)
	if err != nil {
		t.Fatal(err)
	}
	src := []byte("deterministic-xor-check")
	da := append([]byte(nil), src...)
	db := append([]byte(nil), src...)
	a.XORKeyStream(da, da)
	b.XORKeyStream(db, db)
	if !bytes.Equal(da, db) {
		t.Fatal("NewCTR not deterministic for same key/iv")
	}
	if bytes.Equal(da, src) {
		t.Fatal("CTR produced identity; unexpected")
	}
}
