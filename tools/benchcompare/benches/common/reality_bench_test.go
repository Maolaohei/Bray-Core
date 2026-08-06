package internet

import (
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"testing"

	"github.com/cloudflare/circl/sign/mldsa/mldsa65"
	"github.com/xtls/xray-core/common/crypto"
	"golang.org/x/crypto/hkdf"
)

func BenchmarkRealityHandshakeKeyExchange(b *testing.B) {
	privKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		b.Fatal(err)
	}
	pubKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = privKey.ECDH(pubKey.PublicKey())
	}
}

func BenchmarkRealityAEADSeal(b *testing.B) {
	key := make([]byte, 32)
	rand.Read(key)
	aead := crypto.NewAesGcm(key)
	nonce := make([]byte, 12)
	rand.Read(nonce)
	plaintext := make([]byte, 16)
	rand.Read(plaintext)
	additionalData := make([]byte, 128)
	rand.Read(additionalData)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = aead.Seal(plaintext[:0], nonce, plaintext, additionalData)
	}
}

func BenchmarkRealityAEADOpen(b *testing.B) {
	key := make([]byte, 32)
	rand.Read(key)
	aead := crypto.NewAesGcm(key)
	nonce := make([]byte, 12)
	rand.Read(nonce)
	plaintext := make([]byte, 16)
	rand.Read(plaintext)
	additionalData := make([]byte, 128)
	rand.Read(additionalData)
	ciphertext := aead.Seal(nil, nonce, plaintext, additionalData)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = aead.Open(nil, nonce, ciphertext, additionalData)
	}
}

func BenchmarkRealityHKDF(b *testing.B) {
	ikm := make([]byte, 32)
	rand.Read(ikm)
	salt := make([]byte, 20)
	rand.Read(salt)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := hkdf.New(sha256.New, ikm, salt, []byte("REALITY"))
		_, _ = io.ReadAll(r)
	}
}

func BenchmarkRealityECDSA(b *testing.B) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		b.Fatal(err)
	}
	msg := make([]byte, 32)
	rand.Read(msg)
	hash := sha256.Sum256(msg)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ecdsa.SignASN1(rand.Reader, priv, hash[:])
	}
}

func BenchmarkRealityMLDSA65Sign(b *testing.B) {
	_, priv, err := mldsa65.GenerateKey(rand.Reader)
	if err != nil {
		b.Fatal(err)
	}
	msg := make([]byte, 32)
	rand.Read(msg)
	sig := make([]byte, mldsa65.SignatureSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mldsa65.SignTo(priv, msg, nil, false, sig)
	}
}

func BenchmarkRealityMLDSA65Verify(b *testing.B) {
	pub, priv, err := mldsa65.GenerateKey(rand.Reader)
	if err != nil {
		b.Fatal(err)
	}
	msg := make([]byte, 32)
	rand.Read(msg)
	sig := make([]byte, mldsa65.SignatureSize)
	_ = mldsa65.SignTo(priv, msg, nil, false, sig)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mldsa65.Verify(pub, msg, nil, sig)
	}
}
