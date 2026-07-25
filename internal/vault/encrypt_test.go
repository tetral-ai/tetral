package vault

import (
	"encoding/hex"
	"testing"
)

func testHexKey() string {
	// 32 random bytes = 64 hex chars
	return "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
}

func TestEncryptDecryptRoundTrip(t *testing.T) {
	enc, err := NewEncryptor(testHexKey())
	if err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("sk-real-key")
	ciphertext, err := enc.Encrypt(plaintext)
	if err != nil {
		t.Fatal(err)
	}

	decrypted, err := enc.Decrypt(ciphertext)
	if err != nil {
		t.Fatal(err)
	}

	if string(decrypted) != "sk-real-key" {
		t.Errorf("expected 'sk-real-key', got %q", string(decrypted))
	}
}

func TestEncryptProducesDifferentCiphertext(t *testing.T) {
	enc, _ := NewEncryptor(testHexKey())

	ct1, _ := enc.Encrypt([]byte("same"))
	ct2, _ := enc.Encrypt([]byte("same"))

	if hex.EncodeToString(ct1) == hex.EncodeToString(ct2) {
		t.Error("expected different ciphertexts for same plaintext (random nonce)")
	}
}

func TestDecryptWithWrongKey(t *testing.T) {
	encA, _ := NewEncryptor("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	encB, _ := NewEncryptor("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

	ciphertext, _ := encA.Encrypt([]byte("secret"))

	_, err := encB.Decrypt(ciphertext)
	if err == nil {
		t.Fatal("expected error decrypting with wrong key")
	}
}

func TestNewEncryptorRejectsInvalidKeyLength(t *testing.T) {
	// Too short.
	_, err := NewEncryptor("short")
	if err == nil {
		t.Error("expected error for short key")
	}

	// Too long (128 hex chars = 64 bytes).
	_, err = NewEncryptor("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	if err == nil {
		t.Error("expected error for 128 hex char key")
	}

	// Exactly right (64 hex chars = 32 bytes).
	_, err = NewEncryptor(testHexKey())
	if err != nil {
		t.Errorf("expected no error for valid 64 hex char key, got %v", err)
	}
}
