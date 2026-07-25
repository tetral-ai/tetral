package encryption

import (
	"encoding/hex"
	"testing"
)

func testHexKey() string {
	return "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
}

func TestAES256GCMEncryptorRoundTrips(t *testing.T) {
	encryptor, err := NewAES256GCMEncryptor(testHexKey())
	if err != nil {
		t.Fatal(err)
	}

	ciphertext, err := encryptor.Encrypt([]byte("sk-real-key"))
	if err != nil {
		t.Fatal(err)
	}

	plaintext, err := encryptor.Decrypt(ciphertext)
	if err != nil {
		t.Fatal(err)
	}

	if string(plaintext) != "sk-real-key" {
		t.Errorf("plaintext = %q; want sk-real-key", string(plaintext))
	}
}

func TestAES256GCMEncryptorUsesRandomNonce(t *testing.T) {
	encryptor, err := NewAES256GCMEncryptor(testHexKey())
	if err != nil {
		t.Fatal(err)
	}

	first, err := encryptor.Encrypt([]byte("same"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := encryptor.Encrypt([]byte("same"))
	if err != nil {
		t.Fatal(err)
	}

	if hex.EncodeToString(first) == hex.EncodeToString(second) {
		t.Fatal("ciphertexts matched; want random nonce per encryption")
	}
}

func TestAES256GCMEncryptorRejectsWrongKey(t *testing.T) {
	first, err := NewAES256GCMEncryptor("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewAES256GCMEncryptor("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err != nil {
		t.Fatal(err)
	}

	ciphertext, err := first.Encrypt([]byte("secret"))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := second.Decrypt(ciphertext); err == nil {
		t.Fatal("Decrypt succeeded with wrong key; want authentication failure")
	}
}

func TestNewAES256GCMEncryptorRejectsInvalidKey(t *testing.T) {
	cases := []string{
		"short",
		"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}

	for _, key := range cases {
		if _, err := NewAES256GCMEncryptor(key); err == nil {
			t.Fatalf("NewAES256GCMEncryptor(%q) succeeded; want error", key)
		}
	}
}
