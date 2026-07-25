package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"fmt"
)

// AES256GCMEncryptor provides AES-256-GCM encryption with generated random nonces.
type AES256GCMEncryptor struct {
	gcm cipher.AEAD
}

// NewAES256GCMEncryptor creates an encryptor from a hex-encoded 256-bit key.
func NewAES256GCMEncryptor(hexKey string) (*AES256GCMEncryptor, error) {
	keyBytes, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, fmt.Errorf("invalid hex key: %w", err)
	}
	if len(keyBytes) != 32 {
		return nil, fmt.Errorf("key must be 32 bytes (64 hex chars), got %d bytes", len(keyBytes))
	}

	block, err := aes.NewCipher(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}
	gcm, err := cipher.NewGCMWithRandomNonce(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	return &AES256GCMEncryptor{gcm: gcm}, nil
}

// Encrypt encrypts plaintext using AES-256-GCM.
func (e *AES256GCMEncryptor) Encrypt(plaintext []byte) ([]byte, error) {
	return e.gcm.Seal(nil, nil, plaintext, nil), nil
}

// Decrypt decrypts AES-256-GCM ciphertext.
func (e *AES256GCMEncryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	plaintext, err := e.gcm.Open(nil, nil, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}
	return plaintext, nil
}
