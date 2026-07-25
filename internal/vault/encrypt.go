package vault

import (
	"github.com/tetral-ai/tetral/internal/encryption"
)

// Encryptor is the neutral AES-256-GCM encryptor used by Vault credentials.
type Encryptor = encryption.AES256GCMEncryptor

// NewEncryptor creates an Encryptor from a hex-encoded 256-bit key.
func NewEncryptor(hexKey string) (*Encryptor, error) {
	return encryption.NewAES256GCMEncryptor(hexKey)
}
