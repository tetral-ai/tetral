package vault_test

import (
	"testing"

	"github.com/tetral-ai/tetral/internal/vault"
)

func TestCredentialStoreAcceptsConsumerEncryptionInterface(t *testing.T) {
	store := vault.NewPostgreSQLCredentialStore(nil, fakeCredentialCipher{})
	if store == nil {
		t.Fatal("NewPostgreSQLCredentialStore returned nil")
	}
}

type fakeCredentialCipher struct{}

func (fakeCredentialCipher) Encrypt(value []byte) ([]byte, error) {
	return append([]byte("encrypted:"), value...), nil
}

func (fakeCredentialCipher) Decrypt(value []byte) ([]byte, error) {
	return append([]byte("decrypted:"), value...), nil
}
