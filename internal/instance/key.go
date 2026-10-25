package instance

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"log/slog"
)

// KeyPrefix starts every connection key, so Auth tells the credential class
// apart before any lookup.
const KeyPrefix = "tetral_ck_" //nolint:gosec // G101: public key prefix, not a credential.

// KeyLength is the length of every connection key: KeyPrefix and the unpadded
// base64url encoding of 32 random bytes (43 characters).
const KeyLength = len(KeyPrefix) + 43

const keySecretBytes = 32

// ConnectionKey is a newly generated connection key. The plaintext leaves it
// only through Reveal; fmt and slog print a placeholder, so a logged value
// never holds the key.
type ConnectionKey struct {
	plaintext string
	digest    [sha256.Size]byte
}

// GenerateKey returns a key holding 256 bits from crypto/rand.
func GenerateKey() ConnectionKey {
	var secret [keySecretBytes]byte
	_, _ = rand.Read(secret[:]) // crypto/rand.Read never returns an error.
	plaintext := KeyPrefix + base64.RawURLEncoding.EncodeToString(secret[:])
	return ConnectionKey{plaintext: plaintext, digest: sha256.Sum256([]byte(plaintext))}
}

// Reveal returns the plaintext for the one response that may hold it.
func (k ConnectionKey) Reveal() string { return k.plaintext }

// Digest returns the SHA-256 digest of the plaintext, the only form stored.
func (k ConnectionKey) Digest() []byte { return append([]byte(nil), k.digest[:]...) }

func (ConnectionKey) String() string       { return "[connection key]" }
func (ConnectionKey) GoString() string     { return "[connection key]" }
func (ConnectionKey) LogValue() slog.Value { return slog.StringValue("[connection key]") }
