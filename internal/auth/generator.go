package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"io"
)

// TokenPrefix is the non-secret public prefix every generated Engine
// API key carries, so callers can recognize an Engine-generated key
// without parsing the full token.
const TokenPrefix = "tetral_sk_" //nolint:gosec // G101: documented public prefix, not a secret

// TokenSecretBytes is the number of cryptographically random bytes
// that go into every generated API key. The generator uses exactly
// 32 bytes (256 bits) to produce a fixed-length encoded suffix.
const TokenSecretBytes = 32

// keyPrefixLength is the number of leading ASCII characters of the
// raw token stored in api_keys.key_prefix as identification metadata.
// 16 is wide enough that "tetral_sk_" + a few base64url characters
// uniquely identifies an issued key in support tooling, narrow enough
// that the prefix carries no meaningful entropy.
const keyPrefixLength = 16

// GenerateAPIKey produces a single-token Engine API key:
//
//	"tetral_sk_" + base64url(no padding, 32 random bytes)
//
// Bytes come from crypto/rand. The returned string is ASCII and
// copyable as a single token. The function panics for no input —
// callers always receive (token, nil) on success or ("", err) when
// the OS random source fails (an extremely rare event that Engine
// surfaces as a startup or request failure).
func GenerateAPIKey() (string, error) {
	return GenerateAPIKeyWithReader(rand.Reader)
}

// GenerateAPIKeyWithReader is the test-injectable variant of
// GenerateAPIKey. Tests pass a deterministic reader to assert the
// expected token suffix without weakening the production path.
func GenerateAPIKeyWithReader(reader io.Reader) (string, error) {
	if reader == nil {
		return "", &GeneratorError{Reason: "nil random reader"}
	}
	var raw [TokenSecretBytes]byte
	n, err := io.ReadFull(reader, raw[:])
	if err != nil {
		return "", &GeneratorError{Reason: "random read failed"}
	}
	if n != TokenSecretBytes {
		return "", &GeneratorError{Reason: "short random read"}
	}
	encoded := base64.RawURLEncoding.EncodeToString(raw[:])
	return TokenPrefix + encoded, nil
}

// DigestAPIKey returns the deterministic non-recoverable digest of
// rawToken stored in api_keys.key_digest. SHA-256 over the full ASCII
// token is sufficient because generated tokens carry 32 bytes of
// entropy, and a fixed-length digest keeps the column shape stable.
func DigestAPIKey(rawToken string) []byte {
	sum := sha256.Sum256([]byte(rawToken))
	return sum[:]
}

// KeyPrefixFor returns the first 16 ASCII characters of rawToken.
// Used as the stored key_prefix metadata field. Tokens shorter than
// 16 characters return the entire token; this only happens for the
// bootstrap key derived from ENGINE_API_KEY, which the strength floor
// enforces is at least 32 bytes — so the truncation case never
// applies to validated bootstrap keys but is handled defensively.
func KeyPrefixFor(rawToken string) string {
	if len(rawToken) <= keyPrefixLength {
		return rawToken
	}
	return rawToken[:keyPrefixLength]
}
