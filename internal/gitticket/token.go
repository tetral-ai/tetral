package gitticket

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
)

const (
	HeaderName = "X-Tetral-Git-Ticket"

	TokenBytes = 32
	TokenChars = 43

	StatusPending = "pending"
	StatusLive    = "live"
	StatusRotated = "rotated"
)

var (
	ErrInvalidToken = errors.New("git ticket token is invalid")
	ErrInvalidHash  = errors.New("git ticket token hash is invalid")
)

// GenerateToken returns a 256-bit base64url ticket without padding.
func GenerateToken(random io.Reader) (string, error) {
	if random == nil {
		return "", ErrInvalidToken
	}
	raw := make([]byte, TokenBytes)
	if _, err := io.ReadFull(random, raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func HashToken(token string) ([]byte, error) {
	if err := ValidateToken(token); err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(token))
	return sum[:], nil
}

func ValidateToken(token string) error {
	if len(token) != TokenChars {
		return ErrInvalidToken
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(decoded) != TokenBytes {
		return ErrInvalidToken
	}
	return nil
}

func ValidateHash(hash []byte) error {
	if len(hash) != sha256.Size {
		return ErrInvalidHash
	}
	return nil
}
