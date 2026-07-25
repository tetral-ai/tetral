package gitticket

import (
	"bytes"
	"errors"
	"testing"
)

func TestGenerateTokenAndHashUseContractShape(t *testing.T) {
	token, err := GenerateToken(bytes.NewReader(bytes.Repeat([]byte{7}, TokenBytes)))
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	if len(token) != TokenChars {
		t.Fatalf("token length = %d; want %d", len(token), TokenChars)
	}
	if err := ValidateToken(token); err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	hash, err := HashToken(token)
	if err != nil {
		t.Fatalf("HashToken: %v", err)
	}
	if len(hash) != TokenBytes {
		t.Fatalf("hash length = %d; want %d", len(hash), TokenBytes)
	}
	if bytes.Contains(hash, []byte(token)) {
		t.Fatalf("hash contains raw token bytes")
	}
}

func TestValidateTokenRejectsMalformedValues(t *testing.T) {
	for _, token := range []string{
		"",
		"short",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	} {
		t.Run(token, func(t *testing.T) {
			if err := ValidateToken(token); !errors.Is(err, ErrInvalidToken) {
				t.Fatalf("ValidateToken(%q) = %v; want ErrInvalidToken", token, err)
			}
		})
	}
}

func TestValidateHashRejectsWrongLength(t *testing.T) {
	for _, hash := range [][]byte{nil, bytes.Repeat([]byte{1}, TokenBytes-1), bytes.Repeat([]byte{1}, TokenBytes+1)} {
		if err := ValidateHash(hash); !errors.Is(err, ErrInvalidHash) {
			t.Fatalf("ValidateHash(len=%d) = %v; want ErrInvalidHash", len(hash), err)
		}
	}
}
