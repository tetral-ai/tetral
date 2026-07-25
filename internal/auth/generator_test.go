package auth_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/auth"
)

func TestGenerateAPIKeyHasTetralSKPrefix(t *testing.T) {
	token, err := auth.GenerateAPIKey()
	if err != nil {
		t.Fatalf("GenerateAPIKey: %v", err)
	}
	if !strings.HasPrefix(token, auth.TokenPrefix) {
		t.Errorf("token %q must start with %q", token, auth.TokenPrefix)
	}
}

func TestGenerateAPIKeyIsASCIIAndSingleToken(t *testing.T) {
	token, err := auth.GenerateAPIKey()
	if err != nil {
		t.Fatalf("GenerateAPIKey: %v", err)
	}
	for i, r := range token {
		if r > 127 {
			t.Errorf("token byte %d is non-ASCII (%q); Engine tokens must be ASCII-only", i, r)
		}
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			t.Errorf("token contains whitespace at byte %d (%q); Engine tokens must be a single copyable token", i, r)
		}
	}
}

func TestGenerateAPIKeyDecodes32SecretBytes(t *testing.T) {
	token, err := auth.GenerateAPIKey()
	if err != nil {
		t.Fatalf("GenerateAPIKey: %v", err)
	}
	suffix := strings.TrimPrefix(token, auth.TokenPrefix)
	decoded, err := base64.RawURLEncoding.DecodeString(suffix)
	if err != nil {
		t.Fatalf("decode token suffix: %v", err)
	}
	if len(decoded) != auth.TokenSecretBytes {
		t.Errorf("decoded secret bytes = %d; want %d", len(decoded), auth.TokenSecretBytes)
	}
}

func TestGenerateAPIKeyWithReaderProducesExpectedSuffix(t *testing.T) {
	// Deterministic reader returns 32 bytes equal to byte(i). The
	// suffix must equal base64url(no padding) of that exact byte
	// string, proving the production path uses RawURLEncoding and 32
	// bytes from the supplied reader.
	var raw [auth.TokenSecretBytes]byte
	for i := range raw {
		raw[i] = byte(i)
	}
	expected := auth.TokenPrefix + base64.RawURLEncoding.EncodeToString(raw[:])

	token, err := auth.GenerateAPIKeyWithReader(bytes.NewReader(raw[:]))
	if err != nil {
		t.Fatalf("GenerateAPIKeyWithReader: %v", err)
	}
	if token != expected {
		t.Errorf("token = %q; want %q", token, expected)
	}
}

func TestGenerateAPIKeyTokensDifferAcrossCalls(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 16; i++ {
		token, err := auth.GenerateAPIKey()
		if err != nil {
			t.Fatalf("GenerateAPIKey iter %d: %v", i, err)
		}
		if seen[token] {
			t.Fatalf("duplicate generated token: %q", token)
		}
		seen[token] = true
	}
}

func TestGenerateAPIKeyWithReaderRejectsShortReads(t *testing.T) {
	short := bytes.NewReader([]byte{0x00, 0x01})
	_, err := auth.GenerateAPIKeyWithReader(short)
	if err == nil {
		t.Fatal("expected GeneratorError for short reader, got nil")
	}
	var genErr *auth.GeneratorError
	if !errors.As(err, &genErr) {
		t.Fatalf("expected *auth.GeneratorError, got %T", err)
	}
}

func TestGenerateAPIKeyWithReaderRejectsNilReader(t *testing.T) {
	_, err := auth.GenerateAPIKeyWithReader(nil)
	if err == nil {
		t.Fatal("expected GeneratorError for nil reader, got nil")
	}
}

type erroringReader struct{}

func (erroringReader) Read(_ []byte) (int, error) { return 0, fmt.Errorf("simulated read error") }

func TestGenerateAPIKeyWithReaderSurfacesReaderErrors(t *testing.T) {
	_, err := auth.GenerateAPIKeyWithReader(erroringReader{})
	if err == nil {
		t.Fatal("expected GeneratorError when reader fails, got nil")
	}
	var genErr *auth.GeneratorError
	if !errors.As(err, &genErr) {
		t.Fatalf("expected *auth.GeneratorError, got %T", err)
	}
	// Internal Reason text must not surface raw reader error or
	// secret material; it should describe the failure mode at a high
	// level only.
	if strings.Contains(genErr.Error(), "simulated read error") {
		t.Errorf("error text leaks raw reader error: %q", genErr.Error())
	}
}

func TestDigestAPIKeyIsSHA256(t *testing.T) {
	token := "tetral_sk_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" //nolint:gosec // G101: synthetic test token, not a real secret
	want := sha256.Sum256([]byte(token))
	got := auth.DigestAPIKey(token)
	if !bytes.Equal(got, want[:]) {
		t.Errorf("DigestAPIKey returned a non-SHA-256 digest; got %x want %x", got, want[:])
	}
	if len(got) != 32 {
		t.Errorf("digest length = %d; want 32 (sha256 byte size)", len(got))
	}
}

func TestDigestAPIKeyIsDeterministic(t *testing.T) {
	token, err := auth.GenerateAPIKey()
	if err != nil {
		t.Fatalf("GenerateAPIKey: %v", err)
	}
	a := auth.DigestAPIKey(token)
	b := auth.DigestAPIKey(token)
	if !bytes.Equal(a, b) {
		t.Errorf("digest is not deterministic across calls")
	}
}

func TestKeyPrefixForTakesFirst16Characters(t *testing.T) {
	token := auth.TokenPrefix + "abcdefghijklmnopqrstuvwxyz"
	prefix := auth.KeyPrefixFor(token)
	if len(prefix) != 16 {
		t.Errorf("KeyPrefixFor length = %d; want 16", len(prefix))
	}
	if !strings.HasPrefix(token, prefix) {
		t.Errorf("KeyPrefixFor %q must be a prefix of token %q", prefix, token)
	}
}

func TestKeyPrefixForReturnsWholeShortToken(t *testing.T) {
	token := "abc"
	if got := auth.KeyPrefixFor(token); got != "abc" {
		t.Errorf("KeyPrefixFor(%q) = %q; want %q", token, got, token)
	}
}

// TestGeneratorDoesNotImportInternalID pins that generated API keys do not
// derive from internal/id.New, which produces non-secret resource identifiers
// that are not suitable as bearer credentials. If the generator file ever
// imports github.com/tetral-ai/tetral/internal/id, this test fails.
func TestGeneratorDoesNotImportInternalID(t *testing.T) {
	body, err := os.ReadFile("generator.go")
	if err != nil {
		t.Fatalf("read generator.go: %v", err)
	}
	if strings.Contains(string(body), `"github.com/tetral-ai/tetral/internal/id"`) {
		t.Error("auth/generator.go must not import github.com/tetral-ai/tetral/internal/id; raw key secrets must use crypto/rand directly")
	}
}
