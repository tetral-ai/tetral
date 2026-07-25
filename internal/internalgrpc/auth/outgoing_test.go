package auth

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestServiceAccountTokenCredentialsAttachExactlyOneBearerValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("secret-token\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	credentials := NewServiceAccountTokenCredentials(FileTokenSource{Path: path})
	metadata, err := credentials.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata: %v", err)
	}
	if got := metadata["authorization"]; got != "bearer secret-token" {
		t.Fatalf("authorization metadata = %q; want bearer token", got)
	}
}

func TestServiceAccountTokenCredentialsFailClosedWithSafeErrors(t *testing.T) {
	credentials := NewServiceAccountTokenCredentials(FileTokenSource{Path: filepath.Join(t.TempDir(), "missing")})
	_, err := credentials.GetRequestMetadata(context.Background())
	if err == nil {
		t.Fatal("GetRequestMetadata accepted missing token file")
	}
	if strings.Contains(err.Error(), "secret-token") || strings.Contains(err.Error(), "bearer") {
		t.Fatalf("error leaks token-shaped text: %v", err)
	}
}

func TestServiceAccountTokenCredentialsRedactLoadedInvalidToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	const rawToken = "secret-token with-space"
	if err := os.WriteFile(path, []byte(rawToken), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	credentials := NewServiceAccountTokenCredentials(FileTokenSource{Path: path})
	_, err := credentials.GetRequestMetadata(context.Background())
	if err == nil {
		t.Fatal("GetRequestMetadata accepted malformed token file")
	}
	for _, forbidden := range []string{rawToken, "secret-token", "bearer secret-token"} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("error %q leaks forbidden token text %q", err.Error(), forbidden)
		}
	}
	if err.Error() != "service account token unavailable" {
		t.Fatalf("error = %q; want canonical safe message", err.Error())
	}
}
