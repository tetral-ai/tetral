package auth_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	documentedPrivateKeyCommand = "openssl pkey -in ip.pem -outform DER         | tail -c 32 | base64 -w0"
	documentedPublicKeyCommand  = "openssl pkey -in ip.pem -pubout -outform DER | tail -c 32 | base64 -w0"
)

func TestBootstrapDocumentedEd25519PairRoundTripsAcrossSignerAndVerifier(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	docPath := filepath.Join(filepath.Dir(sourceFile), "..", "..", "docs", "bootstrap.md")
	doc, err := os.ReadFile(docPath) //nolint:gosec // Repository-local documentation path.
	if err != nil {
		t.Fatalf("read bootstrap documentation: %v", err)
	}
	for _, command := range []string{
		"openssl genpkey -algorithm ed25519 -out ip.pem",
		documentedPrivateKeyCommand,
		documentedPublicKeyCommand,
	} {
		if !strings.Contains(string(doc), command) {
			t.Fatalf("bootstrap documentation does not contain %q", command)
		}
	}

	dir := t.TempDir()
	runDocumentedOpenSSLCommand(t, dir, "openssl genpkey -algorithm ed25519 -out ip.pem")
	privateKey := runDocumentedOpenSSLCommand(t, dir, documentedPrivateKeyCommand)
	publicKey := runDocumentedOpenSSLCommand(t, dir, documentedPublicKeyCommand)

	signer, err := auth.NewInternalPrincipalSignerFromBase64(privateKey)
	if err != nil {
		t.Fatalf("build signer from documented private output: %v", err)
	}
	if got := signer.PublicKeyBase64(); got != publicKey {
		t.Fatalf("derived public key = %q; documented public key = %q", got, publicKey)
	}
	verifier, err := auth.NewInternalPrincipalVerifierFromBase64(publicKey)
	if err != nil {
		t.Fatalf("build verifier from documented public output: %v", err)
	}
	principal := auth.Principal{
		Workspace: workspace.Workspace{ID: "docs-round-trip", Type: "workspace"},
		APIKeyID:  "ak_docs_round_trip",
	}
	token, err := signer.Mint(principal, "GET", "/v1/sessions", "req_docs_round_trip", time.Minute)
	if err != nil {
		t.Fatalf("mint with documented private key: %v", err)
	}
	verified, _, err := verifier.Verify(token, "GET", "/v1/sessions")
	if err != nil {
		t.Fatalf("verify with separately documented public key: %v", err)
	}
	if verified.Workspace.ID != principal.Workspace.ID || verified.APIKeyID != principal.APIKeyID {
		t.Fatalf("verified principal = %#v; want %#v", verified, principal)
	}
}

func runDocumentedOpenSSLCommand(t *testing.T, dir string, command string) string {
	t.Helper()
	cmd := exec.CommandContext(context.Background(), "sh", "-ceu", command) //nolint:gosec // Commands are fixed documentation fixtures.
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run %q: %v: %s", command, err, output)
	}
	return strings.TrimSpace(string(output))
}
