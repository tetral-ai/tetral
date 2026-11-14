package auth_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// The commands of the bootstrap document's key block are run as the document
// writes them. Among what they print are a private seed the signer accepts
// and the public key that belongs to it, which the verifier accepts.
func TestBootstrapDocumentCommandsMakeAPairTheSignerAndVerifierAccept(t *testing.T) {
	outputs := runDocumentedCommands(t, filepath.Join("..", "..", "docs", "bootstrap.md"), "ed25519")
	var signer *auth.InternalPrincipalSigner
	for _, output := range outputs {
		candidate, err := auth.NewInternalPrincipalSignerFromBase64(output)
		if err == nil && slices.Contains(outputs, candidate.PublicKeyBase64()) {
			signer = candidate
		}
	}
	if signer == nil {
		t.Fatalf("the %d outputs of the documented commands hold no private seed together with its public key", len(outputs))
	}
	verifier, err := auth.NewInternalPrincipalVerifierFromBase64(signer.PublicKeyBase64())
	if err != nil {
		t.Fatalf("build verifier from the documented public output: %v", err)
	}
	principal := auth.IndependentKeyPrincipal(workspace.Workspace{ID: "docs-round-trip", Type: "workspace"}, "ak_docs_round_trip")
	token, err := signer.Mint(principal, "GET", "/v1/sessions", "req_docs_round_trip", time.Minute)
	if err != nil {
		t.Fatalf("mint with the documented private key: %v", err)
	}
	verified, _, err := verifier.Verify(token, "GET", "/v1/sessions")
	if err != nil {
		t.Fatalf("verify with the documented public key: %v", err)
	}
	if verified.Workspace.ID != principal.Workspace.ID || verified.APIKeyID != principal.APIKeyID {
		t.Fatalf("verified principal = %#v; want %#v", verified, principal)
	}
}

// runDocumentedCommands runs, in one empty directory, each line of the
// document's fenced shell block that mentions subject, and returns what the
// lines print.
func runDocumentedCommands(t *testing.T, document, subject string) []string {
	t.Helper()
	text, err := os.ReadFile(document) //nolint:gosec // Repository-local documentation path.
	if err != nil {
		t.Fatal(err)
	}
	var block []string
	for _, candidate := range fencedBlocks(string(text)) {
		if candidate.info == "bash" && strings.Contains(strings.Join(candidate.lines, "\n"), subject) {
			block = candidate.lines
			break
		}
	}
	if block == nil {
		t.Fatalf("%s has no shell block about %s", document, subject)
	}
	dir := t.TempDir()
	var outputs []string
	for _, line := range block {
		if strings.TrimSpace(line) == "" {
			continue
		}
		command := exec.CommandContext(context.Background(), "sh", "-ceu", line) //nolint:gosec // The repository's own documented commands.
		command.Dir = dir
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("run %q: %v: %s", line, err, output)
		}
		if printed := strings.TrimSpace(string(output)); printed != "" {
			outputs = append(outputs, printed)
		}
	}
	return outputs
}

type fencedBlock struct {
	info  string
	lines []string
}

// fencedBlocks returns the Markdown document's fenced code blocks: a fence
// is a run of at least three backticks or tildes, and a block ends at a run
// of the same character at least as long.
func fencedBlocks(document string) []fencedBlock {
	var blocks []fencedBlock
	var open string
	var current fencedBlock
	for _, line := range strings.Split(document, "\n") {
		trimmed := strings.TrimSpace(line)
		if open == "" {
			for _, mark := range []string{"`", "~"} {
				if run := len(trimmed) - len(strings.TrimLeft(trimmed, mark)); run >= 3 {
					open = strings.Repeat(mark, run)
					current = fencedBlock{}
					if fields := strings.Fields(trimmed[run:]); len(fields) > 0 {
						current.info = fields[0]
					}
				}
			}
			continue
		}
		if strings.HasPrefix(trimmed, open) && strings.Trim(trimmed, open[:1]) == "" {
			blocks = append(blocks, current)
			open = ""
			continue
		}
		current.lines = append(current.lines, line)
	}
	return blocks
}
