package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerifyArchiveIdentityBindsCommitTreeAndBytes(t *testing.T) {
	repository := t.TempDir()
	runGit(t, repository, "init", "--quiet")
	runGit(t, repository, "config", "user.name", "Tetral Test")
	runGit(t, repository, "config", "user.email", "test@example.invalid")
	workflowPath := filepath.Join(repository, ".github", "workflows")
	if err := os.MkdirAll(workflowPath, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workflowPath, "engine-ci.yml"), []byte("name: legacy\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "add", ".")
	runGit(t, repository, "commit", "--quiet", "-m", "fixture")
	commit := strings.TrimSpace(string(runGit(t, repository, "rev-parse", "HEAD")))
	tree := strings.TrimSpace(string(runGit(t, repository, "rev-parse", "HEAD^{tree}")))
	archive := runGit(t, repository, "archive", "--format=tar", commit)

	if err := verifyArchiveIdentity(repository, commit, tree, archive); err != nil {
		t.Fatal(err)
	}
	if err := verifyArchiveIdentity(repository, commit, "wrong-tree", archive); err == nil {
		t.Fatal("wrong tree identity passed")
	}
	tampered := append(append([]byte(nil), archive...), byte('x'))
	if err := verifyArchiveIdentity(repository, commit, tree, tampered); err == nil {
		t.Fatal("tampered archive passed")
	}
}

func runGit(t *testing.T, repository string, args ...string) []byte {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", repository}, args...)...) //nolint:gosec // fixed test executable and generated fixture arguments.
	output, err := command.Output()
	if err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
	return output
}
