package testinfra

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerificationWorkflowStructure(t *testing.T) {
	root := testRepositoryRoot(t)
	if err := VerifyWorkflowSkeletons(root); err != nil {
		t.Fatal(err)
	}
	if err := VerifyPullRequestWorkflow(root); err != nil {
		t.Fatal(err)
	}
	if err := VerifyLegacyShadowSidecar(root); err != nil {
		t.Fatal(err)
	}
}

func TestPullRequestWorkflowRejectsMissingRaceShard(t *testing.T) {
	root := testRepositoryRoot(t)
	fixture := t.TempDir()
	source := filepath.Join(root, ".github", "workflows", "pull-request-verification.yml")
	// The source is a repository-owned workflow fixture.
	//nolint:gosec
	body, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(string(body), "shard: [0, 1, 2, 3]", "shard: [0, 1, 2]", 1))
	destination := filepath.Join(fixture, ".github", "workflows")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	fixturePath := filepath.Join(destination, "pull-request-verification.yml")
	// fixturePath is rooted in t.TempDir and uses a fixed filename.
	//nolint:gosec
	if err := os.WriteFile(fixturePath, body, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyPullRequestWorkflow(fixture); err == nil {
		t.Fatal("missing required Race shard passed")
	}
}
