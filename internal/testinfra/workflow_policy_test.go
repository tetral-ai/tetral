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
	if err := VerifyMainBranchWorkflow(root); err != nil {
		t.Fatal(err)
	}
	if err := VerifyScheduledWorkflow(root); err != nil {
		t.Fatal(err)
	}
}

func TestGoWorkflowsRejectMissingTransportRenderer(t *testing.T) {
	root := testRepositoryRoot(t)
	for _, tc := range []struct {
		file   string
		verify func(string) error
	}{
		{"pull-request-verification.yml", VerifyPullRequestWorkflow},
		{"main-branch-verification.yml", VerifyMainBranchWorkflow},
	} {
		t.Run(tc.file, func(t *testing.T) {
			body, err := os.ReadFile(filepath.Join(root, ".github", "workflows", tc.file))
			if err != nil {
				t.Fatal(err)
			}
			fixture := t.TempDir()
			if err := os.MkdirAll(filepath.Join(fixture, ".github", "workflows"), 0o700); err != nil {
				t.Fatal(err)
			}
			writeTestFile(t, fixture, ".github/workflows/"+tc.file, string(body))
			if err := tc.verify(fixture); err != nil {
				t.Fatalf("positive control: %v", err)
			}
			changed := strings.Replace(string(body), "          shard-count: '4'\n          needs-helm: 'true'", "          shard-count: '4'\n          needs-helm: 'false'", 1)
			if changed == string(body) {
				t.Fatal("missing Go shard renderer input")
			}
			writeTestFile(t, fixture, ".github/workflows/"+tc.file, changed)
			if err := tc.verify(fixture); err == nil {
				t.Fatal("Go workflow accepted missing transport renderer")
			}
			if tc.file == "main-branch-verification.yml" {
				changed = strings.Replace(string(body), "version: v4.2.0", "version: latest", 1)
				writeTestFile(t, fixture, ".github/workflows/"+tc.file, changed)
				if err := tc.verify(fixture); err == nil {
					t.Fatal("coverage accepted an unpinned transport renderer")
				}
			}
		})
	}
}

func TestCoveragePrerequisitesIncludeProductionTransportImages(t *testing.T) {
	plan, err := CoveragePlan(testRepositoryRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, dependency := range []string{"postgresql", "minio", "nats", "sdk", "bun-workspaces", "docker", "envoy", "bun-image"} {
		if !contains(plan.Dependencies, dependency) {
			t.Errorf("coverage omitted %s", dependency)
		}
	}
}
