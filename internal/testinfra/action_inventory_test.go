package testinfra

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRepositoryActionsMatchReviewedInventory(t *testing.T) {
	root := testRepositoryRoot(t)
	inventory, err := LoadActionInventory()
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyActionInventory(root, inventory); err != nil {
		t.Fatal(err)
	}
}

func TestActionInventoryMatchesReferencesByRepositoryAndSHA(t *testing.T) {
	const approved = "55cc8345863c7cc4c66a329aec7e433d2d1c52a9"
	inventory := ActionInventory{Version: 1, ReviewedAt: "2026-10-10", Actions: []ActionRecord{{Repository: "actions/cache", SHA: approved, ReleaseTag: "v6.1.0", ObjectType: "commit", TargetSHA: approved}}}
	for _, tc := range []struct {
		name, uses string
		approved   bool
	}{
		{name: "repository substitution at the approved SHA", uses: "attacker/cache@" + approved},
		{name: "sub-path actions of the approved repository and SHA", uses: "actions/cache/restore@" + approved + "\n      - uses: actions/cache/save@" + approved, approved: true},
		{name: "sub-path action at another SHA", uses: "actions/cache/save@df4cb1c069e1874edd31b4311f1884172cec0e10"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			directory := filepath.Join(root, ".github", "workflows")
			if err := os.MkdirAll(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			workflow := "name: fixture\non: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: " + tc.uses + "\n"
			if err := os.WriteFile(filepath.Join(directory, "fixture.yml"), []byte(workflow), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := VerifyActionInventory(root, inventory); (err == nil) != tc.approved {
				t.Fatalf("inventory verification error = %v; want approved %v", err, tc.approved)
			}
		})
	}
}

func testRepositoryRoot(t *testing.T) string {
	t.Helper()
	current, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(current, "go.mod")); err == nil {
			return current
		}
		parent := filepath.Dir(current)
		if parent == current {
			t.Fatal("repository root not found")
		}
		current = parent
	}
}
