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

func TestActionInventoryRejectsRepositorySubstitutionAtApprovedSHA(t *testing.T) {
	inventory := ActionInventory{Version: 1, ReviewedAt: "2026-08-28", Actions: []ActionRecord{{Repository: "actions/checkout", SHA: "df4cb1c069e1874edd31b4311f1884172cec0e10", ReleaseTag: "v6.0.3", ObjectType: "commit", TargetSHA: "df4cb1c069e1874edd31b4311f1884172cec0e10"}}}
	root := t.TempDir()
	directory := filepath.Join(root, ".github", "workflows")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	workflow := "name: fixture\non: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: attacker/checkout@df4cb1c069e1874edd31b4311f1884172cec0e10\n"
	if err := os.WriteFile(filepath.Join(directory, "fixture.yml"), []byte(workflow), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyActionInventory(root, inventory); err == nil {
		t.Fatal("repository substitution passed")
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
