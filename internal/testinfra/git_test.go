package testinfra

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestInspectRevisionIncludesCommittedStagedUnstagedUntrackedAndRenameSides(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "-q")
	runGit(t, root, "config", "user.name", "Test")
	runGit(t, root, "config", "user.email", "test@example.invalid")
	writeTestFile(t, root, "committed.txt", "before")
	writeTestFile(t, root, "rename-old.txt", "rename")
	runGit(t, root, "add", ".")
	runGit(t, root, "commit", "-qm", "base")
	base := runGit(t, root, "rev-parse", "HEAD")

	writeTestFile(t, root, "committed.txt", "after")
	runGit(t, root, "add", "committed.txt")
	runGit(t, root, "commit", "-qm", "committed")
	writeTestFile(t, root, "staged.txt", "staged")
	runGit(t, root, "add", "staged.txt")
	writeTestFile(t, root, "unstaged.txt", "unstaged")
	writeTestFile(t, root, "untracked.txt", "untracked")
	runGit(t, root, "mv", "rename-old.txt", "rename-new.txt")

	revision := inspectRevision(root, base)
	want := map[string]bool{
		"committed.txt": true, "staged.txt": true, "unstaged.txt": true,
		"untracked.txt": true, "rename-old.txt": true, "rename-new.txt": true,
	}
	for _, path := range revision.ChangedPaths {
		delete(want, path)
	}
	if len(want) != 0 {
		t.Fatalf("missing change shapes: %#v; got %v", want, revision.ChangedPaths)
	}
}

func TestInspectRevisionFailsClosedForMissingBase(t *testing.T) {
	revision := inspectRevision(".", "refs/heads/does-not-exist")
	if revision.FullFallbackCause == "" {
		t.Fatal("missing base did not select a Full fallback")
	}
}

func TestInspectRevisionFailsClosedForUnrelatedBase(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "-q")
	runGit(t, root, "config", "user.name", "Test")
	runGit(t, root, "config", "user.email", "test@example.invalid")
	writeTestFile(t, root, "main.txt", "main")
	runGit(t, root, "add", ".")
	runGit(t, root, "commit", "-qm", "main")
	runGit(t, root, "checkout", "--orphan", "unrelated")
	writeTestFile(t, root, "other.txt", "other")
	runGit(t, root, "add", ".")
	runGit(t, root, "commit", "-qm", "unrelated")
	unrelated := runGit(t, root, "rev-parse", "HEAD")
	runGit(t, root, "checkout", "-q", "master")

	revision := inspectRevision(root, unrelated)
	if revision.FullFallbackCause == "" {
		t.Fatal("unrelated base did not select a Full fallback")
	}
}

func TestInspectRevisionFailsClosedForShallowRepository(t *testing.T) {
	source := t.TempDir()
	runGit(t, source, "init", "-q")
	runGit(t, source, "config", "user.name", "Test")
	runGit(t, source, "config", "user.email", "test@example.invalid")
	writeTestFile(t, source, "main.txt", "main")
	runGit(t, source, "add", ".")
	runGit(t, source, "commit", "-qm", "main")

	clone := filepath.Join(t.TempDir(), "clone")
	command := exec.Command("git", "clone", "-q", "--depth=1", "file://"+source, clone)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("create shallow clone: %v: %s", err, output)
	}
	revision := inspectRevision(clone, "HEAD")
	if revision.FullFallbackCause == "" {
		t.Fatal("shallow repository did not select a Full fallback")
	}
}

func runGit(t *testing.T, root string, arguments ...string) string {
	t.Helper()
	command := exec.Command("git", arguments...)
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		t.Fatalf("git %v: %v", arguments, err)
	}
	return strings.TrimSpace(string(output))
}

func writeTestFile(t *testing.T, root, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}
