package resourceprojection

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/sandbox/driver"
)

func TestDeletedFileCleanupCommandRemovesDirectoryRecursively(t *testing.T) {
	target := filepath.Join(t.TempDir(), "deleted")
	nestedFile := filepath.Join(target, "nested", "deeper", "payload.txt")
	if err := os.MkdirAll(filepath.Dir(nestedFile), 0o755); err != nil {
		t.Fatalf("create deleted target directory tree: %v", err)
	}
	if err := os.WriteFile(nestedFile, []byte("deleted resource"), 0o600); err != nil {
		t.Fatalf("write deleted target nested file: %v", err)
	}

	command := DeletedFileCleanupCommand([]DeletedFileCleanupTarget{{
		ResourceID: "sesrsc_deleted",
		MountPath:  target,
	}}, false)

	if !strings.Contains(command, "sudo rm -rf -- "+shellQuote(target)) {
		t.Fatalf("cleanup command does not recursively remove the target directory:\n%s", command)
	}
	for _, forbidden := range []string{"rm -f -- " + shellQuote(target), "|| true"} {
		if strings.Contains(command, forbidden) {
			t.Fatalf("cleanup command contains failure-acknowledging fragment %q:\n%s", forbidden, command)
		}
	}

	// The production sandbox has passwordless sudo. This test-owned path needs
	// no privilege change, so the shell function preserves command execution
	// semantics while keeping the generated script byte-for-byte unchanged.
	shellCommand := "sudo() { \"$@\"; }\n" + command
	output, err := exec.Command("/bin/sh", "-c", shellCommand).CombinedOutput()
	if err != nil {
		t.Fatalf("execute deleted-file cleanup command: %v\noutput:\n%s\ncommand:\n%s", err, output, command)
	}
	if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleted target still exists after recursive cleanup: stat error = %v", err)
	}
}

func TestRunDeletedFileCleanupPropagatesCommandFailure(t *testing.T) {
	wantErr := errors.New("injected recursive removal failure")
	runner := &failingCleanupCommandRunner{err: wantErr}

	err := RunDeletedFileCleanup(
		context.Background(),
		runner,
		driver.PreparationCommandTarget{ProviderSandboxID: "provider_sandbox"},
		[]DeletedFileCleanupTarget{{ResourceID: "sesrsc_deleted", MountPath: "/mnt/session/uploads/deleted"}},
		false,
		45*time.Second,
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("RunDeletedFileCleanup error = %v; want injected command failure", err)
	}
	if runner.calls != 1 {
		t.Fatalf("command calls = %d; want 1", runner.calls)
	}
}

type failingCleanupCommandRunner struct {
	err   error
	calls int
}

func (r *failingCleanupCommandRunner) RunPreparationCommand(context.Context, driver.PreparationCommandTarget, string, map[string]string, time.Duration) error {
	r.calls++
	return r.err
}
