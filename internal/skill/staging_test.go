package skill_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/skill"
)

// TestNewStageDirCreatesOwnerOnlyDir pins that the helper used to
// build the per-process package staging parent creates a directory
// not visible to other users.
func TestNewStageDirCreatesOwnerOnlyDir(t *testing.T) {
	parent := t.TempDir()
	dir, err := skill.NewStageDir(parent)
	if err != nil {
		t.Fatalf("NewStageDir: %v", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("NewStageDir did not create a dir: %s", dir)
	}
	mode := info.Mode().Perm()
	if mode&0o077 != 0 {
		t.Errorf("stage dir mode = %04o; must not be group/other readable", mode)
	}
	if !strings.HasPrefix(dir, parent+string(filepath.Separator)) {
		t.Errorf("stage dir %q is not under parent %q", dir, parent)
	}
}
