package filetool

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

func TestWriteCreatesParentsAndReportsBytes(t *testing.T) {
	fixture := newWriteFixture(t)
	result, toolErr := Write(fixture.payload(map[string]any{
		"path":    "nested/out.txt",
		"content": "hello",
	}))
	if toolErr != nil {
		t.Fatalf("Write error = %+v", toolErr)
	}
	if !result.Created || result.BytesWritten != len("hello") {
		t.Fatalf("result = %+v; want created with byte count", result)
	}
	if got := fixture.read("nested/out.txt"); got != "hello" {
		t.Fatalf("file content = %q; want hello", got)
	}
	info, err := os.Stat(filepath.Join(fixture.workspace, "nested", "out.txt"))
	if err != nil {
		t.Fatalf("stat written file: %v", err)
	}
	if info.Mode().Perm() != 0o644 {
		t.Fatalf("mode = %v; want 0644 for new file", info.Mode().Perm())
	}
}

func TestWriteRejectsFilePathAliasWithoutPath(t *testing.T) {
	fixture := newWriteFixture(t)
	_, toolErr := Write(fixture.payload(map[string]any{"file_path": "note.txt", "content": "x"}))
	if toolErr == nil || toolErr.Kind != "invalid_input" || toolErr.Message != "path is required" {
		t.Fatalf("Write error = %+v; want missing path invalid_input", toolErr)
	}
	if _, err := os.Stat(filepath.Join(fixture.workspace, "note.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("note.txt stat err = %v; want no write", err)
	}
}

func TestWriteOverwritePreservesExistingMode(t *testing.T) {
	fixture := newWriteFixture(t)
	fixture.write("note.txt", "old", 0o600)
	result, toolErr := Write(fixture.payload(map[string]any{
		"path":    "note.txt",
		"content": "new",
	}))
	if toolErr != nil {
		t.Fatalf("Write error = %+v", toolErr)
	}
	if result.Created || result.BytesWritten != len("new") {
		t.Fatalf("result = %+v; want overwrite byte count", result)
	}
	if got := fixture.read("note.txt"); got != "new" {
		t.Fatalf("content = %q; want new", got)
	}
	info, err := os.Stat(filepath.Join(fixture.workspace, "note.txt"))
	if err != nil {
		t.Fatalf("stat note: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %v; want preserved 0600", info.Mode().Perm())
	}
}

func TestWriteRejectsDirectoryAndReadOnlyRoot(t *testing.T) {
	fixture := newWriteFixture(t)
	if err := os.Mkdir(filepath.Join(fixture.workspace, "dir"), 0o755); err != nil {
		t.Fatalf("mkdir dir: %v", err)
	}
	_, toolErr := Write(fixture.payload(map[string]any{"path": "dir", "content": "x"}))
	if toolErr == nil || toolErr.Kind != "is_directory" {
		t.Fatalf("directory error = %+v; want is_directory", toolErr)
	}

	readOnlyRoot := t.TempDir()
	payload := fixture.payload(map[string]any{"path": filepath.Join(readOnlyRoot, "data.txt"), "content": "x"})
	payload.Roots = append(payload.Roots, protocol.Root{Path: readOnlyRoot, Mode: protocol.RootModeRead})
	_, toolErr = Write(payload)
	if toolErr == nil || toolErr.Kind != "path_escape" {
		t.Fatalf("readonly root error = %+v; want path_escape", toolErr)
	}
}

func TestWritePreservesSpecialModeBits(t *testing.T) {
	fixture := newWriteFixture(t)
	fixture.write("special.txt", "old", 0o640)
	path := filepath.Join(fixture.workspace, "special.txt")
	want := os.FileMode(0o640) | os.ModeSetuid | os.ModeSetgid | os.ModeSticky
	if err := os.Chmod(path, want); err != nil {
		t.Fatalf("chmod special: %v", err)
	}
	if _, toolErr := Write(fixture.payload(map[string]any{"path": "special.txt", "content": "new"})); toolErr != nil {
		t.Fatalf("Write error = %+v", toolErr)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat special: %v", err)
	}
	if got := info.Mode() & (os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky); got != want {
		t.Fatalf("mode = %v; want %v", got, want)
	}
}

func TestWriteTreatsParentFileAsWriteFailure(t *testing.T) {
	fixture := newWriteFixture(t)
	fixture.write("parent", "not a directory", 0o644)

	_, toolErr := Write(fixture.payload(map[string]any{"path": "parent/child.txt", "content": "x"}))
	if toolErr == nil || toolErr.Kind != "write_failed" {
		t.Fatalf("parent-file write error = %+v; want write_failed", toolErr)
	}
	if got := fixture.read("parent"); got != "not a directory" {
		t.Fatalf("parent file content = %q; want unchanged", got)
	}
}

func TestWriteWorkspaceBoundReadOnlyResourceFailsAtHelperBoundary(t *testing.T) {
	fixture := newWriteFixture(t)
	resourceDir := filepath.Join(fixture.workspace, "data")
	if err := os.Mkdir(resourceDir, 0o755); err != nil {
		t.Fatalf("mkdir resource dir: %v", err)
	}
	resourcePath := filepath.Join(resourceDir, "report.csv")
	if err := os.WriteFile(resourcePath, []byte("old"), 0o444); err != nil {
		t.Fatalf("write resource: %v", err)
	}

	payload := fixture.payload(map[string]any{"path": resourcePath, "content": "new"})
	payload.Roots = append(payload.Roots, protocol.Root{Path: resourcePath, Mode: protocol.RootModeRead})

	_, toolErr := Write(payload)
	if toolErr == nil || toolErr.Kind != "path_escape" {
		t.Fatalf("workspace-bound resource write error = %+v; want path_escape", toolErr)
	}
	if got := fixture.read(filepath.Join("data", "report.csv")); got != "old" {
		t.Fatalf("resource content after failed write = %q; want old", got)
	}
}

func TestWriteRenameFailureLeavesOriginalAndRemovesTemp(t *testing.T) {
	fixture := newWriteFixture(t)
	fixture.write("note.txt", "old", 0o600)
	previous := renameForAtomicWrite
	renameForAtomicWrite = func(_, _ string) error { return errors.New("rename boom") }
	t.Cleanup(func() { renameForAtomicWrite = previous })

	_, toolErr := Write(fixture.payload(map[string]any{"path": "note.txt", "content": "new"}))
	if toolErr == nil || toolErr.Kind != "write_failed" {
		t.Fatalf("Write error = %+v; want write_failed", toolErr)
	}
	if got := fixture.read("note.txt"); got != "old" {
		t.Fatalf("content after failed rename = %q; want old", got)
	}
	entries, err := os.ReadDir(fixture.workspace)
	if err != nil {
		t.Fatalf("readdir workspace: %v", err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".tetral-tmp-") {
			t.Fatalf("temporary file left behind: %s", entry.Name())
		}
	}
}

type writeFixture struct {
	t         *testing.T
	workspace string
}

func newWriteFixture(t *testing.T) writeFixture {
	t.Helper()
	return writeFixture{t: t, workspace: t.TempDir()}
}

func (f writeFixture) payload(input map[string]any) protocol.Payload {
	f.t.Helper()
	encoded, err := json.Marshal(input)
	if err != nil {
		f.t.Fatalf("marshal input: %v", err)
	}
	return protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "write",
		ToolUseEventID: "evt_write",
		WorkspaceRoot:  f.workspace,
		Roots:          []protocol.Root{{Path: f.workspace, Mode: protocol.RootModeReadWrite}},
		Input:          encoded,
	}
}

func (f writeFixture) write(name string, content string, mode os.FileMode) {
	f.t.Helper()
	path := filepath.Join(f.workspace, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		f.t.Fatalf("mkdir fixture: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		f.t.Fatalf("write fixture: %v", err)
	}
}

func (f writeFixture) read(name string) string {
	f.t.Helper()
	body, err := os.ReadFile(filepath.Join(f.workspace, filepath.FromSlash(name)))
	if err != nil {
		f.t.Fatalf("read fixture: %v", err)
	}
	return string(body)
}
