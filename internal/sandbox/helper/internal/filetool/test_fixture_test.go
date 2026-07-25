package filetool

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

type readFixture struct {
	t         *testing.T
	workspace string
}

func newReadFixture(t *testing.T) readFixture {
	t.Helper()
	return readFixture{t: t, workspace: t.TempDir()}
}

func (f readFixture) payload(input map[string]any) protocol.Payload {
	f.t.Helper()
	encoded, err := json.Marshal(input)
	if err != nil {
		f.t.Fatalf("marshal input: %v", err)
	}
	return protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "read",
		ToolUseEventID: "evt_read",
		WorkspaceRoot:  f.workspace,
		Roots: []protocol.Root{
			{Path: f.workspace, Mode: protocol.RootModeReadWrite},
		},
		Limits: protocol.Limits{VisibleBytes: readByteBudget, VisibleLines: defaultReadLines},
		Input:  encoded,
	}
}

func (f readFixture) write(name string, content string) {
	f.t.Helper()
	f.writeBytes(name, []byte(content))
}

func (f readFixture) writeBytes(name string, content []byte) {
	f.t.Helper()
	path := filepath.Join(f.workspace, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		f.t.Fatalf("mkdir fixture: %v", err)
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		f.t.Fatalf("write fixture: %v", err)
	}
}

func (f readFixture) writeExternalSymlink(linkName string, outsideDir string, targetName string, content string) {
	f.t.Helper()
	target := filepath.Join(outsideDir, targetName)
	if err := os.WriteFile(target, []byte(content), 0o600); err != nil {
		f.t.Fatalf("write outside: %v", err)
	}
	if err := os.Symlink(target, filepath.Join(f.workspace, linkName)); err != nil {
		f.t.Fatalf("symlink fixture: %v", err)
	}
}
