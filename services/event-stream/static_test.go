package eventstream_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEventStreamProductionCodeKeepsReadOnlyRuntimeBoundary(t *testing.T) {
	root := filepath.Clean("../..")
	for _, packageDir := range []string{"internal/eventstream", "services/event-stream", "services/event-stream/cmd/event-stream"} {
		for _, fileName := range productionGoFiles(t, root, packageDir) {
			body, err := os.ReadFile(fileName) //nolint:gosec // G304: repository-local production file discovered from fixed package roots.
			if err != nil {
				t.Fatalf("read %s: %v", fileName, err)
			}
			source := string(body)
			for _, forbidden := range []string{
				"AppendClientEvents",
				"queue.Enqueue",
				"INSERT INTO session_events",
				"UPDATE session_events",
				"DELETE FROM session_events",
				"session_runtime_inbox",
				"RunTool",
				"SendCommandInput",
				"ReadCommandResult",
				"CancelCommand",
				"ResolveRuntimeTarget",
			} {
				if strings.Contains(source, forbidden) {
					t.Fatalf("%s contains forbidden write/runtime boundary token %q", fileName, forbidden)
				}
			}
		}
	}
}

func TestEventStreamProductionCodeDoesNotImportExecutionOwners(t *testing.T) {
	root := filepath.Clean("../..")
	for _, packageDir := range []string{"internal/eventstream", "services/event-stream", "services/event-stream/cmd/event-stream"} {
		for _, path := range productionGoFiles(t, root, packageDir) {
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			for _, imported := range file.Imports {
				value := strings.Trim(imported.Path.Value, `"`)
				for _, forbidden := range []string{
					"github.com/tetral-ai/tetral/internal/sessionevent",
					"github.com/tetral-ai/tetral/internal/agentruntimebridge",
					"github.com/tetral-ai/tetral/internal/kubernetes",
					"github.com/tetral-ai/tetral/services/bridge",
					"github.com/tetral-ai/tetral/services/agent-runtime",
				} {
					if value == forbidden {
						t.Fatalf("%s imports forbidden execution owner %s", path, value)
					}
				}
			}
		}
	}
}

func productionGoFiles(t *testing.T, root string, packageDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, packageDir))
	if err != nil {
		t.Fatalf("read %s: %v", packageDir, err)
	}
	var paths []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		paths = append(paths, filepath.Join(root, packageDir, entry.Name()))
	}
	return paths
}
