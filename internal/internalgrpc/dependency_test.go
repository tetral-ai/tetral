package internalgrpc_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInternalGRPCDoesNotImportWorkloadSpecificGeneratedServices(t *testing.T) {
	for _, dir := range []string{".", "auth"} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read %s: %v", dir, err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(dir, entry.Name())
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			for _, imported := range file.Imports {
				value := strings.Trim(imported.Path.Value, `"`)
				for _, forbidden := range []string{
					"github.com/tetral-ai/tetral/internal/gen/tetral/agent" + "_runtime_bridge/v1",
					"github.com/tetral-ai/tetral/internal/gen/tetral/mcp" + "_connector/v1",
					"github.com/tetral-ai/tetral/internal/agentruntimebridge",
					"github.com/tetral-ai/tetral/internal/mcp" + "connector",
					"github.com/tetral-ai/tetral/services/bridge",
					"github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1",
				} {
					if value == forbidden {
						t.Fatalf("%s imports workload-specific package %s", path, value)
					}
				}
			}
		}
	}
}
