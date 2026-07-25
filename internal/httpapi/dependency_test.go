package httpapi_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSessionEventPackageDependencyBoundaries(t *testing.T) {
	root := filepath.Clean("../..")
	for _, testCase := range []struct {
		packageDir string
		forbidden  []string
	}{
		{
			packageDir: "internal/sessionevent",
			forbidden: []string{
				"github.com/tetral-ai/tetral/internal/httpapi",
				"github.com/tetral-ai/tetral/internal/kubernetes",
				"github.com/tetral-ai/tetral/internal/" + "runtimeauth",
				"github.com/tetral-ai/tetral/internal/" + "runtimeownership",
				"github.com/tetral-ai/tetral/internal/run",
				"github.com/tetral-ai/tetral/internal/scheduler",
				"github.com/tetral-ai/tetral/internal/" + "worker",
				"github.com/tetral-ai/tetral/internal/" + "workerlaunch",
			},
		},
	} {
		t.Run(testCase.packageDir, func(t *testing.T) {
			scanProductionImports(t, root, testCase.packageDir, testCase.forbidden)
		})
	}
}

func TestPublicAuthMiddlewareDoesNotImportOrCallRuntimeAuth(t *testing.T) {
	for _, fileName := range []string{"middleware.go", "router.go"} {
		source := readProductionFile(t, fileName)
		if strings.Contains(source, "internal/runtimeauth") || strings.Contains(source, "runtimeauth.") {
			t.Fatalf("%s references runtimeauth; public API-key auth must not call runtime worker auth", fileName)
		}
	}
}

func scanProductionImports(t *testing.T, root string, packageDir string, forbidden []string) {
	t.Helper()
	packagePath := filepath.Join(root, packageDir)
	entries, err := os.ReadDir(packagePath)
	if err != nil {
		t.Fatalf("read %s: %v", packageDir, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		filePath := filepath.Join(packagePath, entry.Name())
		file, err := parser.ParseFile(token.NewFileSet(), filePath, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", filePath, err)
		}
		for _, imported := range file.Imports {
			value := strings.Trim(imported.Path.Value, `"`)
			for _, forbiddenImport := range forbidden {
				if value == forbiddenImport {
					t.Fatalf("%s imports %s; violates runtime package boundary", filepath.Join(packageDir, entry.Name()), value)
				}
			}
		}
	}
}

func readProductionFile(t *testing.T, fileName string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Clean(fileName))
	if err != nil {
		t.Fatalf("read %s: %v", fileName, err)
	}
	return string(body)
}
