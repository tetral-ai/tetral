package files_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestFilesPackageDependencyBoundary(t *testing.T) {
	dir := filesPackageDir(t)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("ParseFile %s: %v", entry.Name(), err)
		}
		for _, imp := range file.Imports {
			importPath := strings.Trim(imp.Path.Value, `"`)
			awsSDKRoot := "github.com/" + "aws/" + "aws-sdk-go-v2"
			if strings.HasPrefix(importPath, awsSDKRoot) {
				t.Fatalf("%s imports AWS SDK %q; Files must consume internal/blob.BlobStore only", entry.Name(), importPath)
			}
			for _, forbidden := range []string{
				"github.com/tetral-ai/tetral/internal/httpapi",
				"github.com/tetral-ai/backend",
				"github.com/tetral-ai/gateway",
				"github.com/tetral-ai/runtime",
			} {
				if strings.HasPrefix(importPath, forbidden) {
					t.Fatalf("%s imports forbidden package %q", entry.Name(), importPath)
				}
			}
			if importPath == "net/http" {
				t.Fatalf("%s imports net/http; Files domain must stay below HTTP", entry.Name())
			}
			yamlPath := "gopkg.in/" + "yaml.v3"
			if importPath == yamlPath {
				t.Fatalf("%s imports yaml; YAML remains confined to internal/skill", entry.Name())
			}
		}
	}
}

func TestFilesDependencyTestHasRealAssertions(t *testing.T) {
	// Guard against accidentally neutering the dependency test into a
	// parser-only smoke test. It must inspect import declarations.
	dir := filesPackageDir(t)
	path := filepath.Join(dir, "dependency_test.go")
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("ParseFile: %v", err)
	}
	foundImportLoop := false
	ast.Inspect(file, func(node ast.Node) bool {
		rangeStmt, ok := node.(*ast.RangeStmt)
		if !ok {
			return true
		}
		if ident, ok := rangeStmt.Value.(*ast.Ident); ok && ident.Name == "imp" {
			foundImportLoop = true
		}
		return true
	})
	if !foundImportLoop {
		t.Fatal("dependency test must iterate import declarations")
	}
}

func filesPackageDir(t *testing.T) string {
	t.Helper()
	_, path, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(path)
}
