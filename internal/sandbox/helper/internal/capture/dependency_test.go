package capture_test

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestCapturePackageDependencyBoundary(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	path := filepath.Join(filepath.Dir(thisFile), "capture.go")
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse capture.go: %v", err)
	}
	for _, spec := range file.Imports {
		importPath := strings.Trim(spec.Path.Value, `"`)
		if strings.Contains(importPath, ".") && importPath != "golang.org/x/sys/unix" && importPath != "github.com/tetral-ai/tetral/internal/sandbox/helper/protocol" {
			t.Fatalf("capture.go imports forbidden dependency %q", importPath)
		}
	}
}
