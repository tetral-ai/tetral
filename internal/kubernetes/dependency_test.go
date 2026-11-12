package kubernetes_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func TestKubernetesPackageHasNoMutableGlobals(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), entry.Name(), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		for _, declaration := range file.Decls {
			general, ok := declaration.(*ast.GenDecl)
			if ok && general.Tok == token.VAR {
				t.Fatalf("%s contains package-level mutable var", entry.Name())
			}
		}
	}
}

func TestKubernetesPackageDoesNotOwnControlPlaneState(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		entryName := entry.Name()
		file, err := parser.ParseFile(token.NewFileSet(), entryName, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", entryName, err)
		}
		for _, imported := range file.Imports {
			value := strings.Trim(imported.Path.Value, `"`)
			for _, forbidden := range []string{
				"github.com/tetral-ai/tetral/internal/run",
				"github.com/tetral-ai/tetral/internal/sessionevent",
				"github.com/tetral-ai/tetral/internal/scheduler",
			} {
				if value == forbidden {
					t.Fatalf("%s imports %s; Kubernetes adapter must not own control-plane state", entryName, value)
				}
			}
		}
	}
}
