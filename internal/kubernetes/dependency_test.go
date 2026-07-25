package kubernetes_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestKubernetesDependencyConfinedToKubernetesPackage(t *testing.T) {
	root := filepath.Clean("../..")
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if strings.HasPrefix(rel, "internal/kubernetes/") || strings.HasPrefix(rel, "internal/internalgrpc/auth/") {
			return nil
		}
		// Test files may import k8s.io/* only under the Kubernetes package
		// and its auth consumer (covered by the production prefixes above).
		// Any other test file importing k8s.io/* fails this guard.
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imported := range file.Imports {
			value := strings.Trim(imported.Path.Value, `"`)
			if strings.HasPrefix(value, "k8s.io/") {
				t.Fatalf("%s imports %s; Kubernetes imports must stay in internal/kubernetes or tests", rel, value)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk repo: %v", err)
	}
}

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
