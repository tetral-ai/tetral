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

func TestKubernetesVisibilityCodeDoesNotExposeMutationSurfaces(t *testing.T) {
	forbidden := []string{"Secrets(", "Jobs(", "Deployments(", ".Create(", ".Update(", ".Patch(", ".Delete(", ".DeleteCollection(", "Scale(", "Exec", "Attach"}
	err := filepath.WalkDir(".", func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		body, err := os.ReadFile(path) //nolint:gosec // G304: test scans package-local Go files discovered by WalkDir.
		if err != nil {
			return err
		}
		text := string(body)
		for _, token := range forbidden {
			if strings.Contains(text, token) {
				t.Fatalf("%s exposes forbidden Kubernetes mutation/control surface %q", path, token)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk kubernetes package: %v", err)
	}
}

func TestVisibilityClientOnlyListsAndWatchesPodsAndEndpointSlices(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "visibility_client.go", nil, 0)
	if err != nil {
		t.Fatalf("parse visibility_client.go: %v", err)
	}
	allowed := map[string]bool{
		"ListPods":            true,
		"WatchPods":           true,
		"ListEndpointSlices":  true,
		"WatchEndpointSlices": true,
	}
	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.TYPE {
			continue
		}
		for _, spec := range general.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != "VisibilityClient" {
				continue
			}
			iface, ok := typeSpec.Type.(*ast.InterfaceType)
			if !ok {
				t.Fatal("VisibilityClient is not an interface")
			}
			for _, method := range iface.Methods.List {
				if len(method.Names) != 1 || !allowed[method.Names[0].Name] {
					t.Fatalf("VisibilityClient method %v is outside list/watch Pod/EndpointSlice surface", method.Names)
				}
			}
			return
		}
	}
	t.Fatal("VisibilityClient interface not found")
}
