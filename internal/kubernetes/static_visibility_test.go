package kubernetes_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

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
