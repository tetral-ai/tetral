package static_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"testing"
)

func TestSchemaOwnershipJobRunnerUsesProductionRuntimeDeliveryAssembly(t *testing.T) {
	root := schemaOwnershipEngineRoot(t)
	const path = "services/job-runner/cmd/job-runner/main.go"
	source := readSchemaOwnershipFile(t, filepath.Join(root, path))
	file, err := parser.ParseFile(token.NewFileSet(), path, source, 0)
	if err != nil {
		t.Fatal(err)
	}
	selector := func(expr ast.Expr, name string) bool {
		value, ok := expr.(*ast.SelectorExpr)
		if !ok || value.Sel.Name != name {
			return false
		}
		pkg, ok := value.X.(*ast.Ident)
		return ok && pkg.Name == "jobrunner"
	}
	assembled, wired := false, false
	ast.Inspect(file, func(node ast.Node) bool {
		if assignment, ok := node.(*ast.AssignStmt); ok && len(assignment.Lhs) == 1 && len(assignment.Rhs) == 1 {
			name, ok := assignment.Lhs[0].(*ast.Ident)
			call, called := assignment.Rhs[0].(*ast.CallExpr)
			if ok && called && name.Name == "deliveryStore" && selector(call.Fun, "NewJobRunnerRuntimeDeliveryStore") {
				assembled = true
			}
		}
		runner, ok := node.(*ast.CompositeLit)
		if !ok || !selector(runner.Type, "JobRunner") {
			return true
		}
		for _, element := range runner.Elts {
			field, ok := element.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, ok := field.Key.(*ast.Ident)
			if !ok || key.Name != "Deliverer" {
				continue
			}
			deliverer, ok := field.Value.(*ast.CompositeLit)
			if !ok || !selector(deliverer.Type, "RuntimePodDirectDeliverer") {
				continue
			}
			for _, element := range deliverer.Elts {
				field, ok := element.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, ok := field.Key.(*ast.Ident)
				value, assigned := field.Value.(*ast.Ident)
				if ok && assigned && key.Name == "Store" && value.Name == "deliveryStore" {
					wired = true
				}
			}
		}
		return true
	})
	if !assembled || !wired {
		t.Fatalf("job-runner must assemble deliveryStore with production constructor and assign it inside JobRunner.Deliverer.Store in %s (assembled=%t wired=%t)", path, assembled, wired)
	}
}

func schemaOwnershipEngineRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve engine root: %v", err)
	}
	return root
}

func readSchemaOwnershipFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path) //nolint:gosec // test-owned manifest root path.
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(content)
}
