package files_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFilesMemoryProductionStoresDoNotExposeRawSQLDB(t *testing.T) {
	for _, directory := range []string{".", "../memory"} {
		entries, err := os.ReadDir(directory)
		if err != nil {
			t.Fatalf("read %s: %v", directory, err)
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			path := filepath.Join(directory, name)
			fileSet := token.NewFileSet()
			file, err := parser.ParseFile(fileSet, path, nil, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			sqlAliases := databaseSQLImportAliasesForFilesMemoryBoundaryTest(file)
			ast.Inspect(file, func(node ast.Node) bool {
				if isSQLDBPointerForFilesMemoryBoundaryTest(node, sqlAliases) {
					t.Fatalf("%s exposes *sql.DB in confirmed Files/Memory production Store boundary at %s", path, fileSet.Position(node.Pos()))
				}
				return true
			})
		}
	}
}

func databaseSQLImportAliasesForFilesMemoryBoundaryTest(file *ast.File) map[string]bool {
	aliases := map[string]bool{}
	for _, importSpec := range file.Imports {
		if strings.Trim(importSpec.Path.Value, `"`) != "database/sql" {
			continue
		}
		if importSpec.Name == nil {
			aliases["sql"] = true
			continue
		}
		aliases[importSpec.Name.Name] = true
	}
	return aliases
}

func isSQLDBPointerForFilesMemoryBoundaryTest(node ast.Node, sqlAliases map[string]bool) bool {
	star, ok := node.(*ast.StarExpr)
	if !ok {
		return false
	}
	selector, ok := star.X.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "DB" {
		return false
	}
	ident, ok := selector.X.(*ast.Ident)
	return ok && sqlAliases[ident.Name]
}

func TestFilesDefaultPostgreSQLStoreConstructorForwardsCommitFailureCleanup(t *testing.T) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "postgresql_store.go", nil, 0)
	if err != nil {
		t.Fatalf("parse postgresql_store.go: %v", err)
	}
	assertFilesConstructorDelegates(t, fileSet, file)
	txRunner := filesTxAndCleanupLiteral(t, fileSet, file)
	assertFilesRuntimeClientCleanupCall(t, fileSet, txRunner)
}

func assertFilesConstructorDelegates(t *testing.T, fileSet *token.FileSet, file *ast.File) {
	t.Helper()
	constructor := filesFunctionDeclaration(t, file, "NewPostgreSQLStore")
	seenDelegation := false
	ast.Inspect(constructor.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		callee, ok := call.Fun.(*ast.Ident)
		if ok && callee.Name == "NewPostgreSQLStoreWithLimits" {
			seenDelegation = true
			return false
		}
		return true
	})
	if !seenDelegation {
		t.Fatalf("NewPostgreSQLStore must delegate to NewPostgreSQLStoreWithLimits so the default constructor uses the cleanup-aware tx runner at %s", fileSet.Position(constructor.Pos()))
	}
}

func filesTxAndCleanupLiteral(t *testing.T, fileSet *token.FileSet, file *ast.File) *ast.FuncLit {
	t.Helper()
	constructor := filesFunctionDeclaration(t, file, "NewPostgreSQLStoreWithLimits")
	var txRunner *ast.FuncLit
	ast.Inspect(constructor.Body, func(node ast.Node) bool {
		keyValue, ok := node.(*ast.KeyValueExpr)
		if !ok {
			return true
		}
		key, ok := keyValue.Key.(*ast.Ident)
		if !ok || key.Name != "txAndCleanup" {
			return true
		}
		literal, ok := keyValue.Value.(*ast.FuncLit)
		if !ok {
			t.Fatalf("txAndCleanup in NewPostgreSQLStoreWithLimits must be a function literal at %s", fileSet.Position(keyValue.Value.Pos()))
		}
		txRunner = literal
		return false
	})
	if txRunner == nil {
		t.Fatalf("NewPostgreSQLStoreWithLimits must initialize txAndCleanup at %s", fileSet.Position(constructor.Pos()))
	}
	return txRunner
}

func assertFilesRuntimeClientCleanupCall(t *testing.T, fileSet *token.FileSet, txRunner *ast.FuncLit) {
	t.Helper()
	assertFilesTxRunnerCleanupParameter(t, fileSet, txRunner)
	var cleanupCall *ast.CallExpr
	ast.Inspect(txRunner.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || !isFilesRuntimeClientCleanupCall(call) {
			return true
		}
		cleanupCall = call
		return false
	})
	if cleanupCall == nil {
		t.Fatalf("default files txAndCleanup must call runtimeClient.WithWorkspaceTxAndCleanup at %s", fileSet.Position(txRunner.Pos()))
	}
	if len(cleanupCall.Args) != 5 {
		t.Fatalf("runtimeClient.WithWorkspaceTxAndCleanup args = %d; want 5 at %s", len(cleanupCall.Args), fileSet.Position(cleanupCall.Pos()))
	}
	assertFilesIdentArg(t, fileSet, cleanupCall.Args[0], "ctx")
	assertFilesIdentArg(t, fileSet, cleanupCall.Args[1], "workspaceID")
	assertFilesIdentArg(t, fileSet, cleanupCall.Args[4], "onCommitFailure")
}

func assertFilesTxRunnerCleanupParameter(t *testing.T, fileSet *token.FileSet, txRunner *ast.FuncLit) {
	t.Helper()
	if len(txRunner.Type.Params.List) != 4 {
		t.Fatalf("txAndCleanup params = %d; want ctx, workspaceID, fn, onCommitFailure at %s", len(txRunner.Type.Params.List), fileSet.Position(txRunner.Type.Params.Pos()))
	}
	cleanupParam := txRunner.Type.Params.List[3]
	if len(cleanupParam.Names) != 1 || cleanupParam.Names[0].Name != "onCommitFailure" {
		t.Fatalf("txAndCleanup final parameter must be onCommitFailure at %s", fileSet.Position(cleanupParam.Pos()))
	}
}

func isFilesRuntimeClientCleanupCall(call *ast.CallExpr) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "WithWorkspaceTxAndCleanup" {
		return false
	}
	receiver, ok := selector.X.(*ast.Ident)
	return ok && receiver.Name == "runtimeClient"
}

func assertFilesIdentArg(t *testing.T, fileSet *token.FileSet, expression ast.Expr, want string) {
	t.Helper()
	ident, ok := expression.(*ast.Ident)
	if !ok || ident.Name != want {
		t.Fatalf("runtimeClient.WithWorkspaceTxAndCleanup must receive %s directly, got %T at %s", want, expression, fileSet.Position(expression.Pos()))
	}
}

func filesFunctionDeclaration(t *testing.T, file *ast.File, name string) *ast.FuncDecl {
	t.Helper()
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == name {
			return function
		}
	}
	t.Fatalf("function %s not found", name)
	return nil
}
