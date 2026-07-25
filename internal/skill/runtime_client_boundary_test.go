package skill_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

func TestSkillDefaultPostgreSQLStoreConstructorForwardsCommitFailureCleanup(t *testing.T) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "postgresql_store.go", nil, 0)
	if err != nil {
		t.Fatalf("parse postgresql_store.go: %v", err)
	}
	txRunner := skillTxAndCleanupLiteral(t, fileSet, file)
	assertSkillRuntimeClientCleanupCall(t, fileSet, txRunner)
}

func skillTxAndCleanupLiteral(t *testing.T, fileSet *token.FileSet, file *ast.File) *ast.FuncLit {
	t.Helper()
	constructor := skillFunctionDeclaration(t, file, "NewPostgreSQLStore")
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
			t.Fatalf("txAndCleanup in NewPostgreSQLStore must be a function literal at %s", fileSet.Position(keyValue.Value.Pos()))
		}
		txRunner = literal
		return false
	})
	if txRunner == nil {
		t.Fatalf("NewPostgreSQLStore must initialize txAndCleanup at %s", fileSet.Position(constructor.Pos()))
	}
	return txRunner
}

func assertSkillRuntimeClientCleanupCall(t *testing.T, fileSet *token.FileSet, txRunner *ast.FuncLit) {
	t.Helper()
	assertSkillTxRunnerCleanupParameter(t, fileSet, txRunner)
	var cleanupCall *ast.CallExpr
	ast.Inspect(txRunner.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || !isSkillRuntimeClientCleanupCall(call) {
			return true
		}
		cleanupCall = call
		return false
	})
	if cleanupCall == nil {
		t.Fatalf("default skill txAndCleanup must call runtimeClient.WithWorkspaceTxAndCleanup at %s", fileSet.Position(txRunner.Pos()))
	}
	if len(cleanupCall.Args) != 5 {
		t.Fatalf("runtimeClient.WithWorkspaceTxAndCleanup args = %d; want 5 at %s", len(cleanupCall.Args), fileSet.Position(cleanupCall.Pos()))
	}
	assertSkillIdentArg(t, fileSet, cleanupCall.Args[0], "ctx")
	assertSkillIdentArg(t, fileSet, cleanupCall.Args[1], "workspaceID")
	assertSkillIdentArg(t, fileSet, cleanupCall.Args[4], "onCommitFailure")
}

func assertSkillTxRunnerCleanupParameter(t *testing.T, fileSet *token.FileSet, txRunner *ast.FuncLit) {
	t.Helper()
	if len(txRunner.Type.Params.List) != 4 {
		t.Fatalf("txAndCleanup params = %d; want ctx, workspaceID, fn, onCommitFailure at %s", len(txRunner.Type.Params.List), fileSet.Position(txRunner.Type.Params.Pos()))
	}
	cleanupParam := txRunner.Type.Params.List[3]
	if len(cleanupParam.Names) != 1 || cleanupParam.Names[0].Name != "onCommitFailure" {
		t.Fatalf("txAndCleanup final parameter must be onCommitFailure at %s", fileSet.Position(cleanupParam.Pos()))
	}
}

func isSkillRuntimeClientCleanupCall(call *ast.CallExpr) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "WithWorkspaceTxAndCleanup" {
		return false
	}
	receiver, ok := selector.X.(*ast.Ident)
	return ok && receiver.Name == "runtimeClient"
}

func assertSkillIdentArg(t *testing.T, fileSet *token.FileSet, expression ast.Expr, want string) {
	t.Helper()
	ident, ok := expression.(*ast.Ident)
	if !ok || ident.Name != want {
		t.Fatalf("runtimeClient.WithWorkspaceTxAndCleanup must receive %s directly, got %T at %s", want, expression, fileSet.Position(expression.Pos()))
	}
}

func skillFunctionDeclaration(t *testing.T, file *ast.File, name string) *ast.FuncDecl {
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
