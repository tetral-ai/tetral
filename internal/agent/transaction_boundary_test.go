package agent_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAgentSkillProductionTransactionBoundaryDoesNotExposeSQLTx(t *testing.T) {
	for _, directory := range []string{".", "../skill"} {
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
			sqlAliases := databaseSQLImportAliasesForBoundaryTest(file)
			ast.Inspect(file, func(node ast.Node) bool {
				if isSQLTxPointerForBoundaryTest(node, sqlAliases) {
					t.Fatalf("%s exposes *sql.Tx in production Agent/Skill transaction boundary at %s", path, fileSet.Position(node.Pos()))
				}
				return true
			})
		}
	}
}

func TestAgentServiceSkillValidationRunsInsideAgentTransaction(t *testing.T) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "service.go", nil, 0)
	if err != nil {
		t.Fatalf("parse service.go: %v", err)
	}
	assertMethodValidatesInsideWorkspaceTxForBoundaryTest(t, file, "Create", "s.store.insertAgentSnapshot")
	assertMethodValidatesInsideWorkspaceTxForBoundaryTest(t, file, "Update", "s.updateCurrentLocked")
	assertMethodValidatesInsideWorkspaceTxForBoundaryTest(t, file, "UpdatePatch", "s.updateCurrentLocked")
	assertUpdateCurrentLockedValidationBeforeAgentWriteForBoundaryTest(t, file)
}

func TestSkillStoreAgentReferenceValidationUsesCallerTransaction(t *testing.T) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "../skill/service_agent_refs.go", nil, 0)
	if err != nil {
		t.Fatalf("parse service_agent_refs.go: %v", err)
	}
	validate := findFunctionForBoundaryTest(t, file, "ValidateAgentSkillReferences")
	if !functionParameterNameHasTypeForBoundaryTest(validate, "tx", "agent.Transaction") {
		t.Fatal("ValidateAgentSkillReferences must accept caller tx agent.Transaction")
	}
	if !functionCallsWithIdentifierForBoundaryTest(validate.Body, "storage.AcquireWorkspaceSkillRegistryLock", "tx") {
		t.Fatal("ValidateAgentSkillReferences must acquire registry lock through caller tx")
	}
	if !functionCallsWithIdentifierForBoundaryTest(validate.Body, "s.validateSingleAgentReference", "tx") {
		t.Fatal("ValidateAgentSkillReferences must pass caller tx into reference reads")
	}
	single := findFunctionForBoundaryTest(t, file, "validateSingleAgentReference")
	if !functionParameterNameHasTypeForBoundaryTest(single, "tx", "agent.Transaction") {
		t.Fatal("validateSingleAgentReference must accept caller tx agent.Transaction")
	}
	if !functionCallsWithReceiverForBoundaryTest(single.Body, "tx", "QueryRowScanner") {
		t.Fatal("validateSingleAgentReference must query through caller tx")
	}
	if functionCallsSelectorPrefixForBoundaryTest(file, "s.client.") {
		t.Fatal("Skill Agent-reference validation must not open or use store client transactions")
	}
}

func assertMethodValidatesInsideWorkspaceTxForBoundaryTest(t *testing.T, file *ast.File, methodName string, expectedWork string) {
	t.Helper()
	method := findFunctionForBoundaryTest(t, file, methodName)
	callback := findWorkspaceTxCallbackForBoundaryTest(method.Body)
	if callback == nil {
		t.Fatalf("%s must call s.store.withWorkspaceTx with a callback", methodName)
		return
	}
	if len(callback.Type.Params.List) != 1 || len(callback.Type.Params.List[0].Names) != 1 || callback.Type.Params.List[0].Names[0].Name != "tx" {
		t.Fatalf("%s withWorkspaceTx callback must receive tx parameter", methodName)
	}
	if !functionCallsWithIdentifierForBoundaryTest(callback.Body, "s.resolver.ValidateAgentSkillReferences", "tx") && methodName == "Create" {
		t.Fatalf("%s must validate Skill references with callback tx", methodName)
	}
	if methodName != "Create" && !functionCallsWithIdentifierForBoundaryTest(callback.Body, expectedWork, "tx") {
		t.Fatalf("%s must pass callback tx to %s", methodName, expectedWork)
	}
	if methodName == "Create" && !callOccursBeforeForBoundaryTest(callback.Body, "s.resolver.ValidateAgentSkillReferences", expectedWork) {
		t.Fatalf("%s must validate Skill references before %s inside the same transaction callback", methodName, expectedWork)
	}
}

func assertUpdateCurrentLockedValidationBeforeAgentWriteForBoundaryTest(t *testing.T, file *ast.File) {
	t.Helper()
	function := findFunctionForBoundaryTest(t, file, "updateCurrentLocked")
	if !functionParameterNameHasTypeForBoundaryTest(function, "tx", "Transaction") {
		t.Fatal("updateCurrentLocked must accept the Agent transaction parameter")
	}
	if !functionCallsWithIdentifierForBoundaryTest(function.Body, "s.resolver.ValidateAgentSkillReferences", "tx") {
		t.Fatal("updateCurrentLocked must validate Skill refs with caller tx")
	}
	if !callOccursBeforeForBoundaryTest(function.Body, "s.resolver.ValidateAgentSkillReferences", "s.store.updateAgentSnapshot") {
		t.Fatal("updateCurrentLocked must validate Skill refs before Agent write")
	}
}

func databaseSQLImportAliasesForBoundaryTest(file *ast.File) map[string]bool {
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

func isSQLTxPointerForBoundaryTest(node ast.Node, sqlAliases map[string]bool) bool {
	star, ok := node.(*ast.StarExpr)
	if !ok {
		return false
	}
	selector, ok := star.X.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Tx" {
		return false
	}
	ident, ok := selector.X.(*ast.Ident)
	return ok && sqlAliases[ident.Name]
}

func findFunctionForBoundaryTest(t *testing.T, file *ast.File, name string) *ast.FuncDecl {
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

func findWorkspaceTxCallbackForBoundaryTest(body *ast.BlockStmt) *ast.FuncLit {
	var callback *ast.FuncLit
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || selectorNameForBoundaryTest(call.Fun) != "s.store.withWorkspaceTx" {
			return true
		}
		for _, argument := range call.Args {
			if functionLiteral, ok := argument.(*ast.FuncLit); ok {
				callback = functionLiteral
				return false
			}
		}
		return true
	})
	return callback
}

func functionParameterNameHasTypeForBoundaryTest(function *ast.FuncDecl, parameterName string, typeName string) bool {
	if function.Type.Params == nil {
		return false
	}
	for _, field := range function.Type.Params.List {
		if expressionNameForBoundaryTest(field.Type) != typeName {
			continue
		}
		for _, name := range field.Names {
			if name.Name == parameterName {
				return true
			}
		}
	}
	return false
}

func functionCallsWithIdentifierForBoundaryTest(body *ast.BlockStmt, callName string, identifier string) bool {
	found := false
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || selectorNameForBoundaryTest(call.Fun) != callName {
			return true
		}
		for _, argument := range call.Args {
			if ident, ok := argument.(*ast.Ident); ok && ident.Name == identifier {
				found = true
				return false
			}
		}
		return true
	})
	return found
}

func functionCallsWithReceiverForBoundaryTest(body *ast.BlockStmt, receiver string, method string) bool {
	found := false
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == method && expressionNameForBoundaryTest(selector.X) == receiver {
			found = true
			return false
		}
		return true
	})
	return found
}

func functionCallsSelectorPrefixForBoundaryTest(file *ast.File, prefix string) bool {
	found := false
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if ok && strings.HasPrefix(selectorNameForBoundaryTest(call.Fun), prefix) {
			found = true
			return false
		}
		return true
	})
	return found
}

func callOccursBeforeForBoundaryTest(body *ast.BlockStmt, first string, second string) bool {
	firstPosition := 0
	secondPosition := 0
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := selectorNameForBoundaryTest(call.Fun)
		if name == first && firstPosition == 0 {
			firstPosition = int(call.Pos())
		}
		if name == second && secondPosition == 0 {
			secondPosition = int(call.Pos())
		}
		return true
	})
	return firstPosition != 0 && secondPosition != 0 && firstPosition < secondPosition
}

func selectorNameForBoundaryTest(expression ast.Expr) string {
	switch value := expression.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.SelectorExpr:
		prefix := selectorNameForBoundaryTest(value.X)
		if prefix == "" {
			return value.Sel.Name
		}
		return prefix + "." + value.Sel.Name
	default:
		return ""
	}
}

func expressionNameForBoundaryTest(expression ast.Expr) string {
	return selectorNameForBoundaryTest(expression)
}
