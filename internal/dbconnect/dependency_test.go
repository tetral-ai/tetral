package dbconnect_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestConfirmedStoreRuntimeClientBoundaryStaticGuards(t *testing.T) {
	engineRoot := engineRootDirForDBConnectTest(t)
	for _, packagePath := range []string{
		"internal/agent",
		"internal/environment",
		"internal/files",
		"internal/memory",
		"internal/session",
		"internal/skill",
		"internal/vault",
	} {
		scanConfirmedStorePackage(t, filepath.Join(engineRoot, packagePath), packagePath)
	}
}

func scanConfirmedStorePackage(t *testing.T, directory string, packagePath string) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read %s: %v", packagePath, err)
	}
	fileSet := token.NewFileSet()
	files := map[string]*ast.File{}
	constants := map[string]ast.Expr{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(directory, name)
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		files[path] = file
		for _, declaration := range file.Decls {
			if general, ok := declaration.(*ast.GenDecl); ok && general.Tok == token.CONST {
				for _, spec := range general.Specs {
					value := spec.(*ast.ValueSpec)
					for i, constant := range value.Names {
						if i < len(value.Values) {
							constants[constant.Name] = value.Values[i]
						}
					}
				}
			}
		}
	}
	for path, file := range files {
		sqlAliases := databaseSQLImportAliasesForDBConnectTest(file)
		dbconnectAliases := dbconnectImportAliasesForDBConnectTest(file)
		ast.Inspect(file, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.TypeSpec:
				if structType, ok := value.Type.(*ast.StructType); ok && strings.Contains(value.Name.Name, "PostgreSQL") {
					failIfFieldsExposeSQLDBForDBConnectTest(t, fileSet, path, structType, sqlAliases)
					failIfFieldsExposeDBConnectTxForDBConnectTest(t, fileSet, path, structType, dbconnectAliases)
				}
				if interfaceType, ok := value.Type.(*ast.InterfaceType); ok {
					failIfInterfaceExposesProviderTransactionForDBConnectTest(t, fileSet, path, interfaceType, sqlAliases, dbconnectAliases)
				}
			case *ast.FuncDecl:
				if strings.HasPrefix(value.Name.Name, "NewPostgreSQL") {
					failIfParamsExposeSQLDBForDBConnectTest(t, fileSet, path, value.Type.Params, sqlAliases)
				}
				failIfFunctionExposesSQLTxForDBConnectTest(t, fileSet, path, value.Type, sqlAliases)
				if ast.IsExported(value.Name.Name) {
					failIfFunctionExposesDBConnectTxForDBConnectTest(t, fileSet, path, value.Type, dbconnectAliases)
				}
			case *ast.CallExpr:
				callName := selectorCallNameForDBConnectTest(value)
				if callName == "storage.WithWorkspaceTx" || callName == "storage.WithWorkspaceTxAndCleanup" {
					t.Fatalf("%s calls legacy %s at %s", path, callName, fileSet.Position(value.Pos()))
				}
				if callName == "WithWorkspaceTx" || strings.HasSuffix(callName, ".WithWorkspaceTx") {
					if len(value.Args) == 3 {
						return true
					}
					assertWorkspaceOperationLabelIsStaticForDBConnectTest(t, fileSet, path, value, constants)
				}
				if callName == "WithWorkspaceTxAndCleanup" || strings.HasSuffix(callName, ".WithWorkspaceTxAndCleanup") {
					if len(value.Args) == 4 {
						return true
					}
					assertWorkspaceOperationLabelIsStaticForDBConnectTest(t, fileSet, path, value, constants)
				}
			}
			return true
		})
	}
}

func assertWorkspaceOperationLabelIsStaticForDBConnectTest(t *testing.T, fileSet *token.FileSet, path string, call *ast.CallExpr, constants map[string]ast.Expr) {
	t.Helper()
	if len(call.Args) < 3 {
		t.Fatalf("%s workspace Runtime Client call at %s has too few arguments", path, fileSet.Position(call.Pos()))
	}
	label, ok := constantStringForDBConnectTest(call.Args[2], constants)
	if !ok {
		t.Fatalf("%s workspace Runtime Client operation label at %s must be a constant string", path, fileSet.Position(call.Args[2].Pos()))
	}
	for _, forbidden := range []string{"SELECT ", "INSERT ", "UPDATE ", "DELETE ", "workspace", "password", "token", "credential"} {
		if strings.Contains(strings.ToLower(label), strings.ToLower(forbidden)) {
			t.Fatalf("%s workspace Runtime Client operation label %q at %s contains forbidden detail", path, label, fileSet.Position(call.Args[2].Pos()))
		}
	}
}

// constantStringForDBConnectTest is the value of a string literal, or of a
// constant declared in the function, the file or another file of the package.
func constantStringForDBConnectTest(expr ast.Expr, constants map[string]ast.Expr) (string, bool) {
	switch value := expr.(type) {
	case *ast.BasicLit:
		if value.Kind == token.STRING {
			text, err := strconv.Unquote(value.Value)
			return text, err == nil
		}
	case *ast.Ident:
		// A name the file resolves is its own declaration; one it does not is
		// declared in another file of the package.
		if value.Obj == nil {
			if declared, ok := constants[value.Name]; ok {
				return constantStringForDBConnectTest(declared, constants)
			}
			return "", false
		}
		if spec, ok := value.Obj.Decl.(*ast.ValueSpec); ok && value.Obj.Kind == ast.Con {
			for i, name := range spec.Names {
				if name.Name == value.Name && i < len(spec.Values) {
					return constantStringForDBConnectTest(spec.Values[i], constants)
				}
			}
		}
	}
	return "", false
}

func failIfFieldsExposeSQLDBForDBConnectTest(t *testing.T, fileSet *token.FileSet, path string, structType *ast.StructType, sqlAliases map[string]bool) {
	t.Helper()
	for _, field := range structType.Fields.List {
		if isSQLSelectorPointerForDBConnectTest(field.Type, sqlAliases, "DB") {
			t.Fatalf("%s PostgreSQL Store struct exposes *sql.DB at %s", path, fileSet.Position(field.Type.Pos()))
		}
	}
}

func failIfFieldsExposeDBConnectTxForDBConnectTest(t *testing.T, fileSet *token.FileSet, path string, structType *ast.StructType, dbconnectAliases map[string]bool) {
	t.Helper()
	for _, field := range structType.Fields.List {
		if typeContainsSelectorForDBConnectTest(field.Type, dbconnectAliases, "Tx") {
			t.Fatalf("%s PostgreSQL Store struct seam exposes dbconnect.Tx at %s", path, fileSet.Position(field.Type.Pos()))
		}
	}
}

func failIfParamsExposeSQLDBForDBConnectTest(t *testing.T, fileSet *token.FileSet, path string, fields *ast.FieldList, sqlAliases map[string]bool) {
	t.Helper()
	if fields == nil {
		return
	}
	for _, field := range fields.List {
		if isSQLSelectorPointerForDBConnectTest(field.Type, sqlAliases, "DB") {
			t.Fatalf("%s PostgreSQL constructor accepts *sql.DB at %s", path, fileSet.Position(field.Type.Pos()))
		}
	}
}

func failIfInterfaceExposesProviderTransactionForDBConnectTest(
	t *testing.T,
	fileSet *token.FileSet,
	path string,
	interfaceType *ast.InterfaceType,
	sqlAliases map[string]bool,
	dbconnectAliases map[string]bool,
) {
	t.Helper()
	for _, method := range interfaceType.Methods.List {
		functionType, ok := method.Type.(*ast.FuncType)
		if !ok {
			continue
		}
		failIfFunctionExposesSQLTxForDBConnectTest(t, fileSet, path, functionType, sqlAliases)
		failIfFunctionExposesDBConnectTxForDBConnectTest(t, fileSet, path, functionType, dbconnectAliases)
	}
}

func failIfFunctionExposesSQLTxForDBConnectTest(t *testing.T, fileSet *token.FileSet, path string, functionType *ast.FuncType, sqlAliases map[string]bool) {
	t.Helper()
	for _, fields := range []*ast.FieldList{functionType.Params, functionType.Results} {
		if fields == nil {
			continue
		}
		for _, field := range fields.List {
			if isSQLSelectorPointerForDBConnectTest(field.Type, sqlAliases, "Tx") {
				t.Fatalf("%s production function exposes *sql.Tx at %s", path, fileSet.Position(field.Type.Pos()))
			}
			if typeContainsSelectorForDBConnectTest(field.Type, sqlAliases, "Tx") {
				t.Fatalf("%s production function exposes nested *sql.Tx at %s", path, fileSet.Position(field.Type.Pos()))
			}
		}
	}
}

func failIfFunctionExposesDBConnectTxForDBConnectTest(t *testing.T, fileSet *token.FileSet, path string, functionType *ast.FuncType, dbconnectAliases map[string]bool) {
	t.Helper()
	for _, fields := range []*ast.FieldList{functionType.Params, functionType.Results} {
		if fields == nil {
			continue
		}
		for _, field := range fields.List {
			if typeContainsSelectorForDBConnectTest(field.Type, dbconnectAliases, "Tx") {
				t.Fatalf("%s production consumer interface exposes dbconnect.Tx at %s", path, fileSet.Position(field.Type.Pos()))
			}
		}
	}
}

func databaseSQLImportAliasesForDBConnectTest(file *ast.File) map[string]bool {
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

func dbconnectImportAliasesForDBConnectTest(file *ast.File) map[string]bool {
	aliases := map[string]bool{}
	for _, importSpec := range file.Imports {
		if strings.Trim(importSpec.Path.Value, `"`) != "github.com/tetral-ai/tetral/internal/dbconnect" {
			continue
		}
		if importSpec.Name == nil {
			aliases["dbconnect"] = true
			continue
		}
		aliases[importSpec.Name.Name] = true
	}
	return aliases
}

func isSQLSelectorPointerForDBConnectTest(node ast.Node, sqlAliases map[string]bool, selectorName string) bool {
	star, ok := node.(*ast.StarExpr)
	if !ok {
		return false
	}
	selector, ok := star.X.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != selectorName {
		return false
	}
	ident, ok := selector.X.(*ast.Ident)
	return ok && sqlAliases[ident.Name]
}

func isSelectorTypeForDBConnectTest(node ast.Node, aliases map[string]bool, selectorName string) bool {
	if star, ok := node.(*ast.StarExpr); ok {
		node = star.X
	}
	selector, ok := node.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != selectorName {
		return false
	}
	ident, ok := selector.X.(*ast.Ident)
	return ok && aliases[ident.Name]
}

func typeContainsSelectorForDBConnectTest(node ast.Node, aliases map[string]bool, selectorName string) bool {
	found := false
	ast.Inspect(node, func(inner ast.Node) bool {
		if inner == nil {
			return true
		}
		if isSelectorTypeForDBConnectTest(inner, aliases, selectorName) {
			found = true
			return false
		}
		return true
	})
	return found
}

func selectorCallNameForDBConnectTest(call *ast.CallExpr) string {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		if ident, identOK := call.Fun.(*ast.Ident); identOK {
			return ident.Name
		}
		return ""
	}
	if ident, ok := selector.X.(*ast.Ident); ok {
		return ident.Name + "." + selector.Sel.Name
	}
	return selector.Sel.Name
}

func dbconnectPackageDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(thisFile)
}

func engineRootDirForDBConnectTest(t *testing.T) string {
	t.Helper()
	return filepath.Clean(filepath.Join(dbconnectPackageDir(t), "..", ".."))
}
