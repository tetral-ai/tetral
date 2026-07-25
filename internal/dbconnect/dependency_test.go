package dbconnect_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRuntimeClientBoundaryHasNoProviderSDKOrObservabilityDependencies(t *testing.T) {
	engineRoot := engineRootDirForDBConnectTest(t)
	err := filepath.WalkDir(engineRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "testdata":
				return filepath.SkipDir
			default:
				return nil
			}
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relativePath, err := filepath.Rel(engineRoot, path)
		if err != nil {
			return err
		}
		relativePath = filepath.ToSlash(relativePath)
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, importSpec := range file.Imports {
			importPath := strings.Trim(importSpec.Path.Value, `"`)
			if isAllowedExistingRuntimeBoundaryImport(relativePath, importPath) {
				continue
			}
			if isForbiddenRuntimeBoundaryImport(importPath) {
				t.Errorf("%s imports forbidden Runtime Client boundary dependency %q", relativePath, importPath)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk engine production files: %v", err)
	}
}

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
				if callName == "WithWorkspaceTx" || callName == "WithWorkspaceTxAndCleanup" ||
					strings.HasSuffix(callName, ".WithWorkspaceTx") || strings.HasSuffix(callName, ".WithWorkspaceTxAndCleanup") {
					if len(value.Args) >= 3 {
						if _, ok := value.Args[2].(*ast.FuncLit); ok {
							return true
						}
					}
					assertWorkspaceOperationLabelIsStaticForDBConnectTest(t, fileSet, path, value)
				}
			}
			return true
		})
	}
}

func assertWorkspaceOperationLabelIsStaticForDBConnectTest(t *testing.T, fileSet *token.FileSet, path string, call *ast.CallExpr) {
	t.Helper()
	if len(call.Args) < 3 {
		t.Fatalf("%s workspace Runtime Client call at %s has too few arguments", path, fileSet.Position(call.Pos()))
	}
	literal, ok := call.Args[2].(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		t.Fatalf("%s workspace Runtime Client operation label at %s must be a static string literal", path, fileSet.Position(call.Args[2].Pos()))
	}
	label := strings.Trim(literal.Value, `"`)
	for _, forbidden := range []string{"SELECT ", "INSERT ", "UPDATE ", "DELETE ", "workspace", "password", "token", "credential"} {
		if strings.Contains(strings.ToLower(label), strings.ToLower(forbidden)) {
			t.Fatalf("%s workspace Runtime Client operation label %q at %s contains forbidden detail", path, label, fileSet.Position(literal.Pos()))
		}
	}
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

func isForbiddenRuntimeBoundaryImport(importPath string) bool {
	// Go's stdlib log/slog is the sanctioned structured logging standard:
	// production logging uses *slog.Logger with slog.NewJSONHandler directly.
	// The plain unstructured "log" package stays banned because it bypasses
	// structured, redaction-aware logging, and the third-party observability/
	// provider SDK bans below remain unchanged.
	if importPath == "log" {
		return true
	}
	for _, prefix := range []string{
		"github.com/" + "aws/" + "aws-sdk-go",
		"github.com/Azure/",
		"cloud.google.com/go/",
		"google.golang.org/api/",
		"github.com/GoogleCloudPlatform/cloud-sql-proxy",
		"github.com/jackc/pgx-gcp",
		"github.com/jackc/pgx-azure",
		"go.opentelemetry.io/",
		"github.com/prometheus/",
		"github.com/rs/zerolog",
		"go.uber.org/zap",
		"github.com/sirupsen/logrus",
		"github.com/go-logr/logr",
		"github.com/cenkalti/backoff",
		"github.com/avast/retry-go",
		"github.com/hashicorp/go-retryablehttp",
	} {
		if strings.HasPrefix(importPath, prefix) {
			return true
		}
	}
	return false
}

func isAllowedExistingRuntimeBoundaryImport(relativePath string, importPath string) bool {
	awsSDKRoot := "github.com/" + "aws/" + "aws-sdk-go-v2"
	if strings.HasPrefix(relativePath, "internal/blob/") && strings.HasPrefix(importPath, awsSDKRoot) {
		return true
	}
	if importPath == "log" {
		return allowedExistingLogImportFilesForDBConnectTest()[relativePath]
	}
	return false
}

func allowedExistingLogImportFilesForDBConnectTest() map[string]bool {
	// No production file imports plain "log" today; the allowlist stays as the
	// mechanism so a future file with a justified plain-log need can be added here.
	return map[string]bool{}
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
