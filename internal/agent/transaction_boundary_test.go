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
