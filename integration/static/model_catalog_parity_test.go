package static_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The public /v1/models catalog (Go) and the Gateway routing catalog
// (TypeScript) are written independently and must agree: the API tells callers
// a model's limits, the Gateway enforces them. A divergence publishes a number
// the platform does not honor.
//
// Scope: this compares each model's BASE limits. Some entries also carry
// supply-mode overrides (a model reached through a caller's own OAuth
// credential can have a smaller window than the same model on a platform key),
// and /v1/models has no route dimension to express that. Which number the
// public catalog should publish when routes disagree is an open product
// question, so this guard deliberately does not assert it.
var typeScriptCatalogEntryPattern = regexp.MustCompile(
	`providerId: "([^"]+)",\s*\n\s*modelId: "([^"]+)",(?s:.*?)modelContextWindowTokens: ([0-9_]+),\s*\n\s*modelOutputTokenLimit: ([0-9_]+),`,
)

type modelCatalogLimits struct {
	inputTokens  int
	outputTokens int
}

func TestModelCatalogLimitsMatchAcrossGoAndTypeScript(t *testing.T) {
	root := schemaOwnershipEngineRoot(t)
	goCatalog := parseGoModelCatalog(t, filepath.Join(root, "internal/httpapi/model_handler.go"))
	tsCatalog := parseTypeScriptModelCatalog(t, filepath.Join(root, "services/gateway/packages/provider-gateway/src/providers/catalog.ts"))

	if len(goCatalog) == 0 {
		t.Fatal("Go model catalog parsed as empty")
	}
	if len(goCatalog) != len(tsCatalog) {
		t.Fatalf("catalog sizes = Go %d / TypeScript %d; every served model must have a Gateway routing entry", len(goCatalog), len(tsCatalog))
	}
	for id, want := range goCatalog {
		got, ok := tsCatalog[id]
		if !ok {
			t.Errorf("model %s is served by /v1/models but has no Gateway catalog entry", id)
			continue
		}
		if got.inputTokens != want.inputTokens {
			t.Errorf("model %s input limit = Go %d / TypeScript %d", id, want.inputTokens, got.inputTokens)
		}
		if got.outputTokens != want.outputTokens {
			t.Errorf("model %s output limit = Go %d / TypeScript %d", id, want.outputTokens, got.outputTokens)
		}
	}
	for id := range tsCatalog {
		if _, ok := goCatalog[id]; !ok {
			t.Errorf("model %s has a Gateway catalog entry but is not served by /v1/models", id)
		}
	}
}

func parseGoModelCatalog(t *testing.T, path string) map[string]modelCatalogLimits {
	t.Helper()
	source, err := os.ReadFile(path) //nolint:gosec // repository-local static test path.
	if err != nil {
		t.Fatalf("read Go model catalog: %v", err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Base(path), source, 0)
	if err != nil {
		t.Fatalf("parse Go model catalog: %v", err)
	}
	catalog := map[string]modelCatalogLimits{}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "currentModelCatalog" || function.Body == nil {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			composite, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			id, limits, ok := goCatalogEntry(composite)
			if ok {
				catalog[id] = limits
			}
			return true
		})
	}
	return catalog
}

func goCatalogEntry(composite *ast.CompositeLit) (string, modelCatalogLimits, bool) {
	var id string
	var limits modelCatalogLimits
	seen := 0
	for _, element := range composite.Elts {
		field, ok := element.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := field.Key.(*ast.Ident)
		if !ok {
			continue
		}
		value, ok := field.Value.(*ast.BasicLit)
		if !ok {
			continue
		}
		switch key.Name {
		case "ID":
			unquoted, err := strconv.Unquote(value.Value)
			if err != nil {
				return "", modelCatalogLimits{}, false
			}
			id = unquoted
			seen++
		case "MaxInputTokens":
			parsed, err := strconv.Atoi(value.Value)
			if err != nil {
				return "", modelCatalogLimits{}, false
			}
			limits.inputTokens = parsed
			seen++
		case "MaxTokens":
			parsed, err := strconv.Atoi(value.Value)
			if err != nil {
				return "", modelCatalogLimits{}, false
			}
			limits.outputTokens = parsed
			seen++
		}
	}
	return id, limits, seen == 3
}

func parseTypeScriptModelCatalog(t *testing.T, path string) map[string]modelCatalogLimits {
	t.Helper()
	source, err := os.ReadFile(path) //nolint:gosec // repository-local static test path.
	if err != nil {
		t.Fatalf("read TypeScript model catalog: %v", err)
	}
	catalog := map[string]modelCatalogLimits{}
	for _, match := range typeScriptCatalogEntryPattern.FindAllStringSubmatch(string(source), -1) {
		inputTokens, err := strconv.Atoi(strings.ReplaceAll(match[3], "_", ""))
		if err != nil {
			t.Fatalf("parse TypeScript context window for %s/%s: %v", match[1], match[2], err)
		}
		outputTokens, err := strconv.Atoi(strings.ReplaceAll(match[4], "_", ""))
		if err != nil {
			t.Fatalf("parse TypeScript output limit for %s/%s: %v", match[1], match[2], err)
		}
		catalog[match[1]+"/"+match[2]] = modelCatalogLimits{inputTokens: inputTokens, outputTokens: outputTokens}
	}
	return catalog
}
