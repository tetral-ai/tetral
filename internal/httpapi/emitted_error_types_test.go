package httpapi

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strconv"
	"testing"
)

// readErrorsSource returns the real internal/httpapi/errors.go source so the
// extractor runs against the committed file, not a constant stand-in.
func readErrorsSource(t *testing.T) string {
	t.Helper()
	source, err := os.ReadFile("errors.go")
	if err != nil {
		t.Fatalf("read errors.go: %v", err)
	}
	return string(source)
}

// extractEmittedErrorTypes walks the AST of an errors.go-shaped source and
// returns every public error `type` string the error classifier can emit.
//
// It descends into the classifyError and classifySandboxError functions and
// collects every string literal used as a public error type via BOTH shapes the
// file may use: classified(status, "type", message) calls and inline
// errorClassification{... Type: "type" ...} / errorDetail{... Type: "type" ...}
// composite literals. It is AST-based (not a constant stand-in) so adding or
// removing an emittable type changes the extracted set.
func extractEmittedErrorTypes(t *testing.T, source string) map[string]struct{} {
	t.Helper()

	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "errors.go", source, 0)
	if err != nil {
		t.Fatalf("parse errors source: %v", err)
	}

	emitted := make(map[string]struct{})
	for _, decl := range file.Decls {
		function, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if function.Name.Name != "classifyError" && function.Name.Name != "classifySandboxError" {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.CallExpr:
				if identifier, ok := typed.Fun.(*ast.Ident); ok && identifier.Name == "classified" && len(typed.Args) >= 2 {
					if value, ok := stringLiteral(typed.Args[1]); ok {
						emitted[value] = struct{}{}
					}
				}
			case *ast.CompositeLit:
				for _, element := range typed.Elts {
					keyValue, ok := element.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					key, ok := keyValue.Key.(*ast.Ident)
					if !ok || key.Name != "Type" {
						continue
					}
					if value, ok := stringLiteral(keyValue.Value); ok {
						emitted[value] = struct{}{}
					}
				}
			}
			return true
		})
	}
	return emitted
}

func stringLiteral(expression ast.Expr) (string, bool) {
	literal, ok := expression.(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return "", false
	}
	value, err := strconv.Unquote(literal.Value)
	if err != nil {
		return "", false
	}
	return value, true
}

func sortedSet(set map[string]struct{}) []string {
	values := make([]string, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

// publicContractErrorTypes is the emittable public-error-type set for this stage.
// Memory path conflicts and precondition failures intentionally fold into
// invalid_request_error for public API compatibility.
var publicContractErrorTypes = map[string]struct{}{
	"authentication_error":  {},
	"not_found_error":       {},
	"invalid_request_error": {},
	"conflict_error":        {},
	"permission_error":      {},
	"request_too_large":     {},
	"not_implemented":       {},
	"api_error":             {},
}

func TestEmittedErrorTypesMatchPublicContractSet(t *testing.T) {
	source := readErrorsSource(t)
	emitted := extractEmittedErrorTypes(t, source)
	for value := range publicContractErrorTypes {
		if _, ok := emitted[value]; !ok {
			t.Errorf("emitted set missing public contract error type %q", value)
		}
	}
	for value := range emitted {
		if _, ok := publicContractErrorTypes[value]; !ok {
			t.Errorf("emitted set has non-contract public error type %q", value)
		}
	}
}

// TestExtractEmittedErrorTypesParsesSourceNotConstants proves the extractor
// actually reads the AST rather than returning a hard-coded set: feeding it a
// synthetic source that adds one inline errorClassification{Type:"..."} grows
// the extracted set by exactly that type.
func TestExtractEmittedErrorTypesParsesSourceNotConstants(t *testing.T) {
	baseSource := `package httpapi
func classifyError(err error) errorClassification {
	switch {
	case true:
		return classified(404, "not_found_error", "x")
	default:
		return classified(500, "api_error", "internal error")
	}
}
`
	withSynthetic := `package httpapi
func classifyError(err error) errorClassification {
	switch {
	case true:
		return classified(404, "not_found_error", "x")
	case false:
		return errorClassification{detail: errorDetail{Type: "synthetic_x_error"}}
	default:
		return classified(500, "api_error", "internal error")
	}
}
`
	base := extractEmittedErrorTypes(t, baseSource)
	if _, ok := base["synthetic_x_error"]; ok {
		t.Fatal("base source must not contain synthetic_x_error")
	}
	grown := extractEmittedErrorTypes(t, withSynthetic)
	if _, ok := grown["synthetic_x_error"]; !ok {
		t.Fatalf("extractor did not read source: synthetic_x_error absent from %v", sortedSet(grown))
	}
	if len(grown) != len(base)+1 {
		t.Fatalf("synthetic source grew set by %d; want exactly 1 (base=%v grown=%v)", len(grown)-len(base), sortedSet(base), sortedSet(grown))
	}
}
