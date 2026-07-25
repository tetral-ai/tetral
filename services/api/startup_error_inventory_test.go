package tetralapi

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// errorReturnSite is one enumerated error-return: the function it lives in and
// the exact source text of the error-position (last) result expression.
type errorReturnSite struct {
	function   string
	expression string
}

// enumerateStartupErrorReturns parses Go source and returns, for every function
// in scanned, each return statement whose error-position (last) result is a
// non-nil expression — rendered to its exact source text. This AST enumeration
// (not comment matching) is the load-bearing anti-omission matcher (A.1b): a free
// comment cannot be reliably bound to a return, so the test counts AST returns.
func enumerateStartupErrorReturns(t *testing.T, source string, scanned map[string]bool) []errorReturnSite {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "tetralapi.go", source, 0)
	if err != nil {
		t.Fatalf("parse source: %v", err)
	}
	var sites []errorReturnSite
	for _, decl := range file.Decls {
		funcDecl, ok := decl.(*ast.FuncDecl)
		if !ok || funcDecl.Body == nil {
			continue
		}
		name := funcDecl.Name.Name
		if !scanned[name] {
			continue
		}
		ast.Inspect(funcDecl.Body, func(node ast.Node) bool {
			returnStmt, ok := node.(*ast.ReturnStmt)
			if !ok || len(returnStmt.Results) == 0 {
				return true
			}
			last := returnStmt.Results[len(returnStmt.Results)-1]
			if identifier, ok := last.(*ast.Ident); ok && identifier.Name == "nil" {
				return true
			}
			sites = append(sites, errorReturnSite{
				function:   name,
				expression: renderExpression(source, fset, last),
			})
			return true
		})
	}
	return sites
}

// renderExpression returns the exact source text spanned by expr, proving the
// test reads the real source rather than comparing against a hard-coded constant.
func renderExpression(source string, fset *token.FileSet, expr ast.Expr) string {
	start := fset.Position(expr.Pos()).Offset
	end := fset.Position(expr.End()).Offset
	return source[start:end]
}

func readStartupSource(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile("tetralapi.go")
	if err != nil {
		t.Fatalf("read tetralapi.go: %v", err)
	}
	return string(body)
}

func scannedFunctionSet() map[string]bool {
	set := make(map[string]bool, len(startupScannedFunctions))
	for _, name := range startupScannedFunctions {
		set[name] = true
	}
	return set
}

// TestStartupErrorInventoryAccountsForEveryErrorReturn (A.1b) enumerates every
// error-return AST node in the scanned composition/helper functions and asserts
// the checked-in inventory accounts for each one as an exact multiset. It FAILS
// if a function gains an error-return absent from the inventory (or loses one),
// which is the structural guard that a future startup surface cannot be silently
// missed.
func TestStartupErrorInventoryAccountsForEveryErrorReturn(t *testing.T) {
	source := readStartupSource(t)
	sites := enumerateStartupErrorReturns(t, source, scannedFunctionSet())

	observed := map[string]int{}
	for _, site := range sites {
		observed[site.function+"|"+site.expression]++
	}
	expected := map[string]int{}
	for function, entries := range startupErrorInventory {
		for _, entry := range entries {
			expected[function+"|"+entry.expression] += entry.count
		}
	}

	for key, count := range observed {
		if expected[key] != count {
			t.Errorf("error-return %q observed %d time(s); inventory accounts for %d — add/update the inventory entry", key, count, expected[key])
		}
	}
	for key, count := range expected {
		if observed[key] != count {
			t.Errorf("inventory entry %q expects %d error-return(s); source has %d — the inventory is stale", key, count, observed[key])
		}
	}

	// Every scanned function must contribute at least one inventory key, so a
	// function silently dropped from startupScannedFunctions is noticed.
	for _, function := range startupScannedFunctions {
		if _, ok := startupErrorInventory[function]; !ok {
			t.Errorf("scanned function %q has no inventory entries", function)
		}
	}
}

// TestStartupErrorInventoryHonorsDecidedBuckets (A.1b) asserts the inventory does
// not re-bucket any doc-decided site, and that every ConfigError-producing return
// is bucketed C. This is the structural guard that a known config surface cannot
// be demoted to D to escape A.3's per-surface proof, and a fixer-introduced new
// config validation cannot be silently mislabeled D.
func TestStartupErrorInventoryHonorsDecidedBuckets(t *testing.T) {
	declared := map[string]string{}
	for function, entries := range startupErrorInventory {
		for _, entry := range entries {
			declared[function+"|"+entry.expression] = entry.bucket
		}
	}

	for key, wantBucket := range startupDecidedBuckets {
		gotBucket, ok := declared[key]
		if !ok {
			t.Errorf("doc-decided site %q is missing from the inventory", key)
			continue
		}
		if gotBucket != wantBucket {
			t.Errorf("doc-decided site %q is bucketed %q; the doc decided %q (re-bucketing forbidden)", key, gotBucket, wantBucket)
		}
	}

	// Structural rule: any return whose expression textually produces a
	// workload.ConfigError MUST be bucket C.
	for function, entries := range startupErrorInventory {
		for _, entry := range entries {
			if producesConfigError(entry.expression) && entry.bucket != bucketConfig {
				t.Errorf("%s|%s produces a workload.ConfigError but is bucketed %q; must be %q", function, entry.expression, entry.bucket, bucketConfig)
			}
		}
	}
}

// producesConfigError reports whether the rendered error expression textually
// yields a *workload.ConfigError: a direct workload.NewConfigError(...), the
// shared asStartupConfigError helper, or the ValidateVaultKey delegate.
func producesConfigError(expression string) bool {
	return strings.Contains(expression, "workload.NewConfigError(") ||
		strings.HasPrefix(expression, "asStartupConfigError(") ||
		strings.HasPrefix(expression, "ValidateVaultKey(")
}

// TestEnumerateStartupErrorReturnsReadsSource is the meta-test proving the AST
// enumerator actually parses source (not a hard-coded set): feeding an in-test
// source STRING that adds one synthetic error-return makes the enumerated set
// grow by exactly that return.
func TestEnumerateStartupErrorReturnsReadsSource(t *testing.T) {
	const baseSource = `package tetralapi

func BuildRouter() error {
	if bad {
		return err
	}
	return nil
}
`
	const grownSource = `package tetralapi

func BuildRouter() error {
	if bad {
		return err
	}
	if worse {
		return synthetic_config_error
	}
	return nil
}
`
	scanned := map[string]bool{"BuildRouter": true}
	base := enumerateStartupErrorReturns(t, baseSource, scanned)
	grown := enumerateStartupErrorReturns(t, grownSource, scanned)
	if len(grown) != len(base)+1 {
		t.Fatalf("enumerated %d returns from grown source; want %d (base %d + 1)", len(grown), len(base)+1, len(base))
	}
	found := false
	for _, site := range grown {
		if site.expression == "synthetic_config_error" {
			found = true
		}
	}
	if !found {
		t.Fatalf("enumerator did not surface the synthetic error-return; sites = %v", grown)
	}
}

// TestStartupDecidedBucketsCoverEveryConfigAndConstructionLeaf is a tripwire that
// the doc-decided set keeps naming remaining blob validation (C) and
// construction (D) leaves, so the anti-demotion guard cannot be hollowed out by
// quietly shrinking startupDecidedBuckets.
func TestStartupDecidedBucketsCoverEveryConfigAndConstructionLeaf(t *testing.T) {
	required := []string{
		`buildOptionalBlobStore|asStartupConfigError(err)`,
		`buildOptionalBlobStore|fmt.Errorf("blob store: %w", err)`,
		`validatePublicAPIConfig|ValidateVaultKey(vaultKey)`,
	}
	missing := []string{}
	for _, key := range required {
		if _, ok := startupDecidedBuckets[key]; !ok {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Fatalf("startupDecidedBuckets dropped required leaf pins: %v", missing)
	}
}
