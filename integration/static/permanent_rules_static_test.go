package static

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestFinalArchitectureGRPCAndProtobufImportsAreConfined(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	var violations []string
	err := filepath.WalkDir(engineRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return finalArchitectureSkipDir(entry)
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel := finalArchitectureRel(t, engineRoot, path)
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imported := range file.Imports {
			value := strings.Trim(imported.Path.Value, `"`)
			if finalArchitectureIsGRPCOrProtobufImport(value) && !finalArchitectureAllowsGRPCOrProtobuf(filepath.ToSlash(filepath.Dir(rel)), value) {
				violations = append(violations, rel+" imports "+value)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan gRPC/protobuf imports: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("gRPC/protobuf imports outside allowed internal surfaces:\n%s", strings.Join(violations, "\n"))
	}
}

func TestQueueJobInsertsStayBehindTheCanonicalQueueBoundary(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	var violations []string
	err := filepath.WalkDir(engineRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return finalArchitectureSkipDir(entry)
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel := finalArchitectureRel(t, engineRoot, path)
		if strings.HasPrefix(rel, "internal/queue/") {
			return nil
		}
		body, err := os.ReadFile(path) //nolint:gosec // repository-local static test path.
		if err != nil {
			return err
		}
		containsInsert, err := goSourceContainsQueueJobInsert(path, body)
		if err != nil {
			return err
		}
		if containsInsert {
			violations = append(violations, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan queue insert sites: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("queue job insert bypasses internal/queue.EnqueueTx:\n%s", strings.Join(violations, "\n"))
	}
}

func TestQueueJobInsertDetectorRejectsCaseWhitespaceAndLiteralSplits(t *testing.T) {
	for _, source := range []string{
		"package sample\nvar query = `insert into queue_jobs (id) values ('x')`\n",
		"package sample\nvar query = `INSERT\nINTO queue_jobs (id) VALUES ('x')`\n",
		"package sample\nvar query = \"INSERT \" + \"INTO queue_jobs (id) VALUES ('x')\"\n",
		"package sample\nconst table = \"queue_jobs\"\nvar query = \"INSERT INTO \" + table + \" (id) VALUES ('x')\"\n",
		"package sample\nfunc insert() { const verb = \"INSERT INTO \"; const table = \"queue_jobs\"; _ = verb + table + \" (id) VALUES ('x')\" }\n",
		"package sample\nfunc insert() { const verb = \"INSERT INTO \"; const table = \"queue_jobs\"; _ = verb + table + \" (id) VALUES ('x')\" }\nfunc mask() { const table = \"other\"; _ = table }\n",
		"package sample\nvar query = `INSERT INTO \"queue_jobs\" (id) VALUES ('x')`\n",
		"package sample\nvar query = `INSERT INTO public.queue_jobs (id) VALUES ('x')`\n",
		"package sample\nimport \"fmt\"\nfunc insert(columns string) string { return fmt.Sprintf(\"INSERT INTO queue_jobs (%s) VALUES ('x')\", columns) }\n",
		"package sample\nfunc insert(columns string) string { return \"INSERT INTO queue_jobs \" + columns }\n",
		"package sample\nvar query = \"INSERT INTO queue_job\u017f\" + \"_archive\"\n",
	} {
		matched, err := goSourceContainsQueueJobInsert("fixture.go", []byte(source))
		if err != nil {
			t.Fatalf("parse detector fixture: %v", err)
		}
		if !matched {
			t.Fatalf("queue INSERT detector missed fixture:\n%s", source)
		}
	}
}

const queueJobInsertPattern = `(?i)\binsert\s+into\s+(?:(?:"?[a-z_][a-z0-9_]*"?)\s*\.\s*)?"?queue_jobs"?(?:\s|\(|$)`

func goSourceContainsQueueJobInsert(filename string, source []byte) (bool, error) {
	file, err := parser.ParseFile(token.NewFileSet(), filename, source, 0)
	if err != nil {
		return false, err
	}
	pattern := regexp.MustCompile(queueJobInsertPattern)
	// A constant string's subexpressions evaluate to substrings of its value, so
	// none can match once the table name is absent. The pre-filter folds case
	// exactly as the pattern does.
	table := regexp.MustCompile(`(?i)queue_jobs`)
	found := false
	ast.Inspect(file, func(node ast.Node) bool {
		if found {
			return false
		}
		expression, ok := node.(ast.Expr)
		if !ok {
			return true
		}
		value, ok := constantGoString(expression, nil)
		if ok && pattern.MatchString(value) {
			found = true
			return false
		}
		return !ok || table.MatchString(value)
	})
	return found, nil
}

// This walker resolves file-local string constants for concatenation checks, so
// the go/ast identifier-object graph is sufficient and a full go/types pass would
// buy nothing. ast.Object is deprecated for cross-file resolution, which this
// never attempts.
//
//nolint:staticcheck // SA1019: file-local constant resolution only; see comment above.
func constantGoString(expression ast.Expr, resolving map[*ast.Object]bool) (string, bool) {
	switch value := expression.(type) {
	case *ast.BasicLit:
		if value.Kind != token.STRING {
			return "", false
		}
		decoded, err := strconv.Unquote(value.Value)
		return decoded, err == nil
	case *ast.BinaryExpr:
		if value.Op != token.ADD {
			return "", false
		}
		left, leftOK := constantGoString(value.X, resolving)
		right, rightOK := constantGoString(value.Y, resolving)
		return left + right, leftOK && rightOK
	case *ast.ParenExpr:
		return constantGoString(value.X, resolving)
	case *ast.Ident:
		object := value.Obj
		if object == nil || object.Kind != ast.Con {
			return "", false
		}
		if resolving == nil {
			resolving = map[*ast.Object]bool{}
		}
		if resolving[object] {
			return "", false
		}
		declaration, ok := object.Decl.(*ast.ValueSpec)
		if !ok {
			return "", false
		}
		index := -1
		for candidateIndex, name := range declaration.Names {
			if name.Obj == object {
				index = candidateIndex
				break
			}
		}
		if index < 0 || index >= len(declaration.Values) {
			return "", false
		}
		resolving[object] = true
		result, resolved := constantGoString(declaration.Values[index], resolving)
		delete(resolving, object)
		return result, resolved
	default:
		return "", false
	}
}

// Every service records through the shared library, internal/workload. A
// record written around it misses the declared tables, the field allowlist,
// redaction and the limiter. The TypeScript services hold the same rule for
// @tetral/ts-observability in their own suites.
func TestProductionCodeDoesNotUseStandardLibraryPrintLogging(t *testing.T) {
	violations, err := printLoggingViolations(finalArchitectureEngineRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, violation := range violations {
		t.Error(violation)
	}
}

func TestPrintLoggingGuardNamesTheFileAndLineOfEachViolation(t *testing.T) {
	slogCalls := []string{"Debug", "Info", "Warn", "Error", "DebugContext", "InfoContext", "WarnContext", "ErrorContext", "Log", "LogAttrs", "New", "NewTextHandler", "SetDefault"}
	records := "package api\n\nimport \"log/slog\"\n\nfunc record() {\n"
	var want []string
	for i, name := range slogCalls {
		records += "\tslog." + name + "(nil)\n"
		want = append(want, fmt.Sprintf("services/api/record.go:%d calls slog.%s; record through internal/workload", 6+i, name))
	}
	records += "}\n"
	root := t.TempDir()
	for rel, body := range map[string]string{
		"services/api/record.go":              records,
		"services/api/handler.go":             "package api\n\nimport logging \"log/slog\"\n\nvar handler = logging.NewJSONHandler(nil, nil)\n",
		"internal/workload/owner.go":          "package workload\n\nimport (\n\t\"fmt\"\n\t\"log/slog\"\n)\n\nfunc install() {\n\tslog.SetDefault(nil)\n\tfmt.Println(\"installed\")\n}\n",
		"integration/harness.go":              "package integration\n\nimport (\n\t\"fmt\"\n\t\"log/slog\"\n)\n\nfunc trace() {\n\tslog.Info(\"harness\")\n\tfmt.Println(\"harness\")\n}\n",
		"services/queue/print.go":             "package queue\n\nimport \"fmt\"\n\nfunc report() {\n\tfmt.Print(\"a\")\n\tfmt.Printf(\"%s\", \"b\")\n\tfmt.Println(\"done\")\n}\n",
		"internal/testinfra/cmd/tool/main.go": "package main\n\nimport (\n\t\"fmt\"\n\t\"log/slog\"\n)\n\nfunc main() {\n\tfmt.Println(\"plan\")\n\tslog.Info(\"plan\")\n}\n",
		// A comment that names a call is no call, and an alias hides none.
		"services/auth/log.go":           "package auth\n\nimport std \"log\"\n\n// log.Printf(\"prose\") and fmt.Fprintln(os.Stderr) are named here only.\nfunc fail() {\n\tstd.Printf(\"a\")\n\tstd.Fatalf(\"b\")\n}\n",
		"services/auth/cmd/auth/main.go": "package main\n\nimport (\n\t\"fmt\"\n\t\"os\"\n)\n\nfunc main() {\n\tfmt.Fprintln(os.Stderr, \"failed\")\n\tfmt.Fprintln(os.Stdout, \"result\")\n}\n",
		"services/auth/render.go":        "package auth\n\nimport (\n\t\"fmt\"\n\t\"os\"\n)\n\nfunc render() {\n\tfmt.Fprintln(os.Stderr, \"outside a command\")\n}\n",
	} {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	violations, err := printLoggingViolations(root)
	if err != nil {
		t.Fatal(err)
	}
	want = append(want,
		"services/api/handler.go:5 calls slog.NewJSONHandler; record through internal/workload",
		"integration/harness.go:10 calls fmt.Println; record through internal/workload",
		"internal/workload/owner.go:10 calls fmt.Println; record through internal/workload",
		"internal/testinfra/cmd/tool/main.go:10 calls slog.Info; record through internal/workload",
		"services/queue/print.go:6 calls fmt.Print; record through internal/workload",
		"services/queue/print.go:7 calls fmt.Printf; record through internal/workload",
		"services/queue/print.go:8 calls fmt.Println; record through internal/workload",
		"services/auth/log.go:7 calls log.Printf; record through internal/workload",
		"services/auth/log.go:8 calls log.Fatalf; record through internal/workload",
		"services/auth/cmd/auth/main.go:9 calls fmt.Fprintln on os.Stderr; record through internal/workload",
	)
	slices.Sort(violations)
	slices.Sort(want)
	if !slices.Equal(violations, want) {
		t.Fatalf("violations = %q; want %q", violations, want)
	}
}

// printLoggingViolations are the calls in production Go under engineRoot that
// write a record around internal/workload: the package-level calls of slog
// and of log, fmt's prints to standard output, and in a command or in
// internal/workload fmt's prints to standard error.
func printLoggingViolations(engineRoot string) ([]string, error) {
	calls := map[string]*regexp.Regexp{
		"log/slog": regexp.MustCompile(`^(Debug|Info|Warn|Error|DebugContext|InfoContext|WarnContext|ErrorContext|Log|LogAttrs|New|New[A-Za-z]*Handler|SetDefault)$`),
		"log":      regexp.MustCompile(`^(Print|Fatal|Panic)(f|ln)?$`),
		"fmt":      regexp.MustCompile(`^Print(f|ln)?$`),
	}
	standardErrorPrint := regexp.MustCompile(`^Fprint(f|ln)?$`)
	var violations []string
	err := filepath.WalkDir(engineRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(engineRoot, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if entry.IsDir() {
			if entry.Name() == "vendor" || entry.Name() == "node_modules" || entry.Name() == ".git" || rel == "cli" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		// internal/workload owns slog and integration/ is test support; the
		// commands under internal/testinfra and internal/release are developer
		// tools whose output is their interface, not service records.
		checked := map[string]bool{
			"log/slog": !strings.HasPrefix(rel, "internal/workload/") && !strings.HasPrefix(rel, "integration/"),
			"log":      true,
			"fmt":      !strings.HasPrefix(rel, "internal/testinfra/"),
		}
		tool := strings.HasPrefix(rel, "internal/testinfra/cmd/") || strings.HasPrefix(rel, "internal/release/cmd/")
		command := !tool && (strings.Contains(rel, "/cmd/") || strings.HasPrefix(rel, "cmd/") || strings.HasPrefix(rel, "internal/workload/"))
		fileSet := token.NewFileSet()
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			return err
		}
		importNames := map[string]string{}
		for _, imported := range file.Imports {
			importPath := strings.Trim(imported.Path.Value, `"`)
			name := filepath.Base(importPath)
			if imported.Name != nil {
				name = imported.Name.Name
			}
			importNames[name] = importPath
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			receiver, ok := selector.X.(*ast.Ident)
			if !ok {
				return true
			}
			imported := importNames[receiver.Name]
			line := fileSet.Position(call.Pos()).Line
			if checked[imported] && calls[imported].MatchString(selector.Sel.Name) {
				violations = append(violations, fmt.Sprintf("%s:%d calls %s.%s; record through internal/workload", rel, line, filepath.Base(imported), selector.Sel.Name))
			}
			if command && imported == "fmt" && standardErrorPrint.MatchString(selector.Sel.Name) && len(call.Args) > 0 {
				if writer, ok := call.Args[0].(*ast.SelectorExpr); ok && writer.Sel.Name == "Stderr" {
					if owner, ok := writer.X.(*ast.Ident); ok && importNames[owner.Name] == "os" {
						violations = append(violations, fmt.Sprintf("%s:%d calls fmt.%s on os.Stderr; record through internal/workload", rel, line, selector.Sel.Name))
					}
				}
			}
			return true
		})
		return nil
	})
	return violations, err
}

func finalArchitectureEngineRoot(t *testing.T) string {
	t.Helper()
	return filepath.Clean("../..")
}

func finalArchitectureRel(t *testing.T, root string, path string) string {
	t.Helper()
	rel, err := filepath.Rel(root, path)
	if err != nil {
		t.Fatalf("rel %s: %v", path, err)
	}
	return filepath.ToSlash(rel)
}

func finalArchitectureSkipDir(entry os.DirEntry) error {
	switch entry.Name() {
	case ".git", ".wrangler", "dist", "node_modules", "vendor":
		return filepath.SkipDir
	default:
		return nil
	}
}

func finalArchitectureIsGRPCOrProtobufImport(importPath string) bool {
	return strings.HasPrefix(importPath, "google.golang.org/grpc") ||
		strings.HasPrefix(importPath, "google.golang.org/protobuf")
}

// finalArchitectureAllowsGRPCOrProtobuf reports whether the package in
// directory may import importPath. Services own their RPCs. Outside them the
// generated code, the shared gRPC plumbing and the two contracts Bridge and
// Runner both consume may; so may the credential owner, which implements the
// handshake of no business RPC, and the fixture that runs the real proxy. The
// helper that compares pinned xDS resources reads protobuf and owns no
// transport.
func finalArchitectureAllowsGRPCOrProtobuf(directory, importPath string) bool {
	if directory == "integration/envoy-gateway-secret-helper" {
		return strings.HasPrefix(importPath, "google.golang.org/protobuf")
	}
	for _, owner := range []string{
		"services",
		"internal/gen",
		"internal/internalgrpc",
		"internal/mcpmanifest",
		"internal/runtimecontrol",
		"internal/transportsecurity",
		"integration/transporttest",
	} {
		if directory == owner || strings.HasPrefix(directory, owner+"/") {
			return true
		}
	}
	return false
}
