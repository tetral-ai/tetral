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
	"strings"
	"testing"
)

// TestSessionStatusHasOneWriter pins runtimecontrol.RefreshSessionStatusTx as
// the only writer of a Session's change between running and idle: apart from
// Session creation's first row and Session termination, no Go source sets
// session_runtime_status.status, and no Go source but the writer writes the
// Session's own session.status_running or session.status_idle.
func TestSessionStatusHasOneWriter(t *testing.T) {
	planted := t.TempDir()
	for rel, body := range map[string]string{
		"internal/runtimecontrol/session_status.go": "package runtimecontrol\n\nfunc RefreshSessionStatusTx() {\n\t_ = \"UPDATE session_runtime_status SET status = 'running' WHERE session_id = $1\"\n\t_ = sessioneventwrite.InitialEvent{Type: \"session.status_running\"}\n}\n",
		"internal/session/postgresql_store.go":      "package session\n\nfunc (t *postgresqlTransaction) CreateSession() {\n\t_ = \"INSERT INTO session_runtime_status (session_id, status) VALUES ($1, 'idle')\"\n}\n",
		"internal/runtimecontrol/termination.go":    "package runtimecontrol\n\nfunc appendRuntimeTerminatedStatusTx() {\n\t_ = \"UPDATE session_runtime_status SET status = 'idle' WHERE session_id = $1\"\n}\n",
		"services/planted/planted.go": `package planted

const statusTable = "session_runtime_status"

func plant() {
	_ = ` + "`" + `update session_runtime_status
		set updated_at = $2, status = 'idle' where session_id = $1` + "`" + `
	_ = "UPDATE " + statusTable + " SET status = 'running' WHERE session_id = $1"
	_ = "INSERT INTO session_runtime_status (session_id, status) VALUES ($1, 'idle')"
	_ = sessioneventwrite.InitialEvent{SessionID: "sesn", Type: "session.status_idle"}
	_ = "UPDATE session_runtime_status SET cleanup_after = $2, status_event_id = $3 WHERE status = 'idle'"
	_ = sessioneventwrite.InitialEvent{SessionID: "sesn", SessionThreadID: "thread", Type: "session.status_idle"}
}
`,
	} {
		path := filepath.Join(planted, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	got, err := sessionStatusWriteViolations(planted)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"services/planted/planted.go:6: status write",
		"services/planted/planted.go:8: status write",
		"services/planted/planted.go:9: status write",
		"services/planted/planted.go:10: Session status event write",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("scan of the planted tree = %q; want %q", got, want)
	}

	violations, err := sessionStatusWriteViolations(finalArchitectureEngineRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) > 0 {
		t.Fatalf("Session status written outside runtimecontrol.RefreshSessionStatusTx, Session creation and Session termination:\n%s", strings.Join(violations, "\n"))
	}
}

// sessionStatusAllowedWriters are the places allowed to set
// session_runtime_status.status, by file and enclosing function ("" for the
// whole file). Each must hold at least one status write.
var sessionStatusAllowedWriters = []struct{ file, function string }{
	{"internal/runtimecontrol/session_status.go", ""},
	{"internal/session/postgresql_store.go", "(*postgresqlTransaction).CreateSession"},
	{"internal/runtimecontrol/termination.go", "appendRuntimeTerminatedStatusTx"},
}

const sessionStatusEventWriter = "internal/runtimecontrol/session_status.go"

const sessionRuntimeStatusTable = `"?session_runtime_status"?`

var (
	sessionStatusTableInsert = regexp.MustCompile(`(?i)\binsert\s+into\s+(?:"?[a-z_][a-z0-9_]*"?\s*\.\s*)?` + sessionRuntimeStatusTable + `(?:\s|\(|$)`)
	sessionStatusTableUpdate = regexp.MustCompile(`(?i)\bupdate\s+(?:"?[a-z_][a-z0-9_]*"?\s*\.\s*)?` + sessionRuntimeStatusTable + `(?:\s|$)`)
	sqlSetKeyword            = regexp.MustCompile(`(?i)\bset\b`)
	sqlSetListEnd            = regexp.MustCompile(`(?i)\b(?:where|returning)\b`)
	sqlSetsStatus            = regexp.MustCompile(`(?i)(?:^|,)\s*status\s*=`)
	sessionEventsInsert      = regexp.MustCompile(`(?i)\binsert\s+into\s+(?:"?[a-z_][a-z0-9_]*"?\s*\.\s*)?"?session_events"?(?:\s|\(|$)`)
)

func sessionStatusWriteViolations(root string) ([]string, error) {
	found := map[string]int{}
	var violations []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			rel, relErr := filepath.Rel(root, path)
			if relErr != nil {
				return relErr
			}
			if entry.Name() == ".claude" || filepath.ToSlash(rel) == "internal/storage/storagetest" {
				return filepath.SkipDir
			}
			return finalArchitectureSkipDir(entry)
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		writes, err := goSourceSessionStatusWrites(path)
		if err != nil {
			return err
		}
		for _, write := range writes {
			allowed := false
			if write.kind == "Session status event write" {
				allowed = rel == sessionStatusEventWriter
			} else {
				for _, writer := range sessionStatusAllowedWriters {
					if rel == writer.file && (writer.function == "" || writer.function == write.function) {
						allowed = true
						found[writer.file+" "+writer.function]++
					}
				}
			}
			if !allowed {
				violations = append(violations, fmt.Sprintf("%s:%d: %s", rel, write.line, write.kind))
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, writer := range sessionStatusAllowedWriters {
		if found[writer.file+" "+writer.function] == 0 {
			place := "the file"
			if writer.function != "" {
				place = writer.function
			}
			violations = append(violations, fmt.Sprintf("%s: %s holds no status write", writer.file, place))
		}
	}
	return violations, nil
}

type sessionStatusWrite struct {
	line     int
	kind     string
	function string
}

// goSourceSessionStatusWrites lists the status writes and Session status event
// writes of one file, each with the line where its constant or literal starts
// and its enclosing function.
func goSourceSessionStatusWrites(path string) ([]sessionStatusWrite, error) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, nil, 0)
	if err != nil {
		return nil, err
	}
	var writes []sessionStatusWrite
	for _, declaration := range file.Decls {
		function := ""
		if funcDecl, ok := declaration.(*ast.FuncDecl); ok {
			function = goFunctionName(funcDecl)
		}
		ast.Inspect(declaration, func(node ast.Node) bool {
			switch expression := node.(type) {
			case *ast.CompositeLit:
				if isInitialEventType(expression.Type) && initialEventWritesSessionStatus(expression) {
					writes = append(writes, sessionStatusWrite{fileSet.Position(expression.Pos()).Line, "Session status event write", function})
				}
				return true
			case *ast.BasicLit, *ast.BinaryExpr, *ast.ParenExpr:
				value, ok := constantGoString(expression.(ast.Expr), nil)
				if !ok {
					return true
				}
				line := fileSet.Position(expression.Pos()).Line
				if sqlWritesSessionStatus(value) {
					writes = append(writes, sessionStatusWrite{line, "status write", function})
				}
				if sessionEventsInsert.MatchString(value) && (strings.Contains(value, "'session.status_running'") || strings.Contains(value, "'session.status_idle'")) {
					writes = append(writes, sessionStatusWrite{line, "Session status event write", function})
				}
				// The parts of a constant string are substrings of its value.
				return false
			}
			return true
		})
	}
	return writes, nil
}

// sqlWritesSessionStatus reports an INSERT into session_runtime_status, or an
// UPDATE of it whose SET list sets status.
func sqlWritesSessionStatus(sql string) bool {
	if sessionStatusTableInsert.MatchString(sql) {
		return true
	}
	for _, match := range sessionStatusTableUpdate.FindAllStringIndex(sql, -1) {
		rest := sql[match[1]:]
		set := sqlSetKeyword.FindStringIndex(rest)
		if set == nil {
			continue
		}
		setList := rest[set[1]:]
		if end := sqlSetListEnd.FindStringIndex(setList); end != nil {
			setList = setList[:end[0]]
		}
		if sqlSetsStatus.MatchString(strings.TrimSpace(setList)) {
			return true
		}
	}
	return false
}

func isInitialEventType(expression ast.Expr) bool {
	switch value := expression.(type) {
	case *ast.SelectorExpr:
		pkg, ok := value.X.(*ast.Ident)
		return ok && pkg.Name == "sessioneventwrite" && value.Sel.Name == "InitialEvent"
	case *ast.Ident:
		return value.Name == "InitialEvent"
	}
	return false
}

// initialEventWritesSessionStatus reports an InitialEvent of the constant type
// session.status_running or session.status_idle that belongs to no thread.
func initialEventWritesSessionStatus(literal *ast.CompositeLit) bool {
	sessionStatus, noThread := false, true
	for _, element := range literal.Elts {
		field, ok := element.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := field.Key.(*ast.Ident)
		if !ok {
			continue
		}
		value, constant := constantGoString(field.Value, nil)
		switch key.Name {
		case "Type":
			sessionStatus = constant && (value == "session.status_running" || value == "session.status_idle")
		case "SessionThreadID":
			noThread = constant && value == ""
		}
	}
	return sessionStatus && noThread
}

func goFunctionName(declaration *ast.FuncDecl) string {
	if declaration.Recv == nil || len(declaration.Recv.List) == 0 {
		return declaration.Name.Name
	}
	receiver := declaration.Recv.List[0].Type
	if index, ok := receiver.(*ast.IndexExpr); ok {
		receiver = index.X
	}
	switch value := receiver.(type) {
	case *ast.StarExpr:
		if ident, ok := value.X.(*ast.Ident); ok {
			return "(*" + ident.Name + ")." + declaration.Name.Name
		}
	case *ast.Ident:
		return value.Name + "." + declaration.Name.Name
	}
	return declaration.Name.Name
}
