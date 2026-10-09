package static

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Session event producers either write a complete event through
// sessioneventwrite.InsertInitialTx or, for the one no-feed task notification,
// insert the row directly. Every public producer stamps processed_at: each
// InitialEvent literal sets ProcessedAt, except admitted inputs (client event
// admission and received agent mail), which stay unprocessed until Runtime
// commits them.
func TestPublicSessionEventProducersStampProcessedAt(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	var inserts, producers int
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "node_modules" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, err := os.ReadFile(path) //nolint:gosec // repository-local producer scan.
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		text := string(body)
		for offset := 0; ; {
			start := strings.Index(text[offset:], "INSERT INTO session_events (")
			if start < 0 {
				break
			}
			start += offset
			end := strings.Index(text[start:], ") VALUES")
			if end < 0 {
				t.Fatalf("unterminated session_events insert in %s", path)
			}
			end += start + len(") VALUES")
			statementEnd := min(len(text), end+200)
			statement := text[start:statementEnd]
			inserts++
			isRuntimeNotification := strings.Contains(statement, "'runtime_notification'")
			if !isRuntimeNotification && !strings.Contains(statement, "processed_at") {
				t.Errorf("public session_events producer %s does not stamp processed_at", relative)
			}
			if isRuntimeNotification && strings.Contains(statement, "processed_at") && !strings.Contains(statement, "NULL") {
				t.Errorf("internal notification %s must leave processed_at NULL", relative)
			}
			offset = end
		}
		if !strings.Contains(text, "sessioneventwrite.InitialEvent{") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, body, 0)
		if err != nil {
			return err
		}
		isInputAdmission := relative == "internal/sessionevent/postgresql_store.go"
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			selector, ok := literal.Type.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "InitialEvent" {
				return true
			}
			if pkg, ok := selector.X.(*ast.Ident); !ok || pkg.Name != "sessioneventwrite" {
				return true
			}
			producers++
			fields := map[string]ast.Expr{}
			for _, element := range literal.Elts {
				if pair, ok := element.(*ast.KeyValueExpr); ok {
					if key, ok := pair.Key.(*ast.Ident); ok {
						fields[key.Name] = pair.Value
					}
				}
			}
			eventType := ""
			if value, ok := fields["Type"].(*ast.BasicLit); ok && value.Kind == token.STRING {
				eventType, _ = strconv.Unquote(value.Value)
			}
			isReceivedMail := eventType == "agent.thread_message_received"
			_, stampsProcessedAt := fields["ProcessedAt"]
			switch {
			case isInputAdmission || isReceivedMail:
				if stampsProcessedAt {
					t.Errorf("admitted input producer %s must leave ProcessedAt unset", relative)
				}
			case !stampsProcessedAt:
				t.Errorf("public session event producer %s does not stamp ProcessedAt", relative)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("scan session event producers: %v", err)
	}
	if inserts < 2 {
		t.Fatalf("direct session_events inserts = %d; want at least the shared initial writer and the task notification writer", inserts)
	}
	if producers < 10 {
		t.Fatalf("session event producer count = %d; want at least 10 for complete producer-matrix coverage", producers)
	}
}
