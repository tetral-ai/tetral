package static

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPublicSessionEventProducersStampProcessedAt(t *testing.T) {
	root := finalArchitectureEngineRoot(t)
	var inserts int
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
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			isInputAdmission := filepath.ToSlash(relative) == "internal/sessionevent/postgresql_store.go"
			isRuntimeNotification := strings.Contains(statement, "'runtime_notification'")
			if !isInputAdmission && !isRuntimeNotification && !strings.Contains(statement, "processed_at") {
				t.Errorf("public session_events producer %s does not stamp processed_at", filepath.ToSlash(relative))
			}
			if (isInputAdmission || isRuntimeNotification) && strings.Contains(statement, "processed_at") && !strings.Contains(statement, "NULL") {
				t.Errorf("internal/input exception %s must leave processed_at NULL", filepath.ToSlash(relative))
			}
			offset = end
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan session event producers: %v", err)
	}
	if inserts < 10 {
		t.Fatalf("session event producer insert count = %d; want at least 10 for complete producer-matrix coverage", inserts)
	}
}
