package filetool

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

func TestEditReplacesUniqueMatch(t *testing.T) {
	fixture := newWriteFixture(t)
	fixture.write("note.txt", "alpha beta gamma", 0o600)
	result, toolErr := Edit(fixture.payload(map[string]any{
		"path":       "note.txt",
		"old_string": "beta",
		"new_string": "BETA",
	}))
	if toolErr != nil {
		t.Fatalf("Edit error = %+v", toolErr)
	}
	if result.Replacements != 1 || result.BytesWritten != len("alpha BETA gamma") {
		t.Fatalf("result = %+v; want one replacement", result)
	}
	if got := fixture.read("note.txt"); got != "alpha BETA gamma" {
		t.Fatalf("content = %q; want replaced content", got)
	}
}

func TestEditRejectsFilePathAliasWithoutPath(t *testing.T) {
	fixture := newWriteFixture(t)
	fixture.write("note.txt", "old", 0o600)
	_, toolErr := Edit(fixture.payload(map[string]any{
		"file_path":  "note.txt",
		"old_string": "old",
		"new_string": "new",
	}))
	if toolErr == nil || toolErr.Kind != "invalid_input" || toolErr.Message != "path is required" {
		t.Fatalf("Edit error = %+v; want missing path invalid_input", toolErr)
	}
	if got := fixture.read("note.txt"); got != "old" {
		t.Fatalf("content after alias rejection = %q; want unchanged", got)
	}
}

func TestEditReplaceAllAndAmbiguousMatch(t *testing.T) {
	fixture := newWriteFixture(t)
	fixture.write("note.txt", "x x x", 0o600)
	_, toolErr := Edit(fixture.payload(map[string]any{
		"path":       "note.txt",
		"old_string": "x",
		"new_string": "y",
	}))
	if toolErr == nil || toolErr.Kind != "match_ambiguous" || toolErr.Detail["matches"] != 3 {
		t.Fatalf("ambiguous error = %+v; want match_ambiguous with count", toolErr)
	}
	if got := fixture.read("note.txt"); got != "x x x" {
		t.Fatalf("content after ambiguity = %q; want unchanged", got)
	}
	result, toolErr := Edit(fixture.payload(map[string]any{
		"path":        "note.txt",
		"old_string":  "x",
		"new_string":  "y",
		"replace_all": true,
	}))
	if toolErr != nil {
		t.Fatalf("Edit replace_all error = %+v", toolErr)
	}
	if result.Replacements != 3 || fixture.read("note.txt") != "y y y" {
		t.Fatalf("replace_all result=%+v content=%q; want three replacements", result, fixture.read("note.txt"))
	}
}

func TestEditRejectsReplaceAllResultAboveFileCapBeforeWrite(t *testing.T) {
	fixture := newWriteFixture(t)
	original := strings.Repeat("b", 30*1024*1024) + "a"
	fixture.write("large.txt", original, 0o600)
	_, toolErr := Edit(fixture.payload(map[string]any{
		"path":        "large.txt",
		"old_string":  "a",
		"new_string":  strings.Repeat("c", 3*1024*1024),
		"replace_all": true,
	}))
	if toolErr == nil || toolErr.Kind != "too_large" {
		t.Fatalf("Edit error = %+v; want too_large", toolErr)
	}
	if got := fixture.read("large.txt"); got != original {
		t.Fatal("oversized replacement changed the target")
	}
}

func TestEditPreservesSpecialModeBits(t *testing.T) {
	fixture := newWriteFixture(t)
	fixture.write("special.txt", "old", 0o640)
	path := filepath.Join(fixture.workspace, "special.txt")
	want := os.FileMode(0o640) | os.ModeSetuid | os.ModeSetgid | os.ModeSticky
	if err := os.Chmod(path, want); err != nil {
		t.Fatalf("chmod special: %v", err)
	}
	if _, toolErr := Edit(fixture.payload(map[string]any{
		"path":       "special.txt",
		"old_string": "old",
		"new_string": "new",
	})); toolErr != nil {
		t.Fatalf("Edit error = %+v", toolErr)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat special: %v", err)
	}
	if got := info.Mode() & (os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky); got != want {
		t.Fatalf("mode = %v; want %v", got, want)
	}
}

func TestEditCurlyQuoteFallbackFindsOriginalSpan(t *testing.T) {
	fixture := newWriteFixture(t)
	fixture.write("note.txt", "please don\u2019t change spacing", 0o600)
	result, toolErr := Edit(fixture.payload(map[string]any{
		"path":       "note.txt",
		"old_string": "don't",
		"new_string": "do not",
	}))
	if toolErr != nil {
		t.Fatalf("Edit curly fallback error = %+v", toolErr)
	}
	if result.Replacements != 1 || fixture.read("note.txt") != "please do not change spacing" {
		t.Fatalf("result=%+v content=%q; want curly quote span replaced", result, fixture.read("note.txt"))
	}
}

func TestEditNoWhitespaceFallback(t *testing.T) {
	fixture := newWriteFixture(t)
	fixture.write("note.txt", "hello  world", 0o600)
	_, toolErr := Edit(fixture.payload(map[string]any{
		"path":       "note.txt",
		"old_string": "hello world",
		"new_string": "hi world",
	}))
	if toolErr == nil || toolErr.Kind != "match_not_found" {
		t.Fatalf("Edit error = %+v; want match_not_found without whitespace fallback", toolErr)
	}
}

func TestEditWorkspaceBoundReadOnlyResourceFailsAtHelperBoundary(t *testing.T) {
	fixture := newWriteFixture(t)
	fixture.write("data/report.csv", "old", 0o600)
	payload := fixture.payload(map[string]any{
		"path":       "data/report.csv",
		"old_string": "old",
		"new_string": "new",
	})
	payload.Roots = append(payload.Roots, protocol.Root{Path: filepath.Join(fixture.workspace, "data", "report.csv"), Mode: protocol.RootModeRead})

	_, toolErr := Edit(payload)
	if toolErr == nil || toolErr.Kind != "path_escape" {
		t.Fatalf("workspace-bound resource edit error = %+v; want path_escape", toolErr)
	}
	if got := fixture.read(filepath.Join("data", "report.csv")); got != "old" {
		t.Fatalf("resource content after failed edit = %q; want old", got)
	}
}

func TestEditRejectsInvalidInputsAndTooLarge(t *testing.T) {
	fixture := newWriteFixture(t)
	fixture.write("note.txt", "old", 0o600)
	for _, tc := range []struct {
		name  string
		input map[string]any
	}{
		{name: "empty old", input: map[string]any{"path": "note.txt", "old_string": "", "new_string": "new"}},
		{name: "identical", input: map[string]any{"path": "note.txt", "old_string": "old", "new_string": "old"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, toolErr := Edit(fixture.payload(tc.input))
			if toolErr == nil || toolErr.Kind != "invalid_input" {
				t.Fatalf("Edit error = %+v; want invalid_input", toolErr)
			}
		})
	}

	largePath := filepath.Join(fixture.workspace, "large.txt")
	file, err := os.Create(largePath)
	if err != nil {
		t.Fatalf("create large: %v", err)
	}
	if err := file.Truncate(editReadMaxBytes + 1); err != nil {
		_ = file.Close()
		t.Fatalf("truncate large: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close large: %v", err)
	}
	_, toolErr := Edit(fixture.payload(map[string]any{
		"path":       "large.txt",
		"old_string": "x",
		"new_string": "y",
	}))
	if toolErr == nil || toolErr.Kind != "too_large" {
		t.Fatalf("large error = %+v; want too_large", toolErr)
	}
}

func TestReadEditFileBoundedStopsAtCapPlusOne(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "large.txt")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create large fixture: %v", err)
	}
	chunk := strings.Repeat("x", 1024)
	for written := 0; written < editReadMaxBytes+len(chunk)*4; written += len(chunk) {
		if _, err := file.WriteString(chunk); err != nil {
			_ = file.Close()
			t.Fatalf("write large fixture: %v", err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close large fixture: %v", err)
	}
	body, err := readEditFileBounded(path)
	if err != nil {
		t.Fatalf("readEditFileBounded: %v", err)
	}
	if len(body) != editReadMaxBytes+1 {
		t.Fatalf("bounded read length = %d; want cap+1 %d", len(body), editReadMaxBytes+1)
	}
}
