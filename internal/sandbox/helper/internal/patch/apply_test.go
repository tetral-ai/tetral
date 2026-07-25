package patch

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

func TestApplyPatchAddUpdateDeleteAndMove(t *testing.T) {
	fs := newPatchFixture(t)
	fs.write("src/main.txt", "hello\nold\n")
	fs.write("delete.txt", "bye\n")
	fs.write("move.txt", "before\n")

	result, toolErr := Apply(fs.payload(`*** Begin Patch
*** Add File: new/file.txt
+one
+two
*** Update File: src/main.txt
@@
 hello
-old
+new
*** Delete File: delete.txt
*** Update File: move.txt
*** Move to: moved.txt
@@
-before
+after
*** End Patch`))
	if toolErr != nil {
		t.Fatalf("Apply error = %+v", toolErr)
	}
	if fs.read("new/file.txt") != "one\ntwo\n" || fs.read("src/main.txt") != "hello\nnew\n" || fs.read("moved.txt") != "after\n" {
		t.Fatalf("patched files = new:%q main:%q moved:%q", fs.read("new/file.txt"), fs.read("src/main.txt"), fs.read("moved.txt"))
	}
	if _, err := os.Stat(fs.path("delete.txt")); !os.IsNotExist(err) {
		t.Fatalf("delete.txt stat err = %v; want removed", err)
	}
	if _, err := os.Stat(fs.path("move.txt")); !os.IsNotExist(err) {
		t.Fatalf("move.txt stat err = %v; want removed", err)
	}
	if got := strings.Join(result.Added, ","); got != "new/file.txt" {
		t.Fatalf("added = %+v; want new/file.txt", result.Added)
	}
	if got := strings.Join(result.Modified, ","); got != "src/main.txt" {
		t.Fatalf("modified = %+v; want src/main.txt", result.Modified)
	}
	if got := strings.Join(result.Deleted, ","); got != "delete.txt" {
		t.Fatalf("deleted = %+v; want delete.txt", result.Deleted)
	}
	if len(result.Moved) != 1 || result.Moved[0].From != "move.txt" || result.Moved[0].To != "moved.txt" {
		t.Fatalf("moved = %+v; want move.txt -> moved.txt", result.Moved)
	}
}

func TestApplyPatchResultUsesEmptyArrays(t *testing.T) {
	fs := newPatchFixture(t)
	result, toolErr := Apply(fs.payload(`*** Begin Patch
*** Add File: only.txt
+ok
*** End Patch`))
	if toolErr != nil {
		t.Fatalf("Apply error = %+v", toolErr)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	for _, want := range []string{`"modified":[]`, `"deleted":[]`, `"moved":[]`} {
		if !strings.Contains(string(encoded), want) {
			t.Fatalf("encoded result = %s; want %s", encoded, want)
		}
	}
}

func TestApplyPatchPreprocessesCRLFAndHeredoc(t *testing.T) {
	variants := []string{"<<EOF", "<<'EOF'", "<<\"EOF\""}
	for _, start := range variants {
		t.Run(start, func(t *testing.T) {
			fs := newPatchFixture(t)
			raw := start + "\r\n*** Begin Patch\r\n*** Add File: heredoc.txt\r\n+ok\r\n*** End Patch\r\nEOF"
			result, toolErr := Apply(fs.payload(raw))
			if toolErr != nil {
				t.Fatalf("Apply heredoc error = %+v", toolErr)
			}
			if fs.read("heredoc.txt") != "ok\n" || len(result.Added) != 1 {
				t.Fatalf("heredoc result = %+v file=%q; want added ok", result, fs.read("heredoc.txt"))
			}
		})
	}
}

func TestApplyPatchParseErrors(t *testing.T) {
	tests := []struct {
		name        string
		patch       string
		wantLine    int
		wantMessage string
	}{
		{name: "empty patch", patch: "", wantLine: 1, wantMessage: "empty patch"},
		{name: "mismatched heredoc not stripped", patch: "<<'EOF'\n*** Begin Patch\n*** Add File: a.txt\n+x\n*** End Patch\n\"EOF\"", wantLine: 1, wantMessage: "patch missing begin marker"},
		{name: "trailing garbage", patch: "*** Begin Patch\n*** Add File: a.txt\n+x\n*** End Patch\ngarbage", wantLine: 5, wantMessage: "trailing content after end marker"},
		{name: "missing end marker", patch: "*** Begin Patch\n*** Add File: a.txt\n+x", wantLine: 3, wantMessage: "patch missing end marker"},
		{name: "invalid hunk header", patch: "*** Begin Patch\n*** Rename File: a.txt\n*** End Patch", wantLine: 2, wantMessage: "invalid hunk header; expected *** Add File: , *** Delete File: , or *** Update File: "},
		{name: "bad add body", patch: "*** Begin Patch\n*** Add File: a.txt\nx\n*** End Patch", wantLine: 3, wantMessage: "add file lines must start with +"},
		{name: "blank add path", patch: "*** Begin Patch\n*** Add File: \n+x\n*** End Patch", wantLine: 2, wantMessage: "add file path is required"},
		{name: "blank delete path", patch: "*** Begin Patch\n*** Delete File: \n*** End Patch", wantLine: 2, wantMessage: "delete file path is required"},
		{name: "blank update path", patch: "*** Begin Patch\n*** Update File: \n@@\n-old\n+new\n*** End Patch", wantLine: 2, wantMessage: "update file path is required"},
		{name: "blank move destination", patch: "*** Begin Patch\n*** Update File: a.txt\n*** Move to: \n@@\n-old\n+new\n*** End Patch", wantLine: 3, wantMessage: "move destination is required"},
		{name: "unexpected update line", patch: "*** Begin Patch\n*** Update File: a.txt\nraw\n*** End Patch", wantLine: 3, wantMessage: "unexpected update line"},
		{name: "empty update", patch: "*** Begin Patch\n*** Update File: a.txt\n@@\n*** End Patch", wantLine: 3, wantMessage: "empty update"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := newPatchFixture(t)
			_, toolErr := Apply(fs.payload(tc.patch))
			if toolErr == nil || toolErr.Kind != "patch_parse" {
				t.Fatalf("Apply error = %+v; want patch_parse", toolErr)
			}
			if toolErr.Message != tc.wantMessage {
				t.Fatalf("parse message = %q; want %q", toolErr.Message, tc.wantMessage)
			}
			if toolErr.Detail == nil || toolErr.Detail["line"] != tc.wantLine {
				t.Fatalf("parse detail = %+v; want line %d", toolErr.Detail, tc.wantLine)
			}
		})
	}
}

func TestApplyPatchIgnoresEnvironmentIDPreamble(t *testing.T) {
	fs := newPatchFixture(t)
	_, toolErr := Apply(fs.payload(`*** Begin Patch
*** Environment ID: env-local
*** Add File: a.txt
+ok
*** End Patch`))
	if toolErr != nil {
		t.Fatalf("Apply environment id preamble error = %+v", toolErr)
	}
	if got := fs.read("a.txt"); got != "ok\n" {
		t.Fatalf("environment id preamble content = %q; want ok", got)
	}
}

func TestApplyPatchMissingPatchFieldIsInvalidInput(t *testing.T) {
	fs := newPatchFixture(t)
	payload := fs.payload("*** Begin Patch\n*** Add File: a.txt\n+x\n*** End Patch")
	payload.Input = json.RawMessage(`{}`)
	_, toolErr := Apply(payload)
	if toolErr == nil || toolErr.Kind != "invalid_input" {
		t.Fatalf("Apply missing patch error = %+v; want invalid_input", toolErr)
	}
}

func TestApplyPatchSeekPassesAndPassOrder(t *testing.T) {
	tests := []struct {
		name    string
		before  string
		oldLine string
		after   string
	}{
		{name: "exact", before: "target\n", oldLine: "target", after: "ok\n"},
		{name: "rtrim", before: "target   \n", oldLine: "target", after: "ok\n"},
		{name: "trim", before: "  target \n", oldLine: "target", after: "ok\n"},
		{name: "normalize", before: "\u201csmart\u201d\u2014dash\u00a0space\n", oldLine: `"smart"-dash space`, after: "ok\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := newPatchFixture(t)
			fs.write("target.txt", tc.before)
			_, toolErr := Apply(fs.payload("*** Begin Patch\n*** Update File: target.txt\n@@\n-" + tc.oldLine + "\n+ok\n*** End Patch"))
			if toolErr != nil {
				t.Fatalf("Apply error = %+v", toolErr)
			}
			if got := fs.read("target.txt"); got != tc.after {
				t.Fatalf("target content = %q; want %q", got, tc.after)
			}
		})
	}

	fs := newPatchFixture(t)
	fs.write("order.txt", " target \ntarget\n")
	_, toolErr := Apply(fs.payload(`*** Begin Patch
*** Update File: order.txt
@@
-target
+ok
*** End Patch`))
	if toolErr != nil {
		t.Fatalf("Apply pass order error = %+v", toolErr)
	}
	if got := fs.read("order.txt"); got != " target \nok\n" {
		t.Fatalf("pass order content = %q; want exact later match before fuzzy earlier match", got)
	}
}

func TestApplyPatchCursorEOFAndPureInsertion(t *testing.T) {
	fs := newPatchFixture(t)
	fs.write("anchor.txt", "header\nold\n")
	_, toolErr := Apply(fs.payload(`*** Begin Patch
*** Update File: anchor.txt
@@ header
-old
+new
*** End Patch`))
	if toolErr != nil {
		t.Fatalf("Apply anchor error = %+v", toolErr)
	}
	if got := fs.read("anchor.txt"); got != "header\nnew\n" {
		t.Fatalf("anchor content = %q", got)
	}

	fs.write("dup.txt", "block\nold\nblock\nold\n")
	_, toolErr = Apply(fs.payload(`*** Begin Patch
*** Update File: dup.txt
@@
 block
-old
+new1
@@
 block
-old
+new2
*** End Patch`))
	if toolErr != nil {
		t.Fatalf("Apply duplicate cursor error = %+v", toolErr)
	}
	if got := fs.read("dup.txt"); got != "block\nnew1\nblock\nnew2\n" {
		t.Fatalf("duplicate cursor content = %q", got)
	}

	fs.write("eof.txt", "old\ntop\nold")
	_, toolErr = Apply(fs.payload(`*** Begin Patch
*** Update File: eof.txt
@@
-old
+new
*** End of File
*** End Patch`))
	if toolErr != nil {
		t.Fatalf("Apply EOF error = %+v", toolErr)
	}
	if got := fs.read("eof.txt"); got != "old\ntop\nnew\n" {
		t.Fatalf("EOF content = %q", got)
	}

	fs.write("insert.txt", "alpha\nBETA\ngamma\n")
	_, toolErr = Apply(fs.payload(`*** Begin Patch
*** Update File: insert.txt
@@
+INSERTED
@@
-BETA
+beta
*** End Patch`))
	if toolErr != nil {
		t.Fatalf("Apply insertion error = %+v", toolErr)
	}
	if got := fs.read("insert.txt"); got != "alpha\nbeta\ngamma\nINSERTED\n" {
		t.Fatalf("insertion content = %q", got)
	}

	fs.write("eof-miss.txt", "old\ntail\n")
	_, toolErr = Apply(fs.payload(`*** Begin Patch
*** Update File: eof-miss.txt
@@
-old
+new
*** End of File
*** End Patch`))
	if toolErr == nil || toolErr.Kind != "patch_verify" {
		t.Fatalf("Apply EOF non-tail match error = %+v; want patch_verify", toolErr)
	}
	if got := fs.read("eof-miss.txt"); got != "old\ntail\n" {
		t.Fatalf("EOF non-tail match changed content = %q", got)
	}

	fs.write("retry.txt", "a\nb")
	_, toolErr = Apply(fs.payload(`*** Begin Patch
*** Update File: retry.txt
@@
-b

+c
*** End Patch`))
	if toolErr != nil {
		t.Fatalf("Apply trailing empty retry error = %+v", toolErr)
	}
	if got := fs.read("retry.txt"); got != "a\n\nc\n" {
		t.Fatalf("trailing empty retry content = %q", got)
	}
}

func TestApplyPatchPreservesCodexTrailingBlankBytes(t *testing.T) {
	fs := newPatchFixture(t)
	_, toolErr := Apply(fs.payload(`*** Begin Patch
*** Add File: blank.txt
+one
+
*** Add File: empty.txt
*** End Patch`))
	if toolErr != nil {
		t.Fatalf("Apply add trailing blanks error = %+v", toolErr)
	}
	if got := fs.read("blank.txt"); got != "one\n\n" {
		t.Fatalf("blank add content = %q; want preserved blank line", got)
	}
	body, err := os.ReadFile(fs.path("empty.txt"))
	if err != nil {
		t.Fatalf("read empty add: %v", err)
	}
	if len(body) != 0 {
		t.Fatalf("empty add bytes = %q; want 0-byte file", body)
	}

	fs.write("retry-blank.txt", "a\nb")
	_, toolErr = Apply(fs.payload(`*** Begin Patch
*** Update File: retry-blank.txt
@@
-b
+c

*** End Patch`))
	if toolErr != nil {
		t.Fatalf("Apply trailing-empty retry trim error = %+v", toolErr)
	}
	if got := fs.read("retry-blank.txt"); got != "a\nc\n" {
		t.Fatalf("trailing-empty retry content = %q; want one final newline", got)
	}
}

func TestApplyPatchVerifyFailuresAreAtomic(t *testing.T) {
	fs := newPatchFixture(t)
	fs.write("target.txt", "actual\n")
	before := fs.snapshot()
	_, toolErr := Apply(fs.payload(`*** Begin Patch
*** Add File: new.txt
+new
*** Update File: target.txt
@@
-missing
+changed
*** End Patch`))
	if toolErr == nil || toolErr.Kind != "patch_verify" {
		t.Fatalf("Apply error = %+v; want patch_verify", toolErr)
	}
	if fs.read("target.txt") != "actual\n" {
		t.Fatalf("target changed during verify failure: %q", fs.read("target.txt"))
	}
	if _, err := os.Stat(fs.path("new.txt")); !os.IsNotExist(err) {
		t.Fatalf("new.txt stat err = %v; want no phase-2 write", err)
	}
	if after := fs.snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatalf("workspace snapshot changed during verify failure:\nbefore=%+v\nafter=%+v", before, after)
	}
}

func TestApplyPatchVerifyDiagnosticsAreCapped(t *testing.T) {
	fs := newPatchFixture(t)
	fs.write("target.txt", "actual\n")
	longAnchor := strings.Repeat("a", patchDiagnosticMaxBytes+1024)
	_, toolErr := Apply(fs.payload("*** Begin Patch\n*** Update File: target.txt\n@@ " + longAnchor + "\n-old\n+new\n*** End Patch"))
	if toolErr == nil || toolErr.Kind != "patch_verify" {
		t.Fatalf("Apply error = %+v; want patch_verify", toolErr)
	}
	if len(toolErr.Message) > patchDiagnosticMaxBytes {
		t.Fatalf("diagnostic bytes = %d; want <= %d", len(toolErr.Message), patchDiagnosticMaxBytes)
	}
}

func TestApplyPatchDuplicateAndPathErrors(t *testing.T) {
	fs := newPatchFixture(t)
	_, toolErr := Apply(fs.payload(`*** Begin Patch
*** Add File: same.txt
+one
*** Add File: same.txt
+two
*** End Patch`))
	if toolErr == nil || toolErr.Kind != "patch_verify" {
		t.Fatalf("duplicate error = %+v; want patch_verify", toolErr)
	}

	fs.mkdir("dir")
	_, toolErr = Apply(fs.payload("*** Begin Patch\n*** Add File: dir\n+x\n*** End Patch"))
	if toolErr == nil || toolErr.Kind != "is_directory" {
		t.Fatalf("directory error = %+v; want is_directory", toolErr)
	}
	_, toolErr = Apply(fs.payload("*** Begin Patch\n*** Update File: missing.txt\n@@\n-old\n+new\n*** End Patch"))
	if toolErr == nil || toolErr.Kind != "not_found" {
		t.Fatalf("missing error = %+v; want not_found", toolErr)
	}

	oversized := fs.path("huge.txt")
	file, err := os.Create(oversized)
	if err != nil {
		t.Fatalf("create oversized: %v", err)
	}
	if err := file.Truncate(patchReadMaxBytes + 1); err != nil {
		_ = file.Close()
		t.Fatalf("truncate oversized: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close oversized: %v", err)
	}
	_, toolErr = Apply(fs.payload("*** Begin Patch\n*** Update File: huge.txt\n@@\n-old\n+new\n*** End Patch"))
	if toolErr == nil || toolErr.Kind != "too_large" {
		t.Fatalf("oversized error = %+v; want too_large", toolErr)
	}

	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("secret\n"), 0o600); err != nil {
		t.Fatalf("write outside: %v", err)
	}
	if err := os.Symlink(filepath.Join(outside, "secret.txt"), fs.path("escape.txt")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	_, toolErr = Apply(fs.payload("*** Begin Patch\n*** Delete File: escape.txt\n*** End Patch"))
	if toolErr == nil || toolErr.Kind != "path_escape" {
		t.Fatalf("escape error = %+v; want path_escape", toolErr)
	}
	_, toolErr = Apply(fs.payload("*** Begin Patch\n*** Add File: /tmp/tetral-runtime/patch.txt\n+x\n*** End Patch"))
	if toolErr == nil || toolErr.Kind != "forbidden_path" {
		t.Fatalf("forbidden error = %+v; want forbidden_path", toolErr)
	}
}

func TestApplyPatchWorkspaceBoundReadOnlyResourceFailsAtHelperBoundary(t *testing.T) {
	fs := newPatchFixture(t)
	fs.write("data/report.csv", "old\n")
	payload := fs.payload("*** Begin Patch\n*** Update File: data/report.csv\n@@\n-old\n+new\n*** End Patch")
	payload.Roots = append(payload.Roots, protocol.Root{Path: fs.path("data"), Mode: protocol.RootModeRead})

	_, toolErr := Apply(payload)
	if toolErr == nil || toolErr.Kind != "path_escape" {
		t.Fatalf("workspace-bound resource patch error = %+v; want path_escape", toolErr)
	}
	if got := fs.read("data/report.csv"); got != "old\n" {
		t.Fatalf("resource content after rejected patch = %q; want old", got)
	}
}

func TestApplyPatchWorkspaceBoundReadOnlyFileResourceFailsAtHelperBoundary(t *testing.T) {
	fs := newPatchFixture(t)
	fs.write("data/report.csv", "old\n")
	payload := fs.payload("*** Begin Patch\n*** Update File: data/report.csv\n@@\n-old\n+new\n*** End Patch")
	payload.Roots = append(payload.Roots, protocol.Root{Path: fs.path("data/report.csv"), Mode: protocol.RootModeRead})

	_, toolErr := Apply(payload)
	if toolErr == nil || toolErr.Kind != "path_escape" {
		t.Fatalf("workspace-bound resource patch error = %+v; want path_escape", toolErr)
	}
	if got := fs.read("data/report.csv"); got != "old\n" {
		t.Fatalf("resource content after failed patch = %q; want old", got)
	}
}

func TestApplyPatchWriteFailureReportsAppliedPrefix(t *testing.T) {
	fs := newPatchFixture(t)
	original := atomicWriteFile
	t.Cleanup(func() { atomicWriteFile = original })
	atomicWriteFile = func(target string, content []byte, mode os.FileMode) error {
		if strings.HasSuffix(target, "second.txt") {
			return errors.New("boom")
		}
		return original(target, content, mode)
	}

	_, toolErr := Apply(fs.payload(`*** Begin Patch
*** Add File: first.txt
+one
*** Add File: second.txt
+two
*** End Patch`))
	if toolErr == nil || toolErr.Kind != "write_failed" {
		t.Fatalf("write error = %+v; want write_failed", toolErr)
	}
	applied, ok := toolErr.Detail["applied"].([]string)
	if !ok || len(applied) != 1 || applied[0] != "first.txt" || toolErr.Detail["failed_at"] != "second.txt" {
		t.Fatalf("write detail = %+v; want applied first and failed_at second", toolErr.Detail)
	}
	if got := fs.read("first.txt"); got != "one\n" {
		t.Fatalf("first content = %q; want committed", got)
	}
	if _, err := os.Stat(fs.path("second.txt")); !os.IsNotExist(err) {
		t.Fatalf("second stat err = %v; want absent", err)
	}
}

func TestApplyPatchWriteFailureAfterMoveReportsDestinationAndSource(t *testing.T) {
	fs := newPatchFixture(t)
	fs.write("move.txt", "old\n")
	original := atomicWriteFile
	t.Cleanup(func() { atomicWriteFile = original })
	atomicWriteFile = func(target string, content []byte, mode os.FileMode) error {
		if strings.HasSuffix(target, "fail.txt") {
			return errors.New("boom")
		}
		return original(target, content, mode)
	}

	_, toolErr := Apply(fs.payload(`*** Begin Patch
*** Update File: move.txt
*** Move to: moved.txt
@@
-old
+new
*** Add File: fail.txt
+fail
*** End Patch`))
	if toolErr == nil || toolErr.Kind != "write_failed" {
		t.Fatalf("write error = %+v; want write_failed", toolErr)
	}
	applied, ok := toolErr.Detail["applied"].([]string)
	if !ok || len(applied) != 2 || applied[0] != "moved.txt" || applied[1] != "move.txt" || toolErr.Detail["failed_at"] != "fail.txt" {
		t.Fatalf("write detail = %+v; want moved destination then removed source before failed add", toolErr.Detail)
	}
	if got := fs.read("moved.txt"); got != "new\n" {
		t.Fatalf("moved content = %q; want committed", got)
	}
	if _, err := os.Stat(fs.path("move.txt")); !os.IsNotExist(err) {
		t.Fatalf("move source stat err = %v; want removed", err)
	}
	if _, err := os.Stat(fs.path("fail.txt")); !os.IsNotExist(err) {
		t.Fatalf("fail stat err = %v; want absent", err)
	}
}

func TestApplyPatchAddOverExisting(t *testing.T) {
	fs := newPatchFixture(t)
	fs.write("existing.txt", "old\n")
	result, toolErr := Apply(fs.payload(`*** Begin Patch
*** Add File: existing.txt
+new
*** End Patch`))
	if toolErr != nil {
		t.Fatalf("Apply add-over-existing error = %+v", toolErr)
	}
	if got := fs.read("existing.txt"); got != "new\n" {
		t.Fatalf("existing content = %q; want overwrite", got)
	}
	if len(result.Added) != 1 || result.Added[0] != "existing.txt" {
		t.Fatalf("added = %+v; want existing.txt", result.Added)
	}
}

func TestApplyPatchMoveUsesDestinationModeRule(t *testing.T) {
	fs := newPatchFixture(t)
	fs.write("source.txt", "old\n")
	if err := os.Chmod(fs.path("source.txt"), 0o600); err != nil {
		t.Fatalf("chmod source: %v", err)
	}
	_, toolErr := Apply(fs.payload(`*** Begin Patch
*** Update File: source.txt
*** Move to: missing-destination.txt
@@
-old
+new
*** End Patch`))
	if toolErr != nil {
		t.Fatalf("Apply move missing destination error = %+v", toolErr)
	}
	if mode := fs.mode("missing-destination.txt"); mode != 0o644 {
		t.Fatalf("missing destination mode = %o; want 0644", mode)
	}

	fs.write("source2.txt", "old\n")
	fs.write("existing-destination.txt", "prior\n")
	if err := os.Chmod(fs.path("source2.txt"), 0o600); err != nil {
		t.Fatalf("chmod source2: %v", err)
	}
	if err := os.Chmod(fs.path("existing-destination.txt"), 0o640); err != nil {
		t.Fatalf("chmod destination: %v", err)
	}
	_, toolErr = Apply(fs.payload(`*** Begin Patch
*** Update File: source2.txt
*** Move to: existing-destination.txt
@@
-old
+new
*** End Patch`))
	if toolErr != nil {
		t.Fatalf("Apply move existing destination error = %+v", toolErr)
	}
	if mode := fs.mode("existing-destination.txt"); mode != 0o640 {
		t.Fatalf("existing destination mode = %o; want 0640", mode)
	}
}

func TestApplyPatchPreservesSpecialModeBits(t *testing.T) {
	fs := newPatchFixture(t)
	fs.write("special.txt", "old\n")
	want := os.FileMode(0o640) | os.ModeSetuid | os.ModeSetgid | os.ModeSticky
	if err := os.Chmod(fs.path("special.txt"), want); err != nil {
		t.Fatalf("chmod special: %v", err)
	}
	_, toolErr := Apply(fs.payload(`*** Begin Patch
*** Update File: special.txt
@@
-old
+new
*** End Patch`))
	if toolErr != nil {
		t.Fatalf("Apply error = %+v", toolErr)
	}
	info, err := os.Stat(fs.path("special.txt"))
	if err != nil {
		t.Fatalf("stat special: %v", err)
	}
	if got := info.Mode() & (os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky); got != want {
		t.Fatalf("mode = %v; want %v", got, want)
	}
}

func TestApplyPatchRejectsResultAndDeleteSourceAboveFileCap(t *testing.T) {
	fs := newPatchFixture(t)
	fs.write("grow.txt", strings.Repeat("b", 30*1024*1024)+"\na\n")
	_, toolErr := Apply(fs.payload("*** Begin Patch\n*** Update File: grow.txt\n@@\n-a\n+" + strings.Repeat("c", 3*1024*1024) + "\n*** End Patch"))
	if toolErr == nil || toolErr.Kind != "too_large" {
		t.Fatalf("growth error = %+v; want too_large", toolErr)
	}
	if !strings.HasSuffix(fs.read("grow.txt"), "\na\n") {
		t.Fatal("oversized patch changed grow.txt")
	}

	deletePath := fs.path("delete.txt")
	file, err := os.Create(deletePath)
	if err != nil {
		t.Fatalf("create delete target: %v", err)
	}
	if err := file.Truncate(patchReadMaxBytes + 1); err != nil {
		_ = file.Close()
		t.Fatalf("truncate delete target: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close delete target: %v", err)
	}
	_, toolErr = Apply(fs.payload("*** Begin Patch\n*** Delete File: delete.txt\n*** End Patch"))
	if toolErr == nil || toolErr.Kind != "too_large" {
		t.Fatalf("delete error = %+v; want too_large", toolErr)
	}
	if _, err := os.Stat(deletePath); err != nil {
		t.Fatalf("oversized delete target was removed: %v", err)
	}
}

type patchFixture struct {
	t         *testing.T
	workspace string
}

func newPatchFixture(t *testing.T) patchFixture {
	t.Helper()
	return patchFixture{t: t, workspace: t.TempDir()}
}

func (f patchFixture) payload(patchText string) protocol.Payload {
	f.t.Helper()
	body, err := json.Marshal(map[string]string{"patch": patchText})
	if err != nil {
		f.t.Fatalf("marshal payload: %v", err)
	}
	return protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           "apply_patch",
		ToolUseEventID: "evt_patch",
		WorkspaceRoot:  f.workspace,
		Roots:          []protocol.Root{{Path: f.workspace, Mode: protocol.RootModeReadWrite}},
		Input:          body,
	}
}

func (f patchFixture) write(name string, content string) {
	f.t.Helper()
	path := f.path(name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		f.t.Fatalf("mkdir fixture: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		f.t.Fatalf("write fixture: %v", err)
	}
}

func (f patchFixture) read(name string) string {
	f.t.Helper()
	body, err := os.ReadFile(f.path(name))
	if err != nil {
		f.t.Fatalf("read fixture %s: %v", name, err)
	}
	return string(body)
}

func (f patchFixture) mkdir(name string) {
	f.t.Helper()
	if err := os.MkdirAll(f.path(name), 0o755); err != nil {
		f.t.Fatalf("mkdir fixture: %v", err)
	}
}

func (f patchFixture) path(name string) string {
	f.t.Helper()
	return filepath.Join(f.workspace, filepath.FromSlash(name))
}

func (f patchFixture) mode(name string) os.FileMode {
	f.t.Helper()
	info, err := os.Stat(f.path(name))
	if err != nil {
		f.t.Fatalf("stat fixture %s: %v", name, err)
	}
	return info.Mode().Perm()
}

func (f patchFixture) snapshot() map[string]string {
	f.t.Helper()
	out := map[string]string{}
	if err := filepath.WalkDir(f.workspace, func(pathValue string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(f.workspace, pathValue)
		if err != nil {
			return err
		}
		body, err := os.ReadFile(pathValue)
		if err != nil {
			return err
		}
		out[filepath.ToSlash(rel)] = string(body)
		return nil
	}); err != nil {
		f.t.Fatalf("snapshot: %v", err)
	}
	return out
}
