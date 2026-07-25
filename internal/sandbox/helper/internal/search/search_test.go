package search

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

func TestSearchGuardDrainsFinalPipeBytesBeforeWait(t *testing.T) {
	fixture := newSearchFixture(t)
	startSearchCPUSpinners(t)
	fillPath := filepath.Join(t.TempDir(), "pipe-fill")
	fill := bytes.Repeat([]byte{'x'}, 64*1024)
	fill[len(fill)-1] = '\n'
	if err := os.WriteFile(fillPath, fill, 0o600); err != nil {
		t.Fatalf("write pipe fill: %v", err)
	}
	fixture.useFakeRG(fmt.Sprintf(
		"dd if=%s bs=65536 count=1 2>/dev/null\n"+
			"i=0\nwhile [ $i -lt 104 ]; do printf 'guard-path-%%03d\\n' \"$i\"; i=$((i+1)); done\n"+
			"printf 'guard-final-sentinel\\n'\n",
		shellQuote(fillPath),
	), 5*time.Second)

	for iteration := 0; iteration < 50; iteration++ {
		collector := &pipeDrainGuardCollector{}
		result, toolErr := runRG(fixture.workspace, nil, collector)
		if toolErr != nil {
			t.Fatalf("iteration %d runRG error = %+v", iteration, toolErr)
		}
		got := result.stdout.(*pipeDrainGuardCollector)
		if !got.sawFill || got.truncated || len(got.paths) != 105 || got.paths[len(got.paths)-1] != "guard-final-sentinel" {
			t.Fatalf("iteration %d collector = saw_fill:%v paths:%d truncated:%v tail:%q; want 64 KiB fill plus 105 paths ending in sentinel",
				iteration, got.sawFill, len(got.paths), got.truncated, lastSearchGuardPath(got.paths))
		}
	}
}

func TestGrepFilesModeRespectsIgnoreAndSortsNewestFirst(t *testing.T) {
	requireRG(t)
	fixture := newSearchFixture(t)
	fixture.mkdir(".git")
	fixture.write(".gitignore", "ignored.txt\n")
	fixture.write("old.txt", "needle\n")
	fixture.write("new.txt", "needle\n")
	fixture.write("ignored.txt", "needle\n")
	fixture.write(".hidden.txt", "needle\n")
	oldTime := time.Date(2026, 7, 3, 10, 0, 0, 0, time.UTC)
	newTime := oldTime.Add(time.Hour)
	hiddenTime := oldTime.Add(30 * time.Minute)
	fixture.chtimes("old.txt", oldTime)
	fixture.chtimes("new.txt", newTime)
	fixture.chtimes(".hidden.txt", hiddenTime)

	result, toolErr := Grep(fixture.payload("grep", map[string]any{
		"pattern":    "needle",
		"mode":       "files",
		"head_limit": 10,
	}))
	if toolErr != nil {
		t.Fatalf("Grep files error = %+v", toolErr)
	}
	if strings.Contains(result.Text, "ignored.txt") {
		t.Fatalf("Grep files included ignored file: %q", result.Text)
	}
	if !strings.Contains(result.Text, ".hidden.txt") {
		t.Fatalf("Grep files omitted hidden file: %q", result.Text)
	}
	lines := nonEmptyLines(result.Text)
	if len(lines) != 3 {
		t.Fatalf("files lines = %v; want 3 visible matches", lines)
	}
	if lines[0] != "new.txt" {
		t.Fatalf("files order = %v; want newest first", lines)
	}
}

func TestGrepUsesWorkspaceRelativePathsForSubdirectorySearch(t *testing.T) {
	requireRG(t)
	fixture := newSearchFixture(t)
	fixture.write("src/a.txt", "needle\n")

	files, toolErr := Grep(fixture.payload("grep", map[string]any{
		"pattern": "needle",
		"path":    "src",
	}))
	if toolErr != nil {
		t.Fatalf("Grep files error = %+v", toolErr)
	}
	if strings.TrimSpace(files.Text) != "src/a.txt" {
		t.Fatalf("files text = %q; want workspace-relative src/a.txt", files.Text)
	}

	content, toolErr := Grep(fixture.payload("grep", map[string]any{
		"pattern": "needle",
		"path":    "src",
		"mode":    "content",
	}))
	if toolErr != nil {
		t.Fatalf("Grep content error = %+v", toolErr)
	}
	if !strings.Contains(content.Text, "src/a.txt:1:needle") {
		t.Fatalf("content text = %q; want workspace-relative path prefix", content.Text)
	}
}

func TestGrepContentCountAndInvalidRegex(t *testing.T) {
	requireRG(t)
	fixture := newSearchFixture(t)
	fixture.write("a.txt", "needle one\nneedle two\n")
	fixture.write("b.txt", "needle three\n")

	content, toolErr := Grep(fixture.payload("grep", map[string]any{
		"pattern":    "needle",
		"mode":       "content",
		"head_limit": 2,
	}))
	if toolErr != nil {
		t.Fatalf("Grep content error = %+v", toolErr)
	}
	if content.MatchCount != 2 || !content.Truncated {
		t.Fatalf("content result = %+v; want head_limit truncation at 2", content)
	}
	if !strings.Contains(content.Text, "needle one") {
		t.Fatalf("content text = %q; want matching line", content.Text)
	}

	count, toolErr := Grep(fixture.payload("grep", map[string]any{
		"pattern": "needle",
		"mode":    "count",
	}))
	if toolErr != nil {
		t.Fatalf("Grep count error = %+v", toolErr)
	}
	if count.MatchCount != 3 || !strings.Contains(count.Text, "a.txt:2") {
		t.Fatalf("count result = %+v; want summed rg count output", count)
	}

	_, toolErr = Grep(fixture.payload("grep", map[string]any{
		"pattern": "[",
		"mode":    "content",
	}))
	if toolErr == nil || toolErr.Kind != "invalid_input" {
		t.Fatalf("invalid regex error = %+v; want invalid_input", toolErr)
	}
	if stderr, ok := toolErr.Detail["stderr"].(string); !ok || stderr == "" || len(stderr) > searchStderrBytes {
		t.Fatalf("stderr detail = %#v; want bounded stderr excerpt", toolErr.Detail)
	}
}

func TestGrepContextDoesNotCountContextLinesAsMatches(t *testing.T) {
	requireRG(t)
	fixture := newSearchFixture(t)
	fixture.write("a.txt", "before\nneedle\n")

	content, toolErr := Grep(fixture.payload("grep", map[string]any{
		"pattern":        "needle",
		"mode":           "content",
		"before_context": 1,
		"head_limit":     1,
	}))
	if toolErr != nil {
		t.Fatalf("Grep context error = %+v", toolErr)
	}
	if content.MatchCount != 1 || !content.Truncated {
		t.Fatalf("context result = %+v; want one real match and truncation", content)
	}
	if !strings.Contains(content.Text, "a.txt-1-before") || !strings.Contains(content.Text, "a.txt:2:needle") {
		t.Fatalf("context text = %q; want context line plus matching line", content.Text)
	}
}

func TestGrepContextHandlesColonDigitFilenames(t *testing.T) {
	requireRG(t)
	fixture := newSearchFixture(t)
	fixture.write("a:1:b.txt", "before\nneedle\n")

	colonFilename, toolErr := Grep(fixture.payload("grep", map[string]any{
		"pattern":        "needle",
		"mode":           "content",
		"before_context": 1,
		"head_limit":     1,
		"globs":          []string{"a:1:b.txt"},
	}))
	if toolErr != nil {
		t.Fatalf("Grep colon filename context error = %+v", toolErr)
	}
	if colonFilename.MatchCount != 1 || !strings.Contains(colonFilename.Text, "a:1:b.txt-1-before") || !strings.Contains(colonFilename.Text, "a:1:b.txt:2:needle") {
		t.Fatalf("colon filename result = %+v; want context plus actual match only", colonFilename)
	}

	noLineNumbers, toolErr := Grep(fixture.payload("grep", map[string]any{
		"pattern":        "needle",
		"mode":           "content",
		"line_numbers":   false,
		"before_context": 1,
		"head_limit":     1,
		"globs":          []string{"a:1:b.txt"},
	}))
	if toolErr != nil {
		t.Fatalf("Grep no-line-number context error = %+v", toolErr)
	}
	if noLineNumbers.MatchCount != 1 || !strings.Contains(noLineNumbers.Text, "a:1:b.txt-before") || !strings.Contains(noLineNumbers.Text, "a:1:b.txt:needle") {
		t.Fatalf("no-line-number result = %+v; want context plus actual match only", noLineNumbers)
	}
}

func TestGrepContextPreservesSeparatorBytesInContent(t *testing.T) {
	requireRG(t)
	fixture := newSearchFixture(t)
	separator := "\x1fM\x1f"
	fixture.write("ctrl.txt", "before"+separator+"context\nneedle\n")

	content, toolErr := Grep(fixture.payload("grep", map[string]any{
		"pattern":        "needle",
		"mode":           "content",
		"before_context": 1,
		"head_limit":     1,
	}))
	if toolErr != nil {
		t.Fatalf("Grep separator content context error = %+v", toolErr)
	}
	if content.MatchCount != 1 || !content.Truncated {
		t.Fatalf("separator content result = %+v; want one real match and truncation", content)
	}
	if !strings.Contains(content.Text, "ctrl.txt-1-before"+separator+"context") ||
		!strings.Contains(content.Text, "ctrl.txt:2:needle") {
		t.Fatalf("separator content text = %q; want preserved context bytes and actual match", content.Text)
	}
}

func TestGrepContextPreservesSeparatorBytesInFilename(t *testing.T) {
	requireRG(t)
	fixture := newSearchFixture(t)
	separator := "\x1fM\x1f"
	name := "a" + separator + "b.txt"
	fixture.write(name, "before\nneedle\n")

	content, toolErr := Grep(fixture.payload("grep", map[string]any{
		"pattern":        "needle",
		"mode":           "content",
		"before_context": 1,
		"head_limit":     1,
	}))
	if toolErr != nil {
		t.Fatalf("Grep separator filename context error = %+v", toolErr)
	}
	if content.MatchCount != 1 || !content.Truncated {
		t.Fatalf("separator filename result = %+v; want one real match and truncation", content)
	}
	if !strings.Contains(content.Text, name+"-1-before") || !strings.Contains(content.Text, name+":2:needle") {
		t.Fatalf("separator filename text = %q; want preserved filename bytes and actual match", content.Text)
	}
}

func TestGrepDeadlineReturnsTimeout(t *testing.T) {
	fixture := newSearchFixture(t)
	fixture.useFakeRG("sleep 5\n", 20*time.Millisecond)

	startedAt := time.Now()
	_, toolErr := Grep(fixture.payload("grep", map[string]any{
		"pattern": "needle",
		"mode":    "content",
	}))
	elapsed := time.Since(startedAt)
	if toolErr == nil || toolErr.Kind != "timeout" {
		t.Fatalf("Grep error = %+v; want timeout", toolErr)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Grep timeout took %s; want process group killed near deadline", elapsed)
	}
}

func TestGrepDeadlineKillsRGWithoutSIGTERMGrace(t *testing.T) {
	fixture := newSearchFixture(t)
	marker := filepath.Join(t.TempDir(), "term.marker")
	fixture.useFakeRG(fmt.Sprintf("trap 'echo term > %s; sleep 1' TERM\nwhile :; do sleep 1; done\n", shellQuote(marker)), 20*time.Millisecond)

	startedAt := time.Now()
	_, toolErr := Grep(fixture.payload("grep", map[string]any{
		"pattern": "needle",
		"mode":    "content",
	}))
	elapsed := time.Since(startedAt)

	if toolErr == nil || toolErr.Kind != "timeout" {
		t.Fatalf("Grep error = %+v; want timeout", toolErr)
	}
	if elapsed > time.Second {
		t.Fatalf("Grep timeout elapsed = %s; want immediate SIGKILL without SIGTERM grace", elapsed)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("SIGTERM trap marker exists; want rg deadline to use SIGKILL only")
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat SIGTERM marker: %v", err)
	}
}

func TestGrepKillsRGWhenHeadLimitIsReached(t *testing.T) {
	fixture := newSearchFixture(t)
	separators := fixture.useRGSeparatorToken("test")
	fixture.useFakeRG(fmt.Sprintf("printf 'a.txt%s1%sneedle\\n'\nsleep 5\n", separators.Match, separators.Match), 5*time.Second)

	startedAt := time.Now()
	result, toolErr := Grep(fixture.payload("grep", map[string]any{
		"pattern":    "needle",
		"mode":       "content",
		"head_limit": 1,
	}))
	elapsed := time.Since(startedAt)
	if toolErr != nil {
		t.Fatalf("Grep error = %+v", toolErr)
	}
	if result.MatchCount != 1 || !result.Truncated || !strings.Contains(result.Text, "needle") {
		t.Fatalf("Grep result = %+v; want one captured line marked truncated", result)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Grep cap stop took %s; want rg process group killed instead of waiting for sleep", elapsed)
	}
}

func TestSearchStdoutReaderStopsAtHardLineCap(t *testing.T) {
	collector := &grepTextCollector{
		limit:      defaultGrepHeadLimit,
		byteCap:    grepVisibleBytes,
		lineCap:    grepVisibleLines,
		separators: rgFieldSeparators{Match: "\x00", Context: "\x01"},
	}
	stopped := readSearchStdout(strings.NewReader(strings.Repeat("x", searchMaxLineBytes+1)), collector)
	if !stopped {
		t.Fatal("readSearchStdout returned false; want early stop at hard line cap")
	}
	if !collector.truncated {
		t.Fatal("collector.truncated = false; want true at hard line cap")
	}
	if len(collector.lines) != 0 {
		t.Fatalf("collector stored %d oversized lines; want none", len(collector.lines))
	}
}

func TestGrepHeadLimitDefaultsAndClamps(t *testing.T) {
	requireRG(t)
	fixture := newSearchFixture(t)
	fixture.write("a.txt", strings.Repeat("needle\n", 3))

	defaulted, toolErr := Grep(fixture.payload("grep", map[string]any{
		"pattern": "needle",
		"mode":    "content",
	}))
	if toolErr != nil {
		t.Fatalf("Grep default head limit error = %+v", toolErr)
	}
	if defaulted.MatchCount != 3 || defaulted.Truncated {
		t.Fatalf("default head_limit result = %+v; want all three matches", defaulted)
	}

	clamped, toolErr := Grep(fixture.payload("grep", map[string]any{
		"pattern":    "needle",
		"mode":       "content",
		"head_limit": 0,
	}))
	if toolErr != nil {
		t.Fatalf("Grep clamped head limit error = %+v", toolErr)
	}
	if clamped.MatchCount != 1 || !clamped.Truncated {
		t.Fatalf("clamped head_limit result = %+v; want explicit zero clamped to one", clamped)
	}
}

func TestGrepFilesModeKeepsTextUnderVisibleByteCap(t *testing.T) {
	fixture := newSearchFixture(t)
	root := searchRoot{WorkspaceRoot: fixture.workspace}
	collector := &grepFilesCollector{}
	for i := 0; i < 600; i++ {
		name := filepath.ToSlash(filepath.Join(
			"very-long-paths",
			fmt.Sprintf("dir-%03d-%s", i, strings.Repeat("x", 80)),
			"needle.txt",
		))
		fixture.write(name, "needle\n")
		collector.paths = append(collector.paths, name)
	}

	result := grepFilesResult(root, collector, 0, 1000, grepVisibleBytes)
	if len(result.Text) > grepVisibleBytes {
		t.Fatalf("files text len = %d; want <= %d", len(result.Text), grepVisibleBytes)
	}
	if result.MatchCount != 600 || !result.Truncated {
		t.Fatalf("files result = %+v; want total match count and byte-cap truncation", result)
	}
}

func TestGrepHonorsLowerPayloadVisibleByteLimit(t *testing.T) {
	requireRG(t)
	fixture := newSearchFixture(t)
	fixture.write("a.txt", strings.Repeat("needle line\n", 10))

	content, toolErr := Grep(fixture.payloadWithLimits("grep", map[string]any{
		"pattern": "needle",
		"mode":    "content",
	}, protocol.Limits{VisibleBytes: 40}))
	if toolErr != nil {
		t.Fatalf("Grep content lower byte limit error = %+v", toolErr)
	}
	if len(content.Text) > 40 || !content.Truncated {
		t.Fatalf("content lower byte limit result = len %d truncated %v; want <= 40 true", len(content.Text), content.Truncated)
	}

	count, toolErr := Grep(fixture.payloadWithLimits("grep", map[string]any{
		"pattern": "needle",
		"mode":    "count",
	}, protocol.Limits{VisibleBytes: 8}))
	if toolErr != nil {
		t.Fatalf("Grep count lower byte limit error = %+v", toolErr)
	}
	if len(count.Text) > 8 || !count.Truncated {
		t.Fatalf("count lower byte limit result = len %d truncated %v; want <= 8 true", len(count.Text), count.Truncated)
	}

	root := searchRoot{WorkspaceRoot: fixture.workspace}
	collector := &grepFilesCollector{paths: []string{"a.txt", "a-very-long-matching-path.txt"}}
	fixture.write("a-very-long-matching-path.txt", "needle\n")
	files := grepFilesResult(root, collector, 0, 10, 8)
	if len(files.Text) > 8 || !files.Truncated {
		t.Fatalf("files lower byte limit result = len %d truncated %v; want <= 8 true", len(files.Text), files.Truncated)
	}
}

func TestGrepHonorsLowerPayloadVisibleLineLimit(t *testing.T) {
	requireRG(t)
	fixture := newSearchFixture(t)
	fixture.write("a.txt", strings.Repeat("needle\n", 5))
	fixture.write("b.txt", "needle\n")
	fixture.write("ctx.txt", "before\nneedle\n")

	content, toolErr := Grep(fixture.payloadWithLimits("grep", map[string]any{
		"pattern":    "needle",
		"mode":       "content",
		"head_limit": 10,
	}, protocol.Limits{VisibleLines: 2}))
	if toolErr != nil {
		t.Fatalf("Grep content lower line limit error = %+v", toolErr)
	}
	if content.MatchCount != 2 || len(nonEmptyLines(content.Text)) != 2 || !content.Truncated {
		t.Fatalf("content lower line limit result = %+v; want two matching lines and truncation", content)
	}

	contextContent, toolErr := Grep(fixture.payloadWithLimits("grep", map[string]any{
		"pattern":        "needle",
		"mode":           "content",
		"before_context": 1,
		"head_limit":     10,
		"globs":          []string{"ctx.txt"},
	}, protocol.Limits{VisibleLines: 1}))
	if toolErr != nil {
		t.Fatalf("Grep context lower line limit error = %+v", toolErr)
	}
	if len(nonEmptyLines(contextContent.Text)) != 1 || !contextContent.Truncated {
		t.Fatalf("context lower line limit result = %+v; want one output line and truncation", contextContent)
	}
	if !strings.Contains(contextContent.Text, "ctx.txt-1-before") || strings.Contains(contextContent.Text, "needle") {
		t.Fatalf("context lower line limit text = %q; want only the first context line", contextContent.Text)
	}

	count, toolErr := Grep(fixture.payloadWithLimits("grep", map[string]any{
		"pattern":    "needle",
		"mode":       "count",
		"head_limit": 10,
	}, protocol.Limits{VisibleLines: 1}))
	if toolErr != nil {
		t.Fatalf("Grep count lower line limit error = %+v", toolErr)
	}
	if len(nonEmptyLines(count.Text)) != 1 || !count.Truncated {
		t.Fatalf("count lower line limit result = %+v; want one count line and truncation", count)
	}

	files, toolErr := Grep(fixture.payloadWithLimits("grep", map[string]any{
		"pattern":    "needle",
		"mode":       "files",
		"head_limit": 10,
	}, protocol.Limits{VisibleLines: 1}))
	if toolErr != nil {
		t.Fatalf("Grep files lower line limit error = %+v", toolErr)
	}
	if len(nonEmptyLines(files.Text)) != 1 || !files.Truncated {
		t.Fatalf("files lower line limit result = %+v; want one file line and truncation", files)
	}
}

func TestGrepDefaultVisibleLineCapStopsRG(t *testing.T) {
	fixture := newSearchFixture(t)
	separators := fixture.useRGSeparatorToken("test")
	fixture.useFakeRG(fmt.Sprintf("i=1\nwhile [ $i -le 2001 ]; do printf 'a.txt%s%%s%sctx\\n' \"$i\"; i=$((i+1)); done\nsleep 5\n", separators.Context, separators.Context), 5*time.Second)

	startedAt := time.Now()
	result, toolErr := Grep(fixture.payload("grep", map[string]any{
		"pattern": "needle",
		"mode":    "content",
	}))
	elapsed := time.Since(startedAt)
	if toolErr != nil {
		t.Fatalf("Grep default line cap error = %+v", toolErr)
	}
	if len(nonEmptyLines(result.Text)) != grepVisibleLines || !result.Truncated || result.MatchCount != 0 {
		t.Fatalf("default line cap result = lines %d match_count %d truncated %v; want %d/0/true", len(nonEmptyLines(result.Text)), result.MatchCount, result.Truncated, grepVisibleLines)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Grep default line cap took %s; want rg killed at cap", elapsed)
	}
}

func TestGlobNoIgnoreHiddenAndOldestFirstWithTruncation(t *testing.T) {
	requireRG(t)
	fixture := newSearchFixture(t)
	fixture.mkdir(".git")
	fixture.write(".gitignore", "ignored.txt\n")
	fixture.write("old.txt", "")
	fixture.write("new.txt", "")
	fixture.write("ignored.txt", "")
	fixture.write(".hidden.txt", "")
	oldTime := time.Date(2026, 7, 3, 10, 0, 0, 0, time.UTC)
	fixture.chtimes("old.txt", oldTime)
	fixture.chtimes("new.txt", oldTime.Add(time.Hour))
	fixture.chtimes("ignored.txt", oldTime.Add(2*time.Hour))
	fixture.chtimes(".hidden.txt", oldTime.Add(3*time.Hour))

	result, toolErr := Glob(fixture.payload("glob", map[string]any{
		"pattern": "*.txt",
	}))
	if toolErr != nil {
		t.Fatalf("Glob error = %+v", toolErr)
	}
	joined := strings.Join(result.Paths, "\n")
	for _, want := range []string{"old.txt", "new.txt", "ignored.txt", ".hidden.txt"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("glob paths = %v; missing %s", result.Paths, want)
		}
	}
	if indexOf(result.Paths, "old.txt") > indexOf(result.Paths, "new.txt") {
		t.Fatalf("glob order = %v; want oldest modified first", result.Paths)
	}

	for i := 0; i < 105; i++ {
		fixture.write(filepath.ToSlash(filepath.Join("many", "f"+leftPad3(i)+".go")), "")
	}
	truncated, toolErr := Glob(fixture.payload("glob", map[string]any{
		"pattern": "many/*.go",
	}))
	if toolErr != nil {
		t.Fatalf("Glob truncation error = %+v", toolErr)
	}
	if len(truncated.Paths) != 100 || !truncated.Truncated {
		t.Fatalf("truncated glob = len %d truncated %v; want 100 true", len(truncated.Paths), truncated.Truncated)
	}
}

func TestGlobKillsRGWhenPathCapIsReached(t *testing.T) {
	fixture := newSearchFixture(t)
	fixture.useFakeRG("i=0\nwhile [ $i -lt 101 ]; do printf 'many/f%03d.go\\n' \"$i\"; i=$((i+1)); done\nsleep 5\n", 5*time.Second)

	startedAt := time.Now()
	result, toolErr := Glob(fixture.payload("glob", map[string]any{
		"pattern": "many/*.go",
	}))
	elapsed := time.Since(startedAt)
	if toolErr != nil {
		t.Fatalf("Glob cap error = %+v", toolErr)
	}
	if len(result.Paths) != 100 || !result.Truncated {
		t.Fatalf("Glob cap result = len %d truncated %v; want 100 true", len(result.Paths), result.Truncated)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Glob cap stop took %s; want rg process group killed instead of waiting for sleep", elapsed)
	}
}

type searchFixture struct {
	t         *testing.T
	workspace string
}

func newSearchFixture(t *testing.T) searchFixture {
	t.Helper()
	return searchFixture{t: t, workspace: t.TempDir()}
}

func (f searchFixture) payload(tool string, input map[string]any) protocol.Payload {
	f.t.Helper()
	return f.payloadWithLimits(tool, input, protocol.Limits{})
}

func (f searchFixture) payloadWithLimits(tool string, input map[string]any, limits protocol.Limits) protocol.Payload {
	f.t.Helper()
	encoded, err := json.Marshal(input)
	if err != nil {
		f.t.Fatalf("marshal input: %v", err)
	}
	return protocol.Payload{
		SchemaVersion:  protocol.SchemaVersion,
		Tool:           tool,
		ToolUseEventID: "evt_search",
		WorkspaceRoot:  f.workspace,
		Roots:          []protocol.Root{{Path: f.workspace, Mode: protocol.RootModeReadWrite}},
		Limits:         limits,
		Input:          encoded,
	}
}

func (f searchFixture) write(name string, content string) {
	f.t.Helper()
	path := filepath.Join(f.workspace, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		f.t.Fatalf("mkdir %s: %v", name, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		f.t.Fatalf("write %s: %v", name, err)
	}
}

func (f searchFixture) mkdir(name string) {
	f.t.Helper()
	if err := os.MkdirAll(filepath.Join(f.workspace, filepath.FromSlash(name)), 0o755); err != nil {
		f.t.Fatalf("mkdir %s: %v", name, err)
	}
}

func (f searchFixture) chtimes(name string, mtime time.Time) {
	f.t.Helper()
	path := filepath.Join(f.workspace, filepath.FromSlash(name))
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		f.t.Fatalf("chtimes %s: %v", name, err)
	}
}

func (f searchFixture) useFakeRG(scriptBody string, deadline time.Duration) {
	f.t.Helper()
	scriptPath := filepath.Join(f.t.TempDir(), "rg")
	script := "#!/bin/sh\n" + scriptBody
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		f.t.Fatalf("write fake rg: %v", err)
	}
	oldBinary := rgBinary
	oldDeadline := rgDeadline
	rgBinary = scriptPath
	rgDeadline = deadline
	f.t.Cleanup(func() {
		rgBinary = oldBinary
		rgDeadline = oldDeadline
	})
}

func (f searchFixture) useRGSeparatorToken(token string) rgFieldSeparators {
	f.t.Helper()
	old := newRGSeparatorToken
	newRGSeparatorToken = func() string { return token }
	separators := newRGFieldSeparators()
	f.t.Cleanup(func() {
		newRGSeparatorToken = old
	})
	return separators
}

func requireRG(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("rg"); err != nil {
		t.Skip("rg is required for search helper tests")
	}
}

func nonEmptyLines(text string) []string {
	var out []string
	for _, line := range strings.Split(text, "\n") {
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}

func indexOf(values []string, needle string) int {
	for i, value := range values {
		if value == needle {
			return i
		}
	}
	return len(values)
}

func leftPad3(value int) string {
	return fmt.Sprintf("%03d", value)
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

type pipeDrainGuardCollector struct {
	sawFill   bool
	paths     []string
	truncated bool
}

func (c *pipeDrainGuardCollector) Accept(line string) bool {
	cleaned := cleanRGLine(line)
	if len(cleaned) == 64*1024-1 && strings.Trim(cleaned, "x") == "" {
		c.sawFill = true
		return true
	}
	c.paths = append(c.paths, cleaned)
	return true
}

func (c *pipeDrainGuardCollector) Result() any { return c }

func startSearchCPUSpinners(t *testing.T) {
	t.Helper()
	var stop atomic.Bool
	var wait sync.WaitGroup
	wait.Add(2 * runtime.NumCPU())
	for i := 0; i < 2*runtime.NumCPU(); i++ {
		go func() {
			defer wait.Done()
			for spins := 0; !stop.Load(); spins++ {
				if spins%1024 == 0 {
					runtime.Gosched()
				}
			}
		}()
	}
	t.Cleanup(func() {
		stop.Store(true)
		wait.Wait()
	})
}

func lastSearchGuardPath(paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	return paths[len(paths)-1]
}
