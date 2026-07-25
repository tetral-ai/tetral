package filetool

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/pathsafe"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

// editReadMaxBytes caps the whole-file read (and the replacement result) at
// 32 MiB; a larger file or result is too_large. Edit reads the entire file into
// memory rather than streaming because exact substring matching cannot be done
// over a stream. The cap matches the apply_patch per-file read cap.
const editReadMaxBytes = 32 * 1024 * 1024

type EditResult struct {
	Replacements int `json:"replacements"`
	BytesWritten int `json:"bytes_written"`
}

type editInput struct {
	Path       string `json:"path"`
	OldString  string `json:"old_string"`
	NewString  string `json:"new_string"`
	ReplaceAll bool   `json:"replace_all"`
}

type editSpan struct {
	start int
	end   int
}

func Edit(payload protocol.Payload) (EditResult, *protocol.ToolError) {
	var input editInput
	if err := json.Unmarshal(payload.Input, &input); err != nil {
		return EditResult{}, invalidInput("input must be an object")
	}
	pathValue := input.Path
	if pathValue == "" {
		return EditResult{}, invalidInput("path is required")
	}
	if input.OldString == "" {
		return EditResult{}, invalidInput("old_string is required")
	}
	if input.OldString == input.NewString {
		return EditResult{}, invalidInput("old_string and new_string must differ")
	}
	resolved, err := (pathsafe.Resolver{WorkspaceRoot: payload.WorkspaceRoot, Roots: payload.Roots}).Resolve(pathValue, true)
	if err != nil {
		return EditResult{}, pathsafe.ToolError(err)
	}
	info, err := os.Stat(resolved.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return EditResult{}, &protocol.ToolError{Kind: "not_found", Message: "path does not exist", Path: resolved.DisplayPath}
		}
		return EditResult{}, &protocol.ToolError{Kind: "not_found", Message: "path could not be read", Path: resolved.DisplayPath}
	}
	if info.IsDir() {
		return EditResult{}, &protocol.ToolError{Kind: "is_directory", Message: "path is a directory", Path: resolved.DisplayPath}
	}
	if !info.Mode().IsRegular() {
		return EditResult{}, &protocol.ToolError{Kind: "not_found", Message: "path could not be read", Path: resolved.DisplayPath}
	}
	if info.Size() > editReadMaxBytes {
		return EditResult{}, &protocol.ToolError{Kind: "too_large", Message: "file exceeds 32 MiB edit cap", Path: resolved.DisplayPath}
	}
	body, err := readEditFileBounded(resolved.Path)
	if err != nil {
		return EditResult{}, &protocol.ToolError{Kind: "not_found", Message: "path could not be read", Path: resolved.DisplayPath}
	}
	if len(body) > editReadMaxBytes {
		return EditResult{}, &protocol.ToolError{Kind: "too_large", Message: "file exceeds 32 MiB edit cap", Path: resolved.DisplayPath}
	}
	if !utf8.Valid(body) {
		return EditResult{}, invalidInput("file content must be UTF-8")
	}
	// Matching runs an exact substring scan first; only if that finds nothing
	// does one fallback pass map curly quotes to ASCII on both sides and rescan.
	// The map is 1 rune -> 1 rune, so a match found in the mapped text is the
	// identical rune span in the original content — the fallback locates spans,
	// it never alters the bytes written. This curly-quote pass is the only
	// tolerated normalization; there is no whitespace or trim fallback. Zero
	// spans is match_not_found; more than one without replace_all is
	// match_ambiguous.
	contentRunes := []rune(string(body))
	oldRunes := []rune(input.OldString)
	spans := scanEditSpans(contentRunes, oldRunes)
	if len(spans) == 0 {
		spans = scanEditSpans(mapCurlyQuotes(contentRunes), mapCurlyQuotes(oldRunes))
	}
	if len(spans) == 0 {
		return EditResult{}, &protocol.ToolError{Kind: "match_not_found", Message: "old_string was not found", Path: resolved.DisplayPath}
	}
	if len(spans) > 1 && !input.ReplaceAll {
		return EditResult{}, &protocol.ToolError{
			Kind:    "match_ambiguous",
			Message: "old_string matched more than once",
			Path:    resolved.DisplayPath,
			Detail:  map[string]any{"matches": len(spans)},
		}
	}
	removedBytes := 0
	for _, span := range spans {
		for _, current := range contentRunes[span.start:span.end] {
			removedBytes += utf8.RuneLen(current)
		}
	}
	nextBytes := int64(len(body)-removedBytes) + int64(len(spans))*int64(len([]byte(input.NewString)))
	if nextBytes > editReadMaxBytes {
		return EditResult{}, &protocol.ToolError{Kind: "too_large", Message: "replacement exceeds 32 MiB edit cap", Path: resolved.DisplayPath}
	}
	var next strings.Builder
	next.Grow(int(nextBytes))
	previous := 0
	for _, span := range spans {
		next.WriteString(string(contentRunes[previous:span.start]))
		next.WriteString(input.NewString)
		previous = span.end
	}
	next.WriteString(string(contentRunes[previous:]))
	nextContent := next.String()
	if err := atomicWriteFile(resolved.Path, []byte(nextContent), preservedFileMode(info.Mode())); err != nil {
		return EditResult{}, &protocol.ToolError{Kind: "write_failed", Message: "write failed", Path: resolved.DisplayPath}
	}
	return EditResult{Replacements: len(spans), BytesWritten: len([]byte(nextContent))}, nil
}

func readEditFileBounded(pathValue string) ([]byte, error) {
	file, err := os.Open(pathValue) //nolint:gosec // Resolved through contract path containment.
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	body, err := io.ReadAll(io.LimitReader(file, editReadMaxBytes+1))
	if err != nil {
		return nil, err
	}
	return body, nil
}

func scanEditSpans(content []rune, needle []rune) []editSpan {
	if len(needle) == 0 || len(content) < len(needle) {
		return nil
	}
	var spans []editSpan
	for i := 0; i <= len(content)-len(needle); {
		if runesEqual(content[i:i+len(needle)], needle) {
			spans = append(spans, editSpan{start: i, end: i + len(needle)})
			i += len(needle)
			continue
		}
		i++
	}
	return spans
}

func runesEqual(a []rune, b []rune) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func mapCurlyQuotes(value []rune) []rune {
	out := make([]rune, len(value))
	for i, r := range value {
		out[i] = mapCurlyQuote(r)
	}
	return out
}

func mapCurlyQuote(r rune) rune {
	switch r {
	case '\u2018', '\u2019', '\u201a', '\u201b':
		return '\''
	case '\u201c', '\u201d', '\u201e', '\u201f':
		return '"'
	default:
		return r
	}
}
