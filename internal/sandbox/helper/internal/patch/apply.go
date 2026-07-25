package patch

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/pathsafe"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

// patchReadMaxBytes caps the per-file read AND the resulting patched file at
// 32 MiB; larger is too_large. It matches the Edit read cap, and the two must
// stay equal. patchDiagnosticMaxBytes caps parse/verify diagnostic messages at
// 4 KiB so an error detail cannot itself blow the envelope.
const patchReadMaxBytes = 32 * 1024 * 1024
const patchDiagnosticMaxBytes = 4 * 1024

type Result struct {
	Added    []string      `json:"added"`
	Modified []string      `json:"modified"`
	Deleted  []string      `json:"deleted"`
	Moved    []MovedResult `json:"moved"`
}

type MovedResult struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type input struct {
	Patch *string `json:"patch"`
}

type replacement struct {
	index    int
	delCount int
	newLines []string
}

type plannedOp struct {
	op          op
	source      pathsafe.Resolved
	destination *pathsafe.Resolved
	content     []byte
	mode        os.FileMode
}

var atomicWriteFile = defaultAtomicWriteFile

func Apply(payload protocol.Payload) (Result, *protocol.ToolError) {
	var in input
	if err := json.Unmarshal(payload.Input, &in); err != nil {
		return Result{}, invalidInput("input must be an object")
	}
	if in.Patch == nil {
		return Result{}, invalidInput("patch is required")
	}
	ops, err := parsePatch(*in.Patch)
	if err != nil {
		return Result{}, patchParseError(err)
	}
	plan, toolErr := verifyPlan(payload, ops)
	if toolErr != nil {
		return Result{}, toolErr
	}
	return applyPlan(plan)
}

func verifyPlan(payload protocol.Payload, ops []op) ([]plannedOp, *protocol.ToolError) {
	resolver := pathsafe.Resolver{WorkspaceRoot: payload.WorkspaceRoot, Roots: payload.Roots}
	planned := make([]plannedOp, 0, len(ops))
	seenTargets := map[string]string{}
	for _, op := range ops {
		source, err := resolver.Resolve(op.path, true)
		if err != nil {
			return nil, pathsafe.ToolError(err)
		}
		item := plannedOp{op: op, source: source}
		targets := []pathsafe.Resolved{source}
		if op.moveTo != nil {
			destination, err := resolver.Resolve(*op.moveTo, true)
			if err != nil {
				return nil, pathsafe.ToolError(err)
			}
			item.destination = &destination
			targets = append(targets, destination)
		}
		for _, target := range targets {
			if previous, ok := seenTargets[target.Path]; ok {
				return nil, patchVerify("duplicate target path", target.DisplayPath, map[string]any{"path": target.DisplayPath, "previous": previous})
			}
			seenTargets[target.Path] = target.DisplayPath
		}
		switch op.kind {
		case opAdd:
			mode, toolErr := modeForAddTarget(source)
			if toolErr != nil {
				return nil, toolErr
			}
			item.mode = mode
			item.content = addFileBytes(op.lines)
		case opDelete:
			info, toolErr := statPatchTarget(source)
			if toolErr != nil {
				return nil, toolErr
			}
			if info.Size() > patchReadMaxBytes {
				return nil, &protocol.ToolError{Kind: "too_large", Message: "file exceeds 32 MiB patch cap", Path: source.DisplayPath}
			}
			item.mode = preservedPatchMode(info.Mode())
		case opUpdate:
			info, toolErr := statPatchTarget(source)
			if toolErr != nil {
				return nil, toolErr
			}
			if info.Size() > patchReadMaxBytes {
				return nil, &protocol.ToolError{Kind: "too_large", Message: "file exceeds 32 MiB patch cap", Path: source.DisplayPath}
			}
			body, toolErr := readPatchFile(source)
			if toolErr != nil {
				return nil, toolErr
			}
			if len(body) > patchReadMaxBytes {
				return nil, &protocol.ToolError{Kind: "too_large", Message: "file exceeds 32 MiB patch cap", Path: source.DisplayPath}
			}
			next, toolErr := applyUpdate(source, string(body), op.chunks)
			if toolErr != nil {
				return nil, toolErr
			}
			if len(next) > patchReadMaxBytes {
				return nil, &protocol.ToolError{Kind: "too_large", Message: "patched file exceeds 32 MiB patch cap", Path: source.DisplayPath}
			}
			item.mode = preservedPatchMode(info.Mode())
			item.content = []byte(next)
			if op.moveTo != nil {
				mode, toolErr := modeForAddTarget(*item.destination)
				if toolErr != nil {
					return nil, toolErr
				}
				item.mode = mode
			}
		}
		planned = append(planned, item)
	}
	return planned, nil
}

func applyPlan(plan []plannedOp) (Result, *protocol.ToolError) {
	result := Result{
		Added:    []string{},
		Modified: []string{},
		Deleted:  []string{},
		Moved:    []MovedResult{},
	}
	var applied []string
	for _, item := range plan {
		switch item.op.kind {
		case opAdd:
			if err := atomicWriteFile(item.source.Path, item.content, item.mode); err != nil {
				return Result{}, writeFailed(item.source.DisplayPath, applied)
			}
			result.Added = append(result.Added, item.source.DisplayPath)
			applied = append(applied, item.source.DisplayPath)
		case opDelete:
			if err := os.Remove(item.source.Path); err != nil {
				return Result{}, writeFailed(item.source.DisplayPath, applied)
			}
			result.Deleted = append(result.Deleted, item.source.DisplayPath)
			applied = append(applied, item.source.DisplayPath)
		case opUpdate:
			target := item.source
			if item.destination != nil {
				target = *item.destination
			}
			if err := atomicWriteFile(target.Path, item.content, item.mode); err != nil {
				return Result{}, writeFailed(target.DisplayPath, applied)
			}
			applied = append(applied, target.DisplayPath)
			if item.destination != nil {
				if err := os.Remove(item.source.Path); err != nil {
					return Result{}, writeFailed(item.source.DisplayPath, applied)
				}
				applied = append(applied, item.source.DisplayPath)
				result.Moved = append(result.Moved, MovedResult{From: item.source.DisplayPath, To: item.destination.DisplayPath})
			} else {
				result.Modified = append(result.Modified, item.source.DisplayPath)
			}
		}
	}
	return result, nil
}

func modeForAddTarget(resolved pathsafe.Resolved) (os.FileMode, *protocol.ToolError) {
	info, err := os.Stat(resolved.Path)
	if err == nil {
		if info.IsDir() {
			return 0, &protocol.ToolError{Kind: "is_directory", Message: "path is a directory", Path: resolved.DisplayPath}
		}
		return preservedPatchMode(info.Mode()), nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return 0o644, nil
	}
	return 0, &protocol.ToolError{Kind: "not_found", Message: "path could not be inspected", Path: resolved.DisplayPath}
}

func preservedPatchMode(mode os.FileMode) os.FileMode {
	return mode & (os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky)
}

func statPatchTarget(resolved pathsafe.Resolved) (os.FileInfo, *protocol.ToolError) {
	info, err := os.Stat(resolved.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, &protocol.ToolError{Kind: "not_found", Message: "path does not exist", Path: resolved.DisplayPath}
		}
		return nil, &protocol.ToolError{Kind: "not_found", Message: "path could not be read", Path: resolved.DisplayPath}
	}
	if info.IsDir() {
		return nil, &protocol.ToolError{Kind: "is_directory", Message: "path is a directory", Path: resolved.DisplayPath}
	}
	if !info.Mode().IsRegular() {
		return nil, &protocol.ToolError{Kind: "not_found", Message: "path could not be read", Path: resolved.DisplayPath}
	}
	return info, nil
}

func readPatchFile(resolved pathsafe.Resolved) ([]byte, *protocol.ToolError) {
	file, err := os.Open(resolved.Path) //nolint:gosec // path was resolved through contract containment.
	if err != nil {
		return nil, &protocol.ToolError{Kind: "not_found", Message: "path could not be read", Path: resolved.DisplayPath}
	}
	defer func() { _ = file.Close() }()
	body, err := io.ReadAll(io.LimitReader(file, patchReadMaxBytes+1))
	if err != nil {
		return nil, &protocol.ToolError{Kind: "not_found", Message: "path could not be read", Path: resolved.DisplayPath}
	}
	return body, nil
}

func applyUpdate(resolved pathsafe.Resolved, content string, chunks []chunk) (string, *protocol.ToolError) {
	orig := strings.Split(content, "\n")
	if len(orig) > 0 && orig[len(orig)-1] == "" {
		orig = orig[:len(orig)-1]
	}
	replacements := make([]replacement, 0, len(chunks))
	cursor := 0
	for _, chunk := range chunks {
		if chunk.anchor != nil {
			idx, ok := seek(orig, []string{*chunk.anchor}, cursor, false)
			if !ok {
				return "", patchVerify("failed to find context '"+*chunk.anchor+"' in "+resolved.DisplayPath, resolved.DisplayPath, nil)
			}
			cursor = idx + 1
		}
		if len(chunk.oldLines) == 0 {
			replacements = append(replacements, replacement{index: len(orig), delCount: 0, newLines: append([]string{}, chunk.newLines...)})
			continue
		}
		oldLines := append([]string{}, chunk.oldLines...)
		newLines := append([]string{}, chunk.newLines...)
		delCount := len(oldLines)
		idx, ok := seek(orig, oldLines, cursor, chunk.isEOF)
		if !ok && len(oldLines) > 0 && oldLines[len(oldLines)-1] == "" {
			oldLines = oldLines[:len(oldLines)-1]
			if len(newLines) > 0 && newLines[len(newLines)-1] == "" {
				newLines = newLines[:len(newLines)-1]
			}
			idx, ok = seek(orig, oldLines, cursor, chunk.isEOF)
			if ok {
				delCount--
			}
		}
		if !ok {
			return "", patchVerify("failed to find expected lines in "+resolved.DisplayPath, resolved.DisplayPath, map[string]any{"expected": capExpectedLines(oldLines)})
		}
		replacements = append(replacements, replacement{index: idx, delCount: delCount, newLines: newLines})
		cursor = idx + delCount
	}
	sort.SliceStable(replacements, func(i, j int) bool {
		return replacements[i].index < replacements[j].index
	})
	for i := len(replacements) - 1; i >= 0; i-- {
		repl := replacements[i]
		if repl.index < 0 || repl.index > len(orig) || repl.index+repl.delCount > len(orig) {
			return "", patchVerify("replacement index is out of range in "+resolved.DisplayPath, resolved.DisplayPath, nil)
		}
		next := make([]string, 0, len(orig)-repl.delCount+len(repl.newLines))
		next = append(next, orig[:repl.index]...)
		next = append(next, repl.newLines...)
		next = append(next, orig[repl.index+repl.delCount:]...)
		orig = next
	}
	result, tooLarge := joinPatchLinesBounded(orig, patchReadMaxBytes)
	if tooLarge {
		return "", &protocol.ToolError{Kind: "too_large", Message: "patched file exceeds 32 MiB patch cap", Path: resolved.DisplayPath}
	}
	return result, nil
}

func joinPatchLinesBounded(lines []string, maxBytes int) (string, bool) {
	if len(lines) == 0 || lines[len(lines)-1] != "" {
		lines = append(lines, "")
	}
	size := len(lines) - 1
	for _, line := range lines {
		size += len(line)
		if size > maxBytes {
			return "", true
		}
	}
	var result strings.Builder
	result.Grow(size)
	for index, line := range lines {
		if index > 0 {
			result.WriteByte('\n')
		}
		result.WriteString(line)
	}
	return result.String(), false
}

func capExpectedLines(lines []string) string {
	text := strings.Join(lines, "\n")
	return capDiagnostic(text)
}

func addFileBytes(lines []string) []byte {
	if len(lines) == 0 {
		return []byte{}
	}
	return []byte(strings.Join(lines, "\n") + "\n")
}

// seek locates a pattern (an anchor text or a chunk's old_lines) through four
// ordered passes: exact, right-trim, full-trim, then Unicode normalize
// (dashes/quotes/spaces). Each pass scans the whole remaining range before the
// next pass runs, so a later-position exact match always beats an
// earlier-position fuzzy one; first hit wins within a pass. There is no
// multiple-match detection anywhere; duplicate blocks are disambiguated only by
// the advancing cursor and @@ anchors of the caller (applyUpdate).
func seek(file []string, pattern []string, start int, isEOF bool) (int, bool) {
	if len(pattern) == 0 {
		return start, true
	}
	if len(pattern) > len(file) {
		return 0, false
	}
	searchStart := start
	if isEOF {
		searchStart = len(file) - len(pattern)
	}
	passes := []func(string) string{
		func(value string) string { return value },
		func(value string) string { return strings.TrimRightFunc(value, unicode.IsSpace) },
		strings.TrimSpace,
		normalizeLine,
	}
	maxStart := len(file) - len(pattern)
	for _, pass := range passes {
		normalizedPattern := make([]string, len(pattern))
		for i, line := range pattern {
			normalizedPattern[i] = pass(line)
		}
		for i := searchStart; i <= maxStart; i++ {
			match := true
			for j := range normalizedPattern {
				if pass(file[i+j]) != normalizedPattern[j] {
					match = false
					break
				}
			}
			if match {
				return i, true
			}
		}
	}
	return 0, false
}

func normalizeLine(value string) string {
	return strings.Map(normalizeRune, strings.TrimSpace(value))
}

func normalizeRune(r rune) rune {
	switch {
	case r >= '\u2010' && r <= '\u2015':
		return '-'
	case r == '\u2212':
		return '-'
	case r >= '\u2018' && r <= '\u201b':
		return '\''
	case r >= '\u201c' && r <= '\u201f':
		return '"'
	case r == '\u00a0' || (r >= '\u2002' && r <= '\u200a') || r == '\u202f' || r == '\u205f' || r == '\u3000':
		return ' '
	default:
		return r
	}
}

func invalidInput(message string) *protocol.ToolError {
	return &protocol.ToolError{Kind: "invalid_input", Message: message}
}

func patchParseError(err error) *protocol.ToolError {
	var parseErr parseError
	if errors.As(err, &parseErr) {
		return &protocol.ToolError{Kind: "patch_parse", Message: capDiagnostic(parseErr.message), Detail: map[string]any{"line": parseErr.line}}
	}
	return &protocol.ToolError{Kind: "patch_parse", Message: capDiagnostic(err.Error())}
}

func patchVerify(message string, pathValue string, detail map[string]any) *protocol.ToolError {
	return &protocol.ToolError{Kind: "patch_verify", Message: capDiagnostic(message), Path: pathValue, Detail: detail}
}

func writeFailed(pathValue string, applied []string) *protocol.ToolError {
	return &protocol.ToolError{
		Kind:    "write_failed",
		Message: "write failed",
		Path:    pathValue,
		Detail:  map[string]any{"applied": append([]string{}, applied...), "failed_at": pathValue},
	}
}

func capDiagnostic(value string) string {
	if len(value) <= patchDiagnosticMaxBytes {
		return value
	}
	cut := patchDiagnosticMaxBytes
	for cut > 0 && !utf8.RuneStart(value[cut]) {
		cut--
	}
	if cut == 0 {
		return value[:patchDiagnosticMaxBytes]
	}
	return value[:cut]
}

func defaultAtomicWriteFile(target string, content []byte, mode os.FileMode) error {
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:gosec // Workspace-created dirs must remain traversable by the runtime user as well as the helper user.
		return err
	}
	tmp, err := createAtomicTemp(dir, filepath.Base(target))
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpPath)
		}
	}()
	n, err := tmp.Write(content)
	if err != nil {
		_ = tmp.Close()
		return err
	}
	if n != len(content) {
		_ = tmp.Close()
		return io.ErrShortWrite
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpPath, mode); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, target); err != nil {
		return err
	}
	committed = true
	fsyncDirectoryBestEffort(dir)
	return nil
}

func createAtomicTemp(dir string, base string) (*os.File, error) {
	for attempt := 0; attempt < 16; attempt++ {
		name, err := atomicTempName(dir, base)
		if err != nil {
			return nil, err
		}
		file, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // temp path is generated in the resolved target directory.
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("could not allocate temporary file")
}

func atomicTempName(dir string, base string) (string, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return filepath.Join(dir, "."+base+".tetral-tmp-"+hex.EncodeToString(raw[:])), nil
}

func fsyncDirectoryBestEffort(dir string) {
	handle, err := os.Open(dir) //nolint:gosec // dir is the resolved target directory.
	if err != nil {
		return
	}
	defer func() { _ = handle.Close() }()
	_ = handle.Sync()
}
