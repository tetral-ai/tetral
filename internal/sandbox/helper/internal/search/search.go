package search

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/pathsafe"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

const (
	// Grep head_limit defaults to 250 and clamps to [1, 1000]; there is no
	// unlimited mode. maxGrepFilesCollected caps files-mode candidate collection
	// at 10000 before the mtime sort and the offset+head_limit slice.
	//
	// maxGlobPathsCollected is 101 by design: Glob collects up to the 101st
	// path and returns the first 100, using the 101st only as the signal that
	// the result was truncated.
	//
	// searchDeadline is the 20 s wall clock for one rg run; expiry SIGKILLs the
	// rg group and returns timeout with no partial results.
	grepVisibleBytes      = 50 * 1024
	grepVisibleLines      = 2000
	defaultGrepHeadLimit  = 250
	maxGrepHeadLimit      = 1000
	maxGrepFilesCollected = 10000
	maxGlobPathsCollected = 101
	searchDeadline        = 20 * time.Second
	searchStderrBytes     = 4 * 1024
	searchStdoutChunkSize = 8 * 1024
	searchMaxLineBytes    = 64 * 1024
	processTerminateGrace = 2 * time.Second
)

var (
	rgBinary            = "rg"
	rgDeadline          = searchDeadline
	newRGSeparatorToken = randomRGSeparatorToken
)

type GrepResult struct {
	Mode       string `json:"mode"`
	Text       string `json:"text"`
	MatchCount int    `json:"match_count"`
	Truncated  bool   `json:"truncated"`
}

type GlobResult struct {
	Paths     []string `json:"paths"`
	Truncated bool     `json:"truncated"`
}

type grepInput struct {
	Pattern         string   `json:"pattern"`
	Path            *string  `json:"path"`
	Globs           []string `json:"globs"`
	FileType        *string  `json:"file_type"`
	Mode            string   `json:"mode"`
	CaseInsensitive bool     `json:"case_insensitive"`
	LineNumbers     *bool    `json:"line_numbers"`
	BeforeContext   int      `json:"before_context"`
	AfterContext    int      `json:"after_context"`
	Context         int      `json:"context"`
	Multiline       bool     `json:"multiline"`
	HeadLimit       *int     `json:"head_limit"`
	Offset          int      `json:"offset"`
}

type globInput struct {
	Pattern string  `json:"pattern"`
	Path    *string `json:"path"`
}

type searchRoot struct {
	Path          string
	DisplayPath   string
	WorkspaceRoot string
	SearchArg     string
}

type searchLineCollector interface {
	Accept(line string) bool
	Result() any
}

type grepFilesCollector struct {
	paths     []string
	truncated bool
}

type grepTextCollector struct {
	lines       []string
	bytes       int
	limit       int
	byteCap     int
	lineCap     int
	countMode   bool
	separators  rgFieldSeparators
	outputLines int
	lineCount   int
	truncated   bool
	matchCount  int
}

type rgFieldSeparators struct {
	Match   string
	Context string
}

type globCollector struct {
	paths     []string
	truncated bool
}

type collectedSearch struct {
	stdout any
	stderr string
	code   int
}

func Grep(payload protocol.Payload) (GrepResult, *protocol.ToolError) {
	var input grepInput
	if err := json.Unmarshal(payload.Input, &input); err != nil {
		return GrepResult{}, invalidInput("input must be an object")
	}
	if input.Pattern == "" {
		return GrepResult{}, invalidInput("pattern is required")
	}
	mode := input.Mode
	if mode == "" {
		mode = "files"
	}
	if mode != "files" && mode != "content" && mode != "count" {
		return GrepResult{}, invalidInput("mode must be files, content, or count")
	}
	if input.Offset < 0 {
		return GrepResult{}, invalidInput("offset must be non-negative")
	}
	headLimit := grepHeadLimit(input.HeadLimit, payload)
	byteCap := grepByteCap(payload)
	lineCap := grepLineCap(payload)
	root, toolErr := resolveSearchRoot(payload, input.Path)
	if toolErr != nil {
		return GrepResult{}, toolErr
	}
	separators := rgFieldSeparators{}
	if mode == "content" {
		separators = newRGFieldSeparators()
	}
	args := grepArgs(input, mode, root.SearchArg, separators)
	var collector searchLineCollector
	switch mode {
	case "files":
		collector = &grepFilesCollector{}
	case "content":
		collector = &grepTextCollector{limit: headLimit, byteCap: byteCap, lineCap: lineCap, separators: separators}
	case "count":
		collector = &grepTextCollector{limit: headLimit, byteCap: byteCap, lineCap: lineCap, countMode: true}
	}
	collected, toolErr := runRG(root.WorkspaceRoot, args, collector)
	if toolErr != nil {
		return GrepResult{}, toolErr
	}
	switch mode {
	case "files":
		return grepFilesResult(root, collected.stdout.(*grepFilesCollector), input.Offset, headLimit, byteCap), nil
	default:
		text := collected.stdout.(*grepTextCollector)
		return GrepResult{
			Mode:       mode,
			Text:       strings.Join(text.lines, ""),
			MatchCount: text.matchCount,
			Truncated:  text.truncated,
		}, nil
	}
}

func Glob(payload protocol.Payload) (GlobResult, *protocol.ToolError) {
	var input globInput
	if err := json.Unmarshal(payload.Input, &input); err != nil {
		return GlobResult{}, invalidInput("input must be an object")
	}
	if input.Pattern == "" {
		return GlobResult{}, invalidInput("pattern is required")
	}
	root, toolErr := resolveSearchRoot(payload, input.Path)
	if toolErr != nil {
		return GlobResult{}, toolErr
	}
	collector := &globCollector{}
	args := []string{"--files", "--glob", input.Pattern, "--sort=modified", "--no-ignore", "--hidden", root.SearchArg}
	collected, toolErr := runRG(root.WorkspaceRoot, args, collector)
	if toolErr != nil {
		return GlobResult{}, toolErr
	}
	out := collected.stdout.(*globCollector)
	paths := out.paths
	truncated := out.truncated
	if len(paths) > 100 {
		paths = paths[:100]
		truncated = true
	}
	return GlobResult{Paths: paths, Truncated: truncated}, nil
}

func resolveSearchRoot(payload protocol.Payload, inputPath *string) (searchRoot, *protocol.ToolError) {
	pathValue := payload.WorkspaceRoot
	if inputPath != nil && *inputPath != "" {
		pathValue = *inputPath
	}
	resolved, err := (pathsafe.Resolver{WorkspaceRoot: payload.WorkspaceRoot, Roots: payload.Roots}).Resolve(pathValue, false)
	if err != nil {
		return searchRoot{}, pathsafe.ToolError(err)
	}
	info, err := os.Stat(resolved.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return searchRoot{}, &protocol.ToolError{Kind: "not_found", Message: "path does not exist", Path: resolved.DisplayPath}
		}
		return searchRoot{}, &protocol.ToolError{Kind: "not_found", Message: "path could not be read", Path: resolved.DisplayPath}
	}
	if !info.IsDir() {
		return searchRoot{}, &protocol.ToolError{Kind: "not_a_directory", Message: "path is not a directory", Path: resolved.DisplayPath}
	}
	searchArg := filepath.ToSlash(resolved.DisplayPath)
	if searchArg == "" {
		searchArg = "."
	}
	return searchRoot{
		Path:          resolved.Path,
		DisplayPath:   resolved.DisplayPath,
		WorkspaceRoot: filepath.Clean(payload.WorkspaceRoot),
		SearchArg:     searchArg,
	}, nil
}

func grepArgs(input grepInput, mode string, searchArg string, separators rgFieldSeparators) []string {
	args := []string{
		"--hidden",
		"--glob", "!.git",
		"--glob", "!.svn",
		"--glob", "!.hg",
		"--glob", "!.bzr",
		"--glob", "!.jj",
		"--max-columns", "500",
	}
	if input.Multiline {
		args = append(args, "-U", "--multiline-dotall")
	}
	if input.FileType != nil && *input.FileType != "" {
		args = append(args, "--type", *input.FileType)
	}
	for _, glob := range input.Globs {
		if glob != "" {
			args = append(args, "--glob", glob)
		}
	}
	if input.CaseInsensitive {
		args = append(args, "--ignore-case")
	}
	switch mode {
	case "files":
		args = append(args, "--files-with-matches")
	case "count":
		args = append(args, "--count")
	case "content":
		args = append(args, "--field-match-separator", separators.Match, "--field-context-separator", separators.Context)
		lineNumbers := true
		if input.LineNumbers != nil {
			lineNumbers = *input.LineNumbers
		}
		if lineNumbers {
			args = append(args, "--line-number")
		} else {
			args = append(args, "--no-line-number")
		}
		if input.Context > 0 {
			args = append(args, "--context", fmt.Sprint(input.Context))
		} else {
			if input.BeforeContext > 0 {
				args = append(args, "--before-context", fmt.Sprint(input.BeforeContext))
			}
			if input.AfterContext > 0 {
				args = append(args, "--after-context", fmt.Sprint(input.AfterContext))
			}
		}
	}
	args = append(args, "--", input.Pattern, searchArg)
	return args
}

func grepHeadLimit(value *int, payload protocol.Payload) int {
	limit := clampHeadLimit(value)
	if payload.Limits.VisibleLines > 0 && payload.Limits.VisibleLines < limit {
		return payload.Limits.VisibleLines
	}
	return limit
}

func clampHeadLimit(value *int) int {
	if value == nil {
		return defaultGrepHeadLimit
	}
	if *value < 1 {
		return 1
	}
	if *value > maxGrepHeadLimit {
		return maxGrepHeadLimit
	}
	return *value
}

func grepByteCap(payload protocol.Payload) int {
	if payload.Limits.VisibleBytes > 0 && payload.Limits.VisibleBytes < grepVisibleBytes {
		return payload.Limits.VisibleBytes
	}
	return grepVisibleBytes
}

func grepLineCap(payload protocol.Payload) int {
	if payload.Limits.VisibleLines > 0 && payload.Limits.VisibleLines < grepVisibleLines {
		return payload.Limits.VisibleLines
	}
	return grepVisibleLines
}

func runRG(root string, args []string, collector searchLineCollector) (collectedSearch, *protocol.ToolError) {
	ctx, cancel := context.WithTimeout(context.Background(), rgDeadline)
	defer cancel()
	cmd := exec.Command(rgBinary, args...) //nolint:gosec // rg args are structured; shell is not involved.
	cmd.Dir = root
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return collectedSearch{}, invalidInput("rg stdout pipe failed")
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return collectedSearch{}, invalidInput("rg stderr pipe failed")
	}
	if err := cmd.Start(); err != nil {
		return collectedSearch{}, invalidInput("rg could not start")
	}
	processDone := make(chan struct{})
	forceClosePipes := sync.OnceFunc(func() {
		_ = stdout.Close()
		_ = stderr.Close()
	})
	var startTerminate sync.Once
	requestTerminate := func() {
		startTerminate.Do(func() {
			go func() {
				terminateProcessGroupWithGrace(cmd.Process, processDone)
				forceClosePipes()
			}()
		})
	}
	requestImmediateKill := func() {
		killProcessGroup(cmd.Process)
		forceClosePipes()
	}
	go func() {
		select {
		case <-ctx.Done():
			requestImmediateKill()
		case <-processDone:
		}
	}()
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	defer signal.Stop(signalCh)
	go func() {
		select {
		case sig := <-signalCh:
			requestTerminate()
			select {
			case <-processDone:
			case <-time.After(processTerminateGrace + time.Second):
			}
			exitForSignal(sig)
		case <-processDone:
		}
	}()
	stderrDone := make(chan string, 1)
	go func() { stderrDone <- readBoundedText(stderr, searchStderrBytes) }()

	stoppedEarly := readSearchStdout(stdout, collector)
	if stoppedEarly {
		requestImmediateKill()
	}
	stderrText := <-stderrDone
	err = cmd.Wait()
	close(processDone)
	if ctx.Err() == context.DeadlineExceeded {
		return collectedSearch{}, &protocol.ToolError{Kind: "timeout", Message: "search timed out"}
	}
	code := exitCode(err)
	if stoppedEarly {
		code = 0
	}
	switch code {
	case 0, 1:
		return collectedSearch{stdout: collector.Result(), stderr: stderrText, code: code}, nil
	case 2:
		return collectedSearch{}, &protocol.ToolError{Kind: "invalid_input", Message: "rg rejected the query", Detail: map[string]any{"stderr": stderrText}}
	default:
		return collectedSearch{}, &protocol.ToolError{Kind: "invalid_input", Message: "rg failed", Detail: map[string]any{"stderr": stderrText}}
	}
}

func readSearchStdout(reader io.Reader, collector searchLineCollector) bool {
	buffer := make([]byte, searchStdoutChunkSize)
	line := make([]byte, 0, searchStdoutChunkSize)
	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			chunk := buffer[:n]
			for len(chunk) > 0 {
				newline := bytes.IndexByte(chunk, '\n')
				if newline < 0 {
					if len(line)+len(chunk) > searchMaxLineBytes {
						markSearchCollectorTruncated(collector)
						return true
					}
					line = append(line, chunk...)
					break
				}
				segment := chunk[:newline+1]
				if len(line)+len(segment) > searchMaxLineBytes {
					markSearchCollectorTruncated(collector)
					return true
				}
				line = append(line, segment...)
				if !collector.Accept(string(line)) {
					return true
				}
				line = line[:0]
				chunk = chunk[newline+1:]
			}
		}
		if err != nil {
			if len(line) > 0 && !collector.Accept(string(line)) {
				return true
			}
			return false
		}
	}
}

func markSearchCollectorTruncated(collector searchLineCollector) {
	switch typed := collector.(type) {
	case *grepFilesCollector:
		typed.truncated = true
	case *grepTextCollector:
		typed.truncated = true
	case *globCollector:
		typed.truncated = true
	}
}

func readBoundedText(reader io.Reader, limit int) string {
	var out bytes.Buffer
	_, _ = io.Copy(&out, io.LimitReader(reader, int64(limit)))
	_, _ = io.Copy(io.Discard, reader)
	return out.String()
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

func terminateProcessGroupWithGrace(process *os.Process, done <-chan struct{}) {
	if process == nil {
		return
	}
	_ = syscall.Kill(-process.Pid, syscall.SIGTERM)
	timer := time.NewTimer(processTerminateGrace)
	defer timer.Stop()
	select {
	case <-done:
		return
	case <-timer.C:
	}
	killProcessGroup(process)
}

func killProcessGroup(process *os.Process) {
	if process == nil {
		return
	}
	_ = syscall.Kill(-process.Pid, syscall.SIGKILL)
}

func exitForSignal(sig os.Signal) {
	if signalValue, ok := sig.(syscall.Signal); ok {
		os.Exit(128 + int(signalValue))
	}
	os.Exit(1)
}

func (c *grepFilesCollector) Accept(line string) bool {
	if len(c.paths) >= maxGrepFilesCollected {
		c.truncated = true
		return false
	}
	c.paths = append(c.paths, cleanRGLine(line))
	if len(c.paths) >= maxGrepFilesCollected {
		c.truncated = true
		return false
	}
	return true
}

func (c *grepFilesCollector) Result() any { return c }

func (c *grepTextCollector) Accept(line string) bool {
	if (c.countMode && c.lineCount >= c.limit) || c.bytes >= c.byteCap || (c.lineCap > 0 && c.outputLines >= c.lineCap) {
		c.truncated = true
		return false
	}
	cleaned := cleanRGOutputLine(line)
	normalized := cleaned
	if !c.countMode {
		normalized = normalizeRGFieldSeparators(cleaned, c.separators)
	}
	if c.bytes+len(normalized) > c.byteCap {
		c.truncated = true
		return false
	}
	c.lines = append(c.lines, normalized)
	c.bytes += len(normalized)
	c.outputLines++
	if c.countMode {
		c.lineCount++
		c.matchCount += parseRGCount(normalized)
		if c.lineCount >= c.limit {
			c.truncated = true
			return false
		}
	} else {
		if rgLineHasMatchFieldSeparator(cleaned, c.separators) {
			c.matchCount++
		}
		if c.matchCount >= c.limit {
			c.truncated = true
			return false
		}
	}
	if c.bytes >= c.byteCap || (c.lineCap > 0 && c.outputLines >= c.lineCap) {
		c.truncated = true
		return false
	}
	return true
}

func (c *grepTextCollector) Result() any { return c }

func parseRGCount(line string) int {
	cleaned := cleanRGLine(line)
	idx := strings.LastIndex(cleaned, ":")
	if idx < 0 {
		return 1
	}
	var count int
	if _, err := fmt.Sscanf(cleaned[idx+1:], "%d", &count); err != nil || count < 0 {
		return 1
	}
	return count
}

func (c *globCollector) Accept(line string) bool {
	if len(c.paths) >= maxGlobPathsCollected {
		c.truncated = true
		return false
	}
	c.paths = append(c.paths, cleanRGLine(line))
	if len(c.paths) >= maxGlobPathsCollected {
		c.truncated = true
		return false
	}
	return true
}

func (c *globCollector) Result() any { return c }

func grepFilesResult(root searchRoot, collected *grepFilesCollector, offset int, headLimit int, byteCap int) GrepResult {
	entries := make([]grepFileEntry, 0, len(collected.paths))
	for _, rel := range collected.paths {
		info, err := os.Stat(searchResultStatPath(root, rel))
		if err != nil {
			continue
		}
		entries = append(entries, grepFileEntry{path: rel, mtime: info.ModTime()})
	}
	sort.Slice(entries, func(i, j int) bool {
		if !entries[i].mtime.Equal(entries[j].mtime) {
			return entries[i].mtime.After(entries[j].mtime)
		}
		return entries[i].path < entries[j].path
	})
	truncated := collected.truncated || offset > 0 || offset+headLimit < len(entries)
	if offset > len(entries) {
		offset = len(entries)
	}
	end := offset + headLimit
	if end > len(entries) {
		end = len(entries)
	}
	paths := make([]string, 0, end-offset)
	textBytes := 0
	for _, entry := range entries[offset:end] {
		line := entry.path + "\n"
		if textBytes+len(line) > byteCap {
			truncated = true
			break
		}
		paths = append(paths, entry.path)
		textBytes += len(line)
	}
	text := ""
	if len(paths) > 0 {
		text = strings.Join(paths, "\n") + "\n"
	}
	return GrepResult{Mode: "files", Text: text, MatchCount: len(entries), Truncated: truncated}
}

type grepFileEntry struct {
	path  string
	mtime time.Time
}

func cleanRGLine(line string) string {
	cleaned := strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	return strings.TrimPrefix(cleaned, "./")
}

func cleanRGOutputLine(line string) string {
	hasNewline := strings.HasSuffix(line, "\n")
	cleaned := cleanRGLine(line)
	if hasNewline {
		return cleaned + "\n"
	}
	return cleaned
}

func newRGFieldSeparators() rgFieldSeparators {
	token := newRGSeparatorToken()
	return rgFieldSeparators{
		Match:   "\x1ftetral-match-" + token + "\x1f",
		Context: "\x1ftetral-context-" + token + "\x1f",
	}
}

func randomRGSeparatorToken() string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(bytes[:])
}

func normalizeRGFieldSeparators(line string, separators rgFieldSeparators) string {
	index, separator, replacement, _, ok := firstRGFieldSeparator(line, separators)
	if !ok {
		return line
	}
	tail := line[index+len(separator):]
	if second := strings.Index(tail, separator); second >= 0 && allASCIIDigits(tail[:second]) {
		return line[:index] + replacement + tail[:second] + replacement + tail[second+len(separator):]
	}
	return line[:index] + replacement + tail
}

func rgLineHasMatchFieldSeparator(line string, separators rgFieldSeparators) bool {
	_, _, _, match, ok := firstRGFieldSeparator(line, separators)
	return ok && match
}

func firstRGFieldSeparator(line string, separators rgFieldSeparators) (int, string, string, bool, bool) {
	matchIndex := strings.Index(line, separators.Match)
	contextIndex := strings.Index(line, separators.Context)
	if matchIndex < 0 && contextIndex < 0 {
		return 0, "", "", false, false
	}
	if matchIndex >= 0 && (contextIndex < 0 || matchIndex < contextIndex) {
		return matchIndex, separators.Match, ":", true, true
	}
	return contextIndex, separators.Context, "-", false, true
}

func allASCIIDigits(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func searchResultStatPath(root searchRoot, resultPath string) string {
	cleaned := filepath.FromSlash(resultPath)
	if filepath.IsAbs(cleaned) {
		return cleaned
	}
	return filepath.Join(root.WorkspaceRoot, cleaned)
}

func invalidInput(message string) *protocol.ToolError {
	return &protocol.ToolError{Kind: "invalid_input", Message: message}
}
