package testinfra

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"unicode"
)

// A failing step's complete diagnostic stays in the evidence artifact. The
// runner also prints a bounded excerpt of it to the job log, and to the job
// summary under GitHub Actions, so a red job names what failed without an
// artifact download. The excerpt keeps a diagnostic's start, where a panic or
// build error begins, and its end, where a failed test reports; its 80 lines
// hold a complete two-package Bun audit with more than a dozen advisories.
const (
	excerptHeadLines = 20
	excerptTailLines = 60
	excerptLineBytes = 300
	stepSummaryEnv   = "GITHUB_STEP_SUMMARY"
)

var (
	// Go packages fail concurrently; each excerpt is written whole.
	excerptOutput sync.Mutex
	terminalStyle = regexp.MustCompile(`\x1b\[[0-9;]*[A-Za-z]`)
)

func reportFailedStep(group string, spec commandSpec, diagnosticPath string) {
	lines := diagnosticExcerpt(diagnosticPath, spec.Kind == "go-json", spec.RejectSkip)
	if len(lines) == 0 {
		return
	}
	command := truncateExcerptLine(strings.Join(spec.Arguments, " "))
	diagnostic := filepath.Base(diagnosticPath)
	excerptOutput.Lock()
	defer excerptOutput.Unlock()
	fmt.Printf("%s step failed: %s\nexcerpt of diagnostic %s:\n  | %s\n", group, command, diagnostic, strings.Join(lines, "\n  | "))
	summaryPath := os.Getenv(stepSummaryEnv)
	if summaryPath == "" {
		return
	}
	excerpt := strings.Join(lines, "\n")
	fence := "```"
	for strings.Contains(excerpt, fence) {
		fence += "`"
	}
	// GitHub Actions names the job summary file this step appends to.
	//nolint:gosec
	summary, err := os.OpenFile(summaryPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return
	}
	_, _ = fmt.Fprintf(summary, "#### %s step failed\n\n`%s` (diagnostic `%s`)\n\n%stext\n%s\n%s\n\n", group, command, diagnostic, fence, excerpt, fence)
	_ = summary.Close()
}

// diagnosticExcerpt keeps the first and last lines of a diagnostic. A go test
// -json report is first reduced to the decoded output of what did not pass.
func diagnosticExcerpt(path string, goJSON, rejectSkip bool) []string {
	// The path is the runner-owned diagnostic of the failed step.
	//nolint:gosec
	body, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	if goJSON {
		body = goTestOutputNotPassed(body, rejectSkip)
	}
	// The Actions runner also ends a line at a bare carriage return, so each one
	// becomes a line break that gets the excerpt prefix and cannot start a
	// workflow command; other control characters are dropped.
	text := strings.ReplaceAll(terminalStyle.ReplaceAllString(string(body), ""), "\r\n", "\n")
	text = strings.Map(func(r rune) rune {
		switch {
		case r == '\r':
			return '\n'
		case r == '\n' || r == '\t':
			return r
		case unicode.IsControl(r):
			return -1
		}
		return r
	}, text)
	text = strings.TrimRight(text, " \t\n")
	if strings.TrimSpace(text) == "" {
		return nil
	}
	lines := strings.Split(text, "\n")
	if omitted := len(lines) - excerptHeadLines - excerptTailLines; omitted > 0 {
		lines = slices.Concat(lines[:excerptHeadLines], []string{fmt.Sprintf("... %d lines omitted ...", omitted)}, lines[len(lines)-excerptTailLines:])
	}
	for index, line := range lines {
		lines[index] = truncateExcerptLine(line)
	}
	return lines
}

// goTestOutputNotPassed returns, in report order, the output of every test and
// package without a pass event: failed tests, a test interrupted by a panic or
// timeout, and package and build failures. A skip counts as a pass, except a
// test's skip in a step that rejects skips.
func goTestOutputNotPassed(report []byte, rejectSkip bool) []byte {
	type runnable struct{ pkg, test string }
	type output struct {
		owner runnable
		text  string
	}
	var outputs []output
	passed := map[runnable]bool{}
	for _, line := range bytes.Split(report, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var event struct{ Action, Package, Test, Output string }
		if json.Unmarshal(line, &event) != nil {
			// The go command's own stderr shares the diagnostic file.
			outputs = append(outputs, output{text: string(line) + "\n"})
			continue
		}
		owner := runnable{pkg: event.Package, test: event.Test}
		switch event.Action {
		case "output", "build-output":
			outputs = append(outputs, output{owner: owner, text: event.Output})
		case "pass":
			passed[owner] = true
		case "skip":
			passed[owner] = !rejectSkip || event.Test == ""
		}
	}
	var notPassed bytes.Buffer
	for _, item := range outputs {
		if !passed[item.owner] {
			notPassed.WriteString(item.text)
		}
	}
	return notPassed.Bytes()
}

func truncateExcerptLine(line string) string {
	if len(line) <= excerptLineBytes {
		return line
	}
	return strings.ToValidUTF8(line[:excerptLineBytes], "") + " ..."
}
