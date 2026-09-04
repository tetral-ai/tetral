package static

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const frozenForkSDKCommit = "def73de473079295dcd408a65dee45b42686fcf6"

func requireWorkflowActionsUseFullSHAs(t *testing.T, name string, text string) {
	t.Helper()
	actionRef := regexp.MustCompile(`^\s*(?:-\s*)?uses:\s*([^@\s#]+)(?:@([^\s#]+))?`)
	shaRef := regexp.MustCompile(`^[0-9a-fA-F]{40}$`)
	for index, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		matches := actionRef.FindStringSubmatch(line)
		if matches == nil {
			continue
		}
		action := matches[1]
		ref := matches[2]
		if strings.HasPrefix(action, "./") || strings.HasPrefix(action, "../") {
			continue
		}
		if ref == "" {
			t.Fatalf("%s:%d action %q must pin an explicit ref", name, index+1, action)
		}
		if !shaRef.MatchString(ref) {
			t.Fatalf("%s:%d action %q ref %q must be a full commit SHA", name, index+1, action, ref)
		}
	}
}

func TestSandboxSmokeUsesReleaseRecipe(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	smokeBody, err := os.ReadFile(filepath.Join(engineRoot, "scripts", "run-sandbox-local-image-smoke.sh")) //nolint:gosec // Repository-local script.
	if err != nil {
		t.Fatalf("read local Sandbox smoke: %v", err)
	}
	if strings.Contains(string(smokeBody), "SANDBOX_HELPER_BASE_IMAGE") || strings.Contains(string(smokeBody), "--build-arg") {
		t.Fatal("local Sandbox smoke substitutes a release Dockerfile build argument")
	}
	releaseBody, err := os.ReadFile(filepath.Join(engineRoot, ".github", "workflows", "engine-release.yml")) //nolint:gosec // Repository-local workflow.
	if err != nil {
		t.Fatalf("read release workflow: %v", err)
	}
	if strings.Contains(string(releaseBody), "build_args:") || strings.Contains(string(releaseBody), "build-args:") {
		t.Fatal("release workflow retains an empty build-argument channel")
	}
}

// govulncheckCommand is the exact, whole invocation the merge-gating
// govulncheck step must run. The Makefile owns the tool version so local and CI
// vulnerability scans cannot drift.
const govulncheckCommand = "make vulncheck"
const moduleScanCommand = "go tool govulncheck -scan module"

// TestEngineCIWorkflowGatesOnGovulncheck pins the vulnerability gate. The
// whole-repository symbol-level scan needs roughly 20 GiB of resident memory
// (the sandbox service's kubernetes/AWS import closure dominates the
// analysis), which no hosted runner offers, so the workflow scans every other
// package tree at symbol level one process at a time and covers the sandbox
// closure with a module-level scan. This guard pins the triggers, the
// module-graph-scoped pull-request paths, the exact slice list, strict
// exit-status propagation for both scan steps, the runtime slice-coverage
// sweep, and the Makefile's full symbol-level target for local triage.
func TestEngineCIWorkflowGatesOnGovulncheck(t *testing.T) {
	repoRoot := finalArchitectureEngineRoot(t)
	workflowPath := filepath.Join(repoRoot, ".github", "workflows", "engine-vulncheck.yml")

	content, err := os.ReadFile(workflowPath) //nolint:gosec // test-only static read of repo workflow
	if err != nil {
		t.Fatalf("read %s: %v", workflowPath, err)
	}
	text := string(content)
	for _, trigger := range []string{"schedule:", "cron:", "workflow_dispatch:"} {
		if !strings.Contains(text, trigger) {
			t.Errorf("engine-vulncheck must declare %s so the scan runs on a schedule and on demand", trigger)
		}
	}
	// Pull requests run the gate only when the module graph (or the gate
	// itself) can change; the daily schedule owns newly published advisories.
	for _, path := range []string{"'go.mod'", "'go.sum'", "'.github/workflows/engine-vulncheck.yml'"} {
		if !strings.Contains(text, path) {
			t.Errorf("engine-vulncheck pull_request paths must include %s", path)
		}
	}

	// (a) go.mod pins the govulncheck version through its tool directive, so
	// the scanner is resolved and checksummed exactly like every other
	// dependency, and the Makefile keeps the full symbol-level verdict for
	// module-level triage on a machine with enough memory.
	moduleFile, err := os.ReadFile(filepath.Join(repoRoot, "go.mod")) //nolint:gosec // repository-local static test path.
	if err != nil {
		t.Fatalf("read engine go.mod: %v", err)
	}
	if !strings.Contains(string(moduleFile), "tool golang.org/x/vuln/cmd/govulncheck") {
		t.Error("engine go.mod must declare govulncheck as a tool dependency so its version is pinned and checksummed")
	}
	makefileContent, err := os.ReadFile(filepath.Join(repoRoot, "Makefile")) //nolint:gosec // test-only static read of repository Makefile
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	const vulncheckTarget = "vulncheck:\n\tgo tool govulncheck ./..."
	if !strings.Contains(string(makefileContent), vulncheckTarget) {
		t.Errorf("Makefile must keep the full symbol-level verdict over ./...; want:\n%s", vulncheckTarget)
	}

	job := workflowJobForStaticTest(t, text, "govulncheck:")
	if strings.Contains(job, "continue-on-error: true") {
		t.Errorf("the govulncheck job (or a step in it) must not set continue-on-error: true; job was:\n%s", job)
	}

	// (b) the symbol-level step scans the pinned slice list one process per
	// tree, under a fail-fast shell, with the scan as the sole and standalone
	// command line so no iteration's failure can be absorbed.
	const symbolInvocation = `go tool govulncheck "${slice}"`
	if count := strings.Count(job, symbolInvocation); count != 1 {
		t.Fatalf("symbol-level scan invocation count = %d; want exactly one %q", count, symbolInvocation)
	}
	for _, line := range strings.Split(job, "\n") {
		if strings.Contains(line, symbolInvocation) && strings.TrimSpace(line) != symbolInvocation {
			t.Errorf("the symbol-level scan must be a standalone command line; got: %s", strings.TrimSpace(line))
		}
	}
	if !strings.Contains(job, "set -euo pipefail") {
		t.Error("the symbol-level scan step must run under set -euo pipefail so a failing slice fails the job")
	}
	expectedSlices := []string{
		"./database/...",
		"./deploy/...",
		"./integration/...",
		"./internal/...",
		"./services/agent-runtime/...",
		"./services/api/...",
		"./services/auth/...",
		"./services/bridge/...",
		"./services/cleanup/...",
		"./services/event-stream/...",
		"./services/gateway/...",
		"./services/git-proxy/...",
		"./services/queue/...",
		"./services/web-connector/...",
	}
	slices, ok := extractWorkflowSliceList(job)
	if !ok {
		t.Fatalf("could not extract the slices=( ... ) list from the govulncheck job:\n%s", job)
	}
	if len(slices) != len(expectedSlices) {
		t.Errorf("symbol-level slice list = %v; want %v (services/sandbox stays module-level by design)", slices, expectedSlices)
	} else {
		for index := range expectedSlices {
			if slices[index] != expectedSlices[index] {
				t.Errorf("symbol-level slice[%d] = %q; want %q", index, slices[index], expectedSlices[index])
			}
		}
	}

	// (c) a runtime guard fails the job when a package tree appears outside
	// the slice list, so the split cannot silently under-cover new code.
	if !strings.Contains(job, "go list ./...") || !strings.Contains(job, "exit 1") {
		t.Error("the govulncheck job must sweep go list ./... and exit non-zero on trees missing from the slice list")
	}

	// (d) the module-level step covers the sandbox closure from a directory
	// that contains Go files (module mode rejects patterns) and propagates
	// its exit status by STRICT EQUALITY of the final executed line.
	moduleStep, found := workflowStepNamed(job, "Module-level scan")
	if !found {
		t.Fatalf("the govulncheck job must contain a module-level scan step; job was:\n%s", job)
	}
	if !strings.Contains(moduleStep, "working-directory: services/sandbox") {
		t.Errorf("the module-level scan must run from services/sandbox; step was:\n%s", moduleStep)
	}
	scalar, ok := extractRunScalar(moduleStep)
	if !ok {
		t.Fatalf("could not extract the run: scalar from the module scan step; step was:\n%s", moduleStep)
	}
	if reason := govulncheckRunPropagatesExit(scalar, extractStepShell(moduleStep), moduleScanCommand); reason != "" {
		t.Errorf("module-level gate does not propagate its exit status: %s\nrun scalar was:\n%s", reason, scalar)
	}
}

// extractWorkflowSliceList returns the entries of the symbol-scan step's
// `slices=( ... )` array in order.
func extractWorkflowSliceList(job string) ([]string, bool) {
	start := strings.Index(job, "slices=(")
	if start < 0 {
		return nil, false
	}
	rest := job[start+len("slices=("):]
	end := strings.Index(rest, ")")
	if end < 0 {
		return nil, false
	}
	var entries []string
	for _, line := range strings.Split(rest[:end], "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		entries = append(entries, trimmed)
	}
	return entries, true
}

// workflowStepNamed returns the job step whose name contains the given label.
func workflowStepNamed(job string, label string) (string, bool) {
	const stepBoundary = "\n      - name:"
	for _, step := range splitJobIntoSteps(job, stepBoundary) {
		if strings.Contains(step, label) {
			return step, true
		}
	}
	return "", false
}

// TestGovulncheckRunPropagatesExitRejectsNeuteredForms proves the structural
// strict-equality check actually rejects the known gate-neutering forms and
// accepts only the bare standalone command.
// This is the load-bearing proof that (d) is structural, not a finite operator
// blacklist.
func TestGovulncheckRunPropagatesExitRejectsNeuteredForms(t *testing.T) {
	accepted := []struct {
		name   string
		scalar string
		shell  string
	}{
		{
			name:   "bare_inline_command",
			scalar: govulncheckCommand,
		},
		{
			name:   "bare_block_command_with_leading_step",
			scalar: "go env GOFLAGS\n" + govulncheckCommand,
		},
		{
			name:   "bare_block_command_with_bash_pipefail_shell",
			scalar: govulncheckCommand,
			shell:  "bash",
		},
		{
			// A trailing comment cannot neuter the gate: govulncheck remains the
			// final EXECUTED command and its exit still propagates.
			name:   "trailing_comment_after_command",
			scalar: govulncheckCommand + "\n# vuln gate",
		},
	}
	for _, testCase := range accepted {
		t.Run("accept_"+testCase.name, func(t *testing.T) {
			if reason := govulncheckRunPropagatesExit(testCase.scalar, testCase.shell, govulncheckCommand); reason != "" {
				t.Errorf("expected the bare standalone govulncheck command to be accepted, got rejection: %s", reason)
			}
		})
	}

	rejected := []struct {
		name   string
		scalar string
		shell  string
	}{
		{
			name:   "trailing_exit_zero",
			scalar: govulncheckCommand + " ; exit 0",
		},
		{
			name:   "backgrounded",
			scalar: govulncheckCommand + " &",
		},
		{
			name:   "github_expansion_appended",
			scalar: govulncheckCommand + " ${{ env.X }}",
		},
		{
			name:   "if_then_swallows_exit",
			scalar: "if " + govulncheckCommand + "; then :; fi",
		},
		{
			name:   "preceding_line_short_circuits_via_continuation",
			scalar: "true || \\\n" + govulncheckCommand,
		},
		{
			name:   "preceding_line_dangling_or_operator",
			scalar: "true ||\n" + govulncheckCommand,
		},
		{
			name:   "pipe_to_truthy",
			scalar: govulncheckCommand + " | cat",
		},
		{
			name:   "non_pipefail_shell",
			scalar: govulncheckCommand,
			shell:  "sh",
		},
		{
			name:   "custom_bash_without_pipefail",
			scalar: govulncheckCommand,
			shell:  "bash --noprofile --norc -e {0}",
		},
	}
	for _, testCase := range rejected {
		t.Run("reject_"+testCase.name, func(t *testing.T) {
			if reason := govulncheckRunPropagatesExit(testCase.scalar, testCase.shell, govulncheckCommand); reason == "" {
				t.Errorf("expected the neutered form to be rejected, but it was accepted:\n%s", testCase.scalar)
			}
		})
	}
}

func workflowJobForStaticTest(t *testing.T, text string, jobName string) string {
	t.Helper()
	start := strings.Index(text, "\n  "+jobName)
	if start < 0 {
		t.Fatalf("workflow missing job %q", jobName)
	}
	lines := strings.Split(text[start+1:], "\n")
	var jobLines []string
	for index, line := range lines {
		if index > 0 && strings.HasPrefix(line, "  ") && len(line) > 2 && line[2] != ' ' {
			break
		}
		jobLines = append(jobLines, line)
	}
	return strings.Join(jobLines, "\n")
}

// splitJobIntoSteps slices a job block into its individual `- name:` steps.
func splitJobIntoSteps(job string, stepBoundary string) []string {
	indices := allIndices(job, stepBoundary)
	if len(indices) == 0 {
		return nil
	}
	var steps []string
	for position, start := range indices {
		end := len(job)
		if position+1 < len(indices) {
			end = indices[position+1]
		}
		steps = append(steps, job[start:end])
	}
	return steps
}

func allIndices(haystack string, needle string) []int {
	var indices []int
	offset := 0
	for {
		next := strings.Index(haystack[offset:], needle)
		if next < 0 {
			return indices
		}
		indices = append(indices, offset+next)
		offset += next + len(needle)
	}
}

// extractRunScalar pulls the shell program from a step block's `run:` key,
// handling both the inline (`run: <cmd>`) and block-scalar (`run: |`,
// `run: >-`, etc.) forms. The returned scalar is dedented to the indentation
// of its first content line so downstream structural checks see the raw shell.
func extractRunScalar(step string) (string, bool) {
	lines := strings.Split(step, "\n")
	for index, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "run:") {
			continue
		}
		afterKey := strings.TrimSpace(strings.TrimPrefix(trimmed, "run:"))
		if afterKey != "" && !isBlockScalarIndicator(afterKey) {
			// Inline form: `run: go run ... ./...`.
			return afterKey, true
		}
		// Block form: collect the indented lines that follow, more indented
		// than the `run:` key itself.
		keyIndent := indentWidth(line)
		var body []string
		bodyIndent := -1
		for _, bodyLine := range lines[index+1:] {
			if strings.TrimSpace(bodyLine) == "" {
				body = append(body, "")
				continue
			}
			if indentWidth(bodyLine) <= keyIndent {
				break
			}
			if bodyIndent < 0 {
				bodyIndent = indentWidth(bodyLine)
			}
			body = append(body, dedent(bodyLine, bodyIndent))
		}
		return strings.Join(body, "\n"), true
	}
	return "", false
}

// isBlockScalarIndicator reports whether the text after `run:` is a YAML block
// scalar header (`|`, `>`, with optional chomping/indentation indicators and
// trailing comment) rather than an inline command.
func isBlockScalarIndicator(after string) bool {
	if after == "" {
		return false
	}
	// Strip a trailing comment.
	if hash := strings.Index(after, " #"); hash >= 0 {
		after = strings.TrimSpace(after[:hash])
	}
	if after == "" {
		return false
	}
	if after[0] != '|' && after[0] != '>' {
		return false
	}
	for _, char := range after[1:] {
		if char != '-' && char != '+' && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}

func indentWidth(line string) int {
	return len(line) - len(strings.TrimLeft(line, " "))
}

func dedent(line string, width int) string {
	if indentWidth(line) < width {
		return strings.TrimLeft(line, " ")
	}
	return line[width:]
}

// extractStepShell returns the value of a step's `shell:` key, or "" when the
// step relies on the default shell.
func extractStepShell(step string) string {
	for _, line := range strings.Split(step, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "shell:") {
			return strings.TrimSpace(strings.TrimPrefix(trimmed, "shell:"))
		}
	}
	return ""
}

// govulncheckRunPropagatesExit implements the structural strict-equality
// check. It returns "" when the run scalar ends with the govulncheck
// command as a standalone top-level command whose non-zero exit cannot be
// suppressed, and a human-readable reason otherwise. This is deliberately NOT a
// blacklist of forbidden operators: the load-bearing assertion is that the
// final executed line IS exactly the govulncheck command and that nothing
// before it leaves the command in a position to be appended to, backgrounded,
// expanded, short-circuited, or absorbed.
func govulncheckRunPropagatesExit(scalar string, shell string, expectedCommand string) string {
	// A non-default shell must be bash with pipefail (a non-pipefail shell lets
	// a pipe-to-truthy swallow govulncheck's failure). GitHub's `bash` keyword
	// already runs with `set -eo pipefail`, so the bare value `bash` is safe; a
	// custom shell command line must name both `bash` and `pipefail`; any other
	// interpreter (sh, pwsh, python, …) is rejected.
	if shell != "" {
		normalizedShell := strings.TrimSpace(shell)
		safeShell := normalizedShell == "bash" ||
			(strings.Contains(normalizedShell, "bash") && strings.Contains(normalizedShell, "pipefail"))
		if !safeShell {
			return "non-default shell must be bash with pipefail, got: " + shell
		}
	}

	rawLines := strings.Split(scalar, "\n")

	// Determine the final executed line: the last line that is neither blank nor
	// a pure comment. A trailing comment must NOT displace the command from the
	// final-line position.
	finalIndex := -1
	for index := len(rawLines) - 1; index >= 0; index-- {
		trimmed := strings.TrimSpace(rawLines[index])
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		finalIndex = index
		break
	}
	if finalIndex < 0 {
		return "run scalar has no executable command"
	}

	finalLine := strings.TrimSpace(rawLines[finalIndex])
	if finalLine != expectedCommand {
		return "final executed line must be exactly " + expectedCommand + ", got: " + finalLine
	}

	// The preceding non-empty line must not leave a dangling continuation or
	// operator that would absorb the final command into a compound expression.
	for index := finalIndex - 1; index >= 0; index-- {
		trimmed := strings.TrimSpace(rawLines[index])
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.HasSuffix(trimmed, "\\") {
			return "preceding line ends with a continuation `\\` that absorbs the govulncheck command: " + trimmed
		}
		if strings.HasSuffix(trimmed, "||") || strings.HasSuffix(trimmed, "&&") || strings.HasSuffix(trimmed, "|") {
			return "preceding line ends with a dangling operator that short-circuits the govulncheck command: " + trimmed
		}
		break
	}

	// No line in the scalar may open an unterminated compound (if/while/until/
	// for ... do|then, or an unmatched `(`/`{`) that encloses the final command,
	// and the final line must itself be a bare command (no leading keyword, no
	// trailing operator/expansion/redirect).
	if reason := finalLineIsBareCommand(finalLine, expectedCommand); reason != "" {
		return reason
	}
	if reason := noEnclosingCompound(rawLines[:finalIndex]); reason != "" {
		return reason
	}
	return ""
}

// finalLineIsBareCommand verifies the final line is exactly the govulncheck
// command with no leading control keyword and no trailing token of any kind.
// Because finalLine has already been asserted equal to govulncheckCommand, this
// is a defensive restatement that keeps the meta-test honest if the equality
// check is ever loosened.
func finalLineIsBareCommand(finalLine string, expectedCommand string) string {
	if finalLine != expectedCommand {
		return "final line is not the bare govulncheck command: " + finalLine
	}
	leadingKeywords := []string{"if ", "while ", "until ", "for ", "case ", "{ ", "( "}
	for _, keyword := range leadingKeywords {
		if strings.HasPrefix(finalLine, keyword) {
			return "final command is wrapped in a control keyword: " + finalLine
		}
	}
	return ""
}

// noEnclosingCompound reports whether the lines preceding the final command
// open a compound (if/while/until/for, or unmatched `(`/`{`) that the final
// command would run inside, where the compound's own exit (not govulncheck's)
// becomes the step result.
func noEnclosingCompound(precedingLines []string) string {
	openCompounds := 0
	parenDepth := 0
	braceDepth := 0
	for _, line := range precedingLines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		fields := strings.Fields(trimmed)
		if len(fields) > 0 {
			switch fields[0] {
			case "if", "while", "until", "for", "case":
				openCompounds++
			case "fi", "done", "esac":
				if openCompounds > 0 {
					openCompounds--
				}
			}
		}
		parenDepth += strings.Count(trimmed, "(") - strings.Count(trimmed, ")")
		braceDepth += strings.Count(trimmed, "{") - strings.Count(trimmed, "}")
	}
	if openCompounds > 0 {
		return "the govulncheck command runs inside an unterminated if/while/until/for/case compound"
	}
	if parenDepth > 0 {
		return "the govulncheck command runs inside an unterminated subshell `(`"
	}
	if braceDepth > 0 {
		return "the govulncheck command runs inside an unterminated brace group `{`"
	}
	return ""
}
