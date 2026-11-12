package testinfra

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// The stub stands in for the Go command in the vulnerability job's run steps:
// `go list` prints GO_LIST_PACKAGES, `go tool govulncheck` exits with
// GOVULNCHECK_EXIT, and each records its call.
const vulncheckStubGo = `#!/bin/sh
case "$1 $2" in
  "list "*) echo list >> "$STUB_CALLS"; printf '%s\n' "$GO_LIST_PACKAGES" ;;
  "tool govulncheck") echo govulncheck >> "$STUB_CALLS"; exit "$GOVULNCHECK_EXIT" ;;
  *) echo "unexpected go $*" >&2; exit 2 ;;
esac
`

// VerifyVulnerabilityWorkflow runs each run step of engine-vulncheck.yml's job
// against a stub Go command. A step that runs govulncheck must fail when the
// scanner reports a finding and pass when it reports none; a step that lists
// the packages must fail when a package is in no slice and pass when it is in
// one. The job and those steps never continue on error.
func VerifyVulnerabilityWorkflow(root string) error {
	document, err := parseYAMLFile(filepath.Join(root, ".github", "workflows", "engine-vulncheck.yml"))
	if err != nil {
		return err
	}
	jobs := mappingValue(document.Content[0], "jobs")
	if jobs == nil || len(jobs.Content) != 2 {
		return fmt.Errorf("vulnerability workflow must have exactly one job")
	}
	job := jobs.Content[1]
	if mappingValue(job, "continue-on-error") != nil {
		return fmt.Errorf("vulnerability job continues on error")
	}
	directory, err := os.MkdirTemp("", "vulncheck-steps-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory)
	bin := filepath.Join(directory, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(vulncheckStubGo), 0o755); err != nil { //nolint:gosec // The stub must be executable.
		return err
	}
	const (
		covered   = "github.com/tetral-ai/tetral/internal/example"
		uncovered = "github.com/tetral-ai/tetral/unlisted/example"
	)
	scans, guards := 0, 0
	for index, step := range sequenceNodes(mappingValue(job, "steps")) {
		if mappingValue(step, "run") == nil {
			continue
		}
		run := func(packages, scannerExit string) (map[string]bool, error) {
			return runVulncheckStep(step, job, directory, bin, index, packages, scannerExit)
		}
		calls, clean := run(covered, "0")
		neither := !calls["govulncheck"] && !calls["list"]
		if neither {
			continue
		}
		if mappingValue(step, "continue-on-error") != nil {
			return fmt.Errorf("vulnerability step %d continues on error", index)
		}
		if clean != nil {
			return fmt.Errorf("vulnerability step %d fails with no finding and every package in a slice: %w", index, clean)
		}
		if calls["govulncheck"] {
			scans++
			if _, err := run(covered, "3"); err == nil {
				return fmt.Errorf("vulnerability step %d passes when govulncheck reports a finding", index)
			}
		}
		if calls["list"] {
			guards++
			if _, err := run(covered+"\n"+uncovered, "0"); err == nil {
				return fmt.Errorf("vulnerability step %d passes with a package in no slice", index)
			}
		}
	}
	if scans == 0 || guards == 0 {
		return fmt.Errorf("vulnerability job has %d scan steps and %d slice guards; want at least one of each", scans, guards)
	}
	return nil
}

// runVulncheckStep runs one step's script as GitHub runs a step without a
// shell key (bash -e), in its working directory, with the job's and the
// step's environment, and returns which stub commands it called.
func runVulncheckStep(step, job *workflowYAMLNode, directory, bin string, index int, packages, scannerExit string) (map[string]bool, error) {
	if shell := scalar(mappingValue(step, "shell")); shell != "" {
		return nil, fmt.Errorf("vulnerability step %d uses shell %q; the check runs bash steps only", index, shell)
	}
	work := filepath.Join(directory, "checkout", scalar(mappingValue(step, "working-directory")))
	if err := os.MkdirAll(work, 0o755); err != nil {
		return nil, err
	}
	calls := filepath.Join(directory, "calls")
	if err := os.WriteFile(calls, nil, 0o600); err != nil {
		return nil, err
	}
	command := exec.Command("bash", "--noprofile", "--norc", "-e", "-c", scalar(mappingValue(step, "run")))
	command.Dir = work
	command.Env = os.Environ()
	for _, owner := range []*workflowYAMLNode{job, step} {
		environment := mappingValue(owner, "env")
		for at := 0; environment != nil && at+1 < len(environment.Content); at += 2 {
			command.Env = append(command.Env, environment.Content[at].Value+"="+scalar(environment.Content[at+1]))
		}
	}
	command.Env = append(command.Env, "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"STUB_CALLS="+calls, "GO_LIST_PACKAGES="+packages, "GOVULNCHECK_EXIT="+scannerExit)
	output, runErr := command.CombinedOutput()
	body, err := os.ReadFile(calls) //nolint:gosec // The stub's call record in this check's directory.
	if err != nil {
		return nil, err
	}
	called := map[string]bool{}
	for _, call := range strings.Fields(string(body)) {
		called[call] = true
	}
	if runErr != nil {
		return called, fmt.Errorf("%w: %s", runErr, output)
	}
	return called, nil
}
