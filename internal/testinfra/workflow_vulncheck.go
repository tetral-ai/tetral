package testinfra

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
)

// The stub stands in for the Go command in the vulnerability job's run steps:
// `go list` prints GO_LIST_PACKAGES, `go tool govulncheck` exits with
// GOVULNCHECK_EXIT, and each records its call, govulncheck with its working
// directory and arguments.
const vulncheckStubGo = `#!/bin/bash
case "$1 $2" in
  "list "*) echo list >> "$STUB_CALLS"; printf '%s\n' "$GO_LIST_PACKAGES" ;;
  "tool govulncheck") shift 2; printf 'govulncheck\t%s\t%s\n' "$PWD" "$*" >> "$STUB_CALLS"; exit "$GOVULNCHECK_EXIT" ;;
  *) echo "unexpected go $*" >&2; exit 2 ;;
esac
`

// VerifyVulnerabilityWorkflow checks engine-vulncheck.yml against the module's
// packages (import paths under module). Its pull request trigger must include
// go.mod and go.sum, each path it triggers on must also make the local
// runner's dependency audit run, and it must run on a schedule. Each run step
// of its job runs against a stub Go command: a step that runs govulncheck must
// fail when the scanner reports a finding and pass when it reports none; a
// step that lists the packages must pass with the module's packages and fail
// with a package outside the trees it accepts; and every package must lie in
// a tree a scan covers. The job and those steps never continue on error.
func VerifyVulnerabilityWorkflow(root, module string, packages []string) error {
	document, err := parseYAMLFile(filepath.Join(root, ".github", "workflows", "engine-vulncheck.yml"))
	if err != nil {
		return err
	}
	if err := verifyVulnerabilityTriggers(mappingValue(document.Content[0], "on")); err != nil {
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
	defer func() { _ = os.RemoveAll(directory) }()
	bin := filepath.Join(directory, "bin")
	if err := os.MkdirAll(bin, 0o750); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(vulncheckStubGo), 0o755); err != nil { //nolint:gosec // The stub must be executable.
		return err
	}
	covered := strings.Join(packages, "\n")
	uncovered := module + "/unlisted/example"
	checkout := filepath.Join(directory, "checkout")
	var scanned []string
	scans, guards := 0, 0
	for index, step := range sequenceNodes(mappingValue(job, "steps")) {
		if mappingValue(step, "run") == nil {
			continue
		}
		run := func(packages, scannerExit string) (map[string]bool, error) {
			calls, _, err := runVulncheckStep(document.Content[0], step, job, directory, bin, index, packages, scannerExit)
			return calls, err
		}
		calls, trees, clean := runVulncheckStep(document.Content[0], step, job, directory, bin, index, covered, "0")
		scanned = append(scanned, trees...)
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
	for _, name := range packages {
		directory := strings.TrimPrefix(strings.TrimPrefix(name, module), "/")
		if !slices.ContainsFunc(scanned, func(tree string) bool {
			return tree == "." || directory == tree || strings.HasPrefix(directory, tree+"/")
		}) {
			return fmt.Errorf("package %s is in no tree a vulnerability scan covers (scanned %s relative to %s)", name, strings.Join(scanned, ", "), checkout)
		}
	}
	return nil
}

// verifyVulnerabilityTriggers checks the workflow's triggers against the local
// runner's filter for the same audit.
func verifyVulnerabilityTriggers(on *workflowYAMLNode) error {
	if len(sequenceNodes(mappingValue(on, "schedule"))) == 0 {
		return fmt.Errorf("vulnerability workflow has no schedule; a newly published advisory would go unreported")
	}
	paths := sequenceScalars(mappingValue(mappingValue(on, "pull_request"), "paths"))
	for _, required := range []string{"go.mod", "go.sum"} {
		if !slices.Contains(paths, required) {
			return fmt.Errorf("vulnerability workflow's pull request paths do not include %s", required)
		}
	}
	for _, path := range paths {
		changed := strings.Replace(path, "**", "example", 1)
		if !helperVulnerabilityAuditNeeded(Plan{Revision: Revision{ChangedPaths: []string{changed}}}, DependencyAuditChanged) {
			return fmt.Errorf("vulnerability workflow runs for %s, which the local runner's dependency audit does not", path)
		}
	}
	return nil
}

// runVulncheckStep runs one step's script as GitHub runs a step without a
// shell key (bash -e), in its working directory, with the workflow's, the
// job's and the step's environment. It returns which stub commands it called and, for each
// govulncheck call, the tree it scans relative to the checkout: a package
// pattern's directory, or the working directory of a module scan.
func runVulncheckStep(workflow, step, job *workflowYAMLNode, directory, bin string, index int, packages, scannerExit string) (map[string]bool, []string, error) {
	if shell := scalar(mappingValue(step, "shell")); shell != "" {
		return nil, nil, fmt.Errorf("vulnerability step %d uses shell %q; the check runs bash steps only", index, shell)
	}
	checkout := filepath.Join(directory, "checkout")
	work := filepath.Join(checkout, scalar(mappingValue(step, "working-directory")))
	if err := os.MkdirAll(work, 0o750); err != nil {
		return nil, nil, err
	}
	calls := filepath.Join(directory, "calls")
	if err := os.WriteFile(calls, nil, 0o600); err != nil {
		return nil, nil, err
	}
	command := exec.Command("bash", "--noprofile", "--norc", "-e", "-c", scalar(mappingValue(step, "run"))) //nolint:gosec // The repository's own workflow step under test.
	command.Dir = work
	command.Env = append(append(os.Environ(), stepEnvironment(workflow, job, step)...), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"STUB_CALLS="+calls, "GO_LIST_PACKAGES="+packages, "GOVULNCHECK_EXIT="+scannerExit)
	output, runErr := command.CombinedOutput()
	body, err := os.ReadFile(calls) //nolint:gosec // The stub's call record in this check's directory.
	if err != nil {
		return nil, nil, err
	}
	called := map[string]bool{}
	var trees []string
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		fields := strings.Split(line, "\t")
		called[fields[0]] = fields[0] != ""
		if fields[0] != "govulncheck" || len(fields) != 3 {
			continue
		}
		tree := fields[1]
		if arguments := strings.Fields(fields[2]); !slices.Contains(arguments, "module") {
			tree = filepath.Join(tree, strings.TrimSuffix(arguments[len(arguments)-1], "..."))
		}
		if relative, err := filepath.Rel(checkout, tree); err == nil && !strings.HasPrefix(relative, "..") {
			trees = append(trees, filepath.ToSlash(relative))
		}
	}
	if runErr != nil {
		return called, trees, fmt.Errorf("%w: %s", runErr, output)
	}
	return called, trees, nil
}
