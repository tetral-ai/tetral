package testinfra

import (
	"strings"
	"testing"
)

// rewriteRuns replaces text in every run step of the workflow's jobs that
// holds it, and fails the row when no step does.
func rewriteRuns(file, old, replacement string) workflowEdit {
	return workflowEdit{file, func(t *testing.T, workflow *workflowYAMLNode) {
		t.Helper()
		rewritten := 0
		jobs := yamlAt(t, workflow, "jobs")
		for index := 1; index < len(jobs.Content); index += 2 {
			for _, step := range sequenceNodes(mappingValue(jobs.Content[index], "steps")) {
				if run := mappingValue(step, "run"); run != nil && strings.Contains(run.Value, old) {
					run.Value = strings.ReplaceAll(run.Value, old, replacement)
					rewritten++
				}
			}
		}
		if rewritten == 0 {
			t.Fatalf("no run step of %s holds %q", file, old)
		}
	}}
}

func TestVulnerabilityWorkflowFailsOnAFindingAndOnAPackageInNoSlice(t *testing.T) {
	const vulncheck = "workflows/engine-vulncheck.yml"
	const symbolScan = `go tool govulncheck "${slice}"`
	tests := []struct {
		name string
		edit workflowEdit
		want string
	}{
		{"a symbol scan whose finding is ignored", rewriteRuns(vulncheck, symbolScan, symbolScan+" || true"), "passes when govulncheck reports a finding"},
		{"a module scan whose finding is ignored", rewriteRuns(vulncheck, "go tool govulncheck -scan module", "go tool govulncheck -scan module || true"), "passes when govulncheck reports a finding"},
		{"a guard that only reports", rewriteRuns(vulncheck, "  exit 1\n", ""), "passes with a package in no slice"},
		{"a job that continues on error", setIn(vulncheck, "true", "jobs", "govulncheck", "continue-on-error"), "job continues on error"},
		{"a scan step that continues on error", setIn(vulncheck, "true", "jobs", "govulncheck", "steps", "working-directory=services/sandbox", "continue-on-error"), "continues on error"},
		{"a slice list that misses a tree", rewriteRuns(vulncheck, "|internal|", "|"), "fails with no finding and every package in a slice"},
		{"steps renamed", setIn(vulncheck, "Scan the sandbox closure by module", "jobs", "govulncheck", "steps", "working-directory=services/sandbox", "name"), ""},
		{"a guard that words its report differently", rewriteRuns(vulncheck, "package trees missing from the vulncheck slice list:", "not in any slice:"), ""},
		{"a symbol scan that stops at the first finding by itself", rewriteRuns(vulncheck, symbolScan, `if ! go tool govulncheck "${slice}"; then exit 3; fi`), ""},
	}
	if err := VerifyVulnerabilityWorkflow(testRepositoryRoot(t)); err != nil {
		t.Fatalf("repository vulnerability workflow rejected: %v", err)
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := VerifyVulnerabilityWorkflow(editedWorkflows(t, []workflowEdit{tc.edit}))
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("vulnerability workflow error = %v; want %q", err, tc.want)
			}
		})
	}
}
