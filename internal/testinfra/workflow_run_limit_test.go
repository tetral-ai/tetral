package testinfra

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPullRequestWorkflowsKeepEachChainOfJobsWithinTheRunLimit(t *testing.T) {
	root := testRepositoryRoot(t)
	if err := VerifyPullRequestRunLimits(root); err != nil {
		t.Fatal(err)
	}
	const file = ".github/workflows/pull-request-verification.yml"
	for _, row := range []struct {
		name   string
		change func(t *testing.T, workflow *workflowYAMLNode)
		want   string
	}{
		{name: "a job's timeout raised beyond what its chain leaves",
			change: func(t *testing.T, workflow *workflowYAMLNode) {
				yamlSet(t, workflow, "26", "jobs", "go-race", "timeout-minutes")
			},
			want: "pull-request-verification.yml: the timeouts of decide, go-race, merge-gate sum to 31 minutes"},
		{name: "a job without a timeout",
			change: func(t *testing.T, workflow *workflowYAMLNode) {
				yamlRemove(t, workflow, "jobs", "decide", "timeout-minutes")
			},
			want: "pull-request-verification.yml: the timeouts of decide, "},
		{name: "a workflow that only a label on a pull request starts",
			change: func(t *testing.T, workflow *workflowYAMLNode) {
				yamlSet(t, workflow, "26", "jobs", "go-race", "timeout-minutes")
				yamlPut(t, workflow, "pull_request: {types: [labeled]}", "on")
			}},
		{name: "a workflow that names the pull request events that start it",
			change: func(t *testing.T, workflow *workflowYAMLNode) {
				yamlSet(t, workflow, "26", "jobs", "go-race", "timeout-minutes")
				yamlPut(t, workflow, "pull_request: {types: [opened, synchronize, reopened]}", "on")
			},
			want: "sum to 31 minutes"},
		{name: "a workflow whose only event is the pull request",
			change: func(t *testing.T, workflow *workflowYAMLNode) {
				yamlSet(t, workflow, "26", "jobs", "go-race", "timeout-minutes")
				yamlPut(t, workflow, "pull_request", "on")
			},
			want: "sum to 31 minutes"},
	} {
		t.Run(row.name, func(t *testing.T) {
			fixture := t.TempDir()
			body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(file)))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(fixture, ".github", "workflows"), 0o700); err != nil {
				t.Fatal(err)
			}
			writeTestFile(t, fixture, file, string(body))
			editWorkflowFile(t, fixture, file, row.change)
			err = VerifyPullRequestRunLimits(fixture)
			if row.want == "" && err != nil || row.want != "" && (err == nil || !strings.Contains(err.Error(), row.want)) {
				t.Fatalf("error = %v, want %q", err, row.want)
			}
		})
	}
}
