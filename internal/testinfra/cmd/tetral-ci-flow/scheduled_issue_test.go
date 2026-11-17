package main

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/tetral-ai/tetral/internal/testinfra/ciflow"
)

// The titles must be the ones each workflow's own reporter computes from its
// name and the cron that started it, or the reporter closes a live schedule's
// issue as removed.
func TestScheduleTitlesAreThoseTheWorkflowsReportUnder(t *testing.T) {
	repository, err := filepath.Abs("../../../../.github/workflows")
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(t.TempDir(), ".github", "workflows")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"named.yml":   "name: Nightly\non:\n  schedule:\n    - cron: '41 6 * * *'\n    - cron: '17 7 * * 1'\n  workflow_dispatch:\n",
		"unnamed.yml": "on:\n  schedule:\n    - cron: '11 5 * * *'\n",
		"list.yaml":   "name: Listed\non: [push, pull_request]\n",
		"scalar.yml":  "name: Pushed\non: push\n",
	} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(filepath.Dir(filepath.Dir(directory)))
	titles, err := scheduleTitles(".github/workflows")
	slices.Sort(titles)
	want := []string{
		ciflow.ScheduledIssueTitle(".github/workflows/unnamed.yml", "11 5 * * *"),
		ciflow.ScheduledIssueTitle("Nightly", "17 7 * * 1"),
		ciflow.ScheduledIssueTitle("Nightly", "41 6 * * *"),
	}
	if err != nil || !slices.Equal(titles, want) {
		t.Fatalf("titles = %q, %v; want %q", titles, err, want)
	}
	// Every workflow of this repository reads, and some have schedules.
	if titles, err := scheduleTitles(repository); err != nil || len(titles) == 0 {
		t.Fatalf("this repository's schedule titles = %q, %v", titles, err)
	}
}
