package ciflow

import (
	"strings"
	"testing"
)

func TestChainOverLimitSumsTheTimeoutsOfDependentJobs(t *testing.T) {
	for _, row := range []struct {
		name        string
		jobs        []JobLimit
		wantFailure string
	}{
		{name: "three jobs in a row",
			jobs:        []JobLimit{{Job: "decide", Minutes: 2}, {Job: "tests", Needs: []string{"decide"}, Minutes: 28}, {Job: "gate", Needs: []string{"decide", "tests"}, Minutes: 2}},
			wantFailure: "decide, tests, gate sum to 32 minutes"},
		{name: "three jobs in a row within the limit",
			jobs: []JobLimit{{Job: "decide", Minutes: 5}, {Job: "tests", Needs: []string{"decide"}, Minutes: 20}, {Job: "gate", Needs: []string{"decide", "tests"}, Minutes: 5}}},
		{name: "the longer of two branches, whatever the order of the jobs",
			jobs: []JobLimit{{Job: "gate", Needs: []string{"quick", "slow"}, Minutes: 3}, {Job: "slow", Needs: []string{"decide"}, Minutes: 25},
				{Job: "quick", Needs: []string{"decide"}, Minutes: 4}, {Job: "decide", Minutes: 3}},
			wantFailure: "decide, slow, gate sum to 31 minutes"},
		{name: "a job that waits for nothing",
			jobs:        []JobLimit{{Job: "decide", Minutes: 3}, {Job: "audit", Minutes: 45}},
			wantFailure: "audit sum to 45 minutes"},
	} {
		t.Run(row.name, func(t *testing.T) {
			err := ChainOverLimit(row.jobs)
			if row.wantFailure == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), row.wantFailure) {
				t.Fatalf("error = %v, want one that holds %q", err, row.wantFailure)
			}
		})
	}
}
