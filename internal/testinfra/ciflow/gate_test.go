// Layer 1: the merge gate's decision over recorded jobs.
package ciflow

import (
	"fmt"
	"strings"
	"testing"
)

func TestGatePassesOnlyOnTheResultsItsSelectionAllows(t *testing.T) {
	results := []string{"success", "failure", "cancelled", "skipped", ""}
	passes := map[string]bool{
		// deciding result, selection, the result of one product job
		"success true success":  true,
		"success false success": true,
		"success false skipped": true,
	}
	for _, deciding := range results {
		for _, selected := range []bool{true, false} {
			for _, job := range results {
				name := fmt.Sprintf("%s %v %s", deciding, selected, job)
				t.Run(name, func(t *testing.T) {
					needs := Needs{
						Deciding: JobResult{Job: "decide", Result: deciding},
						Selected: selected,
						Jobs:     []JobResult{{Job: "build", Result: "success"}, {Job: "test", Result: job}},
					}
					failures := needs.Failures()
					if passed := len(failures) == 0; passed != passes[name] {
						t.Fatalf("passed = %v with %q, want %v", passed, failures, passes[name])
					}
				})
			}
		}
	}
}

func TestParseNeedsReadsTheDecisionAndTheJobs(t *testing.T) {
	needs := func(deciding, outputs string) string {
		return `{"decide":{"result":"` + deciding + `","outputs":` + outputs + `},` +
			`"test":{"result":"skipped","outputs":{}},"build":{"result":"success","outputs":{}}}`
	}
	for _, row := range []struct {
		name, body, product string
		want                Needs
		wantError           string
	}{
		{name: "the product is selected", body: needs("success", `{"engine":"true","cli":"false"}`), product: "engine",
			want: Needs{Deciding: JobResult{"decide", "success"}, Selected: true, Jobs: []JobResult{{"build", "success"}, {"test", "skipped"}}}},
		{name: "the other product is selected", body: needs("success", `{"engine":"true","cli":"false"}`), product: "cli",
			want: Needs{Deciding: JobResult{"decide", "success"}, Jobs: []JobResult{{"build", "success"}, {"test", "skipped"}}}},
		{name: "a failed deciding job selects nothing", body: needs("failure", `{}`), product: "engine",
			want: Needs{Deciding: JobResult{"decide", "failure"}, Jobs: []JobResult{{"build", "success"}, {"test", "skipped"}}}},
		{name: "a deciding job that succeeded without deciding", body: needs("success", `{}`), product: "engine",
			wantError: `gave "" for engine`},
		{name: "no deciding job among the needed jobs", body: `{"build":{"result":"success","outputs":{}}}`, product: "engine",
			wantError: "does not need the deciding job"},
		{name: "not the results of jobs", body: `[]`, product: "engine", wantError: "decode"},
	} {
		t.Run(row.name, func(t *testing.T) {
			got, err := ParseNeeds(row.body, "decide", row.product)
			if row.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), row.wantError) {
					t.Fatalf("error = %v, want one that holds %q", err, row.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(got) != fmt.Sprint(row.want) {
				t.Fatalf("needs = %+v, want %+v", got, row.want)
			}
		})
	}
}
