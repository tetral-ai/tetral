package ciflow

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// JobResult is the result of a job a gate needs, as GitHub reports it:
// success, failure, cancelled or skipped.
type JobResult struct {
	Job    string
	Result string
}

// Needs are the jobs a gate needs: the deciding job, whether it selected the
// gate's product, and the product's jobs.
type Needs struct {
	Deciding JobResult
	Selected bool
	Jobs     []JobResult
}

// ParseNeeds reads what toJSON(needs) gives a gate job. The deciding job
// states the selection in the output named after the product.
func ParseNeeds(body, deciding, product string) (Needs, error) {
	var jobs map[string]struct {
		Result  string            `json:"result"`
		Outputs map[string]string `json:"outputs"`
	}
	if err := json.Unmarshal([]byte(body), &jobs); err != nil {
		return Needs{}, fmt.Errorf("decode the results of the needed jobs: %w", err)
	}
	decision, exists := jobs[deciding]
	if !exists {
		return Needs{}, fmt.Errorf("the gate does not need the deciding job %s", deciding)
	}
	needs := Needs{Deciding: JobResult{Job: deciding, Result: decision.Result}}
	if decision.Result == "success" {
		switch decision.Outputs[product] {
		case "true":
			needs.Selected = true
		case "false":
		default:
			return Needs{}, fmt.Errorf("the deciding job %s gave %q for %s", deciding, decision.Outputs[product], product)
		}
	}
	for job, result := range jobs {
		if job != deciding {
			needs.Jobs = append(needs.Jobs, JobResult{Job: job, Result: result.Result})
		}
	}
	slices.SortFunc(needs.Jobs, func(left, right JobResult) int { return strings.Compare(left.Job, right.Job) })
	return needs, nil
}

// Failures lists why the gate does not pass. The deciding job must have
// succeeded, and every other job must have ended success, or skipped when
// the deciding job did not select the product.
func (needs Needs) Failures() []string {
	if needs.Deciding.Result != "success" {
		return []string{fmt.Sprintf("the deciding job %s ended %s", needs.Deciding.Job, resultName(needs.Deciding.Result))}
	}
	var failures []string
	for _, job := range needs.Jobs {
		switch {
		case job.Result == "success":
		case job.Result == "skipped" && !needs.Selected:
		case needs.Selected:
			failures = append(failures, fmt.Sprintf("%s was selected and ended %s", job.Job, resultName(job.Result)))
		default:
			failures = append(failures, fmt.Sprintf("%s was not selected and ended %s", job.Job, resultName(job.Result)))
		}
	}
	return failures
}

func resultName(result string) string {
	if result == "" {
		return "without a result"
	}
	return result
}
