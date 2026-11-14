package testinfra

import (
	"fmt"
	"path/filepath"
	"slices"
	"strconv"

	"github.com/tetral-ai/tetral/internal/testinfra/ciflow"
)

// githubDefaultJobTimeoutMinutes is what a job without timeout-minutes gets.
const githubDefaultJobTimeoutMinutes = 360

// VerifyPullRequestRunLimits fails when, in a workflow that a pull request's
// commits start, the timeouts of a chain of dependent jobs sum above the time
// a run may take.
func VerifyPullRequestRunLimits(root string) error {
	files, err := filepath.Glob(filepath.Join(root, ".github", "workflows", "*.yml"))
	if err != nil {
		return err
	}
	for _, file := range files {
		document, err := parseYAMLFile(file)
		if err != nil {
			return err
		}
		workflow := document.Content[0]
		if !startsOnPullRequestUpdates(workflow) {
			continue
		}
		var limits []ciflow.JobLimit
		jobs := mappingValue(workflow, "jobs")
		for index := 0; jobs != nil && index+1 < len(jobs.Content); index += 2 {
			job := jobs.Content[index+1]
			minutes := githubDefaultJobTimeoutMinutes
			if timeout := mappingValue(job, "timeout-minutes"); timeout != nil {
				if minutes, err = strconv.Atoi(scalar(timeout)); err != nil {
					return fmt.Errorf("%s: job %q has a timeout-minutes that is no number", filepath.Base(file), jobs.Content[index].Value)
				}
			}
			limits = append(limits, ciflow.JobLimit{Job: jobs.Content[index].Value, Needs: jobNeeds(job), Minutes: minutes})
		}
		if err := ciflow.ChainOverLimit(limits); err != nil {
			return fmt.Errorf("%s: %w", filepath.Base(file), err)
		}
	}
	return nil
}

// startsOnPullRequestUpdates says a pull request's commits start the
// workflow. One that only a label starts, such as a release rehearsal, is not
// the pull request's own run. on is one event, a list of events or a mapping.
func startsOnPullRequestUpdates(workflow *workflowYAMLNode) bool {
	const event = "pull_request"
	triggers := mappingValue(workflow, "on")
	if triggers == nil {
		return false
	}
	switch triggers.Kind {
	case workflowYAMLMapping:
		pullRequest := mappingValue(triggers, event)
		if pullRequest == nil {
			return false
		}
		types := mappingValue(pullRequest, "types")
		return types == nil || slices.Contains(sequenceScalars(types), "synchronize")
	case workflowYAMLSequence:
		return slices.Contains(sequenceScalars(triggers), event)
	}
	return triggers.Value == event
}
