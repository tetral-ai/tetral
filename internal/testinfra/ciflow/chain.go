package ciflow

import (
	"fmt"
	"strings"
)

// JobLimit is a workflow job's timeout and the jobs it waits for.
type JobLimit struct {
	Job     string
	Needs   []string
	Minutes int
}

// ChainOverLimit says which chain of dependent jobs could, by its timeouts,
// keep a run going beyond RunLimit.
func ChainOverLimit(jobs []JobLimit) error {
	chain, minutes := longestChain(jobs)
	if limit := int(RunLimit.Minutes()); minutes > limit {
		return fmt.Errorf("the timeouts of %s sum to %d minutes; the limit is %d", strings.Join(chain, ", "), minutes, limit)
	}
	return nil
}

// longestChain gives the chain of dependent jobs whose timeouts sum highest,
// in the order the jobs run, and that sum.
func longestChain(jobs []JobLimit) (chain []string, minutes int) {
	byName := map[string]JobLimit{}
	for _, job := range jobs {
		byName[job.Job] = job
	}
	type end struct {
		chain   []string
		minutes int
	}
	known := map[string]end{}
	var endingAt func(name string, walking map[string]bool) end
	endingAt = func(name string, walking map[string]bool) end {
		if found, exists := known[name]; exists {
			return found
		}
		job := byName[name]
		var before end
		walking[name] = true
		for _, needed := range job.Needs {
			// GitHub refuses a workflow whose jobs need each other in a ring.
			if _, exists := byName[needed]; !exists || walking[needed] {
				continue
			}
			if candidate := endingAt(needed, walking); candidate.minutes > before.minutes {
				before = candidate
			}
		}
		delete(walking, name)
		found := end{chain: append(append([]string{}, before.chain...), name), minutes: before.minutes + job.Minutes}
		known[name] = found
		return found
	}
	var longest end
	for _, job := range jobs {
		if candidate := endingAt(job.Job, map[string]bool{}); candidate.minutes > longest.minutes {
			longest = candidate
		}
	}
	return longest.chain, longest.minutes
}
