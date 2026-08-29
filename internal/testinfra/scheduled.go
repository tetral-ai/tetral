package testinfra

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const scheduledAttemptCount = 3

//go:embed scheduled_owners.json
var scheduledOwnersJSON []byte

type scheduledOwnerInventory struct {
	Version int `json:"version"`
	Owners  []struct {
		Package string `json:"package"`
		Reason  string `json:"reason"`
	} `json:"owners"`
}

type ScheduledReport struct {
	Revision            Revision  `json:"revision"`
	StartedAt           time.Time `json:"started_at"`
	FinishedAt          time.Time `json:"finished_at"`
	Status              string    `json:"status"`
	FirstFailureAttempt int       `json:"first_failure_attempt,omitempty"`
	LaterAttemptPassed  bool      `json:"later_attempt_passed,omitempty"`
	Attempts            []Result  `json:"attempts"`
}

// ExecuteScheduled repeats only the concurrency-sensitive package owners.
// A later pass is diagnostic evidence and never erases an earlier failure.
func ExecuteScheduled(ctx context.Context, root, outputDir string) (ScheduledReport, error) {
	plan, err := BuildPlan(root, ProfileFull, "")
	if err != nil {
		return ScheduledReport{}, err
	}
	plan, err = scheduledConcurrencyPlan(plan)
	if err != nil {
		return ScheduledReport{}, err
	}
	report := ScheduledReport{Revision: plan.Revision, StartedAt: time.Now().UTC(), Status: "pass"}
	var firstErr error
	for attempt := 1; attempt <= scheduledAttemptCount; attempt++ {
		result, runErr := Execute(ctx, plan, RunOptions{
			Root:      root,
			OutputDir: filepath.Join(outputDir, fmt.Sprintf("attempt-%d", attempt)),
		})
		recordScheduledAttempt(&report, result, runErr, attempt)
		if runErr != nil && firstErr == nil {
			firstErr = runErr
		}
		if ctx.Err() != nil {
			break
		}
	}
	report.FinishedAt = time.Now().UTC()
	if err := os.MkdirAll(outputDir, 0o700); err != nil {
		return report, errors.Join(firstErr, err)
	}
	body, err := json.MarshalIndent(report, "", "  ")
	if err == nil {
		err = os.WriteFile(filepath.Join(outputDir, "scheduled-result.json"), append(body, '\n'), 0o600)
	}
	return report, errors.Join(firstErr, err)
}

func recordScheduledAttempt(report *ScheduledReport, result Result, err error, attempt int) {
	report.Attempts = append(report.Attempts, result)
	if err != nil && report.FirstFailureAttempt == 0 {
		report.FirstFailureAttempt = attempt
		report.Status = "fail"
	}
	if report.FirstFailureAttempt != 0 && err == nil {
		report.LaterAttemptPassed = true
	}
}

func scheduledConcurrencyPlan(plan Plan) (Plan, error) {
	var inventory scheduledOwnerInventory
	if err := json.Unmarshal(scheduledOwnersJSON, &inventory); err != nil || inventory.Version != 1 || len(inventory.Owners) == 0 {
		return Plan{}, fmt.Errorf("scheduled owner inventory is malformed")
	}
	owners := map[string]bool{}
	for _, owner := range inventory.Owners {
		if owner.Package == "" || owner.Reason == "" || owners[owner.Package] {
			return Plan{}, fmt.Errorf("scheduled owner inventory has an invalid row")
		}
		owners[owner.Package] = true
	}
	selected := plan.Selections[:0:0]
	for _, selection := range plan.Selections {
		if selection.Group != "go" || len(selection.Packages) != 1 {
			continue
		}
		for packageName := range owners {
			if selection.Packages[0] == packageName {
				selected = append(selected, selection)
				delete(owners, packageName)
				break
			}
		}
	}
	if len(owners) != 0 {
		return Plan{}, fmt.Errorf("scheduled concurrency inventory is missing %d package owner(s)", len(owners))
	}
	plan.Selections = selected
	plan.Dependencies = selectedDependencies(selected)
	return plan, nil
}
