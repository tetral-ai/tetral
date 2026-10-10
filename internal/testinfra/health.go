package testinfra

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type CIHealthReport struct {
	Repository          string             `json:"repository"`
	RunID               int64              `json:"run_id"`
	RunAttempt          int                `json:"run_attempt"`
	CollectedAt         time.Time          `json:"collected_at"`
	QueueTime           time.Duration      `json:"queue_time_ns"`
	RunnerMinutes       float64            `json:"runner_minutes"`
	JobConclusions      map[string]int     `json:"job_conclusions"`
	EvidenceStatuses    map[string]int     `json:"evidence_statuses"`
	StepDurations       map[string]int64   `json:"step_durations_ns"`
	TestDurations       map[string]float64 `json:"test_durations_seconds"`
	GoShardMinutes      map[string]float64 `json:"go_shard_minutes"`
	DependencySetup     time.Duration      `json:"dependency_setup_ns"`
	DuplicateExecutions []string           `json:"duplicate_executions,omitempty"`
	RerunEvidence       int                `json:"rerun_evidence"`
	ActionReviewedAt    string             `json:"action_inventory_reviewed_at"`
	ShardCalibratedAt   string             `json:"go_shard_calibrated_at"`
	HistoricalOwners    []string           `json:"historical_concurrency_owners"`
	ApparatusNotes      []string           `json:"apparatus_notes,omitempty"`
}

func BuildCIHealthReport(ctx context.Context, artifactsRoot, repository string, runID int64, runAttempt int) (CIHealthReport, error) {
	report := CIHealthReport{
		Repository: repository, RunID: runID, RunAttempt: runAttempt, CollectedAt: time.Now().UTC(),
		JobConclusions: map[string]int{}, EvidenceStatuses: map[string]int{}, StepDurations: map[string]int64{},
		TestDurations: map[string]float64{}, GoShardMinutes: map[string]float64{},
	}
	inventory, err := LoadActionInventory()
	if err != nil {
		return report, err
	}
	report.ActionReviewedAt = inventory.ReviewedAt
	calibration, err := loadGoShardCalibration()
	if err != nil {
		return report, err
	}
	report.ShardCalibratedAt = calibration.CalibratedAt
	var owners scheduledOwnerInventory
	if err := json.Unmarshal(scheduledOwnersJSON, &owners); err != nil {
		return report, err
	}
	for _, owner := range owners.Owners {
		report.HistoricalOwners = append(report.HistoricalOwners, owner.Package)
	}
	sort.Strings(report.HistoricalOwners)
	seen := map[string]bool{}
	err = filepath.WalkDir(artifactsRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		switch entry.Name() {
		case "result.json":
			// Artifacts are downloaded beneath the explicit runner-owned root.
			//nolint:gosec
			body, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			var result Result
			if err := json.Unmarshal(body, &result); err != nil {
				return fmt.Errorf("decode health result: %w", err)
			}
			identity := result.Execution.RunID + "/" + result.Execution.RunAttempt + "/" + result.Execution.Producer
			if seen[identity] {
				report.DuplicateExecutions = append(report.DuplicateExecutions, identity)
			}
			seen[identity] = true
			if result.Execution.RunAttempt != "" && result.Execution.RunAttempt != "1" {
				report.RerunEvidence++
			}
			report.EvidenceStatuses[result.Status]++
			report.DependencySetup += result.Setup
			for _, step := range result.Steps {
				key := step.Group + ":" + strings.Join(step.Command, " ")
				report.StepDurations[key] += int64(step.Elapsed)
				if strings.HasPrefix(result.Execution.Producer, "go-") {
					report.GoShardMinutes[result.Execution.Producer] += step.Elapsed.Minutes()
				}
			}
		default:
			if strings.HasSuffix(entry.Name(), ".jsonl") {
				if err := collectGoTestDurations(path, report.TestDurations); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return report, err
	}
	live, liveErr := readWorkflowHealth(ctx, commandGitHubClient{}, repository, runID, runAttempt)
	if liveErr != nil {
		report.ApparatusNotes = append(report.ApparatusNotes, liveErr.Error())
	} else {
		if !live.CreatedAt.IsZero() && !live.StartedAt.Before(live.CreatedAt) {
			report.QueueTime = live.StartedAt.Sub(live.CreatedAt)
		}
		collectCompletedJobHealth(&report, live.Jobs)
	}
	sort.Strings(report.DuplicateExecutions)
	return report, nil
}

func collectCompletedJobHealth(report *CIHealthReport, jobs []workflowJob) {
	for _, job := range jobs {
		if job.Status != "completed" || job.Conclusion == "" || job.StartedAt.IsZero() || job.CompletedAt.Before(job.StartedAt) {
			continue
		}
		duration := job.CompletedAt.Sub(job.StartedAt)
		report.JobConclusions[job.Conclusion]++
		report.RunnerMinutes += duration.Minutes()
		if strings.HasPrefix(job.Name, "Go Race (shard ") {
			report.GoShardMinutes[job.Name] = duration.Minutes()
		}
	}
}

type workflowHealth struct {
	CreatedAt time.Time
	StartedAt time.Time
	Jobs      []workflowJob
}

func readWorkflowHealth(ctx context.Context, client githubAPIClient, repository string, runID int64, runAttempt int) (workflowHealth, error) {
	var run githubWorkflowRun
	if err := client.JSON(ctx, fmt.Sprintf("repos/%s/actions/runs/%d/attempts/%d", repository, runID, runAttempt), &run); err != nil {
		return workflowHealth{}, err
	}
	jobs, err := readAttemptJobs(ctx, client, repository, runID, runAttempt)
	if err != nil {
		return workflowHealth{}, err
	}
	return workflowHealth{CreatedAt: run.CreatedAt, StartedAt: run.RunStartedAt, Jobs: jobs}, nil
}

func collectGoTestDurations(path string, target map[string]float64) error {
	report, err := readGoTestReport(path)
	if err != nil {
		return err
	}
	for _, test := range report.tests {
		if test.passed {
			target[test.pkg+"/"+test.name] += test.elapsed
		}
	}
	return nil
}

type goTestOutcome struct {
	pkg, name string
	elapsed   float64
	passed    bool
}

// goTestReport is what CI evidence consumers read from one go test -json
// report: the first test start and every top-level test's final outcome.
type goTestReport struct {
	firstRun time.Time
	tests    []goTestOutcome
}

func readGoTestReport(path string) (goTestReport, error) {
	var report goTestReport
	// The path is discovered beneath an explicit downloaded artifact root.
	//nolint:gosec
	file, err := os.Open(path)
	if err != nil {
		return report, err
	}
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		var event struct {
			Time    time.Time `json:"Time"`
			Action  string    `json:"Action"`
			Package string    `json:"Package"`
			Test    string    `json:"Test"`
			Elapsed float64   `json:"Elapsed"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return report, fmt.Errorf("decode Go test event in %s: %w", filepath.Base(path), err)
		}
		if event.Test == "" {
			continue
		}
		if event.Action == "run" && report.firstRun.IsZero() {
			report.firstRun = event.Time
		}
		if !strings.Contains(event.Test, "/") && (event.Action == "pass" || event.Action == "fail") {
			report.tests = append(report.tests, goTestOutcome{pkg: event.Package, name: event.Test, elapsed: event.Elapsed, passed: event.Action == "pass"})
		}
	}
	return report, scanner.Err()
}
