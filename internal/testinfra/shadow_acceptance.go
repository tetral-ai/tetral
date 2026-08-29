package testinfra

import (
	"fmt"
	"sort"
	"time"
)

const ShadowEstimatorVersion = 1

var requiredShadowChangeClasses = []string{
	"ci-test-infrastructure",
	"database-heavy-go",
	"protocol-generated",
	"runtime-or-gateway",
}

type ShadowAcceptanceReport struct {
	EstimatorVersion     int                 `json:"estimator_version"`
	Ready                bool                `json:"ready"`
	DistinctTuples       int                 `json:"distinct_tuples"`
	EligibleTuples       int                 `json:"eligible_tuples"`
	LegacyMedian         time.Duration       `json:"legacy_median_wall_ns"`
	ShadowMedian         time.Duration       `json:"shadow_median_wall_ns"`
	LegacyRunnerMedian   float64             `json:"legacy_median_runner_minutes"`
	ShadowRunnerMedian   float64             `json:"shadow_median_runner_minutes"`
	WallRatio            float64             `json:"shadow_to_legacy_wall_ratio"`
	RunnerRatio          float64             `json:"shadow_to_legacy_runner_ratio"`
	CoveredChangeClasses []string            `json:"covered_change_classes"`
	RealForkObserved     bool                `json:"real_fork_observed"`
	Blockers             []string            `json:"blockers,omitempty"`
	ReliabilityRows      []ShadowReliability `json:"reliability_rows"`
}

type ShadowReliability struct {
	PullRequest      int      `json:"pull_request"`
	TestMergeSHA     string   `json:"test_merge_sha"`
	LegacyConclusion string   `json:"legacy_conclusion"`
	ShadowConclusion string   `json:"shadow_conclusion"`
	Eligible         bool     `json:"eligible"`
	Reasons          []string `json:"reasons,omitempty"`
}

func EvaluateShadowAcceptance(rows []ShadowLedgerRow, excludedPullRequest int) ShadowAcceptanceReport {
	report := ShadowAcceptanceReport{EstimatorVersion: ShadowEstimatorVersion}
	seen := map[string]bool{}
	covered := map[string]bool{}
	var legacyWalls, shadowWalls []time.Duration
	var legacyMinutes, shadowMinutes []float64

	for _, row := range rows {
		if row.PullRequest == excludedPullRequest {
			continue
		}
		identity := fmt.Sprintf("%s/%d/%s/%s/%s/%s", row.Repository, row.PullRequest, row.EventHeadSHA, row.EventBaseSHA, row.TestMergeSHA, row.WorkflowSourceSHA)
		reliability := ShadowReliability{PullRequest: row.PullRequest, TestMergeSHA: row.TestMergeSHA, LegacyConclusion: row.LegacyConclusion, ShadowConclusion: row.ShadowConclusion}
		if seen[identity] {
			reliability.Reasons = append(reliability.Reasons, "duplicate integration tuple")
			report.Blockers = append(report.Blockers, fmt.Sprintf("PR %d has duplicate selection for one integration tuple", row.PullRequest))
			report.ReliabilityRows = append(report.ReliabilityRows, reliability)
			continue
		}
		seen[identity] = true
		report.DistinctTuples++
		if row.LegacyRunAttempt != 1 || row.ShadowRunAttempt != 1 {
			reliability.Reasons = append(reliability.Reasons, "not a paired first attempt")
		}
		if row.LegacyConclusion != "success" || row.ShadowConclusion != "success" {
			reliability.Reasons = append(reliability.Reasons, "old and new workflows are not both successful")
		}
		if row.RequiredCheckSHA != row.TestMergeSHA || row.SnapshotDigest == "" {
			reliability.Reasons = append(reliability.Reasons, "execution identity is incomplete")
		}
		if err := validateShadowDependencyObservability(row); err != nil {
			reliability.Reasons = append(reliability.Reasons, err.Error())
		}
		if err := validateShadowShardBalance(row); err != nil {
			reliability.Reasons = append(reliability.Reasons, err.Error())
			report.Blockers = append(report.Blockers, fmt.Sprintf("PR %d: %v", row.PullRequest, err))
		}
		if len(reliability.Reasons) == 0 {
			reliability.Eligible = true
			report.EligibleTuples++
			legacyWalls = append(legacyWalls, row.LegacyDuration)
			shadowWalls = append(shadowWalls, row.ShadowDuration)
			legacyMinutes = append(legacyMinutes, row.LegacyRunnerMinutes)
			shadowMinutes = append(shadowMinutes, row.ShadowRunnerMinutes)
			for _, class := range row.ChangeClasses {
				covered[class] = true
			}
			if isRealExternalFork(row) && validForkApproval(row.ForkApproval) {
				report.RealForkObserved = true
			}
		}
		report.ReliabilityRows = append(report.ReliabilityRows, reliability)
	}

	for _, class := range requiredShadowChangeClasses {
		if covered[class] {
			report.CoveredChangeClasses = append(report.CoveredChangeClasses, class)
		} else {
			report.Blockers = append(report.Blockers, "missing eligible "+class+" change")
		}
	}
	if report.EligibleTuples < 10 {
		report.Blockers = append(report.Blockers, fmt.Sprintf("eligible distinct tuples = %d; want at least 10", report.EligibleTuples))
	}
	if !report.RealForkObserved {
		report.Blockers = append(report.Blockers, "missing real external-fork approval observation")
	}
	if report.EligibleTuples > 0 {
		report.LegacyMedian = medianDuration(legacyWalls)
		report.ShadowMedian = medianDuration(shadowWalls)
		report.LegacyRunnerMedian = medianFloat(legacyMinutes)
		report.ShadowRunnerMedian = medianFloat(shadowMinutes)
		if report.LegacyMedian > 0 {
			report.WallRatio = float64(report.ShadowMedian) / float64(report.LegacyMedian)
		}
		if report.LegacyRunnerMedian > 0 {
			report.RunnerRatio = report.ShadowRunnerMedian / report.LegacyRunnerMedian
		}
		if report.WallRatio > 0.75 {
			report.Blockers = append(report.Blockers, fmt.Sprintf("shadow median wall ratio %.3f exceeds 0.750", report.WallRatio))
		}
		if report.RunnerRatio > 1.10 {
			report.Blockers = append(report.Blockers, fmt.Sprintf("shadow runner-minute ratio %.3f exceeds 1.100", report.RunnerRatio))
		}
	}
	sort.Strings(report.Blockers)
	report.Ready = len(report.Blockers) == 0
	return report
}

func validateShadowDependencyObservability(row ShadowLedgerRow) error {
	for _, result := range row.ShadowResults {
		if result.StartedAt.IsZero() || !result.FinishedAt.After(result.StartedAt) || len(result.Steps) == 0 {
			return fmt.Errorf("producer %q lacks bounded setup/test/teardown evidence", result.Execution.Producer)
		}
		for _, dependency := range result.Dependencies {
			if dependency.Name == "" || dependency.Identity == "" {
				return fmt.Errorf("producer %q has incomplete dependency identity", result.Execution.Producer)
			}
		}
	}
	return nil
}

func validateShadowShardBalance(row ShadowLedgerRow) error {
	var durations []time.Duration
	for _, name := range []string{"Go Race (shard 0)", "Go Race (shard 1)", "Go Race (shard 2)", "Go Race (shard 3)"} {
		found := false
		for _, job := range row.ShadowExecution.Jobs {
			if job.Name == name {
				durations = append(durations, job.CompletedAt.Sub(job.StartedAt))
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("missing %s", name)
		}
	}
	median := medianDuration(durations)
	for _, duration := range durations {
		if median > 0 && float64(duration)/float64(median) > 1.5 {
			return fmt.Errorf("Go Race shard exceeds 150 percent of shard median")
		}
	}
	return nil
}

func isRealExternalFork(row ShadowLedgerRow) bool {
	if row.HeadRepository == "" || row.HeadRepository == row.Repository {
		return false
	}
	switch row.AuthorAssociation {
	case "OWNER", "MEMBER", "COLLABORATOR":
		return false
	default:
		return true
	}
}

func validForkApproval(value *ShadowForkApproval) bool {
	return value != nil && !value.PendingObservedAt.IsZero() && value.ApprovedAt.After(value.PendingObservedAt)
}

func medianDuration(values []time.Duration) time.Duration {
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	if len(ordered)%2 == 1 {
		return ordered[len(ordered)/2]
	}
	return (ordered[len(ordered)/2-1] + ordered[len(ordered)/2]) / 2
}

func medianFloat(values []float64) float64 {
	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	if len(ordered)%2 == 1 {
		return ordered[len(ordered)/2]
	}
	return (ordered[len(ordered)/2-1] + ordered[len(ordered)/2]) / 2
}
