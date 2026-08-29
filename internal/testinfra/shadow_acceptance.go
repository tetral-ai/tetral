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

type ShadowAcceptanceAuthority struct {
	IntroductionPullRequest int       `json:"introduction_pull_request"`
	IntroducedByCommit      string    `json:"introduced_by_commit"`
	WorkflowSourceSHA       string    `json:"workflow_source_sha"`
	EligibleAfter           time.Time `json:"eligible_after"`
}

type ShadowObservationUniverse struct {
	Version       int                       `json:"version"`
	Repository    string                    `json:"repository"`
	EligibleAfter time.Time                 `json:"eligible_after"`
	EnumeratedAt  time.Time                 `json:"enumerated_at"`
	Members       []ShadowObservationMember `json:"members"`
}

type ShadowObservationMember struct {
	PullRequest      int       `json:"pull_request"`
	EventHeadSHA     string    `json:"event_head_sha"`
	LegacyRunID      int64     `json:"legacy_run_id"`
	LegacyRunAttempt int       `json:"legacy_run_attempt"`
	ShadowRunID      int64     `json:"shadow_run_id"`
	ShadowRunAttempt int       `json:"shadow_run_attempt"`
	ShadowCreatedAt  time.Time `json:"shadow_created_at"`
}

type ShadowReliability struct {
	PullRequest      int      `json:"pull_request"`
	TestMergeSHA     string   `json:"test_merge_sha"`
	LegacyConclusion string   `json:"legacy_conclusion"`
	ShadowConclusion string   `json:"shadow_conclusion"`
	Eligible         bool     `json:"eligible"`
	Reasons          []string `json:"reasons,omitempty"`
}

func EvaluateShadowAcceptance(rows []ShadowLedgerRow, authority ShadowAcceptanceAuthority, universe ShadowObservationUniverse) ShadowAcceptanceReport {
	report := ShadowAcceptanceReport{EstimatorVersion: ShadowEstimatorVersion}
	if authority.IntroductionPullRequest <= 0 || authority.IntroducedByCommit == "" || authority.WorkflowSourceSHA == "" || authority.EligibleAfter.IsZero() ||
		authority.IntroducedByCommit != authority.WorkflowSourceSHA {
		report.Blockers = append(report.Blockers, "shadow observation authority is incomplete")
		report.Ready = false
		return report
	}
	selectedRows, blockers := reconcileShadowUniverse(rows, authority, universe)
	if len(blockers) > 0 {
		report.Blockers = append(report.Blockers, blockers...)
		report.Ready = false
		return report
	}
	rows = selectedRows
	seen := map[string]bool{}
	covered := map[string]bool{}
	observedDependencies := map[string]bool{}
	var legacyWalls, shadowWalls []time.Duration
	var legacyMinutes, shadowMinutes []float64

	for _, row := range rows {
		if row.PullRequest == authority.IntroductionPullRequest {
			continue
		}
		if !row.ShadowExecution.CreatedAt.After(authority.EligibleAfter) {
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
		if row.WorkflowSourceSHA != authority.WorkflowSourceSHA {
			reliability.Reasons = append(reliability.Reasons, "workflow source is outside the approved observation window")
		}
		if len(row.ChangedPaths) == 0 || len(row.ChangeClasses) == 0 {
			reliability.Reasons = append(reliability.Reasons, "head has no classified meaningful source change")
		}
		if row.LegacyRunAttempt != 1 || row.ShadowRunAttempt != 1 {
			reliability.Reasons = append(reliability.Reasons, "not a paired first attempt")
		}
		if row.LegacyConclusion != "success" || row.ShadowConclusion != "success" {
			reliability.Reasons = append(reliability.Reasons, "old and new workflows are not both successful")
		}
		if row.GateConclusion != "success" || row.ArtifactConclusion != "success" {
			reliability.Reasons = append(reliability.Reasons, "Merge Gate or producer evidence did not reconcile successfully")
		}
		if row.RequiredCheckSHA != row.EventHeadSHA || row.SnapshotDigest == "" {
			reliability.Reasons = append(reliability.Reasons, "execution identity is incomplete")
		}
		dependencies, err := validateShadowDependencyObservability(row)
		if err != nil {
			reliability.Reasons = append(reliability.Reasons, err.Error())
		}
		if err := validateShadowShardBalance(row); err != nil {
			reliability.Reasons = append(reliability.Reasons, err.Error())
			report.Blockers = append(report.Blockers, fmt.Sprintf("PR %d: %v", row.PullRequest, err))
		}
		if len(reliability.Reasons) > 0 && !validShadowDisposition(row.Disposition) {
			report.Blockers = append(report.Blockers, fmt.Sprintf("PR %d has unexplained shadow evidence: %v", row.PullRequest, reliability.Reasons))
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
			for dependency := range dependencies {
				observedDependencies[dependency] = true
			}
			if isRealExternalFork(row) && validForkApproval(row.ForkApproval) &&
				row.ForkApproval.HeadSHA == row.EventHeadSHA && row.ForkApproval.RunID == row.ShadowRunID {
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
	for _, dependency := range []string{"postgresql", "minio"} {
		if !observedDependencies[dependency] {
			report.Blockers = append(report.Blockers, "missing separately measured "+dependency+" lifecycle")
		}
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

func reconcileShadowUniverse(rows []ShadowLedgerRow, authority ShadowAcceptanceAuthority, universe ShadowObservationUniverse) ([]ShadowLedgerRow, []string) {
	if universe.Version != 1 || universe.Repository == "" || !universe.EligibleAfter.Equal(authority.EligibleAfter) || !universe.EnumeratedAt.After(universe.EligibleAfter) {
		return nil, []string{"shadow observation universe is incomplete"}
	}
	expected := map[int]ShadowObservationMember{}
	var blockers []string
	for _, member := range universe.Members {
		if member.PullRequest <= 0 || member.EventHeadSHA == "" {
			return nil, []string{"shadow observation universe contains an invalid member"}
		}
		if member.PullRequest == authority.IntroductionPullRequest {
			continue
		}
		if _, exists := expected[member.PullRequest]; exists {
			return nil, []string{"shadow observation universe contains a duplicate pull request"}
		}
		expected[member.PullRequest] = member
		if member.LegacyRunID <= 0 || member.LegacyRunAttempt <= 0 || member.ShadowRunID <= 0 || member.ShadowRunAttempt <= 0 || !member.ShadowCreatedAt.After(authority.EligibleAfter) {
			blockers = append(blockers, fmt.Sprintf("shadow universe lacks a paired workflow run for PR %d head %s", member.PullRequest, member.EventHeadSHA))
		}
	}
	actual := map[int]ShadowLedgerRow{}
	for _, row := range rows {
		if row.PullRequest == authority.IntroductionPullRequest || !row.ShadowExecution.CreatedAt.After(authority.EligibleAfter) {
			continue
		}
		member, exists := expected[row.PullRequest]
		if !exists {
			blockers = append(blockers, fmt.Sprintf("shadow ledger contains unenumerated PR %d head %s", row.PullRequest, row.EventHeadSHA))
			continue
		}
		if row.EventHeadSHA != member.EventHeadSHA {
			continue
		}
		if _, exists := actual[row.PullRequest]; exists {
			blockers = append(blockers, fmt.Sprintf("shadow ledger duplicates enumerated PR %d head %s", row.PullRequest, row.EventHeadSHA))
			continue
		}
		actual[row.PullRequest] = row
		if row.Repository != universe.Repository || row.LegacyRunID != member.LegacyRunID || row.LegacyRunAttempt != member.LegacyRunAttempt ||
			row.ShadowRunID != member.ShadowRunID || row.ShadowRunAttempt != member.ShadowRunAttempt || !row.ShadowExecution.CreatedAt.Equal(member.ShadowCreatedAt) {
			blockers = append(blockers, fmt.Sprintf("shadow ledger identity differs from enumeration for PR %d head %s", row.PullRequest, row.EventHeadSHA))
		}
	}
	selected := make([]ShadowLedgerRow, 0, len(expected))
	for pullRequest, member := range expected {
		row, exists := actual[pullRequest]
		if !exists {
			blockers = append(blockers, fmt.Sprintf("shadow ledger is missing enumerated PR %d head %s", member.PullRequest, member.EventHeadSHA))
			continue
		}
		selected = append(selected, row)
	}
	sort.Strings(blockers)
	sort.Slice(selected, func(i, j int) bool { return selected[i].PullRequest < selected[j].PullRequest })
	return selected, blockers
}

func validateShadowDependencyObservability(row ShadowLedgerRow) (map[string]bool, error) {
	if row.EstimatorVersion != ShadowEstimatorVersion || len(row.LegacyIntervals) == 0 || len(row.ShadowIntervals) == 0 {
		return nil, fmt.Errorf("row lacks the versioned substantive-step estimator")
	}
	observed := map[string]bool{}
	for _, result := range row.ShadowResults {
		if result.StartedAt.IsZero() || !result.FinishedAt.After(result.StartedAt) || len(result.Steps) == 0 {
			return nil, fmt.Errorf("producer %q lacks bounded setup/test/teardown evidence", result.Execution.Producer)
		}
		expected := map[string]bool{}
		for _, dependency := range result.Plan.Dependencies {
			expected[dependency] = true
		}
		actual := map[string]bool{}
		for _, dependency := range result.Dependencies {
			if dependency.Name == "" || dependency.Identity == "" || dependency.SetupElapsed <= 0 {
				return nil, fmt.Errorf("producer %q has incomplete dependency setup evidence", result.Execution.Producer)
			}
			if dependency.Source == "runner-container" && dependency.TeardownElapsed <= 0 {
				return nil, fmt.Errorf("producer %q has incomplete dependency teardown evidence", result.Execution.Producer)
			}
			if dependency.Name == "postgresql" && (dependency.TemplateIdentity == "" || dependency.TemplateSetupElapsed <= 0) {
				return nil, fmt.Errorf("producer %q lacks PostgreSQL template setup evidence", result.Execution.Producer)
			}
			actual[dependency.Name] = true
			observed[dependency.Name] = true
		}
		if !sameStringSet(expected, actual) {
			return nil, fmt.Errorf("producer %q dependency plan and evidence differ", result.Execution.Producer)
		}
		for _, step := range result.Steps {
			if step.Status == "" || step.Elapsed <= 0 || len(step.Command) == 0 {
				return nil, fmt.Errorf("producer %q has incomplete test-step evidence", result.Execution.Producer)
			}
		}
	}
	return observed, nil
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
			return fmt.Errorf("go Race shard exceeds 150 percent of shard median")
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
	return value != nil && value.HeadSHA != "" && value.RunID > 0 && value.PendingStatus == "action_required" &&
		!value.PendingObservedAt.IsZero() && value.PendingCaptureSHA256 != "" && value.ApprovalState == "approved" &&
		value.ApprovalActorID > 0 && value.ApprovalCaptureSHA256 != "" && value.AgreedIssueNumber > 0 &&
		value.AgreementCommentID > 0 && value.AgreementCaptureSHA256 != "" &&
		(value.CleanupState == "closed" || value.CleanupState == "merged") &&
		value.CleanupObservedAt.After(value.PendingObservedAt) && value.CleanupCaptureSHA256 != ""
}

func validShadowDisposition(value *ShadowDisposition) bool {
	return value != nil && value.Classification != "" && value.Explanation != "" && len(value.ReproductionCommand) > 0 &&
		value.ReproductionResultDigest != "" && value.ResolvedByHeadSHA != ""
}

func sameStringSet(left, right map[string]bool) bool {
	if len(left) != len(right) {
		return false
	}
	for value := range left {
		if !right[value] {
			return false
		}
	}
	return true
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
