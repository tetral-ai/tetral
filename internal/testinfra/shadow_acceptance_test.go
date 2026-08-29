package testinfra

import (
	"fmt"
	"testing"
	"time"
)

func TestShadowAcceptanceRequiresCompleteMeasuredGate(t *testing.T) {
	rows := make([]ShadowLedgerRow, 10)
	classes := []string{"database-heavy-go", "runtime-or-gateway", "protocol-generated", "ci-test-infrastructure"}
	for index := range rows {
		rows[index] = acceptanceRow(t, index)
		rows[index].ChangeClasses = []string{classes[index%len(classes)]}
	}
	rows[9].HeadRepository = "external/example"
	rows[9].AuthorAssociation = "CONTRIBUTOR"
	rows[9].ForkApproval = validForkEvidence(rows[9])

	report := EvaluateShadowAcceptance(rows, acceptanceAuthority())
	if !report.Ready || report.EligibleTuples != 10 || !report.RealForkObserved || report.WallRatio > 0.75 || report.RunnerRatio > 1.10 {
		t.Fatalf("complete shadow gate = %+v", report)
	}
}

func TestShadowAcceptanceRejectsMissingForkDuplicateAndPerformanceRegression(t *testing.T) {
	rows := make([]ShadowLedgerRow, 10)
	classes := []string{"database-heavy-go", "runtime-or-gateway", "protocol-generated", "ci-test-infrastructure"}
	for index := range rows {
		rows[index] = acceptanceRow(t, index)
		rows[index].ChangeClasses = []string{classes[index%len(classes)]}
		rows[index].ShadowDuration = 14 * time.Minute
	}
	rows[9] = rows[8]
	report := EvaluateShadowAcceptance(rows, acceptanceAuthority())
	if report.Ready || report.DistinctTuples != 9 || len(report.Blockers) < 3 {
		t.Fatalf("incomplete shadow gate passed: %+v", report)
	}
}

func TestShadowAcceptanceRejectsOneDisagreementAmongTenGreenRows(t *testing.T) {
	rows := make([]ShadowLedgerRow, 11)
	classes := []string{"database-heavy-go", "runtime-or-gateway", "protocol-generated", "ci-test-infrastructure"}
	for index := range rows {
		rows[index] = acceptanceRow(t, index)
		rows[index].ChangeClasses = []string{classes[index%len(classes)]}
	}
	rows[9].HeadRepository = "external/example"
	rows[9].AuthorAssociation = "CONTRIBUTOR"
	rows[9].ForkApproval = validForkEvidence(rows[9])
	rows[10].ShadowConclusion = "failure"
	report := EvaluateShadowAcceptance(rows, acceptanceAuthority())
	if report.Ready || len(report.Blockers) == 0 {
		t.Fatalf("unexplained disagreement passed: %+v", report)
	}
}

func acceptanceRow(t *testing.T, index int) ShadowLedgerRow {
	t.Helper()
	snapshot := validShadowSnapshot(t)
	suffix := fmt.Sprintf("-%d", index)
	snapshot.PullRequest = 200 + index
	snapshot.EventHeadSHA += suffix
	snapshot.EventBaseSHA += suffix
	snapshot.TestMergeSHA += suffix
	snapshot.Legacy.CheckHeadSHA = snapshot.EventHeadSHA
	snapshot.Shadow.CheckHeadSHA = snapshot.EventHeadSHA
	for item := range snapshot.Legacy.Checks {
		snapshot.Legacy.Checks[item].HeadSHA = snapshot.EventHeadSHA
	}
	for item := range snapshot.Shadow.Checks {
		snapshot.Shadow.Checks[item].HeadSHA = snapshot.EventHeadSHA
	}
	for item := range snapshot.LegacyMetadata {
		snapshot.LegacyMetadata[item].CheckedOutSHA = snapshot.TestMergeSHA
		snapshot.LegacyMetadata[item].EventHeadSHA = snapshot.EventHeadSHA
		snapshot.LegacyMetadata[item].EventBaseSHA = snapshot.EventBaseSHA
		snapshot.LegacyMetadata[item].TestMergeSHA = snapshot.TestMergeSHA
	}
	for item := range snapshot.ShadowResults {
		snapshot.ShadowResults[item].Execution.EventHeadSHA = snapshot.EventHeadSHA
		snapshot.ShadowResults[item].Execution.EventBaseSHA = snapshot.EventBaseSHA
		snapshot.ShadowResults[item].Execution.TestMergeSHA = snapshot.TestMergeSHA
		snapshot.ShadowResults[item].Execution.RequiredCheckSHA = snapshot.EventHeadSHA
		snapshot.ShadowResults[item].Plan.Revision.Head = snapshot.TestMergeSHA
	}
	row, err := NormalizeShadowSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	row.LegacyDuration = 15 * time.Minute
	row.ShadowDuration = 8 * time.Minute
	row.LegacyRunnerMinutes = 12
	row.ShadowRunnerMinutes = 12
	row.ShadowResults[0].Plan.Dependencies = []string{"postgresql", "minio"}
	row.ShadowResults[0].Setup = 2 * time.Second
	row.ShadowResults[0].Teardown = time.Second
	row.ShadowResults[0].Dependencies = []DependencyEvidence{
		{Name: "postgresql", Source: "runner-container", Identity: "postgres@sha256:fixture", SetupElapsed: time.Second, TemplateIdentity: "template-digest", TemplateSetupElapsed: time.Second, TeardownElapsed: time.Second},
		{Name: "minio", Source: "runner-container", Identity: "minio@sha256:fixture", SetupElapsed: time.Second, TeardownElapsed: time.Second},
	}
	return row
}

func acceptanceAuthority() ShadowAcceptanceAuthority {
	return ShadowAcceptanceAuthority{
		IntroductionPullRequest: 19,
		IntroducedByCommit:      "source",
		WorkflowSourceSHA:       "source",
		EligibleAfter:           time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC),
	}
}

func validForkEvidence(row ShadowLedgerRow) *ShadowForkApproval {
	return &ShadowForkApproval{
		HeadSHA: row.EventHeadSHA, RunID: row.ShadowRunID, PendingStatus: "action_required",
		PendingObservedAt: time.Date(2026, 8, 28, 11, 0, 0, 0, time.UTC), PendingCaptureSHA256: "sha256:pending",
		ApprovalState: "approved", ApprovalActorID: 42, ApprovalCaptureSHA256: "sha256:approval",
		AgreedIssueNumber: 7, AgreementCommentID: 8, AgreementCaptureSHA256: "sha256:agreement",
		CleanupState: "closed", CleanupObservedAt: time.Date(2026, 8, 28, 13, 0, 0, 0, time.UTC), CleanupCaptureSHA256: "sha256:cleanup",
	}
}
