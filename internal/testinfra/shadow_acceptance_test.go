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
	rows[9].ForkApproval = &ShadowForkApproval{PendingObservedAt: time.Unix(100, 0), ApprovedAt: time.Unix(200, 0)}

	report := EvaluateShadowAcceptance(rows, 0)
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
	report := EvaluateShadowAcceptance(rows, 0)
	if report.Ready || report.DistinctTuples != 9 || len(report.Blockers) < 3 {
		t.Fatalf("incomplete shadow gate passed: %+v", report)
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
	snapshot.Legacy.CheckHeadSHA = snapshot.TestMergeSHA
	snapshot.Shadow.CheckHeadSHA = snapshot.TestMergeSHA
	for item := range snapshot.Legacy.Checks {
		snapshot.Legacy.Checks[item].HeadSHA = snapshot.TestMergeSHA
	}
	for item := range snapshot.Shadow.Checks {
		snapshot.Shadow.Checks[item].HeadSHA = snapshot.TestMergeSHA
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
		snapshot.ShadowResults[item].Execution.RequiredCheckSHA = snapshot.TestMergeSHA
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
	return row
}
