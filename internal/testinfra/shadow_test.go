package testinfra

import (
	"strconv"
	"testing"
	"time"
)

func TestNormalizeShadowSnapshotAcceptsExactComparablePair(t *testing.T) {
	row, err := NormalizeShadowSnapshot(validShadowSnapshot(t))
	if err != nil {
		t.Fatal(err)
	}
	if row.LegacyRunnerMinutes <= 0 || row.ShadowRunnerMinutes <= 0 || row.RequiredCheckSHA != row.TestMergeSHA || row.SnapshotDigest == "" {
		t.Fatalf("incomplete normalized row: %+v", row)
	}
}

func TestNormalizeShadowSnapshotRejectsInvalidJoins(t *testing.T) {
	tests := map[string]func(*ShadowSnapshot){
		"carrier":              func(value *ShadowSnapshot) { value.Shadow.CheckHeadSHA = "wrong" },
		"source":               func(value *ShadowSnapshot) { value.Shadow.WorkflowSourceSHA = "wrong" },
		"app":                  func(value *ShadowSnapshot) { value.Shadow.SourceAppID++ },
		"attempt":              func(value *ShadowSnapshot) { value.Shadow.RunAttempt = 2 },
		"duplicate check":      func(value *ShadowSnapshot) { value.Shadow.Checks = append(value.Shadow.Checks, value.Shadow.Checks[0]) },
		"missing legacy":       func(value *ShadowSnapshot) { value.LegacyMetadata = value.LegacyMetadata[1:] },
		"missing shadow":       func(value *ShadowSnapshot) { value.ShadowResults = value.ShadowResults[1:] },
		"duration":             func(value *ShadowSnapshot) { value.Shadow.CompletedAt = value.Shadow.StartedAt },
		"wrong run":            func(value *ShadowSnapshot) { value.ShadowResults[0].Execution.RunID = "999" },
		"wrong result attempt": func(value *ShadowSnapshot) { value.ShadowResults[0].Execution.RunAttempt = "2" },
		"missing Merge Gate":   func(value *ShadowSnapshot) { value.Shadow.Jobs = value.Shadow.Jobs[:len(value.Shadow.Jobs)-1] },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := validShadowSnapshot(t)
			mutate(&value)
			if _, err := NormalizeShadowSnapshot(value); err == nil {
				t.Fatal("invalid shadow snapshot passed")
			}
		})
	}
}

func validShadowSnapshot(t *testing.T) ShadowSnapshot {
	t.Helper()
	started := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	snapshot := ShadowSnapshot{
		Repository: "tetral-ai/tetral", PullRequest: 101, HeadRepository: "tetral-ai/tetral", AuthorAssociation: "OWNER",
		ChangedPaths: []string{"internal/testinfra/shadow.go"},
		EventHeadSHA: "head", EventBaseSHA: "base", TestMergeSHA: "merge", CollectedAt: started.Add(20 * time.Minute),
		Legacy: ShadowWorkflowExecution{Name: "engine-ci", Path: ".github/workflows/engine-ci.yml", RunID: 1001, RunAttempt: 1, CheckSuiteID: 2001, CheckHeadSHA: "merge", SourceAppID: githubActionsAppID, WorkflowSourceSHA: "source", WorkflowBlobSHA: "legacy-blob", CreatedAt: started.Add(-time.Minute), StartedAt: started, CompletedAt: started.Add(15 * time.Minute)},
		Shadow: ShadowWorkflowExecution{Name: "Pull Request Verification", Path: ".github/workflows/pull-request-verification.yml", RunID: 1002, RunAttempt: 1, CheckSuiteID: 2002, CheckHeadSHA: "merge", SourceAppID: githubActionsAppID, WorkflowSourceSHA: "source", WorkflowBlobSHA: "shadow-blob", CreatedAt: started.Add(-time.Minute), StartedAt: started, CompletedAt: started.Add(8 * time.Minute)},
	}
	jobID := int64(1)
	for producer, name := range legacyShadowProducers {
		job := ShadowJob{ID: jobID, Name: name, Status: "completed", Conclusion: "success", StartedAt: started, CompletedAt: started.Add(time.Minute), Steps: []ShadowJobStep{
			{Name: "Record legacy verification metadata", Status: "completed", Conclusion: "success", StartedAt: started, CompletedAt: started.Add(time.Second)},
			{Name: "Run legacy evidence", Status: "completed", Conclusion: "success", StartedAt: started.Add(time.Second), CompletedAt: started.Add(time.Minute)},
		}}
		check := ShadowCheck{ID: jobID + 100, Name: name, HeadSHA: "merge", AppID: githubActionsAppID, Status: "completed", Conclusion: "success"}
		snapshot.Legacy.Jobs = append(snapshot.Legacy.Jobs, job)
		snapshot.Legacy.Checks = append(snapshot.Legacy.Checks, check)
		snapshot.LegacyMetadata = append(snapshot.LegacyMetadata, LegacyWorkflowMetadata{
			Schema: "tetral.ci-legacy-sidecar/v1", Repository: snapshot.Repository, Producer: producer,
			CheckedOutSHA: "merge", EventHeadSHA: "head", EventBaseSHA: "base", TestMergeSHA: "merge",
			WorkflowSourceSHA: "source", Workflow: "engine-ci", RunID: "1001", RunAttempt: "1", Job: legacyProducerJobIDs[producer],
			StartedAt: started.Format(time.RFC3339), CompletedAt: started.Add(time.Second).Format(time.RFC3339),
		})
		jobID++
	}
	producers, err := LoadPRProducers()
	if err != nil {
		t.Fatal(err)
	}
	for _, producer := range producers {
		name := shadowProducerJobs[producer]
		job := ShadowJob{ID: jobID, Name: name, Status: "completed", Conclusion: "success", StartedAt: started, CompletedAt: started.Add(time.Minute), Steps: []ShadowJobStep{
			{Name: "Run shadow evidence", Status: "completed", Conclusion: "success", StartedAt: started, CompletedAt: started.Add(time.Minute)},
		}}
		check := ShadowCheck{ID: jobID + 100, Name: name, HeadSHA: "merge", AppID: githubActionsAppID, Status: "completed", Conclusion: "success"}
		snapshot.Shadow.Jobs = append(snapshot.Shadow.Jobs, job)
		snapshot.Shadow.Checks = append(snapshot.Shadow.Checks, check)
		snapshot.ShadowResults = append(snapshot.ShadowResults, Result{
			Plan: Plan{Revision: Revision{Head: "merge"}}, Status: "pass",
			Execution: ExecutionEnvelope{Repository: snapshot.Repository, EventHeadSHA: "head", EventBaseSHA: "base", TestMergeSHA: "merge", RequiredCheckSHA: "merge", WorkflowSourceSHA: "source", Workflow: "Pull Request Verification", RunID: "1002", RunAttempt: strconv.Itoa(snapshot.Shadow.RunAttempt), Job: PRProducerJobs()[producer], Producer: producer},
			StartedAt: started, FinishedAt: started.Add(time.Minute), Steps: []StepResult{{Group: producer, Command: []string{"test"}, Status: "pass", Elapsed: time.Minute}},
		})
		jobID++
	}
	gateJob := ShadowJob{ID: jobID, Name: "Merge Gate", Status: "completed", Conclusion: "success", StartedAt: started.Add(7 * time.Minute), CompletedAt: started.Add(8 * time.Minute), Steps: []ShadowJobStep{
		{Name: "Reconcile exact-head evidence", Status: "completed", Conclusion: "success", StartedAt: started.Add(7 * time.Minute), CompletedAt: started.Add(8 * time.Minute)},
	}}
	gateCheck := ShadowCheck{ID: jobID + 100, Name: "Merge Gate", HeadSHA: "merge", AppID: githubActionsAppID, Status: "completed", Conclusion: "success"}
	snapshot.Shadow.Jobs = append(snapshot.Shadow.Jobs, gateJob)
	snapshot.Shadow.Checks = append(snapshot.Shadow.Checks, gateCheck)
	return snapshot
}
