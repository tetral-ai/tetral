package release

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The input is emitted by the Kit's Python round runner and real ledger writer
// against synthetic commands. Keeping the producer's bytes catches protocol and
// canonicalization drift that Go-only fixtures cannot detect.
func TestKitReportThroughReleaseCLI(t *testing.T) {
	body, err := os.ReadFile("testdata/kit-report-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	report, err := DecodeRehearsalReport(body)
	if err != nil {
		t.Fatal(err)
	}
	now := report.FinishedAt.Add(time.Minute).Format(time.RFC3339Nano)
	candidate := validCandidate(t)
	candidate.Version = mustVersion(t, report.Plan.Version)
	candidate.SourceCommit = report.Plan.SourceCommit
	candidate.Chart.RenderCommand = "helm template tetral dist/tetral-" + candidate.Version.Artifact + ".tgz -f release-values.json"
	dir := t.TempDir()
	binary := filepath.Join(dir, "tetral-release")
	if output, err := exec.Command("go", "build", "-o", binary, "./cmd/tetral-release").CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, output)
	}
	run := func(arguments ...string) []byte {
		t.Helper()
		output, err := exec.Command(binary, arguments...).CombinedOutput()
		if err != nil {
			t.Fatalf("CLI %s: %v\n%s", arguments[0], err, output)
		}
		return output
	}
	write := func(name string, value any) string {
		t.Helper()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	layout := filepath.Join(dir, "report-layout")
	run("artifact", "--kind", "rehearsal-report", "--input", "testdata/kit-report-v1.json", "--output", layout)
	artifact, err := ReadSingleOCIArtifact(layout)
	if err != nil {
		t.Fatal(err)
	}
	pulled := filepath.Join(dir, "report.json")
	run("validate-layout", "--kind", "rehearsal-report", "--root", layout, "--output-layer", pulled)
	candidatePath := write("candidate.json", candidate)
	recordArgs := []string{"record-rehearsal", "--candidate", candidatePath, "--candidate-digest", report.Plan.CandidateDigest,
		"--report", pulled, "--report-digest", artifact.ManifestDigest, "--workflow-run-id", "42",
		"--workflow-run-attempt", "1", "--deployment-id", "11", "--now", now}
	var evidence RehearsalEvidence
	if err := json.Unmarshal(run(recordArgs...), &evidence); err != nil {
		t.Fatal(err)
	}
	if evidence.CaseCount != 2 || evidence.Report == nil || len(evidence.Report.Plan.Steps) != 3 {
		t.Fatal("producer's three steps were not reconciled as two parent cases")
	}
	evidencePath := write("evidence.json", evidence)
	validation := []string{"validate-rehearsal", "--candidate", candidatePath, "--candidate-digest", report.Plan.CandidateDigest,
		"--evidence", evidencePath, "--now", now}
	run(append(validation, "--require-report")...)
	// Historical metadata remains readable, but cannot authorize a new promotion.
	evidence.Report, evidence.ReportDigest = nil, ""
	write("evidence.json", evidence)
	run(validation...)
	if _, err := exec.Command(binary, append(validation, "--require-report")...).CombinedOutput(); err == nil {
		t.Fatal("legacy metadata authorized a new publication without a report")
	}
	// The complete OCI/CLI path must reject a failed sibling even if another leg
	// of the same parent case passed later.
	report.Attempts[0].Verdict = "FAIL"
	failed := write("failed-report.json", report)
	if _, err := exec.Command(binary, "artifact", "--kind", "rehearsal-report", "--input", failed,
		"--output", filepath.Join(dir, "failed-layout")).CombinedOutput(); err == nil {
		t.Fatal("a failed sibling was packaged as release acceptance")
	}
}

func reportFixture(t *testing.T) (CandidateManifest, string, RehearsalReport, time.Time) {
	t.Helper()
	candidate := validCandidate(t)
	digest := testDigest("candidate")
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	start := now.Add(-time.Hour)
	observation := func(offset time.Duration, verdict string) RehearsalObservation {
		return RehearsalObservation{
			Verdict: verdict, StartedAt: start.Add(offset), FinishedAt: start.Add(offset + time.Minute),
			EvidenceDigests: []string{testDigest("private evidence retained by kit")},
		}
	}
	report := RehearsalReport{
		Schema: RehearsalReportSchema,
		Plan: RehearsalPlan{
			Schema: RehearsalPlanSchema, RoundID: "round-1", Version: candidate.Version.Git,
			SourceCommit: candidate.SourceCommit, CandidateDigest: digest,
			KitCommit: strings.Repeat("a", 40), SDKRevision: strings.Repeat("b", 40),
			SDKPackageDigest: testDigest("sdk"), ValuesDigest: testDigest("operator values"),
			RenderDigest: testDigest("operator render"),
			Steps:        []RehearsalStep{{ID: "S2.T1.0.crud", CaseID: "T1.0"}, {ID: "S2.T1.0.bad-artifact", CaseID: "T1.0"}},
		},
		StartedAt: start, FinishedAt: now.Add(-time.Minute),
		Attempts: []RehearsalAttempt{
			{StepID: "S2.T1.0.crud", Attempt: 1, RehearsalObservation: observation(time.Minute, "PASS")},
			{StepID: "S2.T1.0.bad-artifact", Attempt: 1, RehearsalObservation: observation(3*time.Minute, "PASS")},
		},
		FinalCheck: observation(10*time.Minute, "PASS"),
	}
	setPlanDigest(t, &report)
	return candidate, digest, report, now
}

func setPlanDigest(t *testing.T, report *RehearsalReport) {
	t.Helper()
	var err error
	report.PlanDigest, err = ContentDigest(report.Plan)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRehearsalReportReconcilesRequiredLegsAndSameLegRetries(t *testing.T) {
	_, _, report, now := reportFixture(t)
	// A late PASS for a sibling must not overwrite an earlier failed leg.
	report.Attempts[0].Verdict = "FAIL"
	if err := ValidateRehearsalReport(report, now); err == nil {
		t.Fatal("a sibling PASS hid the failed required leg")
	}
	retry := report.Attempts[0]
	retry.Attempt, retry.Verdict = 2, "PASS"
	retry.StartedAt, retry.FinishedAt = now.Add(-52*time.Minute), now.Add(-51*time.Minute)
	report.Attempts = append(report.Attempts, retry)
	if err := ValidateRehearsalReport(report, now); err != nil {
		t.Fatalf("a complete retry of the same failed leg must be accepted: %v", err)
	}
	// A newer incomplete attempt cannot fall back to its old passing attempt.
	retry.Attempt, retry.Verdict = 3, ""
	retry.StartedAt, retry.FinishedAt = now.Add(-51*time.Minute), time.Time{}
	report.Attempts = append(report.Attempts, retry)
	if err := ValidateRehearsalReport(report, now); err == nil {
		t.Fatal("incomplete latest attempt fell back to old PASS")
	}
}

func TestRehearsalReportRejectsMissingStaleAndUnrestoredEvidence(t *testing.T) {
	for name, mutate := range map[string]func(*RehearsalReport){
		"missing leg":              func(r *RehearsalReport) { r.Attempts = r.Attempts[:1] },
		"duplicate attempt":        func(r *RehearsalReport) { r.Attempts = append(r.Attempts, r.Attempts[0]) },
		"unknown step":             func(r *RehearsalReport) { r.Attempts[0].StepID = "unplanned" },
		"no evidence":              func(r *RehearsalReport) { r.Attempts[0].EvidenceDigests = nil },
		"evidence outside round":   func(r *RehearsalReport) { r.Attempts[0].StartedAt = r.StartedAt.Add(-time.Second) },
		"failed restoration":       func(r *RehearsalReport) { r.FinalCheck.Verdict = "FAIL" },
		"final check before retry": func(r *RehearsalReport) { r.FinalCheck.StartedAt = r.StartedAt.Add(time.Minute) },
		"absent final check":       func(r *RehearsalReport) { r.FinalCheck = RehearsalObservation{} },
		"inconclusive leg":         func(r *RehearsalReport) { r.Attempts[0].Verdict = "INCONCLUSIVE" },
		"plan tampering":           func(r *RehearsalReport) { r.Plan.Steps = r.Plan.Steps[:1] },
		"duplicate plan identity": func(r *RehearsalReport) {
			r.Plan.Steps[1].ID = r.Plan.Steps[0].ID
			setPlanDigest(t, r)
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, report, now := reportFixture(t)
			mutate(&report)
			if err := ValidateRehearsalReport(report, now); err == nil {
				t.Fatal("accepted invalid rehearsal proof")
			}
		})
	}
}

func TestRecordRehearsalDerivesAndBindsPublishedFacts(t *testing.T) {
	candidate, candidateDigest, report, now := reportFixture(t)
	reportDigest, err := RehearsalReportArtifactDigest(report)
	if err != nil {
		t.Fatal(err)
	}
	recording := RehearsalRecording{WorkflowRunID: 42, WorkflowRunAttempt: 1, DeploymentID: 11, RecordedAt: now}
	evidence, err := RecordRehearsal(candidate, candidateDigest, report, reportDigest, recording)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.CaseCount != 1 || evidence.Result != "pass" || evidence.CaseManifestDigest != report.PlanDigest ||
		evidence.ReportDigest != reportDigest || evidence.Report == nil || evidence.ValuesDigest != report.Plan.ValuesDigest {
		t.Fatalf("published facts were not derived from the complete two-leg report: %#v", evidence)
	}
	for name, mutate := range map[string]func(*RehearsalEvidence){
		"case count":      func(e *RehearsalEvidence) { e.CaseCount++ },
		"operator render": func(e *RehearsalEvidence) { e.RenderDigest = testDigest("different") },
		"report artifact": func(e *RehearsalEvidence) { e.ReportDigest = testDigest("different") },
		"missing report":  func(e *RehearsalEvidence) { e.Report = nil },
	} {
		t.Run(name, func(t *testing.T) {
			changed := evidence
			mutate(&changed)
			if err := ValidateRehearsal(candidate, candidateDigest, changed, now); err == nil {
				t.Fatal("accepted independent modification of report-derived facts")
			}
		})
	}
	if _, err := RecordRehearsal(candidate, testDigest("other candidate"), report, reportDigest, recording); err == nil {
		t.Fatal("accepted report for another candidate")
	}
	recording.RecordedAt = now.Add(8 * 24 * time.Hour)
	if _, err := RecordRehearsal(candidate, candidateDigest, report, reportDigest, recording); err == nil {
		t.Fatal("accepted expired report")
	}
}

func TestRehearsalReportDecoderRejectsPrivateExtensions(t *testing.T) {
	_, _, report, now := reportFixture(t)
	body, err := CanonicalJSON(report)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRehearsalReport(body)
	if err != nil || ValidateRehearsalReport(decoded, now) != nil {
		t.Fatalf("valid public report rejected: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	payload["private-password"] = "/private/local/evidence"
	body, err = json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeRehearsalReport(body); err == nil || strings.Contains(err.Error(), "private-") || strings.Contains(err.Error(), "/private") {
		t.Fatalf("unknown report field was accepted or echoed: %v", err)
	}
}
