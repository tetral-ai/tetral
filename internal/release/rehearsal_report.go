package release

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"time"
)

const (
	RehearsalPlanSchema   = "tetral.rehearsal-plan/v1"
	RehearsalReportSchema = "tetral.rehearsal-report/v1"
)

var rehearsalIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type RehearsalStep struct {
	ID     string `json:"id"`
	CaseID string `json:"case_id"`
}

type RehearsalPlan struct {
	Schema           string          `json:"schema"`
	RoundID          string          `json:"round_id"`
	Version          string          `json:"version"`
	SourceCommit     string          `json:"source_commit"`
	CandidateDigest  string          `json:"candidate_digest"`
	KitCommit        string          `json:"kit_commit"`
	SDKRevision      string          `json:"sdk_revision"`
	SDKPackageDigest string          `json:"sdk_package_digest"`
	ValuesDigest     string          `json:"values_digest"`
	RenderDigest     string          `json:"render_digest"`
	Steps            []RehearsalStep `json:"steps"`
}

type RehearsalObservation struct {
	Verdict         string    `json:"verdict"`
	StartedAt       time.Time `json:"started_at"`
	FinishedAt      time.Time `json:"finished_at"`
	EvidenceDigests []string  `json:"evidence_digests"`
}

type RehearsalAttempt struct {
	StepID  string `json:"step_id"`
	Attempt int    `json:"attempt"`
	RehearsalObservation
}

type RehearsalReport struct {
	Schema     string               `json:"schema"`
	Plan       RehearsalPlan        `json:"plan"`
	PlanDigest string               `json:"plan_digest"`
	StartedAt  time.Time            `json:"started_at"`
	FinishedAt time.Time            `json:"finished_at"`
	Attempts   []RehearsalAttempt   `json:"attempts"`
	FinalCheck RehearsalObservation `json:"final_check"`
}

// DecodeRehearsalReport accepts only the public report contract. Unknown fields
// and raw parser errors must not escape into published metadata or CI logs.
func DecodeRehearsalReport(body []byte) (RehearsalReport, error) {
	var report RehearsalReport
	if len(body) > 8*1024*1024 {
		return report, fmt.Errorf("rehearsal report exceeds the size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&report); err != nil {
		return report, fmt.Errorf("rehearsal report must match the public JSON contract")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return report, fmt.Errorf("rehearsal report must contain one JSON value")
	}
	return report, nil
}

// ValidateRehearsalReport reconciles the selected required steps and their
// attempts. It verifies the report, not the private experiment or evidence bytes;
// the Kit and protected operator boundary retain custody of that evidence.
func ValidateRehearsalReport(report RehearsalReport, now time.Time) error {
	plan := report.Plan
	if report.Schema != RehearsalReportSchema || plan.Schema != RehearsalPlanSchema ||
		!rehearsalIDPattern.MatchString(plan.RoundID) ||
		!commitPattern.MatchString(plan.SourceCommit) ||
		!commitPattern.MatchString(plan.KitCommit) ||
		!commitPattern.MatchString(plan.SDKRevision) {
		return fmt.Errorf("rehearsal report identity is invalid")
	}
	if _, err := ParseVersion(plan.Version); err != nil {
		return fmt.Errorf("rehearsal plan version is invalid")
	}
	for _, digest := range []string{plan.CandidateDigest, plan.SDKPackageDigest, plan.ValuesDigest, plan.RenderDigest} {
		if !digestPattern.MatchString(digest) {
			return fmt.Errorf("rehearsal plan artifact identity is invalid")
		}
	}
	planDigest, err := ContentDigest(plan)
	if err != nil || planDigest != report.PlanDigest {
		return fmt.Errorf("rehearsal plan digest does not match its contents")
	}
	if report.StartedAt.IsZero() || !report.FinishedAt.After(report.StartedAt) ||
		report.FinishedAt.After(now) {
		return fmt.Errorf("rehearsal report time window is invalid")
	}
	if len(plan.Steps) == 0 || len(plan.Steps) > 10000 || len(report.Attempts) > 100000 {
		return fmt.Errorf("rehearsal report has an invalid step or attempt count")
	}
	required := make(map[string]bool, len(plan.Steps))
	for _, step := range plan.Steps {
		if !rehearsalIDPattern.MatchString(step.ID) || !rehearsalIDPattern.MatchString(step.CaseID) || required[step.ID] {
			return fmt.Errorf("rehearsal plan contains invalid or duplicate step identities")
		}
		required[step.ID] = true
	}
	latest := make(map[string]RehearsalAttempt, len(required))
	lastFinished := report.StartedAt
	for _, attempt := range report.Attempts {
		previous := latest[attempt.StepID]
		if !required[attempt.StepID] || attempt.Attempt != previous.Attempt+1 {
			return fmt.Errorf("rehearsal attempt has an unknown step or invalid attempt sequence")
		}
		if err := validateRehearsalObservation(attempt.RehearsalObservation, report); err != nil {
			return err
		}
		if previous.Attempt > 0 && attempt.StartedAt.Before(previous.FinishedAt) {
			return fmt.Errorf("rehearsal attempts for the same step overlap")
		}
		latest[attempt.StepID] = attempt
		if attempt.FinishedAt.After(lastFinished) {
			lastFinished = attempt.FinishedAt
		}
	}
	for _, step := range plan.Steps {
		if latest[step.ID].Verdict != "PASS" {
			return fmt.Errorf("a required rehearsal step is missing or has not passed")
		}
	}
	if err := validateRehearsalObservation(report.FinalCheck, report); err != nil {
		return err
	}
	if report.FinalCheck.Verdict != "PASS" || report.FinalCheck.StartedAt.Before(lastFinished) {
		return fmt.Errorf("rehearsal final health and restoration check has not passed after all attempts")
	}
	return nil
}

func validateRehearsalObservation(observation RehearsalObservation, report RehearsalReport) error {
	switch observation.Verdict {
	case "PASS", "FAIL", "INCONCLUSIVE", "NOT COVERED", "FINDING_CHANGED":
	default:
		return fmt.Errorf("rehearsal observation has an invalid verdict")
	}
	if observation.StartedAt.Before(report.StartedAt) || !observation.FinishedAt.After(observation.StartedAt) ||
		observation.FinishedAt.After(report.FinishedAt) || len(observation.EvidenceDigests) == 0 {
		return fmt.Errorf("rehearsal observation is incomplete or outside the round")
	}
	for _, digest := range observation.EvidenceDigests {
		if !digestPattern.MatchString(digest) {
			return fmt.Errorf("rehearsal observation has invalid evidence identity")
		}
	}
	return nil
}

func RehearsalReportArtifactDigest(report RehearsalReport) (string, error) {
	body, err := CanonicalJSON(report)
	if err != nil {
		return "", err
	}
	artifact, err := BuildOCIArtifact(RehearsalReportType, RehearsalReportType, body)
	if err != nil {
		return "", err
	}
	return artifact.ManifestDigest, nil
}

type RehearsalRecording struct {
	WorkflowRunID      int64
	WorkflowRunAttempt int
	DeploymentID       int64
	RecordedAt         time.Time
}

// RecordRehearsal derives acceptance facts from a verified report. Counts,
// digests, timestamps and result are not separate operator inputs.
func RecordRehearsal(candidate CandidateManifest, candidateDigest string, report RehearsalReport, reportDigest string, recording RehearsalRecording) (RehearsalEvidence, error) {
	if err := ValidateCandidate(candidate); err != nil {
		return RehearsalEvidence{}, err
	}
	if err := ValidateRehearsalReport(report, recording.RecordedAt); err != nil {
		return RehearsalEvidence{}, err
	}
	if report.Plan.CandidateDigest != candidateDigest || report.Plan.SourceCommit != candidate.SourceCommit || report.Plan.Version != candidate.Version.Git {
		return RehearsalEvidence{}, fmt.Errorf("rehearsal report does not identify the candidate")
	}
	expectedDigest, err := RehearsalReportArtifactDigest(report)
	if err != nil || expectedDigest != reportDigest {
		return RehearsalEvidence{}, fmt.Errorf("rehearsal report artifact does not match its contents")
	}
	cases := map[string]bool{}
	for _, step := range report.Plan.Steps {
		cases[step.CaseID] = true
	}
	localDigest, err := ContentDigest(report)
	if err != nil {
		return RehearsalEvidence{}, err
	}
	evidence := RehearsalEvidence{
		Schema: RehearsalSchema, Version: candidate.Version, SourceCommit: candidate.SourceCommit,
		CandidateDigest: candidateDigest, CaseManifestDigest: report.PlanDigest,
		CaseCount: len(cases), LocalEvidenceDigest: localDigest,
		ValuesDigest: report.Plan.ValuesDigest, RenderDigest: report.Plan.RenderDigest,
		Result: "pass", WorkflowRunID: recording.WorkflowRunID, WorkflowRunAttempt: recording.WorkflowRunAttempt,
		DeploymentID: recording.DeploymentID, StartedAt: report.StartedAt, FinishedAt: report.FinishedAt,
		RecordedAt: recording.RecordedAt, ReportDigest: reportDigest, Report: &report,
	}
	if err := ValidateRehearsal(candidate, candidateDigest, evidence, recording.RecordedAt); err != nil {
		return RehearsalEvidence{}, err
	}
	return evidence, nil
}
