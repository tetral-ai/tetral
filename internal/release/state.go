package release

import (
	"fmt"
	"sort"
	"time"
)

type State string

const (
	StateAbsent            State = "absent"
	StateReserved          State = "reserved"
	StateCandidate         State = "candidate"
	StateRehearsed         State = "rehearsed"
	StateAuthorized        State = "authorized"
	StatePartiallyPromoted State = "partially_promoted"
	StateReleased          State = "released"
	StateSuperseded        State = "superseded"
	StateRevoked           State = "revoked"
)

type FinalReferences struct {
	Images              map[string]string `json:"images,omitempty"`
	ChartManifest       string            `json:"chart_manifest,omitempty"`
	ChartPackageDigest  string            `json:"chart_package_digest,omitempty"`
	GitTagCommit        string            `json:"git_tag_commit,omitempty"`
	GitHubReleaseAssets map[string]string `json:"github_release_assets,omitempty"`
}

type Facts struct {
	Reservation         *Reservation       `json:"reservation,omitempty"`
	Candidate           *CandidateManifest `json:"candidate,omitempty"`
	CandidateDigest     string             `json:"candidate_digest,omitempty"`
	Rehearsal           *RehearsalEvidence `json:"rehearsal,omitempty"`
	RehearsalDigest     string             `json:"rehearsal_digest,omitempty"`
	Authorization       *Authorization     `json:"authorization,omitempty"`
	AuthorizationDigest string             `json:"authorization_digest,omitempty"`
	Disposition         *Disposition       `json:"disposition,omitempty"`
	Final               FinalReferences    `json:"final,omitempty"`
}

func Reconstruct(facts Facts, now time.Time) (State, error) {
	if facts.Disposition != nil {
		if facts.Disposition.Schema != DispositionSchema || facts.Disposition.RecordedAt.IsZero() || !digestPattern.MatchString(facts.Disposition.CandidateDigest) {
			return "", fmt.Errorf("release disposition is invalid")
		}
		if facts.Rehearsal != nil || facts.Authorization != nil || hasFinalReferences(facts.Final) {
			return "", fmt.Errorf("release disposition conflicts with later release facts")
		}
		if facts.Candidate == nil || facts.CandidateDigest != facts.Disposition.CandidateDigest || facts.Candidate.Version != facts.Disposition.Version {
			return "", fmt.Errorf("release disposition does not identify its candidate")
		}
		switch facts.Disposition.Kind {
		case string(StateSuperseded):
			return StateSuperseded, nil
		case string(StateRevoked):
			return StateRevoked, nil
		default:
			return "", fmt.Errorf("unknown release disposition %q", facts.Disposition.Kind)
		}
	}
	if facts.Reservation == nil {
		if facts.Candidate != nil || facts.Rehearsal != nil || facts.Authorization != nil || hasFinalReferences(facts.Final) {
			return "", fmt.Errorf("release facts exist without a reservation")
		}
		return StateAbsent, nil
	}
	if err := validateReservation(*facts.Reservation); err != nil {
		return "", err
	}
	if facts.Candidate == nil {
		if facts.Rehearsal != nil || facts.Authorization != nil || hasFinalReferences(facts.Final) {
			return "", fmt.Errorf("release facts exist without a candidate")
		}
		return StateReserved, nil
	}
	if err := ValidateCandidate(*facts.Candidate); err != nil {
		return "", err
	}
	if facts.Candidate.Version != facts.Reservation.Version || facts.Candidate.SourceCommit != facts.Reservation.SourceCommit || !digestPattern.MatchString(facts.CandidateDigest) {
		return "", fmt.Errorf("candidate differs from its reservation")
	}
	if facts.Rehearsal == nil {
		if facts.Authorization != nil || hasFinalReferences(facts.Final) {
			return "", fmt.Errorf("release facts exist without rehearsal evidence")
		}
		return StateCandidate, nil
	}
	rehearsalDecisionTime := now
	if facts.Authorization != nil {
		rehearsalDecisionTime = facts.Authorization.AuthorizedAt
	}
	if err := ValidateRehearsal(*facts.Candidate, facts.CandidateDigest, *facts.Rehearsal, rehearsalDecisionTime); err != nil || !digestPattern.MatchString(facts.RehearsalDigest) {
		return "", fmt.Errorf("rehearsal evidence is invalid")
	}
	if facts.Authorization == nil {
		if hasFinalReferences(facts.Final) {
			return "", fmt.Errorf("release references exist without authorization")
		}
		return StateRehearsed, nil
	}
	if err := validateAuthorization(facts); err != nil {
		return "", err
	}
	complete, count, err := validateFinalReferences(facts)
	if err != nil {
		return "", err
	}
	if complete {
		return StateReleased, nil
	}
	if count > 0 {
		return StatePartiallyPromoted, nil
	}
	return StateAuthorized, nil
}

func PromotionPlan(facts Facts, now time.Time) ([]string, error) {
	state, err := Reconstruct(facts, now)
	if err != nil {
		return nil, err
	}
	if state != StateAuthorized && state != StatePartiallyPromoted && state != StateReleased {
		return nil, fmt.Errorf("release state %q cannot be promoted", state)
	}
	if state == StateReleased {
		return nil, nil
	}
	var steps []string
	for _, name := range SortedImageNames(facts.Candidate.Images) {
		if facts.Final.Images[name] == "" {
			steps = append(steps, "publish-image-reference:"+name)
		}
	}
	if facts.Final.ChartManifest == "" {
		steps = append(steps, "publish-chart")
	}
	if facts.Final.GitTagCommit == "" {
		steps = append(steps, "create-git-tag")
	}
	if len(facts.Final.GitHubReleaseAssets) != 3 {
		steps = append(steps, "create-github-release")
	}
	return steps, nil
}

type CleanupCandidate struct {
	Version   Version   `json:"version"`
	State     State     `json:"state"`
	Digest    string    `json:"digest"`
	CreatedAt time.Time `json:"created_at"`
}

func CleanupPlan(candidates []CleanupCandidate, now time.Time) ([]CleanupCandidate, error) {
	var selected []CleanupCandidate
	for _, candidate := range candidates {
		if !digestPattern.MatchString(candidate.Digest) || candidate.CreatedAt.IsZero() {
			return nil, fmt.Errorf("cleanup candidate identity is incomplete")
		}
		if now.Sub(candidate.CreatedAt) < 30*24*time.Hour {
			continue
		}
		switch candidate.State {
		case StateReserved, StateCandidate, StateSuperseded, StateRevoked:
			selected = append(selected, candidate)
		}
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Version.Sequence < selected[j].Version.Sequence })
	return selected, nil
}

func validateReservation(reservation Reservation) error {
	parsed, err := ParseVersion(reservation.Version.Git)
	if reservation.Schema != ReservationSchema || err != nil || parsed != reservation.Version || !commitPattern.MatchString(reservation.SourceCommit) || reservation.CreatedAt.IsZero() {
		return fmt.Errorf("release reservation is invalid")
	}
	return nil
}

func validateAuthorization(facts Facts) error {
	authorization := facts.Authorization
	if authorization.Schema != AuthorizationSchema || authorization.Version != facts.Candidate.Version || authorization.SourceCommit != facts.Candidate.SourceCommit || authorization.CandidateDigest != facts.CandidateDigest || authorization.EvidenceDigest != facts.RehearsalDigest || authorization.WorkflowRunID < 1 || authorization.WorkflowRunAttempt < 1 || authorization.DeploymentID < 1 || authorization.AuthorizedAt.IsZero() || !digestPattern.MatchString(facts.AuthorizationDigest) {
		return fmt.Errorf("release authorization is invalid")
	}
	return nil
}

func validateFinalReferences(facts Facts) (bool, int, error) {
	count := 0
	if facts.Final.ChartManifest == "" && facts.Final.ChartPackageDigest != "" {
		return false, 0, fmt.Errorf("final Chart package exists without a Chart manifest")
	}
	for name, digest := range facts.Final.Images {
		expected, ok := facts.Candidate.Images[name]
		if !ok || digest != expected.TopLevelDigest {
			return false, 0, fmt.Errorf("final image %q conflicts with the candidate", name)
		}
		count++
	}
	if facts.Final.ChartManifest != "" {
		if !digestPattern.MatchString(facts.Final.ChartManifest) || facts.Final.ChartPackageDigest != facts.Candidate.Chart.PackageDigest {
			return false, 0, fmt.Errorf("final Chart manifest digest is invalid")
		}
		count++
	}
	if facts.Final.GitTagCommit != "" {
		if facts.Final.GitTagCommit != facts.Candidate.SourceCommit {
			return false, 0, fmt.Errorf("final Git tag targets another commit")
		}
		count++
	}
	releaseAssetsComplete := false
	if len(facts.Final.GitHubReleaseAssets) > 0 {
		candidatePayloadDigest, err := ContentDigest(facts.Candidate)
		if err != nil {
			return false, 0, err
		}
		evidencePayloadDigest, err := ContentDigest(facts.Rehearsal)
		if err != nil {
			return false, 0, err
		}
		authorizationPayloadDigest, err := ContentDigest(facts.Authorization)
		if err != nil {
			return false, 0, err
		}
		expectedAssets := map[string]string{
			"candidate.json":     candidatePayloadDigest,
			"evidence.json":      evidencePayloadDigest,
			"authorization.json": authorizationPayloadDigest,
		}
		if len(facts.Final.GitHubReleaseAssets) > len(expectedAssets) {
			return false, 0, fmt.Errorf("GitHub Release contains unexpected assets")
		}
		for name, digest := range facts.Final.GitHubReleaseAssets {
			if expectedAssets[name] != digest {
				return false, 0, fmt.Errorf("GitHub Release asset %q conflicts with authorized records", name)
			}
		}
		releaseAssetsComplete = len(facts.Final.GitHubReleaseAssets) == len(expectedAssets)
		count++
	}
	want := len(facts.Candidate.Images) + 3
	return count == want && releaseAssetsComplete, count, nil
}

func hasFinalReferences(references FinalReferences) bool {
	return len(references.Images) > 0 || references.ChartManifest != "" || references.ChartPackageDigest != "" || references.GitTagCommit != "" || len(references.GitHubReleaseAssets) > 0
}
