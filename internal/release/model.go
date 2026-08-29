// Package release owns immutable release identities and transitions. It does
// not publish artifacts; workflows adapt validated records to Git, GitHub, and
// OCI registries after the protected release boundary authorizes a transition.
package release

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	ReservationSchema   = "tetral.release-reservation/v1"
	CandidateSchema     = "tetral.release-candidate/v1"
	RehearsalSchema     = "tetral.rehearsal-evidence/v1"
	AuthorizationSchema = "tetral.release-authorization/v1"
	DispositionSchema   = "tetral.release-disposition/v1"
	PlatformLinuxAMD64  = "linux/amd64"
)

var (
	alphaVersionPattern = regexp.MustCompile(`^v0\.1\.0-alpha\.([1-9][0-9]*)$`)
	digestPattern       = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	commitPattern       = regexp.MustCompile(`^[0-9a-f]{40}$`)
)

type Version struct {
	Git      string `json:"git"`
	Artifact string `json:"artifact"`
	Sequence int    `json:"sequence"`
}

func ParseVersion(value string) (Version, error) {
	match := alphaVersionPattern.FindStringSubmatch(value)
	if match == nil {
		return Version{}, fmt.Errorf("release version %q must match v0.1.0-alpha.N", value)
	}
	var sequence int
	if _, err := fmt.Sscanf(match[1], "%d", &sequence); err != nil || sequence < 1 {
		return Version{}, fmt.Errorf("release version %q has an invalid sequence", value)
	}
	return Version{Git: value, Artifact: strings.TrimPrefix(value, "v"), Sequence: sequence}, nil
}

type Platform struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
}

type ImageIdentity struct {
	Repository     string   `json:"repository"`
	TopLevelDigest string   `json:"top_level_digest"`
	TopLevelMedia  string   `json:"top_level_media_type"`
	ChildDigest    string   `json:"linux_amd64_child_digest"`
	ChildMedia     string   `json:"linux_amd64_child_media_type"`
	Platform       Platform `json:"platform"`
}

type ChartIdentity struct {
	CandidateManifestDigest string `json:"candidate_manifest_digest"`
	PackageDigest           string `json:"package_sha256"`
	RenderDigest            string `json:"render_sha256"`
	ValuesDigest            string `json:"values_sha256"`
}

type BaseIdentity struct {
	Reference      string   `json:"reference"`
	TopLevelDigest string   `json:"top_level_digest"`
	ChildDigest    string   `json:"linux_amd64_child_digest"`
	Platform       Platform `json:"platform"`
}

type Reservation struct {
	Schema       string    `json:"schema"`
	Version      Version   `json:"version"`
	SourceCommit string    `json:"source_commit"`
	CreatedAt    time.Time `json:"created_at"`
}

type CandidateManifest struct {
	Schema         string                   `json:"schema"`
	Version        Version                  `json:"version"`
	SourceCommit   string                   `json:"source_commit"`
	Platform       string                   `json:"platform"`
	Images         map[string]ImageIdentity `json:"images"`
	Chart          ChartIdentity            `json:"chart"`
	SchemaVersion  int                      `json:"database_schema_version"`
	SchemaChecksum string                   `json:"database_schema_checksum"`
	Bases          []BaseIdentity           `json:"base_images"`
	CreatedAt      time.Time                `json:"created_at"`
}

type RehearsalEvidence struct {
	Schema              string    `json:"schema"`
	Version             Version   `json:"version"`
	SourceCommit        string    `json:"source_commit"`
	CandidateDigest     string    `json:"candidate_digest"`
	CaseManifestDigest  string    `json:"case_manifest_digest"`
	CaseCount           int       `json:"case_count"`
	LocalEvidenceDigest string    `json:"local_evidence_digest"`
	ValuesDigest        string    `json:"values_digest"`
	RenderDigest        string    `json:"render_digest"`
	Result              string    `json:"result"`
	WorkflowRunID       int64     `json:"workflow_run_id"`
	WorkflowRunAttempt  int       `json:"workflow_run_attempt"`
	DeploymentID        int64     `json:"deployment_id"`
	StartedAt           time.Time `json:"started_at"`
	FinishedAt          time.Time `json:"finished_at"`
	RecordedAt          time.Time `json:"recorded_at"`
}

type Authorization struct {
	Schema             string    `json:"schema"`
	Version            Version   `json:"version"`
	SourceCommit       string    `json:"source_commit"`
	CandidateDigest    string    `json:"candidate_digest"`
	EvidenceDigest     string    `json:"evidence_digest"`
	WorkflowRunID      int64     `json:"workflow_run_id"`
	WorkflowRunAttempt int       `json:"workflow_run_attempt"`
	DeploymentID       int64     `json:"deployment_id"`
	AuthorizedAt       time.Time `json:"authorized_at"`
}

type Disposition struct {
	Schema          string    `json:"schema"`
	Version         Version   `json:"version"`
	CandidateDigest string    `json:"candidate_digest"`
	Kind            string    `json:"kind"`
	RecordedAt      time.Time `json:"recorded_at"`
}

func ValidateCandidate(candidate CandidateManifest) error {
	if candidate.Schema != CandidateSchema || candidate.Platform != PlatformLinuxAMD64 || !commitPattern.MatchString(candidate.SourceCommit) {
		return fmt.Errorf("candidate identity is incomplete")
	}
	parsed, err := ParseVersion(candidate.Version.Git)
	if err != nil || parsed != candidate.Version {
		return fmt.Errorf("candidate version is invalid")
	}
	if candidate.SchemaVersion != 1 || !digestPattern.MatchString(candidate.SchemaChecksum) {
		return fmt.Errorf("candidate database identity is invalid")
	}
	for _, name := range []string{"tetral", "gateway", "agent-runtime", "sandbox"} {
		image, ok := candidate.Images[name]
		if !ok {
			return fmt.Errorf("candidate is missing image %q", name)
		}
		if err := ValidateImageIdentity(image); err != nil {
			return fmt.Errorf("candidate image %q: %w", name, err)
		}
	}
	if len(candidate.Images) != 4 || !digestPattern.MatchString(candidate.Chart.CandidateManifestDigest) || !digestPattern.MatchString(candidate.Chart.PackageDigest) || !digestPattern.MatchString(candidate.Chart.RenderDigest) || !digestPattern.MatchString(candidate.Chart.ValuesDigest) {
		return fmt.Errorf("candidate Chart identity is invalid")
	}
	if len(candidate.Bases) == 0 {
		return fmt.Errorf("candidate has no base-image identity")
	}
	for _, base := range candidate.Bases {
		if base.Reference == "" || !digestPattern.MatchString(base.TopLevelDigest) || !digestPattern.MatchString(base.ChildDigest) || base.Platform != (Platform{OS: "linux", Architecture: "amd64"}) {
			return fmt.Errorf("candidate base-image identity is invalid")
		}
	}
	return nil
}

func ValidateImageIdentity(image ImageIdentity) error {
	if image.Repository == "" || !digestPattern.MatchString(image.TopLevelDigest) || !digestPattern.MatchString(image.ChildDigest) || image.TopLevelMedia == "" || image.ChildMedia == "" {
		return fmt.Errorf("image digest or media identity is incomplete")
	}
	if image.Platform != (Platform{OS: "linux", Architecture: "amd64"}) {
		return fmt.Errorf("image platform must be linux/amd64")
	}
	return nil
}

func ValidateRehearsal(candidate CandidateManifest, candidateDigest string, evidence RehearsalEvidence, now time.Time) error {
	if err := ValidateCandidate(candidate); err != nil {
		return err
	}
	if evidence.Schema != RehearsalSchema || evidence.Version != candidate.Version || evidence.SourceCommit != candidate.SourceCommit || evidence.CandidateDigest != candidateDigest {
		return fmt.Errorf("rehearsal does not identify the candidate")
	}
	for _, digest := range []string{candidateDigest, evidence.CaseManifestDigest, evidence.LocalEvidenceDigest, evidence.ValuesDigest, evidence.RenderDigest} {
		if !digestPattern.MatchString(digest) {
			return fmt.Errorf("rehearsal contains an invalid digest")
		}
	}
	if evidence.CaseCount < 1 || evidence.Result != "pass" || evidence.WorkflowRunID < 1 || evidence.WorkflowRunAttempt < 1 || evidence.DeploymentID < 1 || evidence.StartedAt.IsZero() || !evidence.FinishedAt.After(evidence.StartedAt) || evidence.RecordedAt.Before(evidence.FinishedAt) || evidence.FinishedAt.After(now) || now.Sub(evidence.FinishedAt) > 7*24*time.Hour {
		return fmt.Errorf("rehearsal result or time window is invalid")
	}
	if evidence.ValuesDigest != candidate.Chart.ValuesDigest || evidence.RenderDigest != candidate.Chart.RenderDigest {
		return fmt.Errorf("rehearsal Chart render differs from the candidate")
	}
	return nil
}

func ValidateAuthorization(candidate CandidateManifest, candidateDigest, evidenceDigest string, authorization Authorization) error {
	if err := ValidateCandidate(candidate); err != nil {
		return err
	}
	if authorization.Schema != AuthorizationSchema || authorization.Version != candidate.Version || authorization.SourceCommit != candidate.SourceCommit || authorization.CandidateDigest != candidateDigest || authorization.EvidenceDigest != evidenceDigest {
		return fmt.Errorf("authorization does not identify the rehearsed candidate")
	}
	if !digestPattern.MatchString(candidateDigest) || !digestPattern.MatchString(evidenceDigest) || authorization.WorkflowRunID < 1 || authorization.WorkflowRunAttempt < 1 || authorization.DeploymentID < 1 || authorization.AuthorizedAt.IsZero() {
		return fmt.Errorf("authorization identity is incomplete")
	}
	return nil
}

func CanonicalJSON(value any) ([]byte, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}
	return json.Marshal(decoded)
}

func ContentDigest(value any) (string, error) {
	body, err := CanonicalJSON(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func SortedImageNames(images map[string]ImageIdentity) []string {
	names := make([]string, 0, len(images))
	for name := range images {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
