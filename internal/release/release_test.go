package release

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEveryEffectiveDockerBaseMatchesImmutableInventory(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root = filepath.Clean(filepath.Join(root, "..", ".."))
	if err := VerifyBaseInventory(root); err != nil {
		t.Fatal(err)
	}
}

func TestNumericAlphaVersionExcludesHistoricalRCLine(t *testing.T) {
	version, err := ParseVersion("v0.1.0-alpha.1")
	if err != nil || version.Artifact != "0.1.0-alpha.1" || version.Sequence != 1 {
		t.Fatalf("version = %#v, %v", version, err)
	}
	for _, invalid := range []string{"0.1.0-alpha.1", "v0.1.0-alpha", "v0.1.0-alpha.0", "v0.1.0-alpha.rc19.6", "v0.1.1-alpha.1"} {
		if _, err := ParseVersion(invalid); err == nil {
			t.Fatalf("accepted invalid version %q", invalid)
		}
	}
}

func TestOCIArtifactUsesExactMediaTypesAndBytes(t *testing.T) {
	candidate := validCandidate(t)
	artifact, err := BuildJSONArtifact(CandidateType, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateOCIArtifact(artifact, CandidateType, CandidateType); err != nil {
		t.Fatal(err)
	}
	mutated := artifact
	mutated.Layer = append([]byte(nil), artifact.Layer...)
	mutated.Layer[0] ^= 1
	if err := ValidateOCIArtifact(mutated, CandidateType, CandidateType); err == nil {
		t.Fatal("accepted same-name artifact with different bytes")
	}

	chart := []byte("exact packaged chart")
	helmArtifact, err := BuildOCIArtifact(HelmCandidateType, HelmChartLayerType, chart)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateOCIArtifact(helmArtifact, HelmCandidateType, HelmChartLayerType); err != nil || !bytes.Equal(helmArtifact.Layer, chart) {
		t.Fatalf("Helm artifact = %v", err)
	}
}

func TestOCILayoutReplaysExactCandidateAndChartBytes(t *testing.T) {
	candidateArtifact, err := BuildJSONArtifact(CandidateType, validCandidate(t))
	if err != nil {
		t.Fatal(err)
	}
	chartArtifact, err := BuildOCIArtifact(HelmCandidateType, HelmChartLayerType, []byte("exact chart package bytes"))
	if err != nil {
		t.Fatal(err)
	}
	for name, artifact := range map[string]OCIArtifact{"candidate": candidateArtifact, "chart": chartArtifact} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			if err := WriteOCILayout(root, artifact); err != nil {
				t.Fatal(err)
			}
			replayed, err := ReadOCIArtifact(root, artifact.ManifestDigest)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(replayed.ManifestJSON, artifact.ManifestJSON) || !bytes.Equal(replayed.Layer, artifact.Layer) || replayed.ManifestDigest != artifact.ManifestDigest {
				t.Fatal("digest-addressed promotion changed candidate bytes")
			}
		})
	}
}

func TestReleaseStateReconstructsCrashPrefixesWithoutRebuild(t *testing.T) {
	now := time.Date(2026, 8, 29, 3, 0, 0, 0, time.UTC)
	facts := validFacts(t, now)
	wantStates := []State{StateAuthorized, StatePartiallyPromoted, StatePartiallyPromoted, StatePartiallyPromoted, StateReleased}
	for index, want := range wantStates {
		state, err := Reconstruct(facts, now)
		if err != nil || state != want {
			t.Fatalf("prefix %d state = %q, %v; want %q", index, state, err, want)
		}
		steps, err := PromotionPlan(facts, now)
		if err != nil {
			t.Fatal(err)
		}
		switch index {
		case 0:
			facts.Final.Images = map[string]string{"tetral": facts.Candidate.Images["tetral"].TopLevelDigest}
		case 1:
			for name, image := range facts.Candidate.Images {
				facts.Final.Images[name] = image.TopLevelDigest
			}
		case 2:
			facts.Final.ChartManifest = testDigest("released-chart-manifest")
		case 3:
			facts.Final.GitTagCommit = facts.Candidate.SourceCommit
			facts.Final.GitHubRelease = true
		case 4:
			if len(steps) != 0 {
				t.Fatalf("released plan = %v; want empty", steps)
			}
		}
	}

	conflict := facts
	conflict.Final.Images = cloneStrings(facts.Final.Images)
	conflict.Final.Images["tetral"] = testDigest("different-image")
	if _, err := Reconstruct(conflict, now); err == nil {
		t.Fatal("accepted a conflicting promoted image")
	}
}

func TestRehearsalEvidenceBindsCandidateCaseAndChart(t *testing.T) {
	now := time.Date(2026, 8, 29, 3, 0, 0, 0, time.UTC)
	facts := validFacts(t, now)
	if err := ValidateRehearsal(*facts.Candidate, facts.CandidateDigest, *facts.Rehearsal, now); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*RehearsalEvidence){
		"candidate": func(e *RehearsalEvidence) { e.CandidateDigest = testDigest("other-candidate") },
		"case":      func(e *RehearsalEvidence) { e.CaseManifestDigest = "invalid" },
		"result":    func(e *RehearsalEvidence) { e.Result = "fail" },
		"expired":   func(e *RehearsalEvidence) { e.FinishedAt = now.Add(-8 * 24 * time.Hour) },
		"render":    func(e *RehearsalEvidence) { e.RenderDigest = testDigest("other-render") },
	} {
		t.Run(name, func(t *testing.T) {
			copy := *facts.Rehearsal
			mutate(&copy)
			if err := ValidateRehearsal(*facts.Candidate, facts.CandidateDigest, copy, now); err == nil {
				t.Fatal("accepted mismatched rehearsal evidence")
			}
		})
	}
}

func TestCleanupSelectsOnlyOldNonPromotableCandidates(t *testing.T) {
	now := time.Date(2026, 8, 29, 3, 0, 0, 0, time.UTC)
	items := []CleanupCandidate{
		{Version: mustVersion(t, "v0.1.0-alpha.1"), State: StateCandidate, Digest: testDigest("one"), CreatedAt: now.Add(-31 * 24 * time.Hour)},
		{Version: mustVersion(t, "v0.1.0-alpha.2"), State: StateRehearsed, Digest: testDigest("two"), CreatedAt: now.Add(-40 * 24 * time.Hour)},
		{Version: mustVersion(t, "v0.1.0-alpha.3"), State: StateRevoked, Digest: testDigest("three"), CreatedAt: now.Add(-35 * 24 * time.Hour)},
		{Version: mustVersion(t, "v0.1.0-alpha.4"), State: StateCandidate, Digest: testDigest("four"), CreatedAt: now.Add(-2 * 24 * time.Hour)},
	}
	selected, err := CleanupPlan(items, now)
	if err != nil || len(selected) != 2 || selected[0].Version.Sequence != 1 || selected[1].Version.Sequence != 3 {
		t.Fatalf("cleanup = %#v, %v", selected, err)
	}
}

func TestReleaseEnvironmentBundleHasExactMainAndRestorablePreState(t *testing.T) {
	pre := EnvironmentSnapshot{
		Policy:      EnvironmentPolicy{CanAdminsBypass: true},
		SecretNames: []string{"DAYTONA_API_KEY"}, VariableNames: []string{"DAYTONA_TARGET"},
	}
	bundle, err := BuildEnvironmentBundle("tetral-ai/tetral", EnvironmentReviewer{Type: "User", ID: 42}, pre)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyEnvironmentBundle(bundle); err != nil {
		t.Fatal(err)
	}
	mutated := bundle
	mutated.Mutation.Target.BranchPolicy.Branches = []string{"main", "feature"}
	if err := VerifyEnvironmentBundle(mutated); err == nil {
		t.Fatal("accepted a release environment that permits another branch")
	}
}

func TestPackagePreflightRejectsPrivateWrongLinkedOrMovingPackage(t *testing.T) {
	valid := PackageIdentity{
		Found: true,
		Name:  "tetral", Organization: "tetral-ai", Visibility: "public", LinkedRepositoryID: 7,
		ActionsRepositoryIDs: []int64{7}, RepositoryTokenCanRead: true, RepositoryTokenCanWrite: true,
	}
	notFound := PackageIdentity{
		Name: "new-package", Organization: "tetral-ai", CreationAuthorized: true,
		RepositoryTokenCanRead: true, RepositoryTokenCanWrite: true,
	}
	if err := ValidatePackagePreflight(notFound, "tetral-ai", 7, true); err != nil {
		t.Fatal(err)
	}
	notFound.Visibility = "public"
	if err := ValidatePackagePreflight(notFound, "tetral-ai", 7, true); err == nil {
		t.Fatal("accepted fabricated live identity for a not-found package")
	}
	if err := ValidatePackagePreflight(valid, "tetral-ai", 7, true); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*PackageIdentity){
		"private": func(p *PackageIdentity) { p.Visibility = "private" },
		"link":    func(p *PackageIdentity) { p.LinkedRepositoryID = 8 },
		"access":  func(p *PackageIdentity) { p.ActionsRepositoryIDs = nil },
		"latest":  func(p *PackageIdentity) { p.ExistingReferences = []string{"latest"} },
	} {
		t.Run(name, func(t *testing.T) {
			copy := valid
			mutate(&copy)
			if err := ValidatePackagePreflight(copy, "tetral-ai", 7, true); err == nil {
				t.Fatal("accepted invalid package preflight")
			}
		})
	}
}

func validFacts(t *testing.T, now time.Time) Facts {
	t.Helper()
	candidate := validCandidate(t)
	candidateDigest, _ := ContentDigest(candidate)
	evidence := RehearsalEvidence{
		Schema: RehearsalSchema, Version: candidate.Version, SourceCommit: candidate.SourceCommit,
		CandidateDigest: candidateDigest, CaseManifestDigest: testDigest("case-manifest"), CaseCount: 64,
		LocalEvidenceDigest: testDigest("local-evidence"), ValuesDigest: candidate.Chart.ValuesDigest,
		RenderDigest: candidate.Chart.RenderDigest, Result: "pass", WorkflowRunID: 42, WorkflowRunAttempt: 1,
		DeploymentID: 7, StartedAt: now.Add(-time.Hour), FinishedAt: now.Add(-30 * time.Minute), RecordedAt: now.Add(-20 * time.Minute),
	}
	evidenceDigest, _ := ContentDigest(evidence)
	authorization := Authorization{
		Schema: AuthorizationSchema, Version: candidate.Version, SourceCommit: candidate.SourceCommit,
		CandidateDigest: candidateDigest, EvidenceDigest: evidenceDigest, WorkflowRunID: 43,
		WorkflowRunAttempt: 1, DeploymentID: 8, AuthorizedAt: now.Add(-10 * time.Minute),
	}
	authorizationDigest, _ := ContentDigest(authorization)
	return Facts{
		Reservation: &Reservation{Schema: ReservationSchema, Version: candidate.Version, SourceCommit: candidate.SourceCommit, CreatedAt: now.Add(-2 * time.Hour)},
		Candidate:   &candidate, CandidateDigest: candidateDigest, Rehearsal: &evidence, RehearsalDigest: evidenceDigest,
		Authorization: &authorization, AuthorizationDigest: authorizationDigest, Final: FinalReferences{Images: map[string]string{}},
	}
}

func validCandidate(t *testing.T) CandidateManifest {
	t.Helper()
	version := mustVersion(t, "v0.1.0-alpha.1")
	images := map[string]ImageIdentity{}
	for _, name := range []string{"tetral", "gateway", "agent-runtime", "sandbox"} {
		images[name] = ImageIdentity{
			Repository: "ghcr.io/tetral-ai/" + name, TopLevelDigest: testDigest(name + "-top"),
			TopLevelMedia: OCIManifestMediaType, ChildDigest: testDigest(name + "-child"),
			ChildMedia: OCIManifestMediaType, Platform: Platform{OS: "linux", Architecture: "amd64"},
		}
	}
	return CandidateManifest{
		Schema: CandidateSchema, Version: version, SourceCommit: "0123456789abcdef0123456789abcdef01234567", Platform: PlatformLinuxAMD64,
		Images: images, Chart: ChartIdentity{PackageDigest: testDigest("chart"), RenderDigest: testDigest("render"), ValuesDigest: testDigest("values")},
		SchemaVersion: 1, SchemaChecksum: testDigest("schema"), CreatedAt: time.Date(2026, 8, 29, 1, 0, 0, 0, time.UTC),
		Bases: []BaseIdentity{{Reference: "docker.io/library/golang:1.25.13-alpine", TopLevelDigest: testDigest("base-top"), ChildDigest: testDigest("base-child"), Platform: Platform{OS: "linux", Architecture: "amd64"}}},
	}
}

func mustVersion(t *testing.T, value string) Version {
	t.Helper()
	version, err := ParseVersion(value)
	if err != nil {
		t.Fatal(err)
	}
	return version
}

func testDigest(value string) string {
	digest, _ := ContentDigest(value)
	return digest
}

func cloneStrings(input map[string]string) map[string]string {
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}
