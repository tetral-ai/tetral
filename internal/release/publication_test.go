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

// These tests execute the publication scripts and release CLI. Only external
// GitHub/registry commands are replaced by a persistent remote fixture.
func TestPublicationScriptsResumeAfterRemoteWrites(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	binary := filepath.Join(bin, "tetral-release")
	if out, err := exec.Command("go", "build", "-o", binary, "./cmd/tetral-release").CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, out)
	}
	fixture, err := os.ReadFile("testdata/publication_remote.py")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"oras", "gh", "helm", "docker", "git"} {
		if err := os.WriteFile(filepath.Join(bin, name), fixture, 0o700); err != nil { //nolint:gosec // Fixed command names in a private test directory.
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte("#!/usr/bin/env bash\nshift 2\nexec \"$RELEASE_CLI\" \"$@\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, interruption := range []string{"", "authorization", "chart", "asset:candidate.json"} {
		name := interruption
		if name == "" {
			name = "uninterrupted"
		}
		t.Run(name, func(t *testing.T) {
			remote := t.TempDir()
			tags := map[string]string{}
			const repository = "ghcr.io/tetral-ai/tetral-release-metadata"
			candidate, _, report, now := reportFixtureAt(t, time.Now().UTC().Add(-time.Hour))
			packageBody := []byte("the exact rehearsed chart package")
			chart, err := BuildOCIArtifact(HelmCandidateType, HelmChartLayerType, packageBody)
			if err != nil {
				t.Fatal(err)
			}
			save := func(tag string, artifact OCIArtifact) string {
				t.Helper()
				if err := WriteOCILayout(filepath.Join(remote, strings.ReplaceAll(artifact.ManifestDigest, ":", "-")), artifact); err != nil {
					t.Fatal(err)
				}
				tags[repository+":"+tag] = artifact.ManifestDigest
				return artifact.ManifestDigest
			}
			record := func(tag, kind string, value any) string {
				t.Helper()
				artifact, err := BuildJSONArtifact(kind, value)
				if err != nil {
					t.Fatal(err)
				}
				return save(tag, artifact)
			}
			version := candidate.Version.Artifact
			candidate.Chart.CandidateManifestDigest = save("helm-candidate-"+version, chart)
			candidate.Chart.PackageDigest = digestBytes(packageBody)
			candidateDigest := record("candidate-"+version, CandidateType, candidate)
			record("reservation-"+version, ReservationType, Reservation{Schema: ReservationSchema, Version: candidate.Version, SourceCommit: candidate.SourceCommit, CreatedAt: now})
			report.Plan.CandidateDigest = candidateDigest
			setPlanDigest(t, &report)
			reportDigest, err := RehearsalReportArtifactDigest(report)
			if err != nil {
				t.Fatal(err)
			}
			evidence, err := RecordRehearsal(candidate, candidateDigest, report, reportDigest, RehearsalRecording{WorkflowRunID: 42, WorkflowRunAttempt: 1, DeploymentID: 11, RecordedAt: now})
			if err != nil {
				t.Fatal(err)
			}
			evidenceDigest := record("rehearsal-"+version, RehearsalType, evidence)
			statePath := filepath.Join(remote, "state.json")
			body, err := json.Marshal(map[string]any{"tags": tags, "assets": map[string]string{}})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(statePath, body, 0o600); err != nil {
				t.Fatal(err)
			}
			env := append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "RELEASE_CLI="+binary, "PUBLICATION_REMOTE="+remote,
				"RELEASE_METADATA_REPOSITORY="+repository, "GITHUB_REPOSITORY=tetral-ai/tetral", "GH_TOKEN=test",
				"VERSION="+version, "GIT_VERSION="+candidate.Version.Git, "SOURCE_COMMIT="+candidate.SourceCommit,
				"CANDIDATE_DIGEST="+candidateDigest, "EVIDENCE_DIGEST="+evidenceDigest, "WORKFLOW_SHA="+strings.Repeat("a", 40), "RUN_ID=42", "RUN_ATTEMPT=1", "ACTOR_ID=7")
			run := func(script string, arguments []string, overrides ...string) ([]byte, error) {
				command := exec.Command(filepath.Join(root, "scripts", script), arguments...) //nolint:gosec // Repository scripts; external commands use a private fixture.
				command.Dir = root
				command.Env = append(append([]string{}, env...), overrides...)
				return command.CombinedOutput()
			}
			// Wrong source must be rejected before the first authorization or tag write.
			if out, err := run("release-promote.sh", nil, "SOURCE_COMMIT="+strings.Repeat("b", 40)); err == nil {
				t.Fatalf("accepted wrong source: %s", out)
			}
			if _, err := os.Stat(filepath.Join(remote, "writes")); !os.IsNotExist(err) {
				t.Fatalf("wrong source made remote writes: %v", err)
			}
			if interruption != "" {
				if out, err := run("release-promote.sh", nil, "FAIL_AFTER="+interruption); err == nil {
					t.Fatalf("did not stop after %s: %s", interruption, out)
				}
			}
			if out, err := run("release-promote.sh", nil); err != nil {
				t.Fatalf("publication: %v\n%s", err, out)
			}
			writes, err := os.ReadFile(filepath.Join(remote, "writes"))
			if err != nil {
				t.Fatal(err)
			}
			if out, err := run("release-promote.sh", nil); err != nil {
				t.Fatalf("completed rerun: %v\n%s", err, out)
			}
			after, err := os.ReadFile(filepath.Join(remote, "writes"))
			if err != nil {
				t.Fatal(err)
			}
			if string(after) != string(writes) {
				t.Fatalf("completed rerun wrote again:\n%s", after)
			}
			for _, event := range []string{"authorization", "image:tetral", "image:gateway", "image:agent-runtime", "image:sandbox", "chart", "tag", "draft", "asset:candidate.json", "asset:evidence.json", "asset:authorization.json", "published"} {
				if strings.Count("\n"+string(writes), "\n"+event+"\n") != 1 {
					t.Fatalf("expected one %s write:\n%s", event, writes)
				}
			}
			body, err = os.ReadFile(statePath)
			if err != nil {
				t.Fatal(err)
			}
			var remoteState struct {
				Tag     string `json:"tag"`
				Release struct {
					Draft      bool `json:"isDraft"`
					Prerelease bool `json:"isPrerelease"`
				} `json:"release"`
				Assets map[string]string `json:"assets"`
			}
			if err := json.Unmarshal(body, &remoteState); err != nil {
				t.Fatal(err)
			}
			if remoteState.Tag != candidate.SourceCommit || remoteState.Release.Draft || !remoteState.Release.Prerelease {
				t.Fatalf("wrong publication: %+v", remoteState)
			}
			expected, _ := CanonicalJSON(candidate)
			if remoteState.Assets["candidate.json"] != string(expected) {
				t.Fatal("Release does not contain the original candidate bytes")
			}
			// A candidate finalizer must fetch its already persisted package, not package
			// again. The fixture would record a 'package' write on that branch.
			destination := t.TempDir()
			if out, err := run("release-candidate-chart.sh", []string{version, destination}); err != nil {
				t.Fatalf("resume candidate chart: %v\n%s", err, out)
			}
			replayed, err := os.ReadFile(filepath.Join(destination, "tetral-"+version+".tgz"))
			if err != nil {
				t.Fatal(err)
			}
			if string(replayed) != string(packageBody) {
				t.Fatal("candidate resume changed the package")
			}
		})
	}
}
