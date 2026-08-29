package testinfra

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

type fixtureGHClient struct {
	json  map[string]any
	bytes map[string][]byte
}

func (client fixtureGHClient) JSON(_ context.Context, endpoint string, target any) error {
	value, exists := client.json[endpoint]
	if !exists {
		return fmt.Errorf("unexpected fixture endpoint %s", endpoint)
	}
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, target)
}

func (client fixtureGHClient) Bytes(_ context.Context, endpoint string) ([]byte, error) {
	value, exists := client.bytes[endpoint]
	if !exists {
		return nil, fmt.Errorf("unexpected fixture bytes endpoint %s", endpoint)
	}
	return value, nil
}

func TestCollectShadowSnapshotUsesAPICarriersAndArtifactEnvelopes(t *testing.T) {
	expected := validShadowSnapshot(t)
	client := shadowFixtureClient(t, expected)
	legacyMetadata, _, err := collectRunArtifacts(context.Background(), client, expected.Repository, expected.Legacy.RunID)
	if err != nil || len(legacyMetadata) != len(expected.LegacyMetadata) {
		t.Fatalf("legacy artifact fixture = %d/%v", len(legacyMetadata), err)
	}
	_, shadowResults, err := collectRunArtifacts(context.Background(), client, expected.Repository, expected.Shadow.RunID)
	if err != nil || len(shadowResults) != len(expected.ShadowResults) {
		t.Fatalf("shadow artifact fixture = %d/%v", len(shadowResults), err)
	}
	actual, err := collectShadowSnapshot(context.Background(), client, expected.Repository, expected.PullRequest)
	if err != nil {
		t.Fatal(err)
	}
	row, err := NormalizeShadowSnapshot(actual)
	if err != nil {
		t.Fatal(err)
	}
	if row.LegacyExecution.WorkflowBlobSHA != "legacy-blob" || row.ShadowExecution.WorkflowBlobSHA != "shadow-blob" || row.SnapshotDigest == "" {
		t.Fatalf("collector omitted immutable source identity: %+v", row)
	}
}

func TestCollectShadowSnapshotRejectsMissingSourceBlobIdentity(t *testing.T) {
	expected := validShadowSnapshot(t)
	client := shadowFixtureClient(t, expected)
	endpoint := "repos/tetral-ai/tetral/contents/.github/workflows/pull-request-verification.yml?ref=source"
	client.json[endpoint] = map[string]string{"sha": ""}
	if _, err := collectShadowSnapshot(context.Background(), client, expected.Repository, expected.PullRequest); err == nil {
		t.Fatal("missing workflow blob identity passed")
	}
}

func shadowFixtureClient(t *testing.T, snapshot ShadowSnapshot) fixtureGHClient {
	t.Helper()
	legacyRun := githubWorkflowRun{ID: snapshot.Legacy.RunID, Name: snapshot.Legacy.Name, Path: snapshot.Legacy.Path, HeadSHA: snapshot.EventHeadSHA, RunAttempt: snapshot.Legacy.RunAttempt, CheckSuiteID: snapshot.Legacy.CheckSuiteID, Status: "completed", Conclusion: "success", CreatedAt: snapshot.Legacy.CreatedAt, RunStartedAt: snapshot.Legacy.StartedAt, UpdatedAt: snapshot.Legacy.CompletedAt}
	shadowRun := githubWorkflowRun{ID: snapshot.Shadow.RunID, Name: snapshot.Shadow.Name, Path: snapshot.Shadow.Path, HeadSHA: snapshot.EventHeadSHA, RunAttempt: snapshot.Shadow.RunAttempt, CheckSuiteID: snapshot.Shadow.CheckSuiteID, Status: "completed", Conclusion: "success", CreatedAt: snapshot.Shadow.CreatedAt, RunStartedAt: snapshot.Shadow.StartedAt, UpdatedAt: snapshot.Shadow.CompletedAt}
	client := fixtureGHClient{json: map[string]any{}, bytes: map[string][]byte{}}
	client.json["repos/tetral-ai/tetral/pulls/101"] = map[string]any{"head": map[string]string{"sha": snapshot.EventHeadSHA}, "base": map[string]string{"sha": snapshot.EventBaseSHA}}
	client.json["repos/tetral-ai/tetral/actions/runs?event=pull_request&head_sha=head&per_page=100"] = map[string]any{"workflow_runs": []githubWorkflowRun{legacyRun, shadowRun}}
	addWorkflowFixture(t, &client, snapshot.Repository, snapshot.Legacy, snapshot.LegacyMetadata, nil, 11)
	addWorkflowFixture(t, &client, snapshot.Repository, snapshot.Shadow, nil, snapshot.ShadowResults, 12)
	client.json["repos/tetral-ai/tetral/contents/.github/workflows/engine-ci.yml?ref=source"] = map[string]string{"sha": "legacy-blob"}
	client.json["repos/tetral-ai/tetral/contents/.github/workflows/pull-request-verification.yml?ref=source"] = map[string]string{"sha": "shadow-blob"}
	return client
}

func addWorkflowFixture(t *testing.T, client *fixtureGHClient, repository string, execution ShadowWorkflowExecution, metadata []LegacyWorkflowMetadata, results []Result, artifactID int64) {
	t.Helper()
	client.json[fmt.Sprintf("repos/%s/actions/runs/%d/attempts/%d/jobs?per_page=100", repository, execution.RunID, execution.RunAttempt)] = map[string]any{"jobs": execution.Jobs}
	client.json[fmt.Sprintf("repos/%s/check-suites/%d", repository, execution.CheckSuiteID)] = map[string]any{"head_sha": execution.CheckHeadSHA, "app": map[string]int64{"id": execution.SourceAppID}}
	checks := make([]map[string]any, 0, len(execution.Checks))
	for _, check := range execution.Checks {
		checks = append(checks, map[string]any{"id": check.ID, "name": check.Name, "head_sha": check.HeadSHA, "status": check.Status, "conclusion": check.Conclusion, "app": map[string]int64{"id": check.AppID}})
	}
	client.json[fmt.Sprintf("repos/%s/check-suites/%d/check-runs?per_page=100", repository, execution.CheckSuiteID)] = map[string]any{"check_runs": checks}
	artifactName := "legacy-metadata-fixture"
	if len(results) > 0 {
		artifactName = "pr-evidence-fixture"
	}
	client.json[fmt.Sprintf("repos/%s/actions/runs/%d/artifacts?per_page=100", repository, execution.RunID)] = map[string]any{"artifacts": []map[string]any{{"id": artifactID, "name": artifactName, "expired": false}}}
	client.bytes[fmt.Sprintf("repos/%s/actions/artifacts/%d/zip", repository, artifactID)] = shadowFixtureArchive(t, metadata, results)
}

func shadowFixtureArchive(t *testing.T, metadata []LegacyWorkflowMetadata, results []Result) []byte {
	t.Helper()
	var output bytes.Buffer
	archive := zip.NewWriter(&output)
	for index, value := range metadata {
		writeShadowFixtureJSON(t, archive, fmt.Sprintf("metadata-%d/metadata.json", index), value)
	}
	for index, value := range results {
		writeShadowFixtureJSON(t, archive, fmt.Sprintf("result-%d/result.json", index), value)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func writeShadowFixtureJSON(t *testing.T, archive *zip.Writer, name string, value any) {
	t.Helper()
	writer, err := archive.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		t.Fatal(err)
	}
}
