package driver

import (
	"context"
	"net/http"
	"strings"
	"testing"

	daytonaerrors "github.com/daytonaio/daytona/libs/sdk-go/pkg/errors"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"

	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestDaytonaArtifactBuilderCreatesSnapshotFromDeterministicDockerfile(t *testing.T) {
	client := &recordingSnapshotCreator{
		getErr:   daytonaerrors.NewDaytonaNotFoundError("snapshot not found", http.Header{}),
		snapshot: &types.Snapshot{ID: "snapshot_provider_ref", State: "active"},
	}
	builder := NewDaytonaArtifactBuilderForClient(client, "ghcr.io/tetral-ai/sandbox:0.1.0-alpha")

	result, err := builder.BuildArtifact(context.Background(), sandbox.BuildArtifactRequest{
		WorkspaceID:       workspace.ID("ws_test"),
		EnvironmentID:     "env_test",
		Generation:        7,
		ArtifactInputHash: "1234567890abcdef",
		NormalizedPackages: sandbox.PackageSetup{
			"pip": []string{"pandas==2.2.0", "numpy==1.26.0"},
			"apt": []string{"git", "build-essential"},
		},
		AuthorizeProviderCreate: func(context.Context) (bool, error) { return true, nil },
	})
	if err != nil {
		t.Fatalf("BuildArtifact: %v", err)
	}
	if result.ProviderArtifactRef != "snapshot_provider_ref" {
		t.Fatalf("ProviderArtifactRef = %q; want Daytona snapshot id", result.ProviderArtifactRef)
	}
	if client.params == nil {
		t.Fatal("snapshot create params missing")
	}
	if !strings.HasPrefix(client.params.Name, "tetral-") || len(client.params.Name) > 63 {
		t.Fatalf("snapshot name = %q; want bounded deterministic name", client.params.Name)
	}
	image, ok := client.params.Image.(interface{ Dockerfile() string })
	if !ok {
		t.Fatalf("snapshot image = %T; want Dockerfile-backed image", client.params.Image)
	}
	dockerfile := image.Dockerfile()
	want := "FROM ghcr.io/tetral-ai/sandbox:0.1.0-alpha\n" +
		"USER root\n" +
		"RUN apt-get update && apt-get install -y --no-install-recommends 'build-essential' 'git' && rm -rf /var/lib/apt/lists/*\n" +
		"RUN python -m pip install --no-cache-dir 'numpy==1.26.0' 'pandas==2.2.0'\n" +
		"USER daytona\n"
	if dockerfile != want {
		t.Fatalf("dockerfile:\n%s\nwant:\n%s", dockerfile, want)
	}
}

func TestDaytonaArtifactBuilderRejectsActiveSnapshotWithoutProviderID(t *testing.T) {
	request := sandbox.BuildArtifactRequest{
		WorkspaceID: workspace.ID("ws_test"), EnvironmentID: "env_test", Generation: 7,
		ArtifactInputHash: "1234567890abcdef", NormalizedPackages: sandbox.PackageSetup{"apt": []string{"git"}},
	}
	client := &recordingSnapshotCreator{existing: &types.Snapshot{
		Name: deterministicSnapshotName(request), State: "active",
	}}
	builder := NewDaytonaArtifactBuilderForClient(client, "ghcr.io/tetral-ai/sandbox:0.1.0-alpha")

	_, err := builder.BuildArtifact(context.Background(), request)
	providerErr, ok := err.(*sandbox.ProviderError)
	if !ok || providerErr.Kind != sandbox.ProviderErrorMalformedResponse {
		t.Fatalf("BuildArtifact error = %#v; want malformed response for missing Snapshot id", err)
	}
}

func TestDaytonaArtifactBuilderWaitsForCreatedSnapshotToBecomeActive(t *testing.T) {
	client := &recordingSnapshotCreator{
		getErr:   daytonaerrors.NewDaytonaNotFoundError("snapshot not found", http.Header{}),
		snapshot: &types.Snapshot{ID: "snapshot_building", State: "building"},
	}
	builder := NewDaytonaArtifactBuilderForClient(client, "ghcr.io/tetral-ai/sandbox:0.1.0-alpha")

	_, err := builder.BuildArtifact(context.Background(), sandbox.BuildArtifactRequest{
		WorkspaceID: workspace.ID("ws_test"), EnvironmentID: "env_test", Generation: 7,
		ArtifactInputHash: "1234567890abcdef", NormalizedPackages: sandbox.PackageSetup{"apt": []string{"git"}},
		AuthorizeProviderCreate: func(context.Context) (bool, error) { return true, nil },
	})
	providerErr, ok := err.(*sandbox.ProviderError)
	if !ok || !providerErr.Retryable {
		t.Fatalf("BuildArtifact error = %#v; want retryable build observation", err)
	}
}

func TestDaytonaArtifactBuilderDoesNotResubmitAmbiguousCreate(t *testing.T) {
	client := &recordingSnapshotCreator{
		getErr: daytonaerrors.NewDaytonaNotFoundError("snapshot not found", http.Header{}),
		err:    context.DeadlineExceeded,
	}
	authorized := false
	authorize := func(context.Context) (bool, error) {
		if authorized {
			return false, nil
		}
		authorized = true
		return true, nil
	}
	builder := NewDaytonaArtifactBuilderForClient(client, "ghcr.io/tetral-ai/sandbox:0.1.0-alpha")
	request := sandbox.BuildArtifactRequest{
		WorkspaceID: workspace.ID("ws_test"), EnvironmentID: "env_test", Generation: 7,
		ArtifactInputHash: "1234567890abcdef", NormalizedPackages: sandbox.PackageSetup{"apt": []string{"git"}},
		AuthorizeProviderCreate: authorize,
	}
	if _, err := builder.BuildArtifact(context.Background(), request); err == nil {
		t.Fatal("first ambiguous Create succeeded")
	}
	if _, err := builder.BuildArtifact(context.Background(), request); err == nil {
		t.Fatal("observation after ambiguous Create succeeded before Snapshot became visible")
	}
	if client.createCalls != 1 {
		t.Fatalf("Snapshot Create calls = %d; want exactly 1", client.createCalls)
	}
}

func TestDaytonaArtifactBuilderMarksRateLimitRejectionAsNotSubmitted(t *testing.T) {
	client := &recordingSnapshotCreator{
		getErr: daytonaerrors.NewDaytonaNotFoundError("snapshot not found", http.Header{}),
		err:    daytonaerrors.NewDaytonaRateLimitError("busy", nil),
	}
	builder := NewDaytonaArtifactBuilderForClient(client, "ghcr.io/tetral-ai/sandbox:0.1.0-alpha")
	request := sandbox.BuildArtifactRequest{
		WorkspaceID: workspace.ID("ws_test"), EnvironmentID: "env_test", Generation: 7,
		ArtifactInputHash: "1234567890abcdef", NormalizedPackages: sandbox.PackageSetup{"apt": []string{"git"}},
		AuthorizeProviderCreate: func(context.Context) (bool, error) { return true, nil },
	}
	_, err := builder.BuildArtifact(context.Background(), request)
	if err == nil {
		t.Fatal("BuildArtifact succeeded despite provider rate limit")
	}
	if !ProviderOperationWasNotSubmitted(err) {
		t.Fatalf("BuildArtifact error = %T %v; want not-submitted marker", err, err)
	}
}

func TestDeterministicSnapshotNamePreservesIdentityForLongInputs(t *testing.T) {
	base := sandbox.BuildArtifactRequest{
		WorkspaceID: workspace.ID(strings.Repeat("w", 128)), EnvironmentID: "env_" + strings.Repeat("e", 96),
		Generation: 7, ArtifactInputHash: strings.Repeat("a", 64),
	}
	otherGeneration := base
	otherGeneration.Generation++
	otherInput := base
	otherInput.ArtifactInputHash = strings.Repeat("b", 64)

	names := []string{deterministicSnapshotName(base), deterministicSnapshotName(otherGeneration), deterministicSnapshotName(otherInput)}
	seen := map[string]bool{}
	for _, name := range names {
		if len(name) > 63 {
			t.Fatalf("snapshot name length = %d; want <= 63", len(name))
		}
		if seen[name] {
			t.Fatalf("snapshot identity collision for %q", name)
		}
		seen[name] = true
	}
}

func TestDaytonaArtifactBuilderAdoptsExistingSnapshotAfterLostCreateResponse(t *testing.T) {
	request := sandbox.BuildArtifactRequest{
		WorkspaceID: workspace.ID("ws_test"), EnvironmentID: "env_test", Generation: 7,
		ArtifactInputHash: "1234567890abcdef", NormalizedPackages: sandbox.PackageSetup{"apt": []string{"git"}},
	}
	client := &recordingSnapshotCreator{existing: &types.Snapshot{
		ID: "existing_snapshot_ref", Name: deterministicSnapshotName(request), State: "active",
	}}
	builder := NewDaytonaArtifactBuilderForClient(client, "ghcr.io/tetral-ai/sandbox:0.1.0-alpha")

	result, err := builder.BuildArtifact(context.Background(), request)
	if err != nil {
		t.Fatalf("BuildArtifact: %v", err)
	}
	if result.ProviderArtifactRef != "existing_snapshot_ref" {
		t.Fatalf("ProviderArtifactRef = %q; want adopted Snapshot id", result.ProviderArtifactRef)
	}
	if client.params != nil {
		t.Fatalf("snapshot Create called after Get adopted an active Snapshot: %+v", client.params)
	}
}

func TestDaytonaArtifactBuilderObservesExistingBuildWithoutCreatingAgain(t *testing.T) {
	request := sandbox.BuildArtifactRequest{
		WorkspaceID: workspace.ID("ws_test"), EnvironmentID: "env_test", Generation: 7,
		ArtifactInputHash: "1234567890abcdef", NormalizedPackages: sandbox.PackageSetup{"apt": []string{"git"}},
	}
	client := &recordingSnapshotCreator{existing: &types.Snapshot{
		ID: "building_snapshot_ref", Name: deterministicSnapshotName(request), State: "building",
	}}
	builder := NewDaytonaArtifactBuilderForClient(client, "ghcr.io/tetral-ai/sandbox:0.1.0-alpha")

	_, err := builder.BuildArtifact(context.Background(), request)
	providerErr, ok := err.(*sandbox.ProviderError)
	if !ok || !providerErr.Retryable {
		t.Fatalf("BuildArtifact error = %#v; want retryable provider observation", err)
	}
	if client.params != nil {
		t.Fatalf("snapshot Create called while the stable Snapshot is building: %+v", client.params)
	}
}

func TestDaytonaArtifactBuilderRejectsUnsupportedPackageManagerBeforeProviderCall(t *testing.T) {
	client := &recordingSnapshotCreator{
		getErr:   daytonaerrors.NewDaytonaNotFoundError("snapshot not found", http.Header{}),
		snapshot: &types.Snapshot{ID: "snapshot_provider_ref", State: "active"},
	}
	builder := NewDaytonaArtifactBuilderForClient(client, "ghcr.io/tetral-ai/sandbox:0.1.0-alpha")

	_, err := builder.BuildArtifact(context.Background(), sandbox.BuildArtifactRequest{
		WorkspaceID:       workspace.ID("ws_test"),
		EnvironmentID:     "env_test",
		Generation:        7,
		ArtifactInputHash: "1234567890abcdef",
		NormalizedPackages: sandbox.PackageSetup{
			"brew": []string{"jq"},
		},
	})
	if err == nil {
		t.Fatal("BuildArtifact succeeded with unsupported package manager")
	}
	providerErr, ok := err.(*sandbox.ProviderError)
	if !ok {
		t.Fatalf("error = %T; want *sandbox.ProviderError", err)
	}
	if providerErr.Stage != sandbox.StageBuildArtifact || providerErr.Kind != sandbox.ProviderErrorInvalidRequest {
		t.Fatalf("provider error = stage %q kind %q; want build_artifact invalid_request", providerErr.Stage, providerErr.Kind)
	}
	if client.params != nil {
		t.Fatalf("snapshot create called for invalid packages: %+v", client.params)
	}
}

type recordingSnapshotCreator struct {
	params      *types.CreateSnapshotParams
	snapshot    *types.Snapshot
	err         error
	existing    *types.Snapshot
	getErr      error
	createCalls int
}

func (c *recordingSnapshotCreator) Get(_ context.Context, _ string) (*types.Snapshot, error) {
	return c.existing, c.getErr
}

func (c *recordingSnapshotCreator) Create(_ context.Context, params *types.CreateSnapshotParams) (*types.Snapshot, <-chan string, error) {
	c.createCalls++
	c.params = params
	if c.snapshot != nil && c.snapshot.Name == "" {
		c.snapshot.Name = params.Name
	}
	logs := make(chan string)
	close(logs)
	return c.snapshot, logs, c.err
}
