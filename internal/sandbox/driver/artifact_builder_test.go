package driver

import (
	"context"
	"strings"
	"testing"

	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"

	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestDaytonaArtifactBuilderCreatesSnapshotFromDeterministicDockerfile(t *testing.T) {
	client := &recordingSnapshotCreator{snapshot: &types.Snapshot{ID: "snapshot_provider_ref", Name: "snapshot_name"}}
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
	if !strings.HasPrefix(client.params.Name, "tetral-ws-test-env-test-7-1234567890ab") {
		t.Fatalf("snapshot name = %q; want deterministic identity/hash prefix", client.params.Name)
	}
	image, ok := client.params.Image.(interface{ Dockerfile() string })
	if !ok {
		t.Fatalf("snapshot image = %T; want Dockerfile-backed image", client.params.Image)
	}
	dockerfile := image.Dockerfile()
	want := "FROM ghcr.io/tetral-ai/sandbox:0.1.0-alpha\n" +
		"RUN apt-get update && apt-get install -y --no-install-recommends 'build-essential' 'git' && rm -rf /var/lib/apt/lists/*\n" +
		"RUN python -m pip install --no-cache-dir 'numpy==1.26.0' 'pandas==2.2.0'\n"
	if dockerfile != want {
		t.Fatalf("dockerfile:\n%s\nwant:\n%s", dockerfile, want)
	}
}

func TestDaytonaArtifactBuilderRejectsUnsupportedPackageManagerBeforeProviderCall(t *testing.T) {
	client := &recordingSnapshotCreator{snapshot: &types.Snapshot{ID: "snapshot_provider_ref"}}
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
	params   *types.CreateSnapshotParams
	snapshot *types.Snapshot
	err      error
}

func (c *recordingSnapshotCreator) Create(_ context.Context, params *types.CreateSnapshotParams) (*types.Snapshot, <-chan string, error) {
	c.params = params
	logs := make(chan string)
	close(logs)
	return c.snapshot, logs, c.err
}
