package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	stderrors "errors"
	"sort"
	"strconv"
	"strings"

	apiclient "github.com/daytonaio/daytona/libs/api-client-go"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/daytona"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"

	"github.com/tetral-ai/tetral/internal/sandbox"
)

type daytonaSnapshotService interface {
	Get(context.Context, string) (*types.Snapshot, error)
	Create(context.Context, *types.CreateSnapshotParams) (*types.Snapshot, <-chan string, error)
}

type DaytonaArtifactBuilder struct {
	snapshots daytonaSnapshotService
	baseImage string
}

func NewDaytonaArtifactBuilderForClient(snapshots daytonaSnapshotService, baseImage string) *DaytonaArtifactBuilder {
	return &DaytonaArtifactBuilder{snapshots: snapshots, baseImage: baseImage}
}

func (b *DaytonaArtifactBuilder) BuildArtifact(ctx context.Context, request sandbox.BuildArtifactRequest) (sandbox.BuildArtifactResult, error) {
	if b == nil || b.snapshots == nil {
		return sandbox.BuildArtifactResult{}, daytonaProviderError(sandbox.StageBuildArtifact, sandbox.ProviderErrorConfigInvalid, false, 0, "daytona artifact builder is unavailable", nil)
	}
	if b.baseImage == "" {
		return sandbox.BuildArtifactResult{}, daytonaProviderError(sandbox.StageBuildArtifact, sandbox.ProviderErrorConfigInvalid, false, 0, "artifact base image is not configured", nil)
	}
	if request.WorkspaceID == "" || request.EnvironmentID == "" || request.Generation <= 0 {
		return sandbox.BuildArtifactResult{}, daytonaProviderError(sandbox.StageBuildArtifact, sandbox.ProviderErrorInvalidRequest, false, 0, "environment artifact identity is required", nil)
	}
	if err := validateArtifactPackages(request.NormalizedPackages); err != nil {
		return sandbox.BuildArtifactResult{}, err
	}
	name := deterministicSnapshotName(request)
	existing, err := b.snapshots.Get(ctx, name)
	if err == nil {
		return adoptDaytonaSnapshot(existing, name)
	}
	mapped := mapDaytonaError(sandbox.StageBuildArtifact, err)
	var providerErr *sandbox.ProviderError
	if !stderrors.As(mapped, &providerErr) || providerErr.Kind != sandbox.ProviderErrorNotFound {
		return sandbox.BuildArtifactResult{}, mapped
	}
	if request.AuthorizeProviderCreate == nil {
		return sandbox.BuildArtifactResult{}, daytonaProviderError(sandbox.StageBuildArtifact, sandbox.ProviderErrorConfigInvalid, false, 0, "artifact create authorization is unavailable", nil)
	}
	authorized, err := request.AuthorizeProviderCreate(ctx)
	if err != nil {
		return sandbox.BuildArtifactResult{}, err
	}
	if !authorized {
		return sandbox.BuildArtifactResult{}, daytonaProviderError(sandbox.StageBuildArtifact, sandbox.ProviderErrorUnavailable, true, 0, "daytona snapshot create is awaiting provider visibility", nil)
	}
	dockerfile := deterministicArtifactDockerfile(b.baseImage, request.NormalizedPackages)
	snapshot, logs, err := b.snapshots.Create(ctx, &types.CreateSnapshotParams{
		Name:  name,
		Image: daytona.FromDockerfile(dockerfile),
	})
	drainAvailableSnapshotLogs(logs)
	if err != nil {
		mapped := mapDaytonaError(sandbox.StageBuildArtifact, err)
		if daytonaRequestWasRejected(err) {
			mapped = MarkProviderOperationNotSubmitted(mapped)
		}
		return sandbox.BuildArtifactResult{}, mapped
	}
	if snapshot == nil {
		return sandbox.BuildArtifactResult{}, daytonaProviderError(sandbox.StageBuildArtifact, sandbox.ProviderErrorUnknown, true, 0, "daytona snapshot create returned no snapshot", nil)
	}
	return adoptDaytonaSnapshot(snapshot, name)
}

func adoptDaytonaSnapshot(snapshot *types.Snapshot, expectedName string) (sandbox.BuildArtifactResult, error) {
	if snapshot == nil {
		return sandbox.BuildArtifactResult{}, daytonaProviderError(sandbox.StageBuildArtifact, sandbox.ProviderErrorMalformedResponse, false, 0, "daytona snapshot lookup returned no snapshot", nil)
	}
	if snapshot.Name != expectedName {
		return sandbox.BuildArtifactResult{}, daytonaProviderError(sandbox.StageBuildArtifact, sandbox.ProviderErrorConflict, false, 0, "daytona snapshot identity does not match", nil)
	}
	switch apiclient.SnapshotState(snapshot.State) {
	case apiclient.SNAPSHOTSTATE_ACTIVE:
		return daytonaSnapshotResult(snapshot)
	case apiclient.SNAPSHOTSTATE_PENDING, apiclient.SNAPSHOTSTATE_BUILDING, apiclient.SNAPSHOTSTATE_PULLING:
		return sandbox.BuildArtifactResult{}, daytonaProviderError(sandbox.StageBuildArtifact, sandbox.ProviderErrorUnavailable, true, 0, "daytona snapshot build is still in progress", nil)
	case apiclient.SNAPSHOTSTATE_ERROR, apiclient.SNAPSHOTSTATE_BUILD_FAILED:
		return sandbox.BuildArtifactResult{}, daytonaProviderError(sandbox.StageBuildArtifact, sandbox.ProviderErrorUnknown, false, 0, "daytona snapshot build failed", nil)
	default:
		return sandbox.BuildArtifactResult{}, daytonaProviderError(sandbox.StageBuildArtifact, sandbox.ProviderErrorMalformedResponse, false, 0, "daytona snapshot is not usable", nil)
	}
}

func daytonaSnapshotResult(snapshot *types.Snapshot) (sandbox.BuildArtifactResult, error) {
	if snapshot.ID == "" {
		return sandbox.BuildArtifactResult{}, daytonaProviderError(sandbox.StageBuildArtifact, sandbox.ProviderErrorMalformedResponse, false, 0, "daytona snapshot returned no provider id", nil)
	}
	return sandbox.BuildArtifactResult{ProviderArtifactRef: snapshot.ID}, nil
}

func drainAvailableSnapshotLogs(logs <-chan string) {
	if logs == nil {
		return
	}
	for {
		select {
		case _, ok := <-logs:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

func deterministicSnapshotName(request sandbox.BuildArtifactRequest) string {
	identity := strings.Join([]string{
		string(request.WorkspaceID),
		request.EnvironmentID,
		strconv.FormatInt(request.Generation, 10),
		request.ArtifactInputHash,
	}, "\x00")
	digest := sha256.Sum256([]byte(identity))
	return "tetral-" + hex.EncodeToString(digest[:])[:56]
}

func deterministicArtifactDockerfile(baseImage string, packages sandbox.PackageSetup) string {
	lines := []string{"FROM " + baseImage, "USER root"}
	managers := make([]string, 0, len(packages))
	for manager, entries := range packages {
		if len(entries) > 0 {
			managers = append(managers, manager)
		}
	}
	sort.Strings(managers)
	for _, manager := range managers {
		entries := append([]string(nil), packages[manager]...)
		sort.Strings(entries)
		if line := packageInstallLine(manager, entries); line != "" {
			lines = append(lines, line)
		}
	}
	lines = append(lines, "USER daytona")
	return strings.Join(lines, "\n") + "\n"
}

func packageInstallLine(manager string, packages []string) string {
	if len(packages) == 0 {
		return ""
	}
	quoted := make([]string, 0, len(packages))
	for _, pkg := range packages {
		quoted = append(quoted, shellQuote(pkg))
	}
	joined := strings.Join(quoted, " ")
	switch manager {
	case "apt":
		return "RUN apt-get update && apt-get install -y --no-install-recommends " + joined + " && rm -rf /var/lib/apt/lists/*"
	case "cargo":
		return "RUN cargo install " + joined
	case "gem":
		return "RUN gem install " + joined
	case "go":
		return "RUN go install " + joined
	case "npm":
		return "RUN npm install -g " + joined
	case "pip":
		return "RUN python -m pip install --no-cache-dir " + joined
	default:
		return ""
	}
}

func validateArtifactPackages(packages sandbox.PackageSetup) error {
	for manager := range packages {
		if !supportedArtifactPackageManager(manager) {
			return daytonaProviderError(sandbox.StageBuildArtifact, sandbox.ProviderErrorInvalidRequest, false, 0, "unsupported package manager "+manager, nil)
		}
	}
	return nil
}

func supportedArtifactPackageManager(manager string) bool {
	switch manager {
	case "apt", "cargo", "gem", "go", "npm", "pip":
		return true
	default:
		return false
	}
}
