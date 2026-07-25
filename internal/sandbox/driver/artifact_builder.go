package driver

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/daytonaio/daytona/libs/sdk-go/pkg/daytona"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"

	"github.com/tetral-ai/tetral/internal/sandbox"
)

type daytonaSnapshotCreator interface {
	Create(context.Context, *types.CreateSnapshotParams) (*types.Snapshot, <-chan string, error)
}

type DaytonaArtifactBuilder struct {
	snapshots daytonaSnapshotCreator
	baseImage string
}

func NewDaytonaArtifactBuilder(cfg Config) (*DaytonaArtifactBuilder, error) {
	client, err := daytona.NewClientWithConfig(&types.DaytonaConfig{
		APIKey: cfg.DaytonaAPIKey,
		APIUrl: cfg.DaytonaAPIURL,
		Target: cfg.DaytonaTarget,
	})
	if err != nil {
		return nil, err
	}
	return NewDaytonaArtifactBuilderForClient(client.Snapshot), nil
}

func NewDaytonaArtifactBuilderForClient(snapshots daytonaSnapshotCreator) *DaytonaArtifactBuilder {
	return &DaytonaArtifactBuilder{snapshots: snapshots, baseImage: defaultDaytonaImage}
}

func (b *DaytonaArtifactBuilder) BuildArtifact(ctx context.Context, request sandbox.BuildArtifactRequest) (sandbox.BuildArtifactResult, error) {
	if b == nil || b.snapshots == nil {
		return sandbox.BuildArtifactResult{}, daytonaProviderError(sandbox.StageBuildArtifact, sandbox.ProviderErrorConfigInvalid, false, 0, "daytona artifact builder is unavailable", nil)
	}
	if request.WorkspaceID == "" || request.EnvironmentID == "" || request.Generation <= 0 {
		return sandbox.BuildArtifactResult{}, daytonaProviderError(sandbox.StageBuildArtifact, sandbox.ProviderErrorInvalidRequest, false, 0, "environment artifact identity is required", nil)
	}
	if err := validateArtifactPackages(request.NormalizedPackages); err != nil {
		return sandbox.BuildArtifactResult{}, err
	}
	name := deterministicSnapshotName(request)
	dockerfile := deterministicArtifactDockerfile(b.baseImage, request.NormalizedPackages)
	snapshot, logs, err := b.snapshots.Create(ctx, &types.CreateSnapshotParams{
		Name:  name,
		Image: daytona.FromDockerfile(dockerfile),
	})
	drainAvailableSnapshotLogs(logs)
	if err != nil {
		return sandbox.BuildArtifactResult{}, mapDaytonaError(sandbox.StageBuildArtifact, err)
	}
	if snapshot == nil {
		return sandbox.BuildArtifactResult{}, daytonaProviderError(sandbox.StageBuildArtifact, sandbox.ProviderErrorUnknown, true, 0, "daytona snapshot create returned no snapshot", nil)
	}
	ref := snapshot.ID
	if ref == "" {
		ref = snapshot.Name
	}
	if ref == "" {
		return sandbox.BuildArtifactResult{}, daytonaProviderError(sandbox.StageBuildArtifact, sandbox.ProviderErrorUnknown, true, 0, "daytona snapshot create returned no artifact ref", nil)
	}
	return sandbox.BuildArtifactResult{ProviderArtifactRef: ref}, nil
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
	hash := request.ArtifactInputHash
	if len(hash) > 12 {
		hash = hash[:12]
	}
	return sanitizeSnapshotName("tetral-" + string(request.WorkspaceID) + "-" + request.EnvironmentID + "-" + strconv.FormatInt(request.Generation, 10) + "-" + hash)
}

func sanitizeSnapshotName(value string) string {
	value = strings.ToLower(value)
	var builder strings.Builder
	builder.Grow(len(value))
	lastDash := false
	for _, r := range value {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			builder.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			builder.WriteByte('-')
			lastDash = true
		}
	}
	name := strings.Trim(builder.String(), "-")
	if name == "" {
		return "tetral-environment-artifact"
	}
	if len(name) > 63 {
		name = strings.TrimRight(name[:63], "-")
	}
	if name == "" {
		return "tetral-environment-artifact"
	}
	return name
}

func deterministicArtifactDockerfile(baseImage string, packages sandbox.PackageSetup) string {
	if baseImage == "" {
		baseImage = defaultDaytonaImage
	}
	lines := []string{"FROM " + baseImage}
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
