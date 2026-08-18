//go:build daytona_release_smoke

package tetralsandbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/daytonaio/daytona/libs/sdk-go/pkg/daytona"
	daytonaerrors "github.com/daytonaio/daytona/libs/sdk-go/pkg/errors"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"

	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	envDaytonaReleaseSmoke       = "TETRAL_DAYTONA_RELEASE_SMOKE"
	envDaytonaReleaseSmokeImage  = "TETRAL_DAYTONA_RELEASE_SMOKE_IMAGE"
	envDaytonaReleaseSmokeOwner  = "TETRAL_DAYTONA_RELEASE_SMOKE_OWNER"
	releaseSmokeCommandTimeout   = 2 * time.Minute
	releaseSmokeLifecycleTimeout = 20 * time.Minute
	releaseSmokeCleanupTimeout   = 3 * time.Minute
	releaseSmokeCleanupPoll      = 2 * time.Second
)

var (
	immutableSandboxImagePattern = regexp.MustCompile(`^ghcr\.io/[a-z0-9._-]+/sandbox@sha256:[a-f0-9]{64}$`)
	errReleaseSmokePhase         = errors.New("daytona release smoke phase failed")
	errReleaseSmokeOwnership     = errors.New("daytona release smoke ownership could not be proved")
)

type releaseSmokeLifecycle interface {
	ResolveSandbox(context.Context, string, map[string]string) (sandbox.ProviderHandle, bool, error)
	ReleaseSandbox(context.Context, sandbox.ProviderHandle) error
}

type releaseSmokeSnapshotService interface {
	Get(context.Context, string) (*types.Snapshot, error)
	Delete(context.Context, *types.Snapshot) error
}

type releaseSmokeSandboxCleanupPolicy struct {
	waitForVisibility bool
	pollInterval      time.Duration
}

type releaseSmokeCleanupCoordinator struct {
	sandboxAbsenceProved bool
}

func newReleaseSmokeCleanupCoordinator() *releaseSmokeCleanupCoordinator {
	return &releaseSmokeCleanupCoordinator{sandboxAbsenceProved: true}
}

func (c *releaseSmokeCleanupCoordinator) markSandboxUnproven() {
	c.sandboxAbsenceProved = false
}

func (c *releaseSmokeCleanupCoordinator) cleanupSandbox(ctx context.Context, lifecycle releaseSmokeLifecycle, stableName string, labels map[string]string, policy releaseSmokeSandboxCleanupPolicy) error {
	c.markSandboxUnproven()
	if err := cleanupReleaseSmokeSandbox(ctx, lifecycle, stableName, labels, policy); err != nil {
		return err
	}
	c.sandboxAbsenceProved = true
	return nil
}

func (c *releaseSmokeCleanupCoordinator) cleanupSnapshot(ctx context.Context, snapshots releaseSmokeSnapshotService, expectedName string) error {
	if !c.sandboxAbsenceProved {
		return errReleaseSmokeOwnership
	}
	return cleanupReleaseSmokeSnapshot(ctx, snapshots, expectedName)
}

// TestDaytonaPublishedImageProductionAdapterSmoke is a release-only gate. It
// deliberately fails, rather than skips, when explicitly selected without the
// immutable image, release identity, or release-environment credentials.
func TestDaytonaPublishedImageProductionAdapterSmoke(t *testing.T) {
	required := []string{
		envDaytonaReleaseSmoke,
		envDaytonaReleaseSmokeImage,
		envDaytonaReleaseSmokeOwner,
		EnvDaytonaAPIURL,
		EnvDaytonaAPIKey,
	}
	values := make(map[string]string, len(required))
	for _, name := range required {
		values[name] = strings.TrimSpace(os.Getenv(name))
		if values[name] == "" {
			t.Fatalf("required release input %s is missing", name)
		}
	}
	if values[envDaytonaReleaseSmoke] != "1" {
		t.Fatalf("%s must be 1", envDaytonaReleaseSmoke)
	}
	imageRef := values[envDaytonaReleaseSmokeImage]
	if !immutableSandboxImagePattern.MatchString(imageRef) {
		t.Fatalf("%s must be an immutable published Sandbox image", envDaytonaReleaseSmokeImage)
	}

	ctx, cancel := context.WithTimeout(context.Background(), releaseSmokeLifecycleTimeout)
	defer cancel()
	cfg := driver.Config{
		DaytonaAPIURL:  values[EnvDaytonaAPIURL],
		DaytonaTarget:  strings.TrimSpace(os.Getenv(EnvDaytonaTarget)),
		DaytonaAPIKey:  values[EnvDaytonaAPIKey],
		CommandTimeout: releaseSmokeCommandTimeout,
		Lifecycle: driver.LifecyclePolicy{
			StopTimeout: releaseSmokeCleanupTimeout,
		},
	}
	client, err := daytona.NewClientWithConfig(&types.DaytonaConfig{
		APIKey: cfg.DaytonaAPIKey, APIUrl: cfg.DaytonaAPIURL, Target: cfg.DaytonaTarget,
	})
	if err != nil {
		t.Fatal("release smoke phase=client status=failed")
	}
	lifecycle, err := driver.NewDaytonaLifecycleProviderForSDKClient(client, cfg)
	if err != nil {
		t.Fatal("release smoke phase=lifecycle_adapter status=failed")
	}
	helper, err := driver.NewDaytonaHelperExecutorForSDKClient(client, cfg.CommandTimeout)
	if err != nil {
		t.Fatal("release smoke phase=helper_adapter status=failed")
	}

	identity := newReleaseSmokeIdentity(values[envDaytonaReleaseSmokeOwner], imageRef)
	artifactRequest := identity.artifactRequest()
	snapshotName := releaseSmokeSnapshotName(artifactRequest)
	cleanup := newReleaseSmokeCleanupCoordinator()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), releaseSmokeCleanupTimeout)
		defer cleanupCancel()
		if err := cleanup.cleanupSnapshot(cleanupCtx, client.Snapshot, snapshotName); err != nil {
			t.Error("release smoke phase=snapshot_cleanup status=failed")
		}
	})

	builder := driver.NewDaytonaArtifactBuilderForClient(client.Snapshot, imageRef)
	artifact, err := buildReleaseSmokeArtifact(ctx, builder, artifactRequest)
	if err != nil || artifact.ProviderArtifactRef == "" {
		t.Fatal("release smoke phase=artifact_build status=failed")
	}
	t.Log("release smoke phase=artifact_build status=ok")

	setup := identity.sandboxSetup(artifact.ProviderArtifactRef)
	stableName := setup.SandboxID
	ownershipLabels := stableSandboxOwnershipLabels(
		string(setup.WorkspaceID), setup.SessionID, setup.EnvironmentID, setup.SandboxID,
	)
	createMayHaveBeenSubmitted := false
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), releaseSmokeCleanupTimeout)
		defer cleanupCancel()
		policy := releaseSmokeSandboxCleanupPolicy{
			waitForVisibility: createMayHaveBeenSubmitted,
			pollInterval:      releaseSmokeCleanupPoll,
		}
		if err := cleanup.cleanupSandbox(cleanupCtx, lifecycle, stableName, ownershipLabels, policy); err != nil {
			t.Error("release smoke phase=sandbox_cleanup status=failed")
		}
	})

	// Remove an exactly owned orphan from a rerun before Create. The same
	// resolver is the ambiguous-Create recovery path used by Cleanup above.
	if err := cleanup.cleanupSandbox(ctx, lifecycle, stableName, ownershipLabels, releaseSmokeSandboxCleanupPolicy{}); err != nil {
		t.Fatal("release smoke phase=precreate_recovery status=failed")
	}
	cleanup.markSandboxUnproven()
	handle, err := lifecycle.CreateSandbox(ctx, sandbox.CreateSandboxRequest{Setup: setup})
	createMayHaveBeenSubmitted = releaseSmokeCreateMayHaveBeenSubmitted(err)
	if err != nil || handle.SandboxID == "" {
		t.Fatal("release smoke phase=sandbox_create status=failed")
	}
	t.Log("release smoke phase=sandbox_create status=ok")
	if state, err := lifecycle.InspectState(ctx, handle.SandboxID); err != nil || state == "" {
		t.Fatal("release smoke phase=sandbox_inspect status=failed")
	}
	t.Log("release smoke phase=sandbox_inspect status=ok")

	if err := lifecycle.PrepareBaseDirectories(ctx, handle); err != nil {
		t.Fatal("release smoke phase=base_directories status=failed")
	}
	target := identity.toolTarget(handle.SandboxID)
	if err := helper.CheckHealth(ctx, target); err != nil {
		t.Fatal("release smoke phase=helper_health status=failed")
	}
	t.Log("release smoke phase=helper_health status=ok")

	contentCanary := "release-smoke-" + identity.digest[:32]
	writeInput, _ := json.Marshal(map[string]any{
		"file_path": "/workspace/release-smoke/note.txt",
		"content":   contentCanary + "\n",
	})
	writeResult, err := runReleaseSmokeTool(ctx, helper, driver.ToolInvocation{
		Target: target, ToolUseEventID: "sevt_release_smoke_write", ToolName: "Write", InputJSON: string(writeInput),
	})
	if err != nil || !releaseSmokeWriteSucceeded(writeResult, len(contentCanary)+1) {
		t.Fatal("release smoke phase=file_write status=failed")
	}
	readResult, err := runReleaseSmokeTool(ctx, helper, driver.ToolInvocation{
		Target: target, ToolUseEventID: "sevt_release_smoke_read", ToolName: "Read",
		InputJSON: `{"file_path":"/workspace/release-smoke/note.txt"}`,
	})
	if err != nil || !releaseSmokeReadMatched(readResult, contentCanary+"\n") {
		t.Fatal("release smoke phase=file_read status=failed")
	}
	t.Log("release smoke phase=file_operations status=ok")

	gitCommand := strings.Join([]string{
		"set -eu",
		"git -C /workspace/release-smoke init -q",
		"git -C /workspace/release-smoke config user.name 'Tetral Release Smoke'",
		"git -C /workspace/release-smoke config user.email 'release-smoke@tetral.invalid'",
		"git -C /workspace/release-smoke add note.txt",
		`test "$(git -C /workspace/release-smoke status --short)" = "A  note.txt"`,
		"git -C /workspace/release-smoke show :note.txt | cmp -s - /workspace/release-smoke/note.txt",
	}, "; ")
	gitInput, _ := json.Marshal(map[string]any{
		"cmd": gitCommand, "yield_time_ms": 30000, "max_output_tokens": 100,
	})
	gitResult, err := runReleaseSmokeTool(ctx, helper, driver.ToolInvocation{
		Target: target, ToolUseEventID: "sevt_release_smoke_git", ToolName: "exec_command", InputJSON: string(gitInput),
	})
	if err != nil || !releaseSmokeExecSucceeded(gitResult) {
		t.Fatal("release smoke phase=git_operations status=failed")
	}
	t.Log("release smoke phase=git_operations status=ok")
}

type releaseSmokeIdentity struct {
	digest        string
	workspaceID   workspace.ID
	sessionID     string
	environmentID string
	sandboxName   string
}

func newReleaseSmokeIdentity(owner string, imageRef string) releaseSmokeIdentity {
	digest := sha256.Sum256([]byte(owner + "\x00" + imageRef))
	encoded := hex.EncodeToString(digest[:])
	return releaseSmokeIdentity{
		digest:        encoded,
		workspaceID:   workspace.ID("ws_release_" + encoded[:24]),
		sessionID:     "sesn_release_" + encoded[:24],
		environmentID: "env_release_" + encoded[:24],
		sandboxName:   "tetral-release-smoke-" + encoded[:32],
	}
}

func (i releaseSmokeIdentity) artifactRequest() sandbox.BuildArtifactRequest {
	return sandbox.BuildArtifactRequest{
		WorkspaceID: i.workspaceID, EnvironmentID: i.environmentID, Generation: 1,
		ArtifactInputHash: i.digest, NormalizedPackages: sandbox.PackageSetup{},
	}
}

func (i releaseSmokeIdentity) sandboxSetup(providerArtifactRef string) sandbox.SandboxSetup {
	return sandbox.SandboxSetup{
		WorkspaceID: i.workspaceID, SessionID: i.sessionID, SandboxID: i.sandboxName,
		EnvironmentID: i.environmentID, EnvironmentGeneration: 1,
		ProviderArtifactRef: providerArtifactRef,
		Network:             sandbox.NetworkSetup{Type: "unrestricted"},
	}
}

func (i releaseSmokeIdentity) toolTarget(providerSandboxID string) driver.ToolTarget {
	return driver.ToolTarget{
		WorkspaceID: string(i.workspaceID), SessionID: i.sessionID,
		SessionThreadID: "thr_release_" + i.digest[:24], BindingID: "bind_release_" + i.digest[:24],
		BindingGeneration: 1, SandboxID: i.sandboxName, ProviderSandboxID: providerSandboxID,
	}
}

func releaseSmokeSnapshotName(request sandbox.BuildArtifactRequest) string {
	identity := strings.Join([]string{
		string(request.WorkspaceID),
		request.EnvironmentID,
		strconv.FormatInt(request.Generation, 10),
		request.ArtifactInputHash,
	}, "\x00")
	digest := sha256.Sum256([]byte(identity))
	return "tetral-" + hex.EncodeToString(digest[:])[:56]
}

func buildReleaseSmokeArtifact(ctx context.Context, builder sandbox.ArtifactBuilder, request sandbox.BuildArtifactRequest) (sandbox.BuildArtifactResult, error) {
	providerCreateMayStart := false
	request.AuthorizeProviderCreate = func(context.Context) (bool, error) {
		if providerCreateMayStart {
			return false, nil
		}
		providerCreateMayStart = true
		return true, nil
	}
	for {
		result, err := builder.BuildArtifact(ctx, request)
		if err == nil {
			return result, nil
		}
		if driver.ProviderOperationWasNotSubmitted(err) {
			providerCreateMayStart = false
		}
		var providerErr *sandbox.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Stage != sandbox.StageBuildArtifact || !providerErr.Retryable {
			return sandbox.BuildArtifactResult{}, errReleaseSmokePhase
		}
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return sandbox.BuildArtifactResult{}, errReleaseSmokePhase
		case <-timer.C:
		}
	}
}

func releaseSmokeCreateMayHaveBeenSubmitted(err error) bool {
	return err == nil || !driver.ProviderOperationWasNotSubmitted(err)
}

func cleanupReleaseSmokeSandbox(ctx context.Context, lifecycle releaseSmokeLifecycle, stableName string, labels map[string]string, policy releaseSmokeSandboxCleanupPolicy) error {
	pollInterval := policy.pollInterval
	if pollInterval <= 0 {
		pollInterval = releaseSmokeCleanupPoll
	}
	for {
		handle, found, err := lifecycle.ResolveSandbox(ctx, stableName, labels)
		if err != nil {
			return errReleaseSmokeOwnership
		}
		if found {
			if handle.SandboxID == "" {
				return errReleaseSmokeOwnership
			}
			if err := lifecycle.ReleaseSandbox(ctx, handle); err != nil {
				return errReleaseSmokePhase
			}
			return nil
		}
		if !policy.waitForVisibility {
			return nil
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errReleaseSmokePhase
		case <-timer.C:
		}
	}
}

func cleanupReleaseSmokeSnapshot(ctx context.Context, snapshots releaseSmokeSnapshotService, expectedName string) error {
	snapshot, err := snapshots.Get(ctx, expectedName)
	if err != nil {
		var notFound *daytonaerrors.DaytonaNotFoundError
		if errors.As(err, &notFound) {
			return nil
		}
		return errReleaseSmokePhase
	}
	if snapshot == nil || snapshot.ID == "" || snapshot.Name != expectedName {
		return errReleaseSmokeOwnership
	}
	if err := snapshots.Delete(ctx, snapshot); err != nil {
		return errReleaseSmokePhase
	}
	return nil
}

func runReleaseSmokeTool(ctx context.Context, helper *driver.DaytonaHelperExecutor, invocation driver.ToolInvocation) (string, error) {
	prepared, err := helper.PrepareTool(ctx, invocation)
	if err != nil || prepared.ImmediateResult() != nil {
		return "", errReleaseSmokePhase
	}
	result, err := helper.SubmitPreparedTool(ctx, prepared)
	for err == nil && result.ForegroundObservation != nil {
		result, err = helper.ObserveForegroundTool(ctx, *result.ForegroundObservation)
	}
	if err != nil || result.BackgroundTask != nil || result.ForegroundObservation != nil {
		return "", errReleaseSmokePhase
	}
	return result.ResultJSON, nil
}

func releaseSmokeEnvelope(raw string, tool string) (protocol.Envelope, bool) {
	var envelope protocol.Envelope
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		return protocol.Envelope{}, false
	}
	return envelope, envelope.SchemaVersion == protocol.SchemaVersion && envelope.Tool == tool &&
		envelope.Status == protocol.ToolStatusSuccess && envelope.Error == nil
}

func releaseSmokeWriteSucceeded(raw string, wantBytes int) bool {
	envelope, ok := releaseSmokeEnvelope(raw, "write")
	if !ok {
		return false
	}
	var result struct {
		BytesWritten int `json:"bytes_written"`
	}
	return json.Unmarshal(envelope.ResultBytes(), &result) == nil && result.BytesWritten == wantBytes
}

func releaseSmokeReadMatched(raw string, want string) bool {
	envelope, ok := releaseSmokeEnvelope(raw, "read")
	if !ok {
		return false
	}
	var result struct {
		Content string `json:"content"`
	}
	return json.Unmarshal(envelope.ResultBytes(), &result) == nil && result.Content == want
}

func releaseSmokeExecSucceeded(raw string) bool {
	envelope, ok := releaseSmokeEnvelope(raw, "exec")
	if !ok {
		return false
	}
	var result struct {
		ExitCode *int `json:"exit_code"`
	}
	return json.Unmarshal(envelope.ResultBytes(), &result) == nil && result.ExitCode != nil && *result.ExitCode == 0
}
