//go:build daytona_release_smoke

package tetralsandbox

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	daytonaerrors "github.com/daytonaio/daytona/libs/sdk-go/pkg/errors"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"

	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/sandbox/driver"
)

func TestReleaseSmokeSnapshotIdentityMatchesProductionArtifactBuilder(t *testing.T) {
	const imageRef = "ghcr.io/tetral-ai/sandbox@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	identity := newReleaseSmokeIdentity("release-run-123", imageRef)
	request := identity.artifactRequest()
	snapshots := &releaseSmokeRecordingSnapshots{
		getErr: daytonaerrors.NewDaytonaNotFoundError("not found", http.Header{}),
	}
	builder := driver.NewDaytonaArtifactBuilderForClient(snapshots, imageRef)
	request.AuthorizeProviderCreate = func(context.Context) (bool, error) { return true, nil }
	result, err := builder.BuildArtifact(context.Background(), request)
	if err != nil {
		t.Fatalf("BuildArtifact: %v", err)
	}
	if result.ProviderArtifactRef != "snapshot_release_smoke" || snapshots.createParams == nil {
		t.Fatalf("artifact result = %+v params=%v", result, snapshots.createParams)
	}
	if got, want := snapshots.createParams.Name, releaseSmokeSnapshotName(identity.artifactRequest()); got != want {
		t.Fatalf("snapshot name = %q; want cleanup identity %q", got, want)
	}
	image, ok := snapshots.createParams.Image.(interface{ Dockerfile() string })
	if !ok || !strings.HasPrefix(image.Dockerfile(), "FROM "+imageRef+"\n") {
		t.Fatal("production ArtifactBuilder did not use the exact immutable image as its base")
	}
}

func TestReleaseSmokeIdentityEstablishesExactProductionOwnership(t *testing.T) {
	const imageRef = "ghcr.io/tetral-ai/sandbox@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	identity := newReleaseSmokeIdentity("release-run-456", imageRef)
	setup := identity.sandboxSetup("snapshot_release_smoke")
	labels := stableSandboxOwnershipLabels(string(setup.WorkspaceID), setup.SessionID, setup.EnvironmentID, setup.SandboxID)
	want := map[string]string{
		"tetral.workspace_id": string(setup.WorkspaceID), "tetral.session_id": setup.SessionID,
		"tetral.environment_id": setup.EnvironmentID, "tetral.sandbox_id": setup.SandboxID,
		"tetral.lifecycle_owner": "sandbox",
	}
	if setup.SandboxID == "" || !reflect.DeepEqual(labels, want) {
		t.Fatalf("stable identity = (%q, %v); want exact production ownership %v", setup.SandboxID, labels, want)
	}
}

func TestReleaseSmokeCleanupResolvesStableOwnedSandboxAfterAmbiguousCreate(t *testing.T) {
	labels := map[string]string{
		"tetral.workspace_id": "ws_release", "tetral.session_id": "sesn_release",
		"tetral.environment_id": "env_release", "tetral.sandbox_id": "tetral-release-smoke",
		"tetral.lifecycle_owner": "sandbox",
	}
	lifecycle := &releaseSmokeRecordingLifecycle{
		resolveHandle: sandbox.ProviderHandle{Provider: driver.DaytonaProviderName, SandboxID: "provider_recovered"},
		resolveFound:  true,
	}
	policy := releaseSmokeSandboxCleanupPolicy{waitForVisibility: true, pollInterval: time.Nanosecond}
	if err := cleanupReleaseSmokeSandbox(context.Background(), lifecycle, "tetral-release-smoke", labels, policy); err != nil {
		t.Fatalf("cleanupReleaseSmokeSandbox: %v", err)
	}
	if lifecycle.resolveName != "tetral-release-smoke" || !reflect.DeepEqual(lifecycle.resolveLabels, labels) {
		t.Fatalf("resolve identity = (%q, %v); want exact stable ownership", lifecycle.resolveName, lifecycle.resolveLabels)
	}
	if lifecycle.releaseCalls != 1 || lifecycle.released.SandboxID != "provider_recovered" {
		t.Fatalf("release = (%d, %+v); want recovered handle once", lifecycle.releaseCalls, lifecycle.released)
	}
}

func TestReleaseSmokeCleanupPollsUntilSubmittedCreateBecomesVisible(t *testing.T) {
	labels := map[string]string{
		"tetral.workspace_id": "ws_release", "tetral.session_id": "sesn_release",
		"tetral.environment_id": "env_release", "tetral.sandbox_id": "tetral-release-smoke",
		"tetral.lifecycle_owner": "sandbox",
	}
	lifecycle := &releaseSmokeRecordingLifecycle{resolveResults: []releaseSmokeResolveResult{
		{},
		{},
		{handle: sandbox.ProviderHandle{Provider: driver.DaytonaProviderName, SandboxID: "provider_eventual"}, found: true},
	}}
	policy := releaseSmokeSandboxCleanupPolicy{waitForVisibility: true, pollInterval: time.Nanosecond}
	if err := cleanupReleaseSmokeSandbox(context.Background(), lifecycle, "tetral-release-smoke", labels, policy); err != nil {
		t.Fatalf("cleanupReleaseSmokeSandbox: %v", err)
	}
	if lifecycle.resolveCalls != 3 || lifecycle.releaseCalls != 1 || lifecycle.released.SandboxID != "provider_eventual" {
		t.Fatalf("cleanup calls = resolve:%d release:%d handle:%+v; want eventual exact release", lifecycle.resolveCalls, lifecycle.releaseCalls, lifecycle.released)
	}
	for index := range lifecycle.resolveNames {
		if lifecycle.resolveNames[index] != "tetral-release-smoke" || !reflect.DeepEqual(lifecycle.resolveLabelSets[index], labels) {
			t.Fatalf("resolve attempt %d identity = (%q, %v); want stable exact ownership", index+1, lifecycle.resolveNames[index], lifecycle.resolveLabelSets[index])
		}
	}
}

func TestReleaseSmokeDefinitivelyNotSubmittedCleanupIsOneShot(t *testing.T) {
	lifecycle := &releaseSmokeRecordingLifecycle{}
	if releaseSmokeCreateMayHaveBeenSubmitted(driver.MarkProviderOperationNotSubmitted(errors.New("rejected"))) {
		t.Fatal("definitively not-submitted Create was classified as possibly submitted")
	}
	policy := releaseSmokeSandboxCleanupPolicy{waitForVisibility: false, pollInterval: time.Nanosecond}
	if err := cleanupReleaseSmokeSandbox(context.Background(), lifecycle, "tetral-release-smoke", map[string]string{"owner": "release"}, policy); err != nil {
		t.Fatalf("cleanupReleaseSmokeSandbox: %v", err)
	}
	if lifecycle.resolveCalls != 1 || lifecycle.releaseCalls != 0 {
		t.Fatalf("cleanup calls = resolve:%d release:%d; want one-shot absence proof", lifecycle.resolveCalls, lifecycle.releaseCalls)
	}
	if !releaseSmokeCreateMayHaveBeenSubmitted(errors.New("ambiguous")) || !releaseSmokeCreateMayHaveBeenSubmitted(nil) {
		t.Fatal("ambiguous or empty-handle Create path was not classified as possibly submitted")
	}
}

func TestReleaseSmokeCleanupNeverDeletesOwnershipMismatch(t *testing.T) {
	lifecycle := &releaseSmokeRecordingLifecycle{resolveErr: driver.ErrSandboxOwnershipMismatch}
	policy := releaseSmokeSandboxCleanupPolicy{waitForVisibility: true, pollInterval: time.Nanosecond}
	err := cleanupReleaseSmokeSandbox(context.Background(), lifecycle, "tetral-release-smoke", map[string]string{"owner": "release"}, policy)
	if !errors.Is(err, errReleaseSmokeOwnership) {
		t.Fatalf("cleanup error = %v; want ownership refusal", err)
	}
	if lifecycle.releaseCalls != 0 {
		t.Fatalf("release calls = %d; want zero for ownership mismatch", lifecycle.releaseCalls)
	}
}

func TestReleaseSmokeSandboxCleanupFailureBlocksSnapshotDeletion(t *testing.T) {
	cleanup := newReleaseSmokeCleanupCoordinator()
	cleanup.markSandboxUnproven()
	lifecycle := &releaseSmokeRecordingLifecycle{resolveErr: driver.ErrSandboxOwnershipMismatch}
	policy := releaseSmokeSandboxCleanupPolicy{waitForVisibility: true, pollInterval: time.Nanosecond}
	if err := cleanup.cleanupSandbox(context.Background(), lifecycle, "tetral-release-smoke", map[string]string{"owner": "release"}, policy); !errors.Is(err, errReleaseSmokeOwnership) {
		t.Fatalf("Sandbox cleanup error = %v; want ownership refusal", err)
	}
	snapshots := &releaseSmokeRecordingSnapshots{
		getSnapshot: &types.Snapshot{ID: "snapshot_owned", Name: "tetral-owned-snapshot"},
	}
	if err := cleanup.cleanupSnapshot(context.Background(), snapshots, "tetral-owned-snapshot"); !errors.Is(err, errReleaseSmokeOwnership) {
		t.Fatalf("snapshot cleanup error = %v; want blocked ownership proof", err)
	}
	if snapshots.deleteCalls != 0 {
		t.Fatalf("snapshot delete calls = %d; want zero after Sandbox cleanup failure", snapshots.deleteCalls)
	}
}

func TestReleaseSmokeSnapshotCleanupRequiresExactStableName(t *testing.T) {
	snapshots := &releaseSmokeRecordingSnapshots{
		getSnapshot: &types.Snapshot{ID: "snapshot_other", Name: "other-snapshot"},
	}
	err := cleanupReleaseSmokeSnapshot(context.Background(), snapshots, "tetral-owned-snapshot")
	if !errors.Is(err, errReleaseSmokeOwnership) {
		t.Fatalf("cleanup error = %v; want ownership refusal", err)
	}
	if snapshots.deleteCalls != 0 {
		t.Fatalf("snapshot delete calls = %d; want zero for name mismatch", snapshots.deleteCalls)
	}
}

type releaseSmokeRecordingLifecycle struct {
	resolveName      string
	resolveLabels    map[string]string
	resolveNames     []string
	resolveLabelSets []map[string]string
	resolveCalls     int
	resolveResults   []releaseSmokeResolveResult
	resolveHandle    sandbox.ProviderHandle
	resolveFound     bool
	resolveErr       error
	releaseCalls     int
	released         sandbox.ProviderHandle
}

type releaseSmokeResolveResult struct {
	handle sandbox.ProviderHandle
	found  bool
	err    error
}

func (l *releaseSmokeRecordingLifecycle) ResolveSandbox(_ context.Context, name string, labels map[string]string) (sandbox.ProviderHandle, bool, error) {
	l.resolveCalls++
	l.resolveName = name
	l.resolveLabels = mapsClone(labels)
	l.resolveNames = append(l.resolveNames, name)
	l.resolveLabelSets = append(l.resolveLabelSets, mapsClone(labels))
	if len(l.resolveResults) > 0 {
		index := l.resolveCalls - 1
		if index >= len(l.resolveResults) {
			index = len(l.resolveResults) - 1
		}
		result := l.resolveResults[index]
		return result.handle, result.found, result.err
	}
	return l.resolveHandle, l.resolveFound, l.resolveErr
}

func (l *releaseSmokeRecordingLifecycle) ReleaseSandbox(_ context.Context, handle sandbox.ProviderHandle) error {
	l.releaseCalls++
	l.released = handle
	return nil
}

type releaseSmokeRecordingSnapshots struct {
	getSnapshot  *types.Snapshot
	getErr       error
	createParams *types.CreateSnapshotParams
	deleteCalls  int
}

func (s *releaseSmokeRecordingSnapshots) Get(context.Context, string) (*types.Snapshot, error) {
	return s.getSnapshot, s.getErr
}

func (s *releaseSmokeRecordingSnapshots) Create(_ context.Context, params *types.CreateSnapshotParams) (*types.Snapshot, <-chan string, error) {
	s.createParams = params
	logs := make(chan string)
	close(logs)
	return &types.Snapshot{ID: "snapshot_release_smoke", Name: params.Name, State: "active"}, logs, nil
}

func (s *releaseSmokeRecordingSnapshots) Delete(context.Context, *types.Snapshot) error {
	s.deleteCalls++
	return nil
}

func mapsClone(values map[string]string) map[string]string {
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

type releaseSmokeArtifactBuilder struct {
	result sandbox.BuildArtifactResult
	err    error
	calls  int
}

func (b *releaseSmokeArtifactBuilder) BuildArtifact(context.Context, sandbox.BuildArtifactRequest) (sandbox.BuildArtifactResult, error) {
	b.calls++
	return b.result, b.err
}

func TestReleaseSmokeArtifactBuildStopsOnTerminalProviderError(t *testing.T) {
	builder := &releaseSmokeArtifactBuilder{err: &sandbox.ProviderError{
		Provider: driver.DaytonaProviderName, Stage: sandbox.StageBuildArtifact,
		Kind: sandbox.ProviderErrorInvalidRequest, Retryable: false,
	}}
	_, err := buildReleaseSmokeArtifact(context.Background(), builder, sandbox.BuildArtifactRequest{})
	if !errors.Is(err, errReleaseSmokePhase) {
		t.Fatalf("BuildArtifact error = %v; want fixed terminal phase failure", err)
	}
	if builder.calls != 1 {
		t.Fatalf("BuildArtifact calls = %d; want one for terminal provider error", builder.calls)
	}
}
