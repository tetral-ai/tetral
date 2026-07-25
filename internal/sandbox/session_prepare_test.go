package sandbox

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestPrepareSessionClaimsCreatesSandboxAndMarksReady(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	preparations := &recordingSessionPreparationStore{
		preparation: SessionPreparation{
			WorkspaceID:          workspace.DefaultID,
			SessionID:            "sesn_test",
			PreparationAttemptID: "prep_test",
			EnvironmentID:        "env_test",
			SandboxID:            "sandbox_from_preparation",
			Status:               "pending",
		},
		resources: ResourceSetup{
			Files: []FileMount{{ResourceID: "sesrsc_file", SourceFileID: "file_source", SessionFileID: "file_session", MountPath: "/mnt/session/uploads/file_session", ReadOnly: true}},
		},
	}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithSessionPreparationStore(preparations),
	)

	result, err := service.PrepareSession(context.Background(), SessionPrepareRequest{
		WorkspaceID:          workspace.DefaultID,
		SessionID:            "sesn_test",
		PreparationAttemptID: "prep_test",
	})
	if err != nil {
		t.Fatalf("PrepareSession: %v", err)
	}
	if result.Status != SessionPrepareStatusReady {
		t.Fatalf("result = %+v; want ready", result)
	}
	if len(preparations.claims) != 1 || len(preparations.readyUpdates) != 1 || len(preparations.failedUpdates) != 0 {
		t.Fatalf("preparation store calls = claims %d ready %d failed %d", len(preparations.claims), len(preparations.readyUpdates), len(preparations.failedUpdates))
	}
	if preparations.readyUpdates[0].ReadyAt != fixedSandboxTime {
		t.Fatalf("ready_at = %s; want fixed clock", preparations.readyUpdates[0].ReadyAt)
	}
	created := store.sandboxes["sandbox_from_preparation"]
	if created == nil || created.Status != StatusActive || created.StatusRefreshedAt == nil {
		t.Fatalf("created sandbox = %+v; want active sandbox with freshness", created)
	}
	if len(provider.createRequests) != 1 || provider.createRequests[0].Setup.SandboxID != "sandbox_from_preparation" {
		t.Fatalf("provider create requests = %+v; want preparation sandbox id", provider.createRequests)
	}
	if provider.createRequests[0].Setup.ProviderArtifactRef != "artifact_env_test" {
		t.Fatalf("provider artifact ref = %q; want artifact_env_test", provider.createRequests[0].Setup.ProviderArtifactRef)
	}
	if len(provider.baseDirectoryHandles) != 1 || provider.baseDirectoryHandles[0].SandboxID != "provider_sandbox_123" {
		t.Fatalf("base directory handles = %+v; want provider sandbox base directory preparation", provider.baseDirectoryHandles)
	}
}

func TestPrepareSessionPreservesResourceCredentialWithoutImplicitRotation(t *testing.T) {
	store := newRecordingStore()
	store.sandboxes["sandbox_from_preparation"] = &Sandbox{
		ID:                    "sandbox_from_preparation",
		WorkspaceID:           workspace.DefaultID,
		SessionID:             "sesn_test",
		Status:                StatusActive,
		Provider:              "unit-provider",
		EnvironmentID:         "env_test",
		EnvironmentGeneration: 1,
		ProviderSandboxID:     "provider_sandbox_123",
		StatusRefreshedAt:     &fixedSandboxTime,
		CreatedAt:             fixedSandboxTime.Add(-time.Hour),
		UpdatedAt:             fixedSandboxTime,
	}
	provider := newSuccessfulRecordingProvider()
	expiresAt := fixedSandboxTime.Add(2 * time.Hour)
	preparer := &recordingSessionResourcePreparer{}
	preparations := &recordingSessionPreparationStore{
		preparation: SessionPreparation{
			WorkspaceID:           workspace.DefaultID,
			SessionID:             "sesn_test",
			PreparationAttemptID:  "prep_test",
			EnvironmentID:         "env_test",
			EnvironmentGeneration: 1,
			SandboxID:             "sandbox_from_preparation",
			Status:                "pending",
			ResourceCredExpiresAt: &expiresAt,
		},
		resources: ResourceSetup{
			Files: []FileMount{{ResourceID: "sesrsc_file", SourceFileID: "file_source", SessionFileID: "file_session", MountPath: "/mnt/session/uploads/file_session", ReadOnly: true}},
		},
	}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithSessionPreparationStore(preparations),
		WithSessionResourcePreparer(preparer),
	)

	result, err := service.PrepareSession(context.Background(), SessionPrepareRequest{
		WorkspaceID:          workspace.DefaultID,
		SessionID:            "sesn_test",
		PreparationAttemptID: "prep_test",
	})
	if err != nil {
		t.Fatalf("PrepareSession: %v", err)
	}
	if result.Status != SessionPrepareStatusReady {
		t.Fatalf("result = %+v; want ready", result)
	}
	if len(preparer.setups) != 1 {
		t.Fatalf("mount preparer calls = %d; want 1", len(preparer.setups))
	}
	got := preparer.setups[0].Resources
	if got.ResourceCredExpiresAt == nil || !got.ResourceCredExpiresAt.Equal(expiresAt) {
		t.Fatalf("resource credential metadata = expires %v; want preserved expiry without implicit rotation", got.ResourceCredExpiresAt)
	}
}

func TestPrepareSessionRemovesDeletedGitHubCheckoutBeforeSamePathFileProjection(t *testing.T) {
	store := newRecordingStore()
	active := activeRecordingSandbox()
	active.ID = "sandbox_from_preparation"
	store.sandboxes[active.ID] = active
	provider := newSuccessfulRecordingProvider()
	events := []string{}
	preparer := &recordingSessionResourcePreparer{eventsRef: &events}
	materializer := &recordingGitHubRepositoryMaterializer{eventsRef: &events}
	preparations := &recordingSessionPreparationStore{
		eventsRef: &events,
		preparation: SessionPreparation{
			WorkspaceID:          workspace.DefaultID,
			SessionID:            "sesn_test",
			PreparationAttemptID: "prep_test",
			EnvironmentID:        "env_test",
			SandboxID:            active.ID,
			Status:               "pending",
		},
		resources: ResourceSetup{
			Files: []FileMount{{
				ResourceID:    "sesrsc_file",
				SourceFileID:  "file_source",
				SessionFileID: "file_session",
				ObjectID:      "obj_file",
				MountPath:     "/workspace/project",
				ReadOnly:      true,
			}},
			DeletedGitHubRepositories: []GitHubRepositoryMount{{
				ResourceID: "sesrsc_repo_deleted",
				URL:        "https://github.com/tetral-ai/project",
				MountPath:  "/workspace/project",
			}},
		},
	}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithSessionPreparationStore(preparations),
		WithSessionResourcePreparer(preparer),
		WithGitHubRepositoryPreparation(nil, materializer, ""),
	)

	result, err := service.PrepareSession(context.Background(), SessionPrepareRequest{
		WorkspaceID:          workspace.DefaultID,
		SessionID:            "sesn_test",
		PreparationAttemptID: "prep_test",
	})
	if err != nil {
		t.Fatalf("PrepareSession: %v", err)
	}
	if result.Status != SessionPrepareStatusReady {
		t.Fatalf("result = %+v; want ready", result)
	}
	wantEvents := []string{"github_remove", "resource_detach", "resource_prepare"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("resource events = %v; want %v", events, wantEvents)
	}
}

func TestExactPathResourceReplacementMatrix(t *testing.T) {
	tests := []struct {
		name          string
		deletedType   string
		successorType string
		mountPath     string
		legal         bool
		successorStep string
	}{
		{name: "file to file", deletedType: "file", successorType: "file", mountPath: "/workspace/project", legal: true, successorStep: "file-bind"},
		{name: "file to GitHub", deletedType: "file", successorType: "github_repository", mountPath: "/workspace/project", legal: true, successorStep: "github-clone"},
		{name: "Memory to Memory", deletedType: "memory_store", successorType: "memory_store", mountPath: "/mnt/memory/project", legal: true, successorStep: "memory-snapshot"},
		{name: "GitHub to file", deletedType: "github_repository", successorType: "file", mountPath: "/workspace/project", legal: true, successorStep: "file-bind"},
		{name: "GitHub to GitHub", deletedType: "github_repository", successorType: "github_repository", mountPath: "/workspace/project", legal: true, successorStep: "github-clone"},
		{name: "file to Memory", deletedType: "file", successorType: "memory_store", mountPath: "/workspace/project"},
		{name: "Memory to file", deletedType: "memory_store", successorType: "file", mountPath: "/mnt/memory/project"},
		{name: "Memory to GitHub", deletedType: "memory_store", successorType: "github_repository", mountPath: "/mnt/memory/project"},
		{name: "GitHub to Memory", deletedType: "github_repository", successorType: "memory_store", mountPath: "/workspace/project"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const deletedID = "sesrsc_deleted"
			resources := resourceReplacementSetup(tc.deletedType, tc.successorType, tc.mountPath, deletedID)
			events := []string{}

			if err := validateSessionResourceMountPaths(resources); !tc.legal {
				if err == nil {
					t.Fatal("namespace-illegal exact-path replacement passed validation")
				}
				if len(events) != 0 {
					t.Fatalf("events = %v; want validation before cleanup", events)
				}
				return
			} else if err != nil {
				t.Fatalf("legal exact-path replacement validation: %v", err)
			}

			store := &recordingSessionPreparationStore{
				preparation: SessionPreparation{PreparationAttemptID: "prep_test", Status: "preparing"},
				resources:   resources,
				detachErrs: []error{
					errors.New("injected crash after removal before detach"),
					nil,
				},
			}
			coordinator := &sessionPreparationResourceCleanupCoordinator{
				store:                store,
				workspaceID:          workspace.DefaultID,
				sessionID:            "sesn_test",
				preparationAttemptID: "prep_test",
				clock:                func() time.Time { return fixedSandboxTime },
			}
			remove := func(context.Context) error {
				events = append(events, tc.deletedType+"-remove")
				return nil
			}

			if err := coordinator.CleanupSessionResource(context.Background(), deletedID, remove); err == nil {
				t.Fatal("first cleanup succeeded; want crash between removal ACK and detach")
			}
			if len(store.detached) != 0 {
				t.Fatalf("detached resources = %v; want pending after detach-boundary crash", store.detached)
			}
			if err := coordinator.CleanupSessionResource(context.Background(), deletedID, remove); err != nil {
				t.Fatalf("cleanup retry: %v", err)
			}
			events = append(events, "detach")

			// This call models a restart after durable detach but before successor
			// materialization. The old removal must not run a third time.
			if err := coordinator.CleanupSessionResource(context.Background(), deletedID, remove); err != nil {
				t.Fatalf("post-detach replay: %v", err)
			}
			events = append(events, tc.successorStep)

			want := []string{tc.deletedType + "-remove", tc.deletedType + "-remove", "detach", tc.successorStep}
			if !reflect.DeepEqual(events, want) {
				t.Fatalf("events = %v; want %v", events, want)
			}
		})
	}
}

func resourceReplacementSetup(deletedType string, successorType string, mountPath string, deletedID string) ResourceSetup {
	resources := ResourceSetup{}
	switch deletedType {
	case "file":
		resources.DeletedFiles = []FileMount{{ResourceID: deletedID, SessionFileID: "file_deleted", MountPath: mountPath}}
	case "memory_store":
		resources.DeletedMemoryStores = []MemoryStoreMount{{ResourceID: deletedID, MemoryStoreID: "memstore_deleted", MountPath: mountPath}}
	case "github_repository":
		resources.DeletedGitHubRepositories = []GitHubRepositoryMount{{ResourceID: deletedID, URL: "https://github.com/tetral-ai/deleted", MountPath: mountPath}}
	}
	switch successorType {
	case "file":
		resources.Files = []FileMount{{ResourceID: "sesrsc_successor", SessionFileID: "file_successor", MountPath: mountPath}}
	case "memory_store":
		resources.MemoryStores = []MemoryStoreMount{{ResourceID: "sesrsc_successor", MemoryStoreID: "memstore_successor", MountPath: mountPath}}
	case "github_repository":
		resources.GitHubRepositories = []GitHubRepositoryMount{{ResourceID: "sesrsc_successor", URL: "https://github.com/tetral-ai/successor", MountPath: mountPath}}
	}
	return resources
}

func TestPrepareSessionReplacesActiveSandboxWhenProviderReportsReleased(t *testing.T) {
	store := newRecordingStore()
	staleRefreshedAt := fixedSandboxTime.Add(-2 * time.Minute)
	expiresAt := fixedSandboxTime.Add(2 * time.Hour)
	store.sandboxes["sandbox_from_preparation"] = &Sandbox{
		ID:                    "sandbox_from_preparation",
		WorkspaceID:           workspace.DefaultID,
		SessionID:             "sesn_test",
		Status:                StatusActive,
		Provider:              "unit-provider",
		EnvironmentID:         "env_test",
		EnvironmentGeneration: 1,
		ProviderSandboxID:     "provider_sandbox_123",
		StatusRefreshedAt:     &staleRefreshedAt,
		CreatedAt:             fixedSandboxTime.Add(-time.Hour),
		UpdatedAt:             staleRefreshedAt,
	}
	provider := newSuccessfulRecordingProvider()
	provider.status = ProviderStatus{Availability: ProviderMissing, SandboxStatus: StatusReleased, SafeMessage: "sandbox missing"}
	preparer := &recordingSessionResourcePreparer{}
	preparations := &recordingSessionPreparationStore{
		preparation: SessionPreparation{
			WorkspaceID:           workspace.DefaultID,
			SessionID:             "sesn_test",
			PreparationAttemptID:  "prep_test",
			EnvironmentID:         "env_test",
			EnvironmentGeneration: 1,
			SandboxID:             "sandbox_from_preparation",
			Status:                "pending",
			ResourceCredExpiresAt: &expiresAt,
		},
		resources: ResourceSetup{
			Files: []FileMount{{ResourceID: "sesrsc_file", SourceFileID: "file_source", SessionFileID: "file_session", MountPath: "/mnt/session/uploads/file_session", ReadOnly: true}},
		},
	}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithSessionPreparationStore(preparations),
		WithSessionResourcePreparer(preparer),
	)

	result, err := service.PrepareSession(context.Background(), SessionPrepareRequest{
		WorkspaceID:          workspace.DefaultID,
		SessionID:            "sesn_test",
		PreparationAttemptID: "prep_test",
	})
	if err != nil {
		t.Fatalf("PrepareSession: %v", err)
	}
	if result.Status != SessionPrepareStatusReady {
		t.Fatalf("result = %+v; want ready after replacement", result)
	}
	if len(provider.createRequests) != 1 {
		t.Fatalf("provider create requests = %d; want replacement create", len(provider.createRequests))
	}
	if len(provider.releaseReasons) != 1 || provider.releaseReasons[0] != ReleaseReasonDelete {
		t.Fatalf("replacement release reasons = %v; want delete-old-first", provider.releaseReasons)
	}
	if len(preparer.setups) != 1 || len(provider.baseDirectoryHandles) != 1 {
		t.Fatalf("resource preparation calls preparer=%d baseDirs=%d; want replacement preparation", len(preparer.setups), len(provider.baseDirectoryHandles))
	}
	if got := preparer.setups[0].Resources; got.ResourceCredExpiresAt != nil {
		t.Fatalf("replacement resources expiry=%v; want cold remount without old credential metadata", got.ResourceCredExpiresAt)
	}
	got := store.sandboxes["sandbox_from_preparation"]
	if got == nil || got.Status != StatusActive || got.ReleasedAt != nil || got.StatusRefreshedAt == nil || got.EnvironmentGeneration != 1 {
		t.Fatalf("sandbox after replacement = %+v; want fresh active replacement at generation 1", got)
	}
}

func TestPrepareSessionResumesStoppedSandboxBeforeResourcePreparation(t *testing.T) {
	store := newRecordingStore()
	stoppedAt := fixedSandboxTime.Add(-2 * time.Minute)
	expiresAt := fixedSandboxTime.Add(2 * time.Hour)
	store.sandboxes["sandbox_from_preparation"] = &Sandbox{
		ID:                "sandbox_from_preparation",
		WorkspaceID:       workspace.DefaultID,
		SessionID:         "sesn_test",
		Status:            StatusStopped,
		Provider:          "unit-provider",
		ProviderSandboxID: "provider_sandbox_123",
		StatusRefreshedAt: &stoppedAt,
		CreatedAt:         fixedSandboxTime.Add(-time.Hour),
		UpdatedAt:         stoppedAt,
	}
	provider := newSuccessfulRecordingProvider()
	preparer := &recordingSessionResourcePreparer{}
	preparations := &recordingSessionPreparationStore{
		preparation: SessionPreparation{
			WorkspaceID:           workspace.DefaultID,
			SessionID:             "sesn_test",
			PreparationAttemptID:  "prep_test",
			EnvironmentID:         "env_test",
			SandboxID:             "sandbox_from_preparation",
			Status:                "pending",
			ResourceCredExpiresAt: &expiresAt,
		},
		resources: ResourceSetup{
			Files: []FileMount{{ResourceID: "sesrsc_file", SourceFileID: "file_source", SessionFileID: "file_session", MountPath: "/mnt/session/uploads/file_session", ReadOnly: true}},
		},
	}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithSessionPreparationStore(preparations),
		WithSessionResourcePreparer(preparer),
	)

	result, err := service.PrepareSession(context.Background(), SessionPrepareRequest{
		WorkspaceID:          workspace.DefaultID,
		SessionID:            "sesn_test",
		PreparationAttemptID: "prep_test",
	})
	if err != nil {
		t.Fatalf("PrepareSession: %v", err)
	}
	if result.Status != SessionPrepareStatusReady {
		t.Fatalf("result = %+v; want ready after resume", result)
	}
	if len(provider.startHandles) != 1 || provider.startHandles[0].SandboxID != "provider_sandbox_123" {
		t.Fatalf("start handles = %+v; want stopped provider handle", provider.startHandles)
	}
	if len(preparer.setups) != 1 || len(provider.baseDirectoryHandles) != 1 {
		t.Fatalf("resource preparation calls preparer=%d baseDirs=%d; want prepare after start", len(preparer.setups), len(provider.baseDirectoryHandles))
	}
	if got := preparer.setups[0].Resources; got.ResourceCredExpiresAt != nil {
		t.Fatalf("resumed resources expiry=%v; want cold remount without old credential metadata", got.ResourceCredExpiresAt)
	}
	got := store.sandboxes["sandbox_from_preparation"]
	if got == nil || got.Status != StatusActive || got.StatusRefreshedAt == nil {
		t.Fatalf("sandbox after resume = %+v; want active fresh sandbox", got)
	}
}

func TestPrepareSessionWakeVsReplaceStartOutcomeMatrix(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       Status
		startErr     error
		wantReady    bool
		wantRelease  int
		wantCreate   int
		wantTerminal Status
	}{
		{name: "matching stopped wakes same handle", status: StatusStopped, wantReady: true, wantTerminal: StatusActive},
		{name: "matching archived wakes same handle", status: StatusArchived, wantReady: true, wantTerminal: StatusActive},
		{name: "provider missing start replaces", status: StatusStopped, startErr: &ProviderError{Provider: "unit-provider", Stage: StageStartSandbox, Kind: ProviderErrorNotFound, SafeMessage: "missing"}, wantReady: true, wantRelease: 1, wantCreate: 1, wantTerminal: StatusActive},
		{name: "unrecoverable start replaces", status: StatusArchived, startErr: &ProviderError{Provider: "unit-provider", Stage: StageStartSandbox, Kind: ProviderErrorUnknown, SafeMessage: "cannot restore"}, wantReady: true, wantRelease: 1, wantCreate: 1, wantTerminal: StatusActive},
		{name: "retryable start stays in place", status: StatusStopped, startErr: &ProviderError{Provider: "unit-provider", Stage: StageStartSandbox, Kind: ProviderErrorUnavailable, Retryable: true, SafeMessage: "busy"}, wantTerminal: StatusResuming},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newRecordingStore()
			store.sandboxes["sandbox_from_preparation"] = &Sandbox{
				ID: "sandbox_from_preparation", WorkspaceID: workspace.DefaultID, SessionID: "sesn_test",
				Status: tc.status, Provider: "unit-provider", ProviderSandboxID: "provider_wake_existing",
				EnvironmentID: "env_test", EnvironmentGeneration: 3,
				CreatedAt: fixedSandboxTime.Add(-time.Hour), UpdatedAt: fixedSandboxTime.Add(-time.Minute),
			}
			provider := newSuccessfulRecordingProvider()
			provider.startErr = tc.startErr
			preparations := &recordingSessionPreparationStore{preparation: SessionPreparation{
				WorkspaceID: workspace.DefaultID, SessionID: "sesn_test", PreparationAttemptID: "prep_wake_matrix",
				EnvironmentID: "env_test", EnvironmentGeneration: 3, SandboxID: "sandbox_from_preparation", Status: "pending",
			}}
			service := NewService(store, provider, WithProviderName("unit-provider"), WithClock(func() time.Time { return fixedSandboxTime }),
				WithSessionPreparationStore(preparations), WithSessionResourcePreparer(&recordingSessionResourcePreparer{}))

			result, err := service.PrepareSession(context.Background(), SessionPrepareRequest{WorkspaceID: workspace.DefaultID, SessionID: "sesn_test", PreparationAttemptID: "prep_wake_matrix"})
			if tc.wantReady {
				if err != nil || result.Status != SessionPrepareStatusReady {
					t.Fatalf("PrepareSession = %+v err=%v; want ready", result, err)
				}
			} else if err == nil {
				t.Fatal("PrepareSession retryable start error = nil")
			}
			if len(provider.startHandles) != 1 || len(provider.releaseReasons) != tc.wantRelease || len(provider.createRequests) != tc.wantCreate {
				t.Fatalf("start/release/create = %d/%d/%d; want 1/%d/%d", len(provider.startHandles), len(provider.releaseReasons), len(provider.createRequests), tc.wantRelease, tc.wantCreate)
			}
			got := store.sandboxes["sandbox_from_preparation"]
			if got.Status != tc.wantTerminal || got.ProviderSandboxID == "" {
				t.Fatalf("terminal sandbox = %+v; want %s with retained provider identity", got, tc.wantTerminal)
			}
			if tc.wantCreate == 0 && got.ProviderSandboxID != "provider_wake_existing" {
				t.Fatalf("wake changed provider handle to %q", got.ProviderSandboxID)
			}
		})
	}
}

func TestPrepareSessionEnvironmentGenerationChangeDeletesOldSandboxBeforeReplacement(t *testing.T) {
	store := newRecordingStore()
	store.sandboxes["sandbox_from_preparation"] = &Sandbox{
		ID: "sandbox_from_preparation", WorkspaceID: workspace.DefaultID, SessionID: "sesn_test",
		EnvironmentID: "env_test", EnvironmentGeneration: 1,
		Status: StatusStopped, Provider: "unit-provider", ProviderSandboxID: "provider_old_generation",
		CreatedAt: fixedSandboxTime.Add(-time.Hour), UpdatedAt: fixedSandboxTime.Add(-time.Minute),
	}
	provider := newSuccessfulRecordingProvider()
	provider.releaseErr = &ProviderError{Provider: "unit-provider", Stage: StageReleaseSandbox, Kind: ProviderErrorUnavailable, Retryable: true}
	preparations := &recordingSessionPreparationStore{preparation: SessionPreparation{
		WorkspaceID: workspace.DefaultID, SessionID: "sesn_test", PreparationAttemptID: "prep_generation_2",
		EnvironmentID: "env_test", EnvironmentGeneration: 2, SandboxID: "sandbox_from_preparation", Status: "pending",
	}}
	service := NewService(store, provider, WithProviderName("unit-provider"), WithClock(func() time.Time { return fixedSandboxTime }),
		WithSessionPreparationStore(preparations), WithSessionResourcePreparer(&recordingSessionResourcePreparer{}))

	_, err := service.PrepareSession(context.Background(), SessionPrepareRequest{WorkspaceID: workspace.DefaultID, SessionID: "sesn_test", PreparationAttemptID: "prep_generation_2"})
	if err == nil {
		t.Fatal("PrepareSession replacement delete failure = nil; want retryable error")
	}
	if len(provider.createRequests) != 0 {
		t.Fatalf("provider creates after old delete failure = %d; want 0", len(provider.createRequests))
	}
	unchanged := store.sandboxes["sandbox_from_preparation"]
	if unchanged.Status != StatusStopped || unchanged.ProviderSandboxID != "provider_old_generation" || unchanged.EnvironmentGeneration != 1 {
		t.Fatalf("old sandbox overwritten before delete success: %+v", unchanged)
	}

	provider.releaseErr = nil
	result, err := service.PrepareSession(context.Background(), SessionPrepareRequest{WorkspaceID: workspace.DefaultID, SessionID: "sesn_test", PreparationAttemptID: "prep_generation_2"})
	if err != nil || result.Status != SessionPrepareStatusReady {
		t.Fatalf("PrepareSession replacement retry = %+v err=%v", result, err)
	}
	if len(provider.releaseReasons) != 2 || provider.releaseReasons[0] != ReleaseReasonDelete || provider.releaseReasons[1] != ReleaseReasonDelete {
		t.Fatalf("replacement delete attempts = %v; want delete/delete", provider.releaseReasons)
	}
	if len(provider.createRequests) != 1 {
		t.Fatalf("replacement creates = %d; want 1 after delete success", len(provider.createRequests))
	}
	replaced := store.sandboxes["sandbox_from_preparation"]
	if replaced.Status != StatusActive || replaced.EnvironmentID != "env_test" || replaced.EnvironmentGeneration != 2 || replaced.ProviderSandboxID != provider.handle.SandboxID {
		t.Fatalf("replacement sandbox = %+v; want active current generation", replaced)
	}
}

func TestPrepareSessionDeleteOldProviderAckBeforeReplacementRecordReplaysOldIdentity(t *testing.T) {
	store := newRecordingStore()
	store.sandboxes["sandbox_from_preparation"] = &Sandbox{
		ID: "sandbox_from_preparation", WorkspaceID: workspace.DefaultID, SessionID: "sesn_test",
		EnvironmentID: "env_test", EnvironmentGeneration: 1, Status: StatusStopped,
		Provider: "unit-provider", ProviderSandboxID: "provider_old_ack_crash",
		CreatedAt: fixedSandboxTime.Add(-time.Hour), UpdatedAt: fixedSandboxTime.Add(-time.Minute),
	}
	store.prepareReplacementErr = errors.New("forced crash after provider delete ACK")
	provider := newSuccessfulRecordingProvider()
	preparations := &recordingSessionPreparationStore{preparation: SessionPreparation{
		WorkspaceID: workspace.DefaultID, SessionID: "sesn_test", PreparationAttemptID: "prep_delete_ack_crash",
		EnvironmentID: "env_test", EnvironmentGeneration: 2, SandboxID: "sandbox_from_preparation", Status: "pending",
	}}
	service := NewService(store, provider, WithProviderName("unit-provider"), WithClock(func() time.Time { return fixedSandboxTime }), WithSessionPreparationStore(preparations))

	if _, err := service.PrepareSession(context.Background(), SessionPrepareRequest{WorkspaceID: workspace.DefaultID, SessionID: "sesn_test", PreparationAttemptID: "prep_delete_ack_crash"}); err == nil {
		t.Fatal("PrepareSession crash window = nil")
	}
	old := store.sandboxes["sandbox_from_preparation"]
	if old.ProviderSandboxID != "provider_old_ack_crash" || old.Status != StatusStopped || len(provider.releaseHandles) != 1 || provider.releaseHandles[0].SandboxID != "provider_old_ack_crash" || len(provider.createRequests) != 0 {
		t.Fatalf("after ACK crash sandbox=%+v releases=%+v creates=%d; want old identity retained and no create", old, provider.releaseHandles, len(provider.createRequests))
	}
	store.prepareReplacementErr = nil
	result, err := service.PrepareSession(context.Background(), SessionPrepareRequest{WorkspaceID: workspace.DefaultID, SessionID: "sesn_test", PreparationAttemptID: "prep_delete_ack_crash"})
	if err != nil || result.Status != SessionPrepareStatusReady {
		t.Fatalf("PrepareSession replay=%+v err=%v", result, err)
	}
	if len(provider.releaseHandles) != 2 || provider.releaseHandles[1].SandboxID != "provider_old_ack_crash" || len(provider.createRequests) != 1 {
		t.Fatalf("replay releases=%+v creates=%d; want same old identity re-deleted before one create", provider.releaseHandles, len(provider.createRequests))
	}
}

func TestPrepareSessionLegacyUnknownEnvironmentGenerationForcesReplacement(t *testing.T) {
	store := newRecordingStore()
	store.sandboxes["sandbox_from_preparation"] = &Sandbox{
		ID: "sandbox_from_preparation", WorkspaceID: workspace.DefaultID, SessionID: "sesn_test",
		Status: StatusActive, Provider: "unit-provider", ProviderSandboxID: "provider_legacy_unknown_generation",
		CreatedAt: fixedSandboxTime.Add(-time.Hour), UpdatedAt: fixedSandboxTime.Add(-time.Minute),
	}
	provider := newSuccessfulRecordingProvider()
	preparations := &recordingSessionPreparationStore{preparation: SessionPreparation{
		WorkspaceID: workspace.DefaultID, SessionID: "sesn_test", PreparationAttemptID: "prep_legacy_replace",
		EnvironmentID: "env_test", EnvironmentGeneration: 7, SandboxID: "sandbox_from_preparation", Status: "pending",
	}}
	service := NewService(store, provider, WithProviderName("unit-provider"), WithClock(func() time.Time { return fixedSandboxTime }), WithSessionPreparationStore(preparations))

	result, err := service.PrepareSession(context.Background(), SessionPrepareRequest{WorkspaceID: workspace.DefaultID, SessionID: "sesn_test", PreparationAttemptID: "prep_legacy_replace"})
	if err != nil || result.Status != SessionPrepareStatusReady {
		t.Fatalf("PrepareSession legacy replacement = %+v err=%v", result, err)
	}
	if len(provider.startHandles) != 0 || len(provider.releaseReasons) != 1 || provider.releaseReasons[0] != ReleaseReasonDelete || len(provider.createRequests) != 1 {
		t.Fatalf("legacy lifecycle calls start=%d release=%v create=%d; want REPLACE delete-old-first", len(provider.startHandles), provider.releaseReasons, len(provider.createRequests))
	}
	got := store.sandboxes["sandbox_from_preparation"]
	if got.EnvironmentID != "env_test" || got.EnvironmentGeneration != 7 || got.ProviderSandboxID != provider.handle.SandboxID {
		t.Fatalf("legacy replacement = %+v; want stamped current generation", got)
	}
}

func TestPrepareSessionPostCreateTombstoneDeletesExactProviderMachine(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	preparations := &recordingSessionPreparationStore{
		preparation: SessionPreparation{WorkspaceID: workspace.DefaultID, SessionID: "sesn_test", PreparationAttemptID: "prep_post_create_tombstone", EnvironmentID: "env_test", EnvironmentGeneration: 1, SandboxID: "sandbox_post_create_tombstone", Status: "pending"},
	}
	store.postCreateDisposition = postCreatePreparationDeleted
	store.postCreateSettlement = func() {
		preparations.preparation.Status = SessionPrepareStatusFailed
		preparations.preparation.FailureReason = sessionDeletedPreparationFailureReason
	}
	service := NewService(store, provider, WithProviderName("unit-provider"), WithClock(func() time.Time { return fixedSandboxTime }),
		WithSessionPreparationStore(preparations), WithSessionResourcePreparer(&recordingSessionResourcePreparer{}))
	result, err := service.PrepareSession(context.Background(), SessionPrepareRequest{WorkspaceID: workspace.DefaultID, SessionID: "sesn_test", PreparationAttemptID: "prep_post_create_tombstone"})
	if err != nil || result.Status != SessionPrepareStatusFailed || result.FailureReason != sessionDeletedPreparationFailureReason {
		t.Fatalf("post-create tombstone result = %+v err=%v; want durable session-deleted failure", result, err)
	}
	if len(provider.createRequests) != 1 || len(provider.releaseReasons) != 1 || provider.releaseReasons[0] != ReleaseReasonDelete {
		t.Fatalf("provider create/release = %d/%v; want exact created machine deleted", len(provider.createRequests), provider.releaseReasons)
	}
	created := store.sandboxes["sandbox_post_create_tombstone"]
	if created == nil || created.ProviderSandboxID != "" || created.Status != StatusFailed || created.CleanupStatus != CleanupStatusReleased || created.CleanupMethod != "delete" {
		t.Fatalf("durable sandbox after post-create tombstone = %+v; want failed non-owning terminal state", created)
	}

	replay, err := service.PrepareSession(context.Background(), SessionPrepareRequest{WorkspaceID: workspace.DefaultID, SessionID: "sesn_test", PreparationAttemptID: "prep_post_create_tombstone"})
	if err != nil || replay.Status != SessionPrepareStatusFailed || replay.FailureReason != sessionDeletedPreparationFailureReason {
		t.Fatalf("post-create tombstone replay = %+v err=%v; want same durable failure", replay, err)
	}
	if len(provider.createRequests) != 1 || len(provider.releaseReasons) != 1 {
		t.Fatalf("provider calls after replay create=%d release=%v; want exactly-once/idempotent create cleanup", len(provider.createRequests), provider.releaseReasons)
	}
}

func TestPrepareSessionDoesNotRestartFailedSandbox(t *testing.T) {
	store := newRecordingStore()
	failedAt := fixedSandboxTime.Add(-2 * time.Minute)
	store.sandboxes["sandbox_from_preparation"] = &Sandbox{
		ID:                   "sandbox_from_preparation",
		WorkspaceID:          workspace.DefaultID,
		SessionID:            "sesn_test",
		Status:               StatusFailed,
		Provider:             "unit-provider",
		ProviderSandboxID:    "provider_sandbox_123",
		StartupFailureReason: string(SandboxErrorMountFailed),
		CleanupStatus:        CleanupStatusPending,
		CreatedAt:            fixedSandboxTime.Add(-time.Hour),
		UpdatedAt:            failedAt,
		FailedAt:             &failedAt,
	}
	provider := newSuccessfulRecordingProvider()
	preparer := &recordingSessionResourcePreparer{}
	preparations := &recordingSessionPreparationStore{
		preparation: SessionPreparation{
			WorkspaceID:          workspace.DefaultID,
			SessionID:            "sesn_test",
			PreparationAttemptID: "prep_test",
			EnvironmentID:        "env_test",
			SandboxID:            "sandbox_from_preparation",
			Status:               "pending",
		},
	}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithSessionPreparationStore(preparations),
		WithSessionResourcePreparer(preparer),
	)

	result, err := service.PrepareSession(context.Background(), SessionPrepareRequest{
		WorkspaceID:          workspace.DefaultID,
		SessionID:            "sesn_test",
		PreparationAttemptID: "prep_test",
	})
	if err != nil {
		t.Fatalf("PrepareSession: %v", err)
	}
	if result.Status != SessionPrepareStatusFailed || result.FailureReason != "sandbox_preparation_failed" {
		t.Fatalf("result = %+v; want failed sandbox_preparation_failed", result)
	}
	if len(provider.createRequests) != 0 || len(provider.startHandles) != 0 || len(preparer.setups) != 0 {
		t.Fatalf("side effects create=%d start=%d prepare=%d; want no failed-row restart", len(provider.createRequests), len(provider.startHandles), len(preparer.setups))
	}
	if len(provider.releaseHandles) != 1 {
		t.Fatalf("cleanup releases = %d; want one lease-reserved failed-row cleanup", len(provider.releaseHandles))
	}
	if containsString(store.events, "mark_active") {
		t.Fatalf("store events = %v; failed sandbox must not be reactivated", store.events)
	}
	got := store.sandboxes["sandbox_from_preparation"]
	if got == nil || got.Status != StatusFailed || got.FailedAt == nil ||
		got.CleanupStatus != CleanupStatusReleased || got.CleanupAttemptCount != 1 {
		t.Fatalf("sandbox after failed prepare = %+v; want failed row with completed reserved cleanup", got)
	}
	if len(preparations.failedUpdates) != 1 || preparations.failedUpdates[0].FailureReason != "sandbox_preparation_failed" {
		t.Fatalf("failed updates = %+v; want one sandbox_preparation_failed update", preparations.failedUpdates)
	}
}

func TestPrepareSessionPersistsResourceProjectionReadyMetadata(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	expiresAt := fixedSandboxTime.Add(24 * time.Hour)
	preparations := &recordingSessionPreparationStore{
		preparation: SessionPreparation{
			WorkspaceID:          workspace.DefaultID,
			SessionID:            "sesn_test",
			PreparationAttemptID: "prep_test",
			EnvironmentID:        "env_test",
			SandboxID:            "sandbox_from_preparation",
			Status:               "pending",
		},
		resources: ResourceSetup{
			Files: []FileMount{{ResourceID: "sesrsc_file", SourceFileID: "file_source", SessionFileID: "file_session", ObjectID: "obj_file", MountPath: "/mnt/session/uploads/file_session", ReadOnly: true}},
		},
	}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithSessionPreparationStore(preparations),
		WithSessionResourcePreparer(staticReadyMetadataPreparer{
			expiresAt:       expiresAt,
			resourceRoots:   `[{"path":"/mnt/session/uploads/file_session","mode":"read"}]`,
			clearFileMounts: true,
		}),
	)

	result, err := service.PrepareSession(context.Background(), SessionPrepareRequest{
		WorkspaceID:          workspace.DefaultID,
		SessionID:            "sesn_test",
		PreparationAttemptID: "prep_test",
	})
	if err != nil {
		t.Fatalf("PrepareSession: %v", err)
	}
	if result.Status != SessionPrepareStatusReady {
		t.Fatalf("result = %+v; want ready", result)
	}
	if len(preparations.readyUpdates) != 1 {
		t.Fatalf("ready updates = %d; want 1", len(preparations.readyUpdates))
	}
	update := preparations.readyUpdates[0]
	if update.ResourceCredExpiresAt == nil || !update.ResourceCredExpiresAt.Equal(expiresAt) {
		t.Fatalf("ResourceCredExpiresAt = %v; want %s", update.ResourceCredExpiresAt, expiresAt)
	}
	if update.ResourceRootsJSON != `[{"path":"/mnt/session/uploads/file_session","mode":"read"}]` {
		t.Fatalf("ResourceRootsJSON = %q; want file resource roots", update.ResourceRootsJSON)
	}
}

func TestPrepareSessionClearsResourceCredentialOnlyAfterSuccessfulResourcePreparation(t *testing.T) {
	store := newRecordingStore()
	active := activeRecordingSandbox()
	active.ID = "sandbox_from_preparation"
	store.sandboxes[active.ID] = active
	provider := newSuccessfulRecordingProvider()
	expiresAt := fixedSandboxTime.Add(24 * time.Hour)
	wantErr := &ProviderError{
		Provider:    "unit-provider",
		Stage:       StageMountResources,
		Kind:        ProviderErrorUnavailable,
		Retryable:   true,
		StatusCode:  503,
		SafeMessage: "staging unmount unavailable",
	}
	preparer := &recordingSessionResourcePreparer{err: wantErr, clearResourceCredential: true}
	preparations := &recordingSessionPreparationStore{
		preparation: SessionPreparation{
			WorkspaceID:           workspace.DefaultID,
			SessionID:             "sesn_test",
			PreparationAttemptID:  "prep_test",
			EnvironmentID:         "env_test",
			SandboxID:             active.ID,
			Status:                "pending",
			ResourceCredExpiresAt: &expiresAt,
		},
	}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithSessionPreparationStore(preparations),
		WithSessionResourcePreparer(preparer),
	)

	request := SessionPrepareRequest{
		WorkspaceID:          workspace.DefaultID,
		SessionID:            "sesn_test",
		PreparationAttemptID: "prep_test",
	}
	if _, err := service.PrepareSession(context.Background(), request); !errors.Is(err, wantErr) {
		t.Fatalf("first PrepareSession error = %v; want retryable resource cleanup failure", err)
	}
	if len(preparations.readyUpdates) != 0 {
		t.Fatalf("ready updates after failed cleanup = %d; want zero", len(preparations.readyUpdates))
	}
	if preparations.preparation.ResourceCredExpiresAt == nil || !preparations.preparation.ResourceCredExpiresAt.Equal(expiresAt) {
		t.Fatalf("stored preparation expiry after failure = %v; want preserved %s", preparations.preparation.ResourceCredExpiresAt, expiresAt)
	}

	preparer.err = nil
	result, err := service.PrepareSession(context.Background(), request)
	if err != nil {
		t.Fatalf("retry PrepareSession: %v", err)
	}
	if result.Status != SessionPrepareStatusReady {
		t.Fatalf("retry result = %+v; want ready", result)
	}
	if len(preparations.readyUpdates) != 1 {
		t.Fatalf("ready updates after successful retry = %d; want one", len(preparations.readyUpdates))
	}
	if preparations.readyUpdates[0].ResourceCredExpiresAt != nil {
		t.Fatalf("ready update expiry = %v; want nil after successful cleanup", preparations.readyUpdates[0].ResourceCredExpiresAt)
	}
}

func TestPrepareSessionCompensatesFileProjectionWhenMemoryPreparationFails(t *testing.T) {
	stageErr := errors.New("memory projection unavailable")
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	preparer := &recordingSessionResourcePreparer{returnReadyMetadata: true}
	memoryReader := &recordingMemorySnapshotReader{
		snapshots: map[string][]MemorySnapshotFile{
			"memstore_prepare": {{Path: "/notes.md", Content: "remember", ContentSHA256: "sha-notes"}},
		},
	}
	memoryMaterializer := &recordingMemoryStoreMaterializer{err: stageErr}
	preparations := &recordingSessionPreparationStore{
		preparation: SessionPreparation{
			WorkspaceID:          workspace.DefaultID,
			SessionID:            "sesn_test",
			PreparationAttemptID: "prep_test",
			EnvironmentID:        "env_test",
			SandboxID:            "sandbox_from_preparation",
			Status:               "pending",
		},
		resources: ResourceSetup{
			Files: []FileMount{{
				ResourceID:    "sesrsc_file",
				SourceFileID:  "file_source",
				SessionFileID: "file_session",
				ObjectID:      "obj_file",
				MountPath:     "/mnt/session/uploads/file_session",
				ReadOnly:      true,
			}},
			MemoryStores: []MemoryStoreMount{{
				ResourceID:    "sesrsc_memory",
				MemoryStoreID: "memstore_prepare",
				MountPath:     "/mnt/memory/project",
				Access:        "read_write",
			}},
		},
	}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithSessionPreparationStore(preparations),
		WithSessionResourcePreparer(preparer),
		WithMemoryProjection(memoryReader, memoryMaterializer),
	)

	result, err := service.PrepareSession(context.Background(), SessionPrepareRequest{
		WorkspaceID:          workspace.DefaultID,
		SessionID:            "sesn_test",
		PreparationAttemptID: "prep_test",
	})
	if err == nil {
		t.Fatal("PrepareSession succeeded; want retryable memory preparation error")
	}
	if !errors.Is(err, stageErr) {
		t.Fatalf("err = %v; want wrapped memory error", err)
	}
	if result.Status != "" {
		t.Fatalf("result = %+v; want no durable terminal result", result)
	}
	if len(preparations.readyUpdates) != 0 || len(preparations.failedUpdates) != 0 {
		t.Fatalf("preparation updates = ready %d failed %d; want no terminal update", len(preparations.readyUpdates), len(preparations.failedUpdates))
	}
	if len(preparer.compensations) != 1 {
		t.Fatalf("compensations = %d; want 1", len(preparer.compensations))
	}
	compensation := preparer.compensations[0]
	if compensation.SandboxID != "sandbox_from_preparation" ||
		len(compensation.Resources.Files) != 1 ||
		compensation.Resources.Files[0].ResourceID != "sesrsc_file" {
		t.Fatalf("compensation setup = %+v; want original file resource setup", compensation)
	}
}

func TestTPREP6PrepareSessionPersistsFirstFailingRepositoryIdentity(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	preparations := &recordingSessionPreparationStore{
		preparation: SessionPreparation{
			WorkspaceID:          workspace.DefaultID,
			SessionID:            "sesn_test",
			PreparationAttemptID: "prep_test",
			EnvironmentID:        "env_test",
			SandboxID:            "sandbox_from_preparation",
			Status:               "pending",
		},
		resources: ResourceSetup{
			GitHubRepositories: []GitHubRepositoryMount{
				{ResourceID: "sesrsc_ready", URL: "https://github.com/tetral-ai/ready", MountPath: "/workspace/ready"},
				{ResourceID: "sesrsc_repo", URL: "https://github.com/tetral-ai/private", MountPath: "/workspace/private"},
			},
		},
	}
	gitMaterializer := &recordingGitHubRepositoryMaterializer{
		cloneErr: &GitHubPreparationFailure{
			Reason: GitHubCredentialRequiredReason, ResourceID: "sesrsc_repo",
			ResourceURL: "https://github.com/tetral-ai/private", Cause: errors.New("no matching credential"),
		},
	}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithSessionPreparationStore(preparations),
		WithGitHubRepositoryPreparation(&recordingGitTicketRotator{}, gitMaterializer, "git.tetral.test"),
	)

	result, err := service.PrepareSession(context.Background(), SessionPrepareRequest{
		WorkspaceID:          workspace.DefaultID,
		SessionID:            "sesn_test",
		PreparationAttemptID: "prep_test",
	})
	if err != nil {
		t.Fatalf("PrepareSession: %v", err)
	}
	if result.Status != SessionPrepareStatusFailed || result.FailureReason != GitHubCredentialRequiredReason {
		t.Fatalf("result = %+v; want github_credential_required failure", result)
	}
	if len(preparations.readyUpdates) != 0 || len(preparations.failedUpdates) != 1 {
		t.Fatalf("preparation updates = ready %d failed %d", len(preparations.readyUpdates), len(preparations.failedUpdates))
	}
	failure := preparations.failedUpdates[0]
	if failure.FailureReason != GitHubCredentialRequiredReason || failure.LastErrorKind != GitHubCredentialRequiredReason || failure.Retryable {
		t.Fatalf("failure update = %+v; want terminal github_credential_required", failure)
	}
	if failure.FailureResourceID != "sesrsc_repo" || failure.FailureResourceURL != "https://github.com/tetral-ai/private" {
		t.Fatalf("failure identity = %q/%q; want failing repository", failure.FailureResourceID, failure.FailureResourceURL)
	}
	if len(gitMaterializer.calls) != 1 || len(gitMaterializer.calls[0].Repositories) != 2 {
		t.Fatalf("materializer calls = %#v; want ordered multi-repository preparation", gitMaterializer.calls)
	}
	if got := gitMaterializer.calls[0].Repositories; got[0].ResourceID != "sesrsc_ready" || got[1].ResourceID != "sesrsc_repo" {
		t.Fatalf("repository order = %#v; want ready repository before first failing repository", got)
	}
	if got := store.sandboxes["sandbox_from_preparation"]; got == nil || got.Status != StatusArchived || !got.MachineWasUsable {
		t.Fatalf("sandbox after github failure = %+v; want archived usable startup cleanup", got)
	}
}

func TestPrepareSessionTerminalFailureAfterActiveReuseRunsIdempotentCleanupFinalizer(t *testing.T) {
	store := newRecordingStore()
	active := activeRecordingSandbox()
	active.ID = "sandbox_from_preparation"
	store.sandboxes[active.ID] = active
	provider := newSuccessfulRecordingProvider()
	preparations := &recordingSessionPreparationStore{
		preparation: SessionPreparation{
			WorkspaceID:          workspace.DefaultID,
			SessionID:            "sesn_test",
			PreparationAttemptID: "prep_test",
			EnvironmentID:        "env_test",
			SandboxID:            active.ID,
			Status:               "pending",
		},
		resources: ResourceSetup{
			Files: []FileMount{{ResourceID: "sesrsc_file", SourceFileID: "file_source", SessionFileID: "file_session", ObjectID: "obj_file"}},
		},
	}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithSessionPreparationStore(preparations),
		WithSessionResourcePreparer(&recordingSessionResourcePreparer{err: &ValidationError{Message: "terminal projection failure"}}),
	)

	store.markFailedErr = errors.New("cleanup finalizer persistence unavailable")
	request := SessionPrepareRequest{WorkspaceID: workspace.DefaultID, SessionID: "sesn_test", PreparationAttemptID: "prep_test"}
	if _, err := service.PrepareSession(context.Background(), request); err == nil {
		t.Fatal("PrepareSession succeeded; want cleanup finalizer persistence error")
	}
	if len(preparations.failedUpdates) != 1 || preparations.preparation.Status != SessionPrepareStatusFailed {
		t.Fatalf("preparation failure state = status %q updates %d; want one durable failed transition", preparations.preparation.Status, len(preparations.failedUpdates))
	}
	if len(provider.releaseHandles) != 1 {
		t.Fatalf("release calls after first finalizer = %d; want 1", len(provider.releaseHandles))
	}

	replayEvents := []string{}
	store.eventsRef = &replayEvents
	provider.eventsRef = &replayEvents
	store.markFailedErr = nil
	result, err := service.PrepareSession(context.Background(), request)
	if err != nil {
		t.Fatalf("PrepareSession cleanup replay: %v", err)
	}
	if result.Status != SessionPrepareStatusFailed || result.FailureReason != "sandbox_preparation_failed" {
		t.Fatalf("replay result = %+v; want terminal failure", result)
	}
	if len(preparations.failedUpdates) != 1 {
		t.Fatalf("failed updates after replay = %d; want no duplicate preparation transition", len(preparations.failedUpdates))
	}
	if len(provider.releaseHandles) != 2 {
		t.Fatalf("release calls after cleanup replay = %d; want one lease-reserved re-drive after the first durable write failed", len(provider.releaseHandles))
	}
	claimIndex := slices.Index(replayEvents, "claim_startup_cleanup")
	releaseIndex := slices.Index(replayEvents, "release")
	if claimIndex < 0 || releaseIndex < 0 || claimIndex >= releaseIndex {
		t.Fatalf("cleanup replay events = %v; want durable lease claim before provider release", replayEvents)
	}
	got := store.sandboxes[active.ID]
	if got == nil || got.Status != StatusArchived || !got.MachineWasUsable || got.CleanupStatus != CleanupStatusReleased || got.CleanupAttemptCount != 1 {
		t.Fatalf("sandbox after cleanup replay = %+v; want archived usable sandbox with released cleanup", got)
	}
}

func TestPrepareSessionFailureAtCleanupCapDefersToReadOnlyObservation(t *testing.T) {
	store := newRecordingStore()
	active := activeRecordingSandbox()
	active.ID = "sandbox_cleanup_cap_preparation"
	active.CleanupAttemptCount = 20
	store.sandboxes[active.ID] = active
	provider := newSuccessfulRecordingProvider()
	preparations := &recordingSessionPreparationStore{
		preparation: SessionPreparation{
			WorkspaceID:          workspace.DefaultID,
			SessionID:            "sesn_test",
			PreparationAttemptID: "prep_cleanup_cap",
			EnvironmentID:        "env_test",
			SandboxID:            active.ID,
			Status:               "pending",
		},
		resources: ResourceSetup{
			Files: []FileMount{{ResourceID: "sesrsc_file", SourceFileID: "file_source", SessionFileID: "file_session", ObjectID: "obj_file"}},
		},
	}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithCleanupMaxAttempts(20),
		WithSessionPreparationStore(preparations),
		WithSessionResourcePreparer(&recordingSessionResourcePreparer{err: &ValidationError{Message: "terminal projection failure"}}),
	)

	result, err := service.PrepareSession(context.Background(), SessionPrepareRequest{
		WorkspaceID: workspace.DefaultID, SessionID: "sesn_test", PreparationAttemptID: "prep_cleanup_cap",
	})
	if err != nil || result.Status != SessionPrepareStatusFailed {
		t.Fatalf("PrepareSession result=%+v err=%v; want durable failed attempt", result, err)
	}
	if len(provider.releaseHandles) != 0 {
		t.Fatalf("provider releases = %d; want no unreserved call at cleanup cap", len(provider.releaseHandles))
	}
	got := store.sandboxes[active.ID]
	if got == nil || got.Status != StatusFailed || got.CleanupStatus != CleanupStatusPending || got.CleanupAttemptCount != 20 {
		t.Fatalf("sandbox after capped preparation failure = %+v; want pending read-only observation at count 20", got)
	}
}

func TestFailSessionPreparationAfterRetryExhaustionMarksAttemptFailed(t *testing.T) {
	store := newRecordingStore()
	store.sandboxes["sandbox_from_preparation"] = &Sandbox{
		ID:                "sandbox_from_preparation",
		WorkspaceID:       workspace.DefaultID,
		SessionID:         "sesn_test",
		Status:            StatusActive,
		MachineWasUsable:  true,
		Provider:          "unit-provider",
		ProviderSandboxID: "provider_sandbox_123",
		ProviderHandle: ProviderHandle{
			Provider:  "unit-provider",
			SandboxID: "provider_sandbox_123",
		},
		CreatedAt: fixedSandboxTime.Add(-time.Hour),
		UpdatedAt: fixedSandboxTime,
	}
	provider := newSuccessfulRecordingProvider()
	preparations := &recordingSessionPreparationStore{}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithSessionPreparationStore(preparations),
	)

	result, err := service.FailSessionPreparationAfterRetryExhaustion(context.Background(), SessionPrepareRequest{
		WorkspaceID:          workspace.DefaultID,
		SessionID:            "sesn_test",
		PreparationAttemptID: "prep_test",
	}, "session_prepare_error")
	if err != nil {
		t.Fatalf("FailSessionPreparationAfterRetryExhaustion: %v", err)
	}
	if result.Status != SessionPrepareStatusFailed || result.FailureReason != "session_prepare_error" {
		t.Fatalf("result = %+v; want failed session_prepare_error", result)
	}
	if len(preparations.failedUpdates) != 1 {
		t.Fatalf("failed updates = %d; want 1", len(preparations.failedUpdates))
	}
	failure := preparations.failedUpdates[0]
	if failure.FailedAt != fixedSandboxTime ||
		failure.FailureStage != "session_prepare" ||
		failure.LastErrorKind != "session_prepare_error" ||
		failure.FailureReason != "session_prepare_error" ||
		failure.Retryable {
		t.Fatalf("failure update = %+v; want terminal retry-exhausted session_prepare failure", failure)
	}
	if len(provider.releaseHandles) != 1 || provider.releaseHandles[0].SandboxID != "provider_sandbox_123" {
		t.Fatalf("release handles = %+v; want final-failure cleanup release", provider.releaseHandles)
	}
	got := store.sandboxes["sandbox_from_preparation"]
	if got == nil || got.Status != StatusArchived || !got.MachineWasUsable || got.CleanupStatus != CleanupStatusReleased ||
		got.StartupFailureReason != "session_prepare_error" || got.CleanupMethod != string(ReleaseReasonCleanup) ||
		got.CleanupAttemptCount != 1 {
		t.Fatalf("sandbox after retry-exhausted preparation = %+v; want archived usable row with released cleanup", got)
	}
}

func TestFailSessionPreparationDoesNotInferUsabilityFromResuming(t *testing.T) {
	store := newRecordingStore()
	store.sandboxes["sandbox_from_preparation"] = &Sandbox{
		ID:                "sandbox_from_preparation",
		WorkspaceID:       workspace.DefaultID,
		SessionID:         "sesn_test",
		Status:            StatusResuming,
		Provider:          "unit-provider",
		ProviderSandboxID: "provider_sandbox_123",
		ProviderHandle: ProviderHandle{
			Provider:  "unit-provider",
			SandboxID: "provider_sandbox_123",
		},
		CreatedAt: fixedSandboxTime.Add(-time.Hour),
		UpdatedAt: fixedSandboxTime,
	}
	preparations := &recordingSessionPreparationStore{}
	service := NewService(store, newSuccessfulRecordingProvider(),
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithSessionPreparationStore(preparations),
	)

	result, err := service.FailSessionPreparationAfterRetryExhaustion(context.Background(), SessionPrepareRequest{
		WorkspaceID: workspace.DefaultID, SessionID: "sesn_test", PreparationAttemptID: "prep_test",
	}, "session_prepare_error")
	if err != nil {
		t.Fatalf("FailSessionPreparationAfterRetryExhaustion: %v", err)
	}
	got := store.sandboxes["sandbox_from_preparation"]
	if result.Status != SessionPrepareStatusFailed || got == nil || got.Status != StatusFailed || got.MachineWasUsable {
		t.Fatalf("result=%+v sandbox=%+v; want failed without inferred usability", result, got)
	}
}

type staticReadyMetadataPreparer struct {
	expiresAt       time.Time
	resourceRoots   string
	clearFileMounts bool
}

func (p staticReadyMetadataPreparer) PrepareSessionResources(_ context.Context, setup SandboxSetup, _ ProviderHandle) (ResourceSetup, error) {
	prepared := cloneResourceSetup(setup.Resources)
	if p.clearFileMounts {
		prepared.Files = nil
	}
	prepared.ResourceCredExpiresAt = &p.expiresAt
	prepared.ResourceRootsJSON = p.resourceRoots
	return prepared, nil
}

type recordingSessionResourcePreparer struct {
	setups                  []SandboxSetup
	compensations           []SandboxSetup
	err                     error
	compensationErr         error
	returnReadyMetadata     bool
	clearResourceCredential bool
	eventsRef               *[]string
}

func (p *recordingSessionResourcePreparer) PrepareSessionResources(_ context.Context, setup SandboxSetup, _ ProviderHandle) (ResourceSetup, error) {
	if p.eventsRef != nil {
		*p.eventsRef = append(*p.eventsRef, "resource_prepare")
	}
	recorded := setup
	recorded.Resources = cloneResourceSetup(setup.Resources)
	p.setups = append(p.setups, recorded)
	if p.err != nil {
		return ResourceSetup{}, p.err
	}
	prepared := cloneResourceSetup(setup.Resources)
	if p.clearResourceCredential {
		prepared.ResourceCredExpiresAt = nil
	}
	if p.returnReadyMetadata {
		prepared.Files = nil
		prepared.ResourceRootsJSON = `[{"path":"/mnt/session/uploads/file_session","mode":"read"}]`
		expiresAt := fixedSandboxTime.Add(24 * time.Hour)
		prepared.ResourceCredExpiresAt = &expiresAt
	}
	return prepared, nil
}

func (p *recordingSessionResourcePreparer) CompensateSessionResourcePreparation(_ context.Context, setup SandboxSetup, _ ProviderHandle) error {
	recorded := setup
	recorded.Resources = cloneResourceSetup(setup.Resources)
	p.compensations = append(p.compensations, recorded)
	return p.compensationErr
}

func TestPrepareSessionRetriesTransientResourceFailureWithoutFailingAttempt(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	preparer := &recordingSessionResourcePreparer{err: &ProviderError{
		Provider:    "unit-provider",
		Stage:       StageMountResources,
		Kind:        ProviderErrorUnavailable,
		Retryable:   true,
		StatusCode:  503,
		SafeMessage: "daytona resource preparation unavailable",
	}}
	preparations := &recordingSessionPreparationStore{
		preparation: SessionPreparation{
			WorkspaceID:          workspace.DefaultID,
			SessionID:            "sesn_test",
			PreparationAttemptID: "prep_test",
			EnvironmentID:        "env_test",
			SandboxID:            "sandbox_from_preparation",
			Status:               "pending",
		},
		resources: ResourceSetup{
			Files: []FileMount{{ResourceID: "sesrsc_file", SourceFileID: "file_source", SessionFileID: "file_session", MountPath: "/mnt/session/uploads/file_session", ReadOnly: true}},
		},
	}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithSessionPreparationStore(preparations),
		WithSessionResourcePreparer(preparer),
	)

	result, err := service.PrepareSession(context.Background(), SessionPrepareRequest{
		WorkspaceID:          workspace.DefaultID,
		SessionID:            "sesn_test",
		PreparationAttemptID: "prep_test",
	})
	if err == nil {
		t.Fatal("PrepareSession succeeded; want retryable resource preparation error")
	}
	if result.Status != "" {
		t.Fatalf("result = %+v; want no durable terminal result", result)
	}
	if len(preparations.readyUpdates) != 0 || len(preparations.failedUpdates) != 0 {
		t.Fatalf("preparation updates = ready %d failed %d; want no terminal update", len(preparations.readyUpdates), len(preparations.failedUpdates))
	}
	got := store.sandboxes["sandbox_from_preparation"]
	if got == nil || got.Status != StatusActive || got.FailedAt != nil {
		t.Fatalf("sandbox after retryable resource failure = %+v; want active for retry", got)
	}
	if len(provider.releaseHandles) != 0 {
		t.Fatalf("release handles = %+v; want no terminal cleanup for retryable failure", provider.releaseHandles)
	}
}

func TestPrepareSessionReplaysFailedPreparationReason(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	preparations := &recordingSessionPreparationStore{
		preparation: SessionPreparation{
			WorkspaceID:          workspace.DefaultID,
			SessionID:            "sesn_test",
			PreparationAttemptID: "prep_test",
			EnvironmentID:        "env_test",
			SandboxID:            "sandbox_from_preparation",
			Status:               SessionPrepareStatusFailed,
			FailureReason:        GitHubCredentialRequiredReason,
		},
	}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithSessionPreparationStore(preparations),
	)

	result, err := service.PrepareSession(context.Background(), SessionPrepareRequest{
		WorkspaceID:          workspace.DefaultID,
		SessionID:            "sesn_test",
		PreparationAttemptID: "prep_test",
	})
	if err != nil {
		t.Fatalf("PrepareSession: %v", err)
	}
	if result.Status != SessionPrepareStatusFailed || result.FailureReason != GitHubCredentialRequiredReason {
		t.Fatalf("result = %+v; want replayed github_credential_required failure", result)
	}
	if len(provider.createRequests) != 0 || len(preparations.readyUpdates) != 0 || len(preparations.failedUpdates) != 0 {
		t.Fatalf("unexpected side effects: provider=%d ready=%d failed=%d", len(provider.createRequests), len(preparations.readyUpdates), len(preparations.failedUpdates))
	}
}

func TestPrepareSessionReturnsWaitingWhileUsableMachineCleanupIsPending(t *testing.T) {
	for _, cleanupStatus := range []CleanupStatus{
		CleanupStatusPending,
		CleanupStatusInProgress,
		CleanupStatusRetryableFailed,
	} {
		t.Run(string(cleanupStatus), func(t *testing.T) {
			store := newRecordingStore()
			store.sandboxes["sandbox_from_preparation"] = &Sandbox{
				ID:                   "sandbox_from_preparation",
				WorkspaceID:          workspace.DefaultID,
				SessionID:            "sesn_test",
				Status:               StatusFailed,
				Provider:             "unit-provider",
				ProviderSandboxID:    "provider_sandbox_123",
				MachineWasUsable:     true,
				StartupFailureReason: "startup_interrupted",
				CleanupStatus:        cleanupStatus,
				CreatedAt:            fixedSandboxTime.Add(-time.Hour),
				UpdatedAt:            fixedSandboxTime,
			}
			provider := newSuccessfulRecordingProvider()
			preparations := &recordingSessionPreparationStore{
				preparation: SessionPreparation{
					WorkspaceID:          workspace.DefaultID,
					SessionID:            "sesn_test",
					PreparationAttemptID: "prep_test",
					EnvironmentID:        "env_test",
					SandboxID:            "sandbox_from_preparation",
					Status:               "pending",
				},
			}
			service := NewService(store, provider,
				WithProviderName("unit-provider"),
				WithClock(func() time.Time { return fixedSandboxTime }),
				WithSessionPreparationStore(preparations),
			)

			result, err := service.PrepareSession(context.Background(), SessionPrepareRequest{
				WorkspaceID: workspace.DefaultID, SessionID: "sesn_test", PreparationAttemptID: "prep_test",
			})
			if err != nil {
				t.Fatalf("PrepareSession: %v", err)
			}
			if result.Status != SessionPrepareStatusWaitingOnMachine {
				t.Fatalf("result = %+v; want waiting_on_machine", result)
			}
			if len(provider.calls) != 0 || len(preparations.readyUpdates) != 0 || len(preparations.failedUpdates) != 0 {
				t.Fatalf("waiting side effects provider=%v ready=%d failed=%d", provider.calls, len(preparations.readyUpdates), len(preparations.failedUpdates))
			}
		})
	}
}

func TestPrepareSessionNoopsSupersededFailedPreparation(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	preparations := &recordingSessionPreparationStore{
		staleAttempt: true,
		preparation: SessionPreparation{
			WorkspaceID:          workspace.DefaultID,
			SessionID:            "sesn_test",
			PreparationAttemptID: "prep_old",
			EnvironmentID:        "env_test",
			SandboxID:            "sandbox_from_preparation",
			Status:               SessionPrepareStatusFailed,
			FailureReason:        GitHubCredentialRequiredReason,
		},
	}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithSessionPreparationStore(preparations),
	)

	result, err := service.PrepareSession(context.Background(), SessionPrepareRequest{
		WorkspaceID:          workspace.DefaultID,
		SessionID:            "sesn_test",
		PreparationAttemptID: "prep_old",
	})
	if err != nil {
		t.Fatalf("PrepareSession: %v", err)
	}
	if result.Status != SessionPrepareStatusNoop {
		t.Fatalf("result = %+v; want stale failed attempt noop", result)
	}
	if len(provider.createRequests) != 0 || len(preparations.readyUpdates) != 0 || len(preparations.failedUpdates) != 0 {
		t.Fatalf("unexpected side effects: provider=%d ready=%d failed=%d", len(provider.createRequests), len(preparations.readyUpdates), len(preparations.failedUpdates))
	}
}

type recordingSessionPreparationStore struct {
	preparation      SessionPreparation
	resources        ResourceSetup
	claimErr         error
	resourceErr      error
	readyErr         error
	failedErr        error
	claims           []SessionPrepareRequest
	readyUpdates     []SessionPreparationReadyUpdate
	failedUpdates    []SessionPreparationFailureUpdate
	staleAttempt     bool
	staleAfterCreate bool
	eventsRef        *[]string
	detached         []string
	detachErrs       []error
}

func (s *recordingSessionPreparationStore) SessionPreparationStillCurrent(context.Context, workspace.ID, string, string) (bool, error) {
	return !s.staleAfterCreate, nil
}

func (s *recordingSessionPreparationStore) ClaimSessionPreparation(_ context.Context, ws workspace.ID, sessionID string, preparationAttemptID string, _ time.Time) (SessionPreparation, bool, error) {
	s.claims = append(s.claims, SessionPrepareRequest{WorkspaceID: ws, SessionID: sessionID, PreparationAttemptID: preparationAttemptID})
	if s.claimErr != nil {
		return SessionPreparation{}, false, s.claimErr
	}
	preparation := s.preparation
	if preparation.ProviderArtifactRef == "" {
		preparation.ProviderArtifactRef = "artifact_env_test"
	}
	if preparation.Network.Type == "" {
		preparation.Network = NetworkSetup{Type: "unrestricted"}
	}
	preparation.IsCurrent = !s.staleAttempt
	if s.staleAttempt {
		return preparation, false, nil
	}
	if preparation.Status != "pending" && preparation.Status != "preparing" {
		return preparation, false, nil
	}
	preparation.Status = "preparing"
	s.preparation = preparation
	return preparation, true, nil
}

func (s *recordingSessionPreparationStore) ListSessionPreparationResources(context.Context, workspace.ID, string) (ResourceSetup, error) {
	if s.resourceErr != nil {
		return ResourceSetup{}, s.resourceErr
	}
	return cloneResourceSetup(s.resources), nil
}

func (s *recordingSessionPreparationStore) CheckSessionPreparationResourceCleanup(_ context.Context, _ workspace.ID, _ string, _ string, resourceID string) (bool, error) {
	if s.staleAttempt {
		return false, ErrStalePreparationAttempt
	}
	for _, detachedID := range s.detached {
		if detachedID == resourceID {
			return false, nil
		}
	}
	for _, file := range s.resources.DeletedFiles {
		if file.ResourceID == resourceID {
			return true, nil
		}
	}
	for _, memoryStore := range s.resources.DeletedMemoryStores {
		if memoryStore.ResourceID == resourceID {
			return true, nil
		}
	}
	for _, repository := range s.resources.DeletedGitHubRepositories {
		if repository.ResourceID == resourceID {
			return true, nil
		}
	}
	return false, nil
}

func (s *recordingSessionPreparationStore) DetachSessionPreparationResource(_ context.Context, _ workspace.ID, _ string, _ string, resourceID string, _ time.Time) error {
	if s.staleAttempt {
		return ErrStalePreparationAttempt
	}
	if len(s.detachErrs) > 0 {
		err := s.detachErrs[0]
		s.detachErrs = s.detachErrs[1:]
		if err != nil {
			return err
		}
	}
	s.detached = append(s.detached, resourceID)
	if s.eventsRef != nil {
		*s.eventsRef = append(*s.eventsRef, "resource_detach")
	}
	return nil
}

func (s *recordingSessionPreparationStore) MarkSessionPreparationReady(_ context.Context, _ workspace.ID, _ string, _ string, update SessionPreparationReadyUpdate) error {
	s.readyUpdates = append(s.readyUpdates, update)
	return s.readyErr
}

func (s *recordingSessionPreparationStore) MarkSessionPreparationFailed(_ context.Context, _ workspace.ID, _ string, _ string, update SessionPreparationFailureUpdate) error {
	s.failedUpdates = append(s.failedUpdates, update)
	if s.failedErr == nil {
		s.preparation.Status = SessionPrepareStatusFailed
		s.preparation.FailureReason = update.FailureReason
	}
	return s.failedErr
}
