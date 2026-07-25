package sandbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/gitticket"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestCreateForSessionOrchestratesProviderSetupWithStableHandle(t *testing.T) {
	ctx := context.Background()
	store := newRecordingStore()
	provider := &recordingProvider{
		handle: ProviderHandle{
			Provider:  "unit-provider",
			SandboxID: "provider_sandbox_123",
			Metadata:  map[string]string{"region": "iad"},
		},
	}
	memoryReader := &recordingMemorySnapshotReader{
		snapshots: map[string][]MemorySnapshotFile{
			"mem_store": {{Path: "/notes.md", Content: "remember", ContentSHA256: "sha-notes"}},
		},
	}
	memoryMaterializer := &recordingMemoryStoreMaterializer{}
	gitRotator := &recordingGitTicketRotator{}
	gitMaterializer := &recordingGitHubRepositoryMaterializer{}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithIDStrategy(func() string { return "sandbox_test" }),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithMemoryProjection(memoryReader, memoryMaterializer),
		WithGitHubRepositoryPreparation(gitRotator, gitMaterializer, "git.tetral.test"),
		WithGitTicketRandomSource(bytes.NewReader(make([]byte, gitticket.TokenBytes))),
	)

	request := CreateForSessionRequest{
		WorkspaceID:         workspace.DefaultID,
		SessionID:           "sesn_test",
		EnvironmentID:       "env_test",
		ProviderArtifactRef: "artifact_env_test",
		Network: NetworkSetup{
			Type:             "cidr_allow_list",
			NetworkAllowList: "10.0.0.0/8",
		},
		Resources: ResourceSetup{
			Files: []FileMount{{
				ResourceID:    "sesrsc_file",
				SourceFileID:  "file_source",
				SessionFileID: "file_session",
				MountPath:     "/workspace/input.txt",
				ReadOnly:      true,
			}},
			MemoryStores: []MemoryStoreMount{{
				ResourceID:    "sesrsc_memory",
				MemoryStoreID: "mem_store",
				MountPath:     "/mnt/memory/project-memory",
				Access:        "read_only",
				Instructions:  "use carefully",
				Name:          "Project Memory",
				Description:   "stable snapshot",
			}},
			GitHubRepositories: []GitHubRepositoryMount{{
				ResourceID:   "sesrsc_repo",
				URL:          "https://github.com/tetral-ai/tetral",
				MountPath:    "/workspace/tetral",
				CheckoutType: "branch",
				CheckoutRef:  "main",
			}},
		},
	}

	got, err := service.CreateForSession(ctx, request)
	if err != nil {
		t.Fatalf("CreateForSession: %v", err)
	}

	if got.ID != "sandbox_test" || got.Status != StatusActive || got.ProviderSandboxID != "provider_sandbox_123" {
		t.Fatalf("sandbox = %+v; want active sandbox with provider handle", got)
	}
	wantCalls := []string{"create", "health", "base_directories"}
	if !reflect.DeepEqual(provider.calls, wantCalls) {
		t.Fatalf("provider calls = %v; want %v", provider.calls, wantCalls)
	}
	if len(provider.createRequests) != 1 {
		t.Fatalf("create requests = %d; want 1", len(provider.createRequests))
	}
	setup := provider.createRequests[0].Setup
	if setup.WorkspaceID != workspace.DefaultID || setup.SessionID != "sesn_test" || setup.SandboxID != "sandbox_test" || setup.EnvironmentID != "env_test" {
		t.Fatalf("setup identity = %+v", setup)
	}
	if setup.ProviderArtifactRef != "artifact_env_test" {
		t.Fatalf("provider artifact ref = %q; want artifact_env_test", setup.ProviderArtifactRef)
	}
	if !reflect.DeepEqual(setup.Network, request.Network) {
		t.Fatalf("network = %#v; want %#v", setup.Network, request.Network)
	}
	if !reflect.DeepEqual(setup.Resources, request.Resources) {
		t.Fatalf("resources = %#v; want %#v", setup.Resources, request.Resources)
	}
	for _, handle := range []ProviderHandle{provider.healthHandles[0], provider.baseDirectoryHandles[0]} {
		if !reflect.DeepEqual(handle, provider.handle) {
			t.Fatalf("provider stage handle = %#v; want %#v", handle, provider.handle)
		}
	}
	if !reflect.DeepEqual(memoryReader.reads, []string{"mem_store"}) {
		t.Fatalf("memory snapshot reads = %v; want mem_store", memoryReader.reads)
	}
	if len(memoryMaterializer.calls) != 1 ||
		memoryMaterializer.calls[0].ProviderSandboxID != "provider_sandbox_123" ||
		memoryMaterializer.calls[0].MountPath != "/mnt/memory/project-memory" ||
		len(memoryMaterializer.calls[0].Files) != 1 ||
		memoryMaterializer.calls[0].Files[0].Content != "remember" {
		t.Fatalf("memory materializations = %+v; want provider snapshot for mounted memory store", memoryMaterializer.calls)
	}
	if len(gitRotator.pendingCalls) != 1 || len(gitRotator.activationCalls) != 1 {
		t.Fatalf("git ticket pending/activations = %d/%d; want 1/1", len(gitRotator.pendingCalls), len(gitRotator.activationCalls))
	}
	if got := gitRotator.pendingCalls[0]; got.ws != workspace.DefaultID || got.sessionID != "sesn_test" || got.ticketID == "" || got.when != fixedSandboxTime {
		t.Fatalf("git ticket rotation = %+v; want session-scoped rotation", got)
	}
	if gitRotator.activationCalls[0].ticketID != gitRotator.pendingCalls[0].ticketID {
		t.Fatalf("activated ticket = %q; want pending %q", gitRotator.activationCalls[0].ticketID, gitRotator.pendingCalls[0].ticketID)
	}
	if len(gitMaterializer.installs) != 1 || len(gitMaterializer.calls) != 1 {
		t.Fatalf("github config/clone calls = %d/%d; want 1/1", len(gitMaterializer.installs), len(gitMaterializer.calls))
	}
	gitCall := gitMaterializer.calls[0]
	if gitCall.WorkspaceID != workspace.DefaultID || gitCall.SessionID != "sesn_test" || gitCall.ProviderSandboxID != "provider_sandbox_123" ||
		len(gitCall.Repositories) != 1 || gitCall.Repositories[0].URL != "https://github.com/tetral-ai/tetral" {
		t.Fatalf("github materializer call = %+v; want session repo preparation", gitCall)
	}
	configuration := gitMaterializer.installs[0]
	if configuration.GitProxyHost != "git.tetral.test" {
		t.Fatalf("git proxy host = %q; want git.tetral.test", configuration.GitProxyHost)
	}
	hash, err := gitticket.HashToken(configuration.Ticket)
	if err != nil {
		t.Fatalf("materializer ticket is invalid: %v", err)
	}
	if !bytes.Equal(gitRotator.pendingCalls[0].tokenHash, hash) {
		t.Fatalf("pending token hash = %x; want hash of materializer ticket %x", gitRotator.pendingCalls[0].tokenHash, hash)
	}
	wantStoreEvents := []string{"create", "save_handle", "mark_active"}
	if !reflect.DeepEqual(store.events, wantStoreEvents) {
		t.Fatalf("store events = %v; want %v", store.events, wantStoreEvents)
	}
}

func TestStartCreatedSandboxMarksActiveBeforeResourceMaterialization(t *testing.T) {
	ctx := context.Background()
	events := []string{}
	store := newRecordingStore()
	store.eventsRef = &events
	provider := newSuccessfulRecordingProvider()
	provider.eventsRef = &events
	memoryReader := &recordingMemorySnapshotReader{
		snapshots: map[string][]MemorySnapshotFile{
			"memstore_prepare": {{Path: "/todo.md", Content: "ship", ContentSHA256: "sha-ship"}},
		},
	}
	memoryMaterializer := &recordingMemoryStoreMaterializer{eventsRef: &events}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithIDStrategy(func() string { return "sandbox_test" }),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithMemoryProjection(memoryReader, memoryMaterializer),
	)
	request := minimalCreateRequest()
	request.Resources.MemoryStores = []MemoryStoreMount{{
		ResourceID:    "sesrsc_memory",
		MemoryStoreID: "memstore_prepare",
		MountPath:     "/mnt/memory/project",
		Access:        "read_write",
	}}

	created, err := service.CreateSandboxRecordForSession(ctx, request)
	if err != nil {
		t.Fatalf("CreateSandboxRecordForSession: %v", err)
	}
	active, err := service.StartCreatedSandbox(ctx, created, request)
	if err != nil {
		t.Fatalf("StartCreatedSandbox: %v", err)
	}
	if active.Status != StatusActive {
		t.Fatalf("status = %s; want active", active.Status)
	}
	wantEvents := []string{"create", "create", "save_handle", "health", "mark_active", "base_directories", "memory"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %v; want %v", events, wantEvents)
	}
}

func TestStartCreatedSandboxUsesCommittedSandboxRowAndFinalNetworkPolicy(t *testing.T) {
	ctx := context.Background()
	events := []string{}
	store := newRecordingStore()
	store.eventsRef = &events
	provider := newSuccessfulRecordingProvider()
	provider.eventsRef = &events
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithIDStrategy(func() string { return "sandbox_test" }),
		WithClock(func() time.Time { return fixedSandboxTime }),
	)

	created, err := service.CreateSandboxRecordForSession(ctx, minimalCreateRequest())
	if err != nil {
		t.Fatalf("CreateSandboxRecordForSession: %v", err)
	}
	if created.Status != StatusCreating || created.ProviderSandboxID != "" {
		t.Fatalf("created sandbox = %+v; want creating without provider handle", created)
	}

	active, err := service.StartCreatedSandbox(ctx, created, minimalCreateRequest())
	if err != nil {
		t.Fatalf("StartCreatedSandbox: %v", err)
	}
	if active.Status != StatusActive || active.ProviderSandboxID != "provider_sandbox_123" {
		t.Fatalf("active sandbox = %+v; want active with provider handle", active)
	}
	wantEvents := []string{"create", "create", "save_handle", "health", "mark_active", "base_directories"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %v; want %v", events, wantEvents)
	}
}

func TestCreateForSessionReleasesProviderHandleAndPreservesStageError(t *testing.T) {
	cases := []struct {
		name       string
		configure  func(*recordingProvider, error)
		wantStage  ProviderStage
		wantCode   SandboxErrorCode
		wantCalls  []string
		kind       ProviderErrorKind
		retryable  bool
		wantStatus Status
		wantUsable bool
	}{
		{
			name: "health",
			configure: func(provider *recordingProvider, err error) {
				provider.healthErr = err
			},
			wantStage:  StageCheckBaseTemplate,
			wantCode:   SandboxErrorBaseTemplateFailed,
			wantCalls:  []string{"create", "health", "release"},
			kind:       ProviderErrorUnavailable,
			retryable:  true,
			wantStatus: StatusFailed,
			wantUsable: false,
		},
		{
			name: "base directories",
			configure: func(provider *recordingProvider, err error) {
				provider.baseDirectoryErr = err
			},
			wantStage:  StageMountResources,
			wantCode:   SandboxErrorMountFailed,
			wantCalls:  []string{"create", "health", "base_directories", "release"},
			kind:       ProviderErrorInvalidRequest,
			retryable:  false,
			wantStatus: StatusArchived,
			wantUsable: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stageErr := &ProviderError{
				Provider:    "unit-provider",
				Stage:       tc.wantStage,
				Kind:        tc.kind,
				Retryable:   tc.retryable,
				StatusCode:  503,
				SafeMessage: "sandbox setup failed",
				Cause:       errors.New("internal provider detail"),
			}
			store := newRecordingStore()
			provider := newSuccessfulRecordingProvider()
			tc.configure(provider, stageErr)
			service := newTestService(store, provider)

			_, err := service.CreateForSession(context.Background(), minimalCreateRequest())
			if err == nil {
				t.Fatal("CreateForSession succeeded; want provider stage failure")
			}
			var sandboxErr *SandboxError
			if !errors.As(err, &sandboxErr) || sandboxErr.Code != tc.wantCode {
				t.Fatalf("err = %T %v; want SandboxError code %s", err, err, tc.wantCode)
			}
			var providerErr *ProviderError
			if !errors.As(err, &providerErr) || providerErr.Stage != tc.wantStage {
				t.Fatalf("errors.As ProviderError = %+v; want stage %s", providerErr, tc.wantStage)
			}
			if len(provider.releaseHandles) != 1 || !reflect.DeepEqual(provider.releaseHandles[0], provider.handle) {
				t.Fatalf("release handles = %#v; want exactly provider handle", provider.releaseHandles)
			}
			if !reflect.DeepEqual(provider.calls, tc.wantCalls) {
				t.Fatalf("provider calls = %v; want %v", provider.calls, tc.wantCalls)
			}
			got := store.sandboxes["sandbox_test"]
			if got.Status != tc.wantStatus || got.MachineWasUsable != tc.wantUsable || got.FailedAt == nil {
				t.Fatalf("sandbox after failed setup = %+v; want status %q usable=%v with failed_at", got, tc.wantStatus, tc.wantUsable)
			}
			if got.CleanupStatus != CleanupStatusReleased || got.CleanupMethod != string(ReleaseReasonCleanup) || got.CleanupAttemptCount != 1 {
				t.Fatalf("cleanup state after failed setup = status %q method %q attempts %d; want released cleanup attempt", got.CleanupStatus, got.CleanupMethod, got.CleanupAttemptCount)
			}
			if !containsString(store.events, "mark_failed") {
				t.Fatalf("store events = %v; want mark_failed cleanup", store.events)
			}
		})
	}
}

func TestCreateForSessionLeavesCappedInlineFailureForReadOnlyObservation(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	provider.healthErr = &ProviderError{
		Provider: "unit-provider", Stage: StageCheckBaseTemplate,
		Kind: ProviderErrorUnavailable, Retryable: true,
	}
	provider.releaseErr = &ProviderError{
		Provider: "unit-provider", Stage: StageReleaseSandbox,
		Kind: ProviderErrorTimeout, Retryable: true,
	}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithIDStrategy(func() string { return "sandbox_test" }),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithCleanupMaxAttempts(1),
	)

	if _, err := service.CreateForSession(context.Background(), minimalCreateRequest()); err == nil {
		t.Fatal("CreateForSession succeeded; want startup and cleanup failure")
	}
	got := store.sandboxes["sandbox_test"]
	if got == nil || got.CleanupStatus != CleanupStatusRetryableFailed || !got.CleanupRetryable ||
		got.CleanupAttemptCount != 1 {
		t.Fatalf("cleanup result = %+v; want recorded retryable failure at configured cap", got)
	}
	provider.status = ProviderStatus{Availability: ProviderAvailable, SandboxStatus: StatusArchived}
	got, err := service.ReconcileStartupCleanup(context.Background(), workspace.DefaultID, got.ID)
	if err != nil {
		t.Fatalf("ReconcileStartupCleanup cap observation: %v", err)
	}
	if got == nil || got.CleanupStatus != CleanupStatusReleased || got.CleanupAttemptCount != 1 {
		t.Fatalf("cap observation result = %+v; want released without another recorded attempt", got)
	}
	if len(provider.releaseHandles) != 1 {
		t.Fatalf("release calls = %d; want only the inline cleanup call", len(provider.releaseHandles))
	}
}

func TestCreateForSessionReplacementStartsFreshCleanupAttemptLineage(t *testing.T) {
	store := newRecordingStore()
	current := activeRecordingSandbox()
	current.EnvironmentID = "env_old"
	current.EnvironmentGeneration = 1
	current.CleanupAttemptCount = 19
	store.sandboxes[current.ID] = current
	provider := newSuccessfulRecordingProvider()
	provider.healthErr = &ProviderError{
		Provider: "unit-provider", Stage: StageCheckBaseTemplate,
		Kind: ProviderErrorUnavailable, Retryable: true,
	}
	cleanupErr := &ProviderError{
		Provider: "unit-provider", Stage: StageReleaseSandbox,
		Kind: ProviderErrorTimeout, Retryable: true,
	}
	provider.releaseErrors = []error{nil, cleanupErr}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithCleanupMaxAttempts(20),
	)
	request := minimalCreateRequest()
	request.SandboxID = current.ID
	request.EnvironmentGeneration = 2

	_, _, err := service.prepareClaimedSession(context.Background(), request)
	if err == nil {
		t.Fatal("prepareClaimedSession replacement succeeded; want base-template and cleanup failure")
	}
	got := store.sandboxes[current.ID]
	if got == nil || got.CleanupStatus != CleanupStatusRetryableFailed || !got.CleanupRetryable ||
		got.CleanupAttemptCount != 1 {
		t.Fatalf("replacement cleanup error=%v result=%+v; want first attempt in the replacement lineage", err, got)
	}
	if !reflect.DeepEqual(provider.releaseReasons, []ReleaseReason{ReleaseReasonDelete, ReleaseReasonCleanup}) {
		t.Fatalf("release reasons = %v; want delete-old then failed-startup cleanup", provider.releaseReasons)
	}
}

func TestCreateForSessionRetryableMemoryProjectionFailureLeavesActiveForRetry(t *testing.T) {
	stageErr := errors.New("memory projection unavailable")
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	memoryReader := &recordingMemorySnapshotReader{
		snapshots: map[string][]MemorySnapshotFile{
			"memstore_prepare": {{Path: "/todo.md", Content: "ship", ContentSHA256: "sha-ship"}},
		},
	}
	memoryMaterializer := &recordingMemoryStoreMaterializer{err: stageErr}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithIDStrategy(func() string { return "sandbox_test" }),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithMemoryProjection(memoryReader, memoryMaterializer),
	)
	request := minimalCreateRequest()
	request.Resources.MemoryStores = []MemoryStoreMount{{
		ResourceID:    "sesrsc_memory",
		MemoryStoreID: "memstore_prepare",
		MountPath:     "/mnt/memory/project",
		Access:        "read_write",
	}}

	_, err := service.CreateForSession(context.Background(), request)
	if err == nil {
		t.Fatal("CreateForSession succeeded; want memory projection failure")
	}
	var sandboxErr *SandboxError
	if !errors.As(err, &sandboxErr) || sandboxErr.Code != SandboxErrorMountFailed {
		t.Fatalf("err = %T %v; want SandboxError code %s", err, err, SandboxErrorMountFailed)
	}
	if !errors.Is(err, stageErr) {
		t.Fatalf("err = %v; want to wrap memory projection error", err)
	}
	if !reflect.DeepEqual(provider.calls, []string{"create", "health", "base_directories"}) {
		t.Fatalf("provider calls = %v; want create/health/base_directories", provider.calls)
	}
	got := store.sandboxes["sandbox_test"]
	if got.Status != StatusActive || got.FailedAt != nil || got.StartupFailureReason != "" {
		t.Fatalf("sandbox after retryable projection failure = %+v; want active sandbox retained for retry", got)
	}
	if len(provider.releaseHandles) != 0 {
		t.Fatalf("release handles = %+v; want none for retryable projection failure", provider.releaseHandles)
	}
}

func TestCreateForSessionMemoryProjectionFailureCompensatesSkillResidue(t *testing.T) {
	stageErr := errors.New("memory projection unavailable")
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	resourcePreparer := &recordingSessionResourcePreparer{}
	memoryReader := &recordingMemorySnapshotReader{
		snapshots: map[string][]MemorySnapshotFile{
			"memstore_prepare": {{Path: "/todo.md", Content: "ship", ContentSHA256: "sha-ship"}},
		},
	}
	memoryMaterializer := &recordingMemoryStoreMaterializer{err: stageErr}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithIDStrategy(func() string { return "sandbox_test" }),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithSessionResourcePreparer(resourcePreparer),
		WithMemoryProjection(memoryReader, memoryMaterializer),
	)
	request := minimalCreateRequest()
	request.Resources.MemoryStores = []MemoryStoreMount{{
		ResourceID:    "sesrsc_memory",
		MemoryStoreID: "memstore_prepare",
		MountPath:     "/mnt/memory/project",
		Access:        "read_write",
	}}
	request.Resources.Skills = []SkillMount{{
		SkillID:        "skill_finance",
		SkillVersionID: "skill_version_finance",
		Version:        "1",
		Directory:      "finance",
		BlobKey:        "skills/default/skill_finance/1/package.zip",
		SHA256:         strings.Repeat("e", 64),
		SizeBytes:      1,
	}}

	_, err := service.CreateForSession(context.Background(), request)
	if err == nil {
		t.Fatal("CreateForSession succeeded; want memory projection failure")
	}
	if !errors.Is(err, stageErr) {
		t.Fatalf("err = %v; want to wrap memory projection error", err)
	}
	if len(resourcePreparer.compensations) != 1 {
		t.Fatalf("compensations = %d; want one skill compensation", len(resourcePreparer.compensations))
	}
	if got := resourcePreparer.compensations[0].Resources.Skills; len(got) != 1 || got[0].Directory != "finance" {
		t.Fatalf("compensated skills = %+v; want original skill mount", got)
	}
}

func TestCreateForSessionGitHubPreparationRotatesTicketAndFailsClosed(t *testing.T) {
	gitErr := &GitHubPreparationFailure{Reason: GitHubCredentialRequiredReason, Cause: errors.New("missing matching GitHub credential")}
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	gitRotator := &recordingGitTicketRotator{}
	gitMaterializer := &recordingGitHubRepositoryMaterializer{err: gitErr}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithIDStrategy(func() string { return "sandbox_test" }),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithGitHubRepositoryPreparation(gitRotator, gitMaterializer, "git.tetral.test"),
		WithGitTicketRandomSource(bytes.NewReader(make([]byte, gitticket.TokenBytes))),
	)
	request := minimalCreateRequest()
	request.Resources.GitHubRepositories = []GitHubRepositoryMount{{
		ResourceID:   "sesrsc_repo",
		URL:          "https://github.com/tetral-ai/private",
		MountPath:    "/workspace/private",
		CheckoutType: "branch",
		CheckoutRef:  "main",
	}}

	_, err := service.CreateForSession(context.Background(), request)
	if err == nil {
		t.Fatal("CreateForSession succeeded; want github preparation failure")
	}
	if !IsGitHubCredentialRequired(err) {
		t.Fatalf("err = %T %v; want github_credential_required classification", err, err)
	}
	var sandboxErr *SandboxError
	if !errors.As(err, &sandboxErr) || sandboxErr.Code != SandboxErrorMountFailed {
		t.Fatalf("err = %T %v; want SandboxError mount_failed", err, err)
	}
	if len(gitRotator.pendingCalls) != 1 || len(gitRotator.activationCalls) != 0 {
		t.Fatalf("git ticket pending/activations = %d/%d; want 1/0 after failed materialization", len(gitRotator.pendingCalls), len(gitRotator.activationCalls))
	}
	if len(gitMaterializer.installs) != 1 || gitMaterializer.installs[0].Ticket == "" || len(gitMaterializer.calls) != 0 {
		t.Fatalf("github config installs=%+v clone calls=%+v; want installed ticket and no clone", gitMaterializer.installs, gitMaterializer.calls)
	}
	if !reflect.DeepEqual(provider.calls, []string{"create", "health", "base_directories", "release"}) {
		t.Fatalf("provider calls = %v; want create/health/base_directories/release", provider.calls)
	}
	got := store.sandboxes["sandbox_test"]
	if got.Status != StatusArchived || !got.MachineWasUsable || got.FailedAt == nil || got.StartupFailureReason != string(SandboxErrorMountFailed) {
		t.Fatalf("sandbox after github failure = %+v; want archived usable row with mount startup reason", got)
	}
	if got.CleanupStatus != CleanupStatusReleased || got.CleanupAttemptCount != 1 {
		t.Fatalf("cleanup state after github failure = status %q attempts %d; want released cleanup attempt", got.CleanupStatus, got.CleanupAttemptCount)
	}
	if !reflect.DeepEqual(store.events, []string{"create", "save_handle", "mark_active", "mark_failed"}) {
		t.Fatalf("store events = %v; want active readiness before terminal resource failure cleanup", store.events)
	}
}

func TestCreateForSessionProviderCreateFailureMarksSandboxFailed(t *testing.T) {
	createErr := &ProviderError{
		Provider:    "unit-provider",
		Stage:       StageCreateSandbox,
		Kind:        ProviderErrorUnavailable,
		SafeMessage: "create failed",
	}
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	provider.createErr = createErr
	service := newTestService(store, provider)

	_, err := service.CreateForSession(context.Background(), minimalCreateRequest())
	if err == nil {
		t.Fatal("CreateForSession succeeded; want provider create failure")
	}
	if !errors.Is(err, createErr) {
		t.Fatalf("errors.Is createErr = false for %T %v", err, err)
	}
	if len(provider.releaseHandles) != 0 {
		t.Fatalf("release handles = %#v; want none when provider create returned no handle", provider.releaseHandles)
	}
	got := store.sandboxes["sandbox_test"]
	if got.Status != StatusFailed || got.FailedAt == nil {
		t.Fatalf("sandbox after provider create failure = %+v; want failed with failed_at", got)
	}
	if got.CleanupStatus != CleanupStatusPending || got.CleanupAttemptCount != 0 {
		t.Fatalf("cleanup state after provider create failure = status %q attempts %d; want pending name-probe cleanup", got.CleanupStatus, got.CleanupAttemptCount)
	}
	if !containsString(store.events, "mark_failed") {
		t.Fatalf("store events = %v; want mark_failed cleanup", store.events)
	}
}

func TestCreateForSessionReleasesProviderHandleAndPreservesDurableStoreError(t *testing.T) {
	cases := []struct {
		name              string
		configure         func(*recordingStore, error)
		cleanupFailure    bool
		wantCleanupStored bool
	}{
		{
			name: "save provider handle",
			configure: func(store *recordingStore, err error) {
				store.saveHandleErr = err
			},
		},
		{
			name: "mark active",
			configure: func(store *recordingStore, err error) {
				store.markActiveErr = err
			},
		},
		{
			name: "save provider handle cleanup failure",
			configure: func(store *recordingStore, err error) {
				store.saveHandleErr = err
			},
			cleanupFailure:    true,
			wantCleanupStored: true,
		},
		{
			name: "mark active cleanup failure",
			configure: func(store *recordingStore, err error) {
				store.markActiveErr = err
			},
			cleanupFailure:    true,
			wantCleanupStored: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			durableErr := errors.New("durable store failed")
			releaseErr := &ProviderError{Provider: "unit-provider", Stage: StageReleaseSandbox, Kind: ProviderErrorUnavailable, SafeMessage: "release failed"}
			store := newRecordingStore()
			tc.configure(store, durableErr)
			provider := newSuccessfulRecordingProvider()
			if tc.cleanupFailure {
				provider.releaseErr = releaseErr
			}
			service := newTestService(store, provider)

			_, err := service.CreateForSession(context.Background(), minimalCreateRequest())
			if err == nil {
				t.Fatal("CreateForSession succeeded; want durable store failure")
			}
			if !errors.Is(err, durableErr) {
				t.Fatalf("errors.Is durableErr = false for %T %v", err, err)
			}
			var sandboxErr *SandboxError
			if !errors.As(err, &sandboxErr) || sandboxErr.Code != SandboxErrorCreateFailed {
				t.Fatalf("err = %T %v; want create_failed SandboxError", err, err)
			}
			if len(provider.releaseHandles) != 1 || !reflect.DeepEqual(provider.releaseHandles[0], provider.handle) {
				t.Fatalf("release handles = %#v; want exactly provider handle %#v", provider.releaseHandles, provider.handle)
			}
			if tc.wantCleanupStored {
				if sandboxErr.CleanupError == nil {
					t.Fatalf("cleanup error missing from SandboxError: %+v", sandboxErr)
				}
				if !errors.Is(sandboxErr.CleanupError, releaseErr) {
					t.Fatalf("cleanup error = %T %v; want releaseErr", sandboxErr.CleanupError, sandboxErr.CleanupError)
				}
				var providerErr *ProviderError
				if errors.As(err, &providerErr) && providerErr.Stage == StageReleaseSandbox {
					t.Fatalf("returned error resolves as cleanup ProviderError; want durable error preserved: %T %v", err, err)
				}
			}
			got := store.sandboxes["sandbox_test"]
			if got.Status != StatusFailed || got.FailedAt == nil {
				t.Fatalf("sandbox after durable failure = %+v; want failed with failed_at", got)
			}
			if tc.cleanupFailure {
				if got.CleanupStatus != CleanupStatusRetryableFailed || got.CleanupErrorKind != string(ProviderErrorUnavailable) || !got.CleanupRetryable {
					t.Fatalf("cleanup state after retryable cleanup failure = %+v; want retryable_failed unavailable", got)
				}
			} else if got.CleanupStatus != CleanupStatusReleased {
				t.Fatalf("cleanup status after durable failure = %q; want released", got.CleanupStatus)
			}
			if _, findErr := store.FindLiveBySessionID(context.Background(), workspace.DefaultID, "sesn_test"); findErr == nil {
				t.Fatal("FindLiveBySessionID found failed startup sandbox; want not found")
			}
		})
	}
}

func TestCreateForSessionProviderReleaseFailureDoesNotReplaceOriginalError(t *testing.T) {
	healthErr := &ProviderError{Provider: "unit-provider", Stage: StageCheckBaseTemplate, Kind: ProviderErrorUnavailable, SafeMessage: "health failed"}
	releaseErr := &ProviderError{Provider: "unit-provider", Stage: StageReleaseSandbox, Kind: ProviderErrorUnavailable, SafeMessage: "release failed"}
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	provider.healthErr = healthErr
	provider.releaseErr = releaseErr
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithIDStrategy(func() string { return "sandbox_test" }),
		WithClock(func() time.Time { return fixedSandboxTime }),
	)

	_, err := service.CreateForSession(context.Background(), minimalCreateRequest())
	if err == nil {
		t.Fatal("CreateForSession succeeded; want health failure")
	}
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Stage != StageCheckBaseTemplate {
		t.Fatalf("errors.As ProviderError = %+v; want health failure", providerErr)
	}
	var sandboxErr *SandboxError
	if !errors.As(err, &sandboxErr) {
		t.Fatalf("err = %T %v; want SandboxError", err, err)
	}
	if sandboxErr.CleanupError == nil {
		t.Fatalf("cleanup error missing from SandboxError: %+v", sandboxErr)
	}
	got := store.sandboxes["sandbox_test"]
	if got.Status != StatusFailed || got.FailedAt == nil {
		t.Fatalf("sandbox after cleanup failure = %+v; want failed with failed_at", got)
	}
	if _, findErr := store.FindLiveBySessionID(context.Background(), workspace.DefaultID, "sesn_test"); findErr == nil {
		t.Fatal("FindLiveBySessionID found failed startup sandbox; want not found")
	}
}

func TestCreateForSessionDoesNotDeleteSessionResourceCopiesAfterProviderNotFound(t *testing.T) {
	healthErr := &ProviderError{Provider: "unit-provider", Stage: StageCheckBaseTemplate, Kind: ProviderErrorUnavailable, SafeMessage: "health failed"}
	releaseErr := &ProviderError{Provider: "unit-provider", Stage: StageReleaseSandbox, Kind: ProviderErrorNotFound, SafeMessage: "sandbox gone"}
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	provider.healthErr = healthErr
	provider.releaseErr = releaseErr
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithIDStrategy(func() string { return "sandbox_test" }),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithCleanupRetryBackoff(30*time.Second),
	)

	_, err := service.CreateForSession(context.Background(), minimalCreateRequest())
	if err == nil {
		t.Fatal("CreateForSession succeeded; want health failure")
	}
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Stage != StageCheckBaseTemplate {
		t.Fatalf("errors.As ProviderError = %+v; want original health failure", providerErr)
	}
	got := store.sandboxes["sandbox_test"]
	if got.Status != StatusReleased || got.CleanupStatus != CleanupStatusReleased || got.CleanupRetryable || got.CleanupNextAttemptAt != nil {
		t.Fatalf("cleanup state = %+v; want provider-missing terminal release", got)
	}
}

func TestCreateForSessionMapsProviderConfigInvalidToSandboxDomainCode(t *testing.T) {
	providerErr := &ProviderError{
		Provider:    "unit-provider",
		Stage:       StageCreateSandbox,
		Kind:        ProviderErrorConfigInvalid,
		SafeMessage: "provider configuration invalid",
	}
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	provider.createErr = providerErr
	service := newTestService(store, provider)

	_, err := service.CreateForSession(context.Background(), minimalCreateRequest())
	if err == nil {
		t.Fatal("CreateForSession succeeded; want provider config failure")
	}
	var sandboxErr *SandboxError
	if !errors.As(err, &sandboxErr) || sandboxErr.Code != SandboxErrorProviderConfigInvalid {
		t.Fatalf("err = %T %v; want provider_config_invalid SandboxError", err, err)
	}
	if !errors.Is(err, providerErr) {
		t.Fatalf("errors.Is providerErr = false for %T %v", err, err)
	}
}

func TestCreateForSessionWithUnavailableProviderReturnsProviderUnconfigured(t *testing.T) {
	store := newRecordingStore()
	service := NewService(store, NewUnavailableLifecycleProvider(),
		WithProviderName("unavailable"),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithIDStrategy(func() string { return "sandbox_unavailable" }),
	)

	_, err := service.CreateForSession(context.Background(), minimalCreateRequest())
	if err == nil {
		t.Fatal("CreateForSession succeeded; want provider_unconfigured")
	}
	var sandboxErr *SandboxError
	if !errors.As(err, &sandboxErr) || sandboxErr.Code != SandboxErrorProviderUnconfigured {
		t.Fatalf("err = %T %v; want provider_unconfigured SandboxError", err, err)
	}
	got := store.sandboxes["sandbox_unavailable"]
	if got == nil || got.Status != StatusFailed || got.FailedAt == nil {
		t.Fatalf("sandbox after unavailable provider = %+v; want failed retryable record", got)
	}
}

func TestReleaseCreatedSandboxDoesNotMaskProviderNotFound(t *testing.T) {
	releaseErr := &ProviderError{
		Provider:    "unit-provider",
		Stage:       StageReleaseSandbox,
		Kind:        ProviderErrorNotFound,
		SafeMessage: "sandbox missing during create cleanup",
	}
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	provider.releaseErr = releaseErr
	service := newTestService(store, provider)

	err := service.ReleaseCreatedSandbox(context.Background(), activeRecordingSandbox(), ReleaseReasonCleanup)
	if err == nil {
		t.Fatal("ReleaseCreatedSandbox succeeded; want provider not_found failure outside claimed release path")
	}
	if !errors.Is(err, releaseErr) {
		t.Fatalf("errors.Is releaseErr = false for %T %v", err, err)
	}
	assertProviderReleasedStoredHandle(t, provider, activeRecordingSandbox().ProviderHandle)
	if !reflect.DeepEqual(provider.releaseReasons, []ReleaseReason{ReleaseReasonCleanup}) {
		t.Fatalf("release reasons = %v; want cleanup", provider.releaseReasons)
	}
}

func TestReleaseForSessionMapsProviderConfigInvalidToSandboxDomainCode(t *testing.T) {
	providerErr := &ProviderError{
		Provider:    "unit-provider",
		Stage:       StageReleaseSandbox,
		Kind:        ProviderErrorConfigInvalid,
		SafeMessage: "provider configuration invalid",
	}
	store := newRecordingStore()
	store.sandboxes["sandbox_test"] = activeRecordingSandbox()
	provider := newSuccessfulRecordingProvider()
	provider.releaseErr = providerErr
	service := newTestService(store, provider)

	err := service.ReleaseForSession(context.Background(), workspace.DefaultID, "sesn_test", ReleaseReasonDelete)
	if err == nil {
		t.Fatal("ReleaseForSession succeeded; want provider config failure")
	}
	var sandboxErr *SandboxError
	if !errors.As(err, &sandboxErr) || sandboxErr.Code != SandboxErrorProviderConfigInvalid {
		t.Fatalf("err = %T %v; want provider_config_invalid SandboxError", err, err)
	}
	if !errors.Is(err, providerErr) {
		t.Fatalf("errors.Is providerErr = false for %T %v", err, err)
	}
	assertProviderReleasedStoredHandle(t, provider, activeRecordingSandbox().ProviderHandle)
}

func TestReleaseForSessionClaimsBeforeProviderReleaseAndMarksArchived(t *testing.T) {
	ctx := context.Background()
	events := []string{}
	store := newRecordingStore()
	store.eventsRef = &events
	store.sandboxes["sandbox_test"] = activeRecordingSandbox()
	provider := newSuccessfulRecordingProvider()
	provider.eventsRef = &events
	service := newTestService(store, provider)

	if err := service.ReleaseForSession(ctx, workspace.DefaultID, "sesn_test", ReleaseReasonArchive); err != nil {
		t.Fatalf("ReleaseForSession: %v", err)
	}

	wantEvents := []string{"find_live", "mark_releasing", "release", "mark_archived"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %v; want %v", events, wantEvents)
	}
	got := store.sandboxes["sandbox_test"]
	if got.Status != StatusArchived || got.ProviderHandle.SandboxID != "provider_sandbox_123" || got.ReleasedAt != nil {
		t.Fatalf("archived sandbox = %+v", got)
	}
	assertProviderReleasedStoredHandle(t, provider, activeRecordingSandbox().ProviderHandle)
	if !reflect.DeepEqual(provider.releaseReasons, []ReleaseReason{ReleaseReasonArchive}) {
		t.Fatalf("release reasons = %v; want archive", provider.releaseReasons)
	}
}

func TestReleaseForSessionClaimsFailedSandboxWithPendingCleanupRegardlessFailureReason(t *testing.T) {
	store := newRecordingStore()
	failed := activeRecordingSandbox()
	failed.Status = StatusFailed
	failed.StartupFailureReason = string(SandboxErrorMountFailed)
	failed.CleanupStatus = CleanupStatusPending
	failed.FailedAt = timePtr(fixedSandboxTime.Add(-time.Minute))
	store.sandboxes[failed.ID] = failed
	provider := newSuccessfulRecordingProvider()
	service := newTestService(store, provider)

	if err := service.ReleaseForSession(context.Background(), workspace.DefaultID, "sesn_test", ReleaseReasonDelete); err != nil {
		t.Fatalf("ReleaseForSession: %v", err)
	}

	got := store.sandboxes["sandbox_test"]
	if got.Status != StatusReleased || got.ReleasedAt == nil {
		t.Fatalf("failed sandbox release result = %+v; want released", got)
	}
	assertProviderReleasedStoredHandle(t, provider, failed.ProviderHandle)
}

func TestReleaseForSessionDoesNotCompeteWithStartupCleanupLease(t *testing.T) {
	store := newRecordingStore()
	failed := activeRecordingSandbox()
	failed.Status = StatusFailed
	failed.StartupFailureReason = string(SandboxErrorMountFailed)
	failed.CleanupStatus = CleanupStatusInProgress
	failed.CleanupLeaseToken = "lease_active_cleanup"
	failed.CleanupLeaseExpiresAt = timePtr(fixedSandboxTime.Add(time.Minute))
	store.sandboxes[failed.ID] = failed
	provider := newSuccessfulRecordingProvider()
	service := newTestService(store, provider)

	if err := service.ReleaseForSession(context.Background(), workspace.DefaultID, "sesn_test", ReleaseReasonArchive); err != nil {
		t.Fatalf("ReleaseForSession: %v", err)
	}
	if len(provider.releaseHandles) != 0 {
		t.Fatalf("provider release calls = %d; want lease holder to remain exclusive", len(provider.releaseHandles))
	}
	got := store.sandboxes[failed.ID]
	if got.Status != StatusFailed || got.CleanupStatus != CleanupStatusInProgress ||
		got.CleanupLeaseToken != failed.CleanupLeaseToken {
		t.Fatalf("leased cleanup row changed through release path: %+v", got)
	}
}

func TestReleaseForSessionProviderFailureDoesNotMarkReleased(t *testing.T) {
	store := newRecordingStore()
	store.sandboxes["sandbox_test"] = activeRecordingSandbox()
	provider := newSuccessfulRecordingProvider()
	provider.releaseErr = &ProviderError{Provider: "unit-provider", Stage: StageReleaseSandbox, Kind: ProviderErrorUnavailable, SafeMessage: "release failed"}
	service := newTestService(store, provider)

	err := service.ReleaseForSession(context.Background(), workspace.DefaultID, "sesn_test", ReleaseReasonDelete)
	if err == nil {
		t.Fatal("ReleaseForSession succeeded; want provider release failure")
	}
	var sandboxErr *SandboxError
	if !errors.As(err, &sandboxErr) || sandboxErr.Code != SandboxErrorReleaseFailed {
		t.Fatalf("err = %T %v; want release_failed SandboxError", err, err)
	}
	got := store.sandboxes["sandbox_test"]
	if got.Status != StatusReleasing || got.ReleasedAt != nil {
		t.Fatalf("sandbox after release failure = %+v; want releasing and not released", got)
	}
	assertProviderReleasedStoredHandle(t, provider, activeRecordingSandbox().ProviderHandle)
}

func TestReleaseForSessionDoesNotMaskProviderNotFoundOutsideReleaseStage(t *testing.T) {
	releaseErr := &ProviderError{
		Provider:    "unit-provider",
		Stage:       StageCreateSandbox,
		Kind:        ProviderErrorNotFound,
		SafeMessage: "sandbox missing outside release stage",
	}
	store := newRecordingStore()
	store.sandboxes["sandbox_test"] = activeRecordingSandbox()
	provider := newSuccessfulRecordingProvider()
	provider.releaseErr = releaseErr
	service := newTestService(store, provider)

	err := service.ReleaseForSession(context.Background(), workspace.DefaultID, "sesn_test", ReleaseReasonDelete)
	if err == nil {
		t.Fatal("ReleaseForSession succeeded; want non-release-stage provider not_found failure")
	}
	if !errors.Is(err, releaseErr) {
		t.Fatalf("errors.Is releaseErr = false for %T %v", err, err)
	}
	got := store.sandboxes["sandbox_test"]
	if got.Status != StatusReleasing || got.ReleasedAt != nil {
		t.Fatalf("sandbox after non-release-stage not_found = %+v; want releasing and not released", got)
	}
	assertProviderReleasedStoredHandle(t, provider, activeRecordingSandbox().ProviderHandle)
}

func TestReleaseClaimedSandboxTreatsProviderNotFoundAsAlreadyReleased(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	provider.releaseErr = &ProviderError{
		Provider:    "unit-provider",
		Stage:       StageReleaseSandbox,
		Kind:        ProviderErrorNotFound,
		SafeMessage: "sandbox already gone",
	}
	service := newTestService(store, provider)
	claimed := activeRecordingSandbox()
	claimed.Status = StatusReleasing

	if err := service.ReleaseClaimedSandbox(context.Background(), claimed, ReleaseReasonArchive); err != nil {
		t.Fatalf("ReleaseClaimedSandbox: %v", err)
	}
	assertProviderReleasedStoredHandle(t, provider, activeRecordingSandbox().ProviderHandle)
}

func TestReleaseClaimedSandboxDoesNotMaskOtherProviderReleaseErrors(t *testing.T) {
	releaseErr := &ProviderError{
		Provider:    "unit-provider",
		Stage:       StageReleaseSandbox,
		Kind:        ProviderErrorUnavailable,
		SafeMessage: "release failed",
	}
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	provider.releaseErr = releaseErr
	service := newTestService(store, provider)
	claimed := activeRecordingSandbox()
	claimed.Status = StatusReleasing

	err := service.ReleaseClaimedSandbox(context.Background(), claimed, ReleaseReasonDelete)
	if err == nil {
		t.Fatal("ReleaseClaimedSandbox succeeded; want provider release failure")
	}
	if !errors.Is(err, releaseErr) {
		t.Fatalf("errors.Is releaseErr = false for %T %v", err, err)
	}
	assertProviderReleasedStoredHandle(t, provider, activeRecordingSandbox().ProviderHandle)
}

func TestReleaseClaimedSandboxDoesNotMaskProviderNotFoundWithoutReleaseClaim(t *testing.T) {
	releaseErr := &ProviderError{
		Provider:    "unit-provider",
		Stage:       StageReleaseSandbox,
		Kind:        ProviderErrorNotFound,
		SafeMessage: "sandbox missing before release claim",
	}
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	provider.releaseErr = releaseErr
	service := newTestService(store, provider)

	err := service.ReleaseClaimedSandbox(context.Background(), activeRecordingSandbox(), ReleaseReasonArchive)
	if err == nil {
		t.Fatal("ReleaseClaimedSandbox succeeded; want provider not_found failure without release claim")
	}
	if !errors.Is(err, releaseErr) {
		t.Fatalf("errors.Is releaseErr = false for %T %v", err, err)
	}
	assertProviderReleasedStoredHandle(t, provider, activeRecordingSandbox().ProviderHandle)
}

func TestReleaseClaimedSandboxDoesNotMaskInvalidProviderHandle(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	provider.releaseErr = &ProviderError{
		Provider:    "unit-provider",
		Stage:       StageReleaseSandbox,
		Kind:        ProviderErrorNotFound,
		SafeMessage: "sandbox already gone",
	}
	service := newTestService(store, provider)
	claimed := activeRecordingSandbox()
	claimed.Provider = ""
	claimed.ProviderSandboxID = ""
	claimed.ProviderMetadata = nil
	claimed.ProviderHandle = ProviderHandle{}

	err := service.ReleaseClaimedSandbox(context.Background(), claimed, ReleaseReasonArchive)
	if err == nil {
		t.Fatal("ReleaseClaimedSandbox succeeded; want invalid provider handle failure")
	}
	var sandboxErr *SandboxError
	if !errors.As(err, &sandboxErr) || sandboxErr.Code != SandboxErrorReleaseFailed {
		t.Fatalf("err = %T %v; want release_failed SandboxError", err, err)
	}
	if len(provider.releaseHandles) != 0 {
		t.Fatalf("provider release handles = %#v; want none for invalid handle", provider.releaseHandles)
	}
}

func TestReleaseForSessionFinalDurableFailureLeavesRecoverableReleasingState(t *testing.T) {
	finalErr := errors.New("mark released failed")
	store := newRecordingStore()
	stored := activeRecordingSandbox()
	store.sandboxes["sandbox_test"] = stored
	store.markReleasedErr = finalErr
	provider := newSuccessfulRecordingProvider()
	service := newTestService(store, provider)

	err := service.ReleaseForSession(context.Background(), workspace.DefaultID, "sesn_test", ReleaseReasonDelete)
	if err == nil {
		t.Fatal("ReleaseForSession succeeded; want final durable failure")
	}
	if !errors.Is(err, finalErr) {
		t.Fatalf("errors.Is finalErr = false for %T %v", err, err)
	}
	got := store.sandboxes["sandbox_test"]
	if got.Status != StatusReleasing || got.ProviderHandle.SandboxID != "provider_sandbox_123" {
		t.Fatalf("sandbox after final failure = %+v; want recoverable releasing handle", got)
	}
	live, err := store.FindLiveBySessionID(context.Background(), workspace.DefaultID, "sesn_test")
	if err != nil {
		t.Fatalf("FindLiveBySessionID after final failure: %v", err)
	}
	if live.Status != StatusReleasing {
		t.Fatalf("live status = %s; want releasing", live.Status)
	}
	assertProviderReleasedStoredHandle(t, provider, activeRecordingSandbox().ProviderHandle)

	store.markReleasedErr = nil
	provider.releaseHandles = nil
	provider.releaseErr = &ProviderError{
		Provider:    "unit-provider",
		Stage:       StageReleaseSandbox,
		Kind:        ProviderErrorNotFound,
		SafeMessage: "sandbox already gone",
	}
	if err := service.ReleaseForSession(context.Background(), workspace.DefaultID, "sesn_test", ReleaseReasonDelete); err != nil {
		t.Fatalf("ReleaseForSession retry: %v", err)
	}
	assertProviderReleasedStoredHandle(t, provider, stored.ProviderHandle)
	retried := store.sandboxes["sandbox_test"]
	if retried.Status != StatusReleased || retried.ReleasedAt == nil {
		t.Fatalf("sandbox after retry = %+v; want released with released_at", retried)
	}
}

func TestReleaseForSessionNoLiveSandboxIsNoop(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	service := newTestService(store, provider)

	if err := service.ReleaseForSession(context.Background(), workspace.DefaultID, "sesn_missing", ReleaseReasonArchive); err != nil {
		t.Fatalf("ReleaseForSession: %v", err)
	}
	if len(provider.calls) != 0 {
		t.Fatalf("provider calls = %v; want none", provider.calls)
	}
}

func TestProviderHandleMetadataRejectsSuspiciousSecretsAndCredentialURLs(t *testing.T) {
	cases := []struct {
		name     string
		metadata map[string]string
	}{
		{name: "token key", metadata: map[string]string{"access_token": "abc"}},
		{name: "api key", metadata: map[string]string{"api_key": "abc"}},
		{name: "api-key", metadata: map[string]string{"api-key": "abc"}},
		{name: "apikey", metadata: map[string]string{"apikey": "abc"}},
		{name: "bearer token", metadata: map[string]string{"bearer_token": "abc"}},
		{name: "private key", metadata: map[string]string{"private_key": "abc"}},
		{name: "access key", metadata: map[string]string{"access_key": "abc"}},
		{name: "authorization key", metadata: map[string]string{"authorization": "Bearer abc"}},
		{name: "secret key", metadata: map[string]string{"client_secret": "abc"}},
		{name: "credential url", metadata: map[string]string{"endpoint": credentialBearingURLForTest()}},
		{name: "credential query", metadata: map[string]string{"endpoint": "https://example.test/api?token=abc"}},
		{name: "embedded credential url", metadata: map[string]string{"endpoint": "sandbox endpoint=https://user:pass@example.test/api"}},
		{name: "embedded credential query", metadata: map[string]string{"endpoint": "created sandbox at https://example.test/api?token=abc"}},
		{name: "uppercase embedded credential url", metadata: map[string]string{"endpoint": "sandbox endpoint=HTTPS://unit:opaque@example.test/api"}},
		{name: "mixed-case embedded credential query", metadata: map[string]string{"endpoint": "created sandbox at HtTp://example.test/api?Authorization=opaque"}},
		{name: "raw body", metadata: map[string]string{"raw_body": `{"token":"abc"}`}},
		{name: "neutral key token value", metadata: map[string]string{"provider_hint": "token=abc"}},
		{name: "neutral key api key value", metadata: map[string]string{"provider_hint": "api_key=abc"}},
		{name: "neutral key access key value", metadata: map[string]string{"provider_hint": "access_key=abc"}},
		{name: "neutral key private key value", metadata: map[string]string{"provider_hint": "private_key=abc"}},
		{name: "neutral key secret value", metadata: map[string]string{"provider_hint": "client_secret=abc"}},
		{name: "neutral key provider body", metadata: map[string]string{"provider_hint": `{"access_token":"abc"}`}},
		{name: "neutral key encrypted bytes", metadata: map[string]string{"provider_hint": "encrypted_token_bytes:AQID"}},
		{name: "neutral key command output", metadata: map[string]string{"provider_hint": "stdout: installed package"}},
		{name: "command output", metadata: map[string]string{"stdout": "installed secret"}},
		{name: "encrypted token bytes", metadata: map[string]string{"encrypted_token_bytes": "AQID"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handle := ProviderHandle{Provider: "unit-provider", SandboxID: "provider_sandbox", Metadata: tc.metadata}
			if err := ValidateProviderHandle(handle); err == nil {
				t.Fatalf("ValidateProviderHandle accepted metadata %#v; want rejection", tc.metadata)
			}
		})
	}
}

func TestProviderHandleRejectsSecretShapedSandboxID(t *testing.T) {
	cases := []struct {
		name      string
		sandboxID string
	}{
		{name: "bearer", sandboxID: "Bearer raw-token"},
		{name: "credential url", sandboxID: credentialBearingURLForTest()},
		{name: "token term", sandboxID: "provider-token-abc"},
		{name: "api key", sandboxID: "api_key=abc"},
		{name: "raw body", sandboxID: `{"access_token":"abc"}`},
		{name: "command output", sandboxID: "stdout: created sandbox"},
		{name: "encrypted bytes", sandboxID: "encrypted_token_bytes:AQID"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handle := ProviderHandle{Provider: "unit-provider", SandboxID: tc.sandboxID, Metadata: map[string]string{"region": "iad"}}
			if err := ValidateProviderHandle(handle); err == nil {
				t.Fatalf("ValidateProviderHandle accepted sandbox id %q; want rejection", tc.sandboxID)
			}
		})
	}
}

func TestProviderHandleRejectsSecretShapedProviderName(t *testing.T) {
	cases := []struct {
		name     string
		provider string
	}{
		{name: "bearer", provider: "Bearer raw-token"},
		{name: "api key", provider: "api_key=abc"},
		{name: "credential url", provider: credentialBearingURLForTest()},
		{name: "raw body", provider: `{"access_token":"abc"}`},
		{name: "invalid spaces", provider: "unit provider"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handle := ProviderHandle{Provider: tc.provider, SandboxID: "provider_sandbox", Metadata: map[string]string{"region": "iad"}}
			if err := ValidateProviderHandle(handle); err == nil {
				t.Fatalf("ValidateProviderHandle accepted provider %q; want rejection", tc.provider)
			}
		})
	}
}

func TestCreateForSessionNormalizesAdapterProviderName(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	provider.handle.Provider = "adapter_supplied_name"
	service := newTestService(store, provider)

	got, err := service.CreateForSession(context.Background(), minimalCreateRequest())
	if err != nil {
		t.Fatalf("CreateForSession: %v", err)
	}
	if got.Provider != "unit-provider" || got.ProviderHandle.Provider != "unit-provider" {
		t.Fatalf("sandbox provider = %q handle provider = %q; want configured provider", got.Provider, got.ProviderHandle.Provider)
	}
	if provider.healthHandles[0].Provider != "unit-provider" || provider.releaseHandles != nil {
		t.Fatalf("provider handles after normalization = health %#v release %#v", provider.healthHandles, provider.releaseHandles)
	}
}

func TestReconcileStaleCreatingProbesTerminalHandleBeforeClaimAndCleanup(t *testing.T) {
	events := []string{}
	store := newRecordingStore()
	store.eventsRef = &events
	provider := newSuccessfulRecordingProvider()
	provider.status = ProviderStatus{Availability: ProviderAvailable, SandboxStatus: StatusStopped}
	provider.eventsRef = &events
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime.Add(10 * time.Minute) }),
		WithStaleStartupThreshold(time.Minute),
		WithCleanupRetryBackoff(30*time.Second),
	)
	creating := creatingRecordingSandbox()
	creating.ProviderSandboxID = "provider_sandbox_123"
	creating.ProviderHandle = ProviderHandle{Provider: "unit-provider", SandboxID: "provider_sandbox_123", Metadata: map[string]string{"region": "iad"}}
	creating.ProviderMetadata = cloneStringMap(creating.ProviderHandle.Metadata)
	store.sandboxes[creating.ID] = creating

	got, err := service.ReconcileStaleCreating(context.Background(), workspace.DefaultID, creating.ID)
	if err != nil {
		t.Fatalf("ReconcileStaleCreating: %v", err)
	}
	if got.Status != StatusFailed || got.StartupFailureReason != "startup_interrupted" || got.CleanupStatus != CleanupStatusReleased {
		t.Fatalf("reconciled sandbox = %+v; want failed startup_interrupted released cleanup", got)
	}
	if got.CleanupMethod != string(ReleaseReasonCleanup) || got.CleanupAttemptCount != 1 {
		t.Fatalf("cleanup method/count = %q/%d; want cleanup/1", got.CleanupMethod, got.CleanupAttemptCount)
	}
	wantEvents := []string{"inspect_stale_startup", "status", "claim_stale_creating", "claim_startup_cleanup", "release", "mark_cleanup_attempt"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %v; want %v", events, wantEvents)
	}
}

func TestReconcileStaleCreatingNameProbesAndAdoptsActiveMissingHandle(t *testing.T) {
	for _, startupStatus := range []Status{StatusCreating, StatusResuming} {
		t.Run(string(startupStatus), func(t *testing.T) {
			store := newRecordingStore()
			provider := newSuccessfulRecordingProvider()
			service := NewService(store, provider,
				WithProviderName("unit-provider"),
				WithClock(func() time.Time { return fixedSandboxTime.Add(10 * time.Minute) }),
				WithStaleStartupThreshold(time.Minute),
			)
			current := creatingRecordingSandbox()
			current.Status = startupStatus
			store.sandboxes[current.ID] = current

			got, err := service.ReconcileStaleCreating(context.Background(), workspace.DefaultID, current.ID)
			if err != nil {
				t.Fatalf("ReconcileStaleCreating: %v", err)
			}
			if got.Status != StatusActive || got.ProviderSandboxID != current.ID || got.StatusRefreshedAt == nil {
				t.Fatalf("adopted state = %+v; want name-probed active provider adopted", got)
			}
			if len(provider.releaseReasons) != 0 {
				t.Fatalf("provider release reasons = %v; want no destroy after active adoption", provider.releaseReasons)
			}
		})
	}
}

func TestReconcileStaleStartupDeterministicNameAdoptsInitialAndReplacementCreateCrash(t *testing.T) {
	for _, origin := range []string{"initial create", "replacement create"} {
		t.Run(origin, func(t *testing.T) {
			store := newRecordingStore()
			provider := newSuccessfulRecordingProvider()
			provider.handle.SandboxID = "sandbox_test"
			provider.status = ProviderStatus{Availability: ProviderAvailable, SandboxStatus: StatusActive}
			service := NewService(store, provider, WithProviderName("unit-provider"), WithClock(func() time.Time { return fixedSandboxTime.Add(10 * time.Minute) }), WithStaleStartupThreshold(time.Minute))
			crashed := creatingRecordingSandbox()
			crashed.ProviderSandboxID = ""
			crashed.ProviderHandle = ProviderHandle{}
			crashed.ProviderMetadata = nil
			store.sandboxes[crashed.ID] = crashed

			got, err := service.ReconcileStaleCreating(context.Background(), workspace.DefaultID, crashed.ID)
			if err != nil {
				t.Fatalf("ReconcileStaleCreating: %v", err)
			}
			if got.Status != StatusActive || got.ProviderSandboxID != crashed.ID || len(provider.releaseHandles) != 0 {
				t.Fatalf("%s crash reconciliation=%+v releases=%+v; want deterministic-name adoption without leak", origin, got, provider.releaseHandles)
			}
		})
	}
}

func TestReconcileStaleStartupStillStartingRefreshesWithoutDestroyForCreatingAndResuming(t *testing.T) {
	for _, status := range []Status{StatusCreating, StatusResuming} {
		t.Run(string(status), func(t *testing.T) {
			store := newRecordingStore()
			provider := newSuccessfulRecordingProvider()
			provider.status = ProviderStatus{Availability: ProviderUnavailable, SandboxStatus: status}
			service := NewService(store, provider, WithProviderName("unit-provider"), WithClock(func() time.Time { return fixedSandboxTime.Add(10 * time.Minute) }), WithStaleStartupThreshold(time.Minute))
			current := creatingRecordingSandbox()
			current.Status = status
			current.ProviderSandboxID = "provider_starting"
			current.ProviderHandle = ProviderHandle{Provider: "unit-provider", SandboxID: "provider_starting"}
			store.sandboxes[current.ID] = current

			got, err := service.ReconcileStaleCreating(context.Background(), workspace.DefaultID, current.ID)
			if err != nil {
				t.Fatalf("ReconcileStaleCreating: %v", err)
			}
			if got.Status != status || got.StatusRefreshedAt == nil || !got.UpdatedAt.Equal(fixedSandboxTime.Add(10*time.Minute)) {
				t.Fatalf("refreshed startup = %+v; want fresh unchanged %s", got, status)
			}
			if len(provider.releaseReasons) != 0 || store.sandboxes[current.ID].Status == StatusFailed {
				t.Fatalf("still-starting reconciliation release=%v sandbox=%+v; want no destroy/failure", provider.releaseReasons, store.sandboxes[current.ID])
			}
		})
	}
}

func TestReconcileStaleStartupAbsentSettlesReleasedWithoutCleanupProviderCall(t *testing.T) {
	for _, startupStatus := range []Status{StatusCreating, StatusResuming} {
		t.Run(string(startupStatus), func(t *testing.T) {
			store := newRecordingStore()
			provider := newSuccessfulRecordingProvider()
			provider.status = ProviderStatus{Availability: ProviderMissing, SandboxStatus: StatusReleased}
			service := NewService(store, provider, WithProviderName("unit-provider"), WithClock(func() time.Time { return fixedSandboxTime.Add(10 * time.Minute) }), WithStaleStartupThreshold(time.Minute))
			current := creatingRecordingSandbox()
			current.Status = startupStatus
			current.ProviderSandboxID = "provider_absent"
			current.ProviderHandle = ProviderHandle{Provider: "unit-provider", SandboxID: "provider_absent"}
			store.sandboxes[current.ID] = current

			got, err := service.ReconcileStaleCreating(context.Background(), workspace.DefaultID, current.ID)
			if err != nil {
				t.Fatalf("ReconcileStaleCreating: %v", err)
			}
			if got.Status != StatusReleased || got.CleanupStatus != CleanupStatusReleased || got.CleanupAttemptCount != 0 {
				t.Fatalf("absent startup = %+v; want released provider-missing state without cleanup attempt", got)
			}
			if len(provider.releaseReasons) != 0 {
				t.Fatalf("provider releases = %v; want zero for absent provider", provider.releaseReasons)
			}
		})
	}
}

func TestStartCreatedSandboxFailureAtCleanupCapSkipsUnreservedRelease(t *testing.T) {
	store := newRecordingStore()
	current := creatingRecordingSandbox()
	current.CleanupAttemptCount = 20
	store.sandboxes[current.ID] = current
	provider := newSuccessfulRecordingProvider()
	provider.healthErr = &ValidationError{Message: "terminal base-template failure"}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime }),
		WithCleanupMaxAttempts(20),
	)

	_, err := service.StartCreatedSandbox(context.Background(), current, minimalCreateRequest())
	if err == nil {
		t.Fatal("StartCreatedSandbox succeeded; want base-template failure")
	}
	if len(provider.releaseHandles) != 0 {
		t.Fatalf("provider releases = %d; want no unreserved call at cleanup cap", len(provider.releaseHandles))
	}
	got := store.sandboxes[current.ID]
	if got == nil || got.Status != StatusFailed || got.CleanupStatus != CleanupStatusPending || got.CleanupAttemptCount != 20 {
		t.Fatalf("sandbox after capped startup failure = %+v; want pending read-only observation at count 20", got)
	}
}

func TestReconcileStaleStartupNameProbeTerminalAdoptsThenCleansUp(t *testing.T) {
	for _, startupStatus := range []Status{StatusCreating, StatusResuming} {
		t.Run(string(startupStatus), func(t *testing.T) {
			store := newRecordingStore()
			provider := newSuccessfulRecordingProvider()
			provider.status = ProviderStatus{Availability: ProviderAvailable, SandboxStatus: StatusStopped, Metadata: map[string]string{"region": "iad"}}
			service := NewService(store, provider, WithProviderName("unit-provider"), WithClock(func() time.Time { return fixedSandboxTime.Add(10 * time.Minute) }), WithStaleStartupThreshold(time.Minute))
			current := creatingRecordingSandbox()
			current.Status = startupStatus
			store.sandboxes[current.ID] = current

			got, err := service.ReconcileStaleCreating(context.Background(), workspace.DefaultID, current.ID)
			if err != nil {
				t.Fatalf("ReconcileStaleCreating: %v", err)
			}
			if got.Status != StatusFailed || got.CleanupStatus != CleanupStatusReleased || got.ProviderSandboxID != current.ID {
				t.Fatalf("terminal name probe = %+v; want adopted handle cleaned up", got)
			}
			if len(provider.releaseHandles) != 1 || provider.releaseHandles[0].SandboxID != current.ID {
				t.Fatalf("release handles = %+v; want adopted name handle", provider.releaseHandles)
			}
		})
	}
}

func TestReconcileStaleStartupTombstoneSettlesForDeleteOwnerWithoutProviderCall(t *testing.T) {
	for _, status := range []Status{StatusCreating, StatusResuming} {
		t.Run(string(status), func(t *testing.T) {
			store := newRecordingStore()
			provider := newSuccessfulRecordingProvider()
			service := NewService(store, provider, WithProviderName("unit-provider"), WithClock(func() time.Time { return fixedSandboxTime.Add(10 * time.Minute) }), WithStaleStartupThreshold(time.Minute))
			current := creatingRecordingSandbox()
			current.Status = status
			current.ProviderSandboxID = "provider_deleted"
			current.ProviderHandle = ProviderHandle{Provider: "unit-provider", SandboxID: "provider_deleted"}
			store.sandboxes[current.ID] = current
			store.deletedSessions[current.SessionID] = true

			got, err := service.ReconcileStaleCreating(context.Background(), workspace.DefaultID, current.ID)
			if err != nil {
				t.Fatalf("ReconcileStaleCreating: %v", err)
			}
			if got.Status != StatusFailed || got.CleanupStatus != CleanupStatusPending {
				t.Fatalf("tombstoned startup = %+v; want failed pending for delete owner", got)
			}
			if len(provider.calls) != 0 {
				t.Fatalf("provider calls = %v; want delete owner exclusively", provider.calls)
			}
		})
	}
}

func TestReconcileStaleCreatingDoesNotTransitionActiveSandbox(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime.Add(10 * time.Minute) }),
		WithStaleStartupThreshold(time.Minute),
	)
	active := activeRecordingSandbox()
	store.sandboxes[active.ID] = active

	got, err := service.ReconcileStaleCreating(context.Background(), workspace.DefaultID, active.ID)
	if err != nil {
		t.Fatalf("ReconcileStaleCreating: %v", err)
	}
	if got != nil {
		t.Fatalf("reconciled active sandbox = %+v; want no claim", got)
	}
	if store.sandboxes[active.ID].Status != StatusActive {
		t.Fatalf("active sandbox status = %s; want unchanged active", store.sandboxes[active.ID].Status)
	}
	if len(provider.calls) != 0 {
		t.Fatalf("provider calls = %v; want no provider calls for active sandbox", provider.calls)
	}
}

func TestReconcileStaleCreatingClassifiesCleanupFailures(t *testing.T) {
	cases := []struct {
		name             string
		err              error
		wantStatus       CleanupStatus
		wantErrorKind    string
		wantRetryable    bool
		wantNextRetrySet bool
		wantError        bool
	}{
		{
			name: "not found",
			err: &ProviderError{
				Provider:    "unit-provider",
				Stage:       StageReleaseSandbox,
				Kind:        ProviderErrorNotFound,
				SafeMessage: "sandbox already gone",
			},
			wantStatus:    CleanupStatusReleased,
			wantErrorKind: string(ProviderErrorNotFound),
		},
		{
			name: "retryable unavailable",
			err: &ProviderError{
				Provider:    "unit-provider",
				Stage:       StageReleaseSandbox,
				Kind:        ProviderErrorUnavailable,
				SafeMessage: "provider unavailable",
			},
			wantStatus:       CleanupStatusRetryableFailed,
			wantErrorKind:    string(ProviderErrorUnavailable),
			wantRetryable:    true,
			wantNextRetrySet: true,
			wantError:        true,
		},
		{
			name: "retryable timeout",
			err: &ProviderError{
				Provider:    "unit-provider",
				Stage:       StageReleaseSandbox,
				Kind:        ProviderErrorTimeout,
				SafeMessage: "provider timeout",
			},
			wantStatus:       CleanupStatusRetryableFailed,
			wantErrorKind:    string(ProviderErrorTimeout),
			wantRetryable:    true,
			wantNextRetrySet: true,
			wantError:        true,
		},
		{
			name: "permanent config",
			err: &ProviderError{
				Provider:    "unit-provider",
				Stage:       StageReleaseSandbox,
				Kind:        ProviderErrorConfigInvalid,
				SafeMessage: "provider config invalid",
			},
			wantStatus:    CleanupStatusPermanentFailed,
			wantErrorKind: string(ProviderErrorConfigInvalid),
			wantError:     true,
		},
		{
			name: "permanent invalid request",
			err: &ProviderError{
				Provider:    "unit-provider",
				Stage:       StageReleaseSandbox,
				Kind:        ProviderErrorInvalidRequest,
				SafeMessage: "provider rejected request",
			},
			wantStatus:    CleanupStatusPermanentFailed,
			wantErrorKind: string(ProviderErrorInvalidRequest),
			wantError:     true,
		},
		{
			name: "permanent conflict",
			err: &ProviderError{
				Provider:    "unit-provider",
				Stage:       StageReleaseSandbox,
				Kind:        ProviderErrorConflict,
				SafeMessage: "provider rejected release",
			},
			wantStatus:    CleanupStatusPermanentFailed,
			wantErrorKind: string(ProviderErrorConflict),
			wantError:     true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newRecordingStore()
			provider := newSuccessfulRecordingProvider()
			provider.status = ProviderStatus{Availability: ProviderAvailable, SandboxStatus: StatusStopped}
			provider.releaseErr = tc.err
			service := NewService(store, provider,
				WithProviderName("unit-provider"),
				WithClock(func() time.Time { return fixedSandboxTime.Add(10 * time.Minute) }),
				WithStaleStartupThreshold(time.Minute),
				WithCleanupRetryBackoff(30*time.Second),
			)
			creating := creatingRecordingSandbox()
			creating.ProviderSandboxID = "provider_sandbox_123"
			creating.ProviderHandle = ProviderHandle{Provider: "unit-provider", SandboxID: "provider_sandbox_123"}
			store.sandboxes[creating.ID] = creating

			got, err := service.ReconcileStaleCreating(context.Background(), workspace.DefaultID, creating.ID)
			if (err != nil) != tc.wantError {
				t.Fatalf("ReconcileStaleCreating err = %T %v; want error=%v", err, err, tc.wantError)
			}
			if got == nil || got.CleanupStatus != tc.wantStatus || got.CleanupErrorKind != tc.wantErrorKind || got.CleanupRetryable != tc.wantRetryable {
				t.Fatalf("cleanup state = %+v; want status=%s kind=%s retryable=%v", got, tc.wantStatus, tc.wantErrorKind, tc.wantRetryable)
			}
			if (got.CleanupNextAttemptAt != nil) != tc.wantNextRetrySet {
				t.Fatalf("cleanup_next_attempt_at = %v; want set=%v", got.CleanupNextAttemptAt, tc.wantNextRetrySet)
			}
			if tc.wantNextRetrySet && !got.CleanupNextAttemptAt.Equal(fixedSandboxTime.Add(10*time.Minute+30*time.Second)) {
				t.Fatalf("cleanup_next_attempt_at = %v; want retry backoff", got.CleanupNextAttemptAt)
			}
		})
	}
}

func TestReconcileStartupCleanupRetriesPendingAndRetryableFailures(t *testing.T) {
	cases := []struct {
		name        string
		status      CleanupStatus
		nextAttempt *time.Time
		wantClaim   bool
	}{
		{name: "pending", status: CleanupStatusPending, wantClaim: true},
		{name: "due retryable", status: CleanupStatusRetryableFailed, nextAttempt: timePtr(fixedSandboxTime.Add(9 * time.Minute)), wantClaim: true},
		{name: "future retryable", status: CleanupStatusRetryableFailed, nextAttempt: timePtr(fixedSandboxTime.Add(11 * time.Minute))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newRecordingStore()
			provider := newSuccessfulRecordingProvider()
			service := NewService(store, provider,
				WithProviderName("unit-provider"),
				WithClock(func() time.Time { return fixedSandboxTime.Add(10 * time.Minute) }),
				WithCleanupRetryBackoff(30*time.Second),
			)
			failed := creatingRecordingSandbox()
			failed.Status = StatusFailed
			failed.StartupFailureReason = "startup_interrupted"
			failed.CleanupStatus = tc.status
			failed.CleanupNextAttemptAt = tc.nextAttempt
			failed.ProviderSandboxID = "provider_sandbox_123"
			failed.ProviderHandle = ProviderHandle{Provider: "unit-provider", SandboxID: "provider_sandbox_123"}
			store.sandboxes[failed.ID] = failed

			got, err := service.ReconcileStartupCleanup(context.Background(), workspace.DefaultID, failed.ID)
			if err != nil {
				t.Fatalf("ReconcileStartupCleanup: %v", err)
			}
			if !tc.wantClaim {
				if got != nil {
					t.Fatalf("cleanup result = %+v; want no due cleanup claim", got)
				}
				if len(provider.calls) != 0 {
					t.Fatalf("provider calls = %v; want none before retry time", provider.calls)
				}
				return
			}
			if got == nil || got.CleanupStatus != CleanupStatusReleased || got.CleanupAttemptCount != 1 {
				t.Fatalf("cleanup result = %+v; want released with one attempt", got)
			}
			if len(provider.releaseHandles) != 1 || provider.releaseHandles[0].SandboxID != "provider_sandbox_123" {
				t.Fatalf("release handles = %#v; want persisted provider handle", provider.releaseHandles)
			}
		})
	}
}

func TestReconcileStartupCleanupReschedulesRetryableFailure(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	provider.releaseErr = &ProviderError{Provider: "unit-provider", Stage: StageReleaseSandbox, Kind: ProviderErrorTimeout}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime.Add(10 * time.Minute) }),
		WithCleanupRetryBackoff(30*time.Second),
	)
	failed := creatingRecordingSandbox()
	failed.Status = StatusFailed
	failed.StartupFailureReason = "startup_interrupted"
	failed.CleanupStatus = CleanupStatusRetryableFailed
	failed.CleanupNextAttemptAt = timePtr(fixedSandboxTime.Add(9 * time.Minute))
	failed.ProviderSandboxID = "provider_sandbox_123"
	failed.ProviderHandle = ProviderHandle{Provider: "unit-provider", SandboxID: "provider_sandbox_123"}
	store.sandboxes[failed.ID] = failed

	got, err := service.ReconcileStartupCleanup(context.Background(), workspace.DefaultID, failed.ID)
	if err == nil {
		t.Fatal("ReconcileStartupCleanup succeeded; want retryable cleanup error")
	}
	if got == nil || got.CleanupStatus != CleanupStatusRetryableFailed || got.CleanupAttemptCount != 1 || got.CleanupNextAttemptAt == nil {
		t.Fatalf("cleanup result = %+v; want retryable failure rescheduled", got)
	}
	if !got.CleanupNextAttemptAt.Equal(fixedSandboxTime.Add(10*time.Minute + 30*time.Second)) {
		t.Fatalf("cleanup_next_attempt_at = %v; want backoff from current attempt", got.CleanupNextAttemptAt)
	}
}

func TestReconcileStartupCleanupUsesReadOnlyObservationAfterTwentiethAttempt(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	provider.releaseErr = &ProviderError{Provider: "unit-provider", Stage: StageReleaseSandbox, Kind: ProviderErrorTimeout}
	now := fixedSandboxTime.Add(10 * time.Minute)
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return now }),
		WithCleanupRetryBackoff(30*time.Second),
		WithCleanupMaxAttempts(20),
	)
	failed := creatingRecordingSandbox()
	failed.Status = StatusFailed
	failed.StartupFailureReason = "startup_interrupted"
	failed.CleanupStatus = CleanupStatusRetryableFailed
	failed.CleanupNextAttemptAt = timePtr(fixedSandboxTime.Add(9 * time.Minute))
	failed.CleanupAttemptCount = 19
	failed.ProviderSandboxID = "provider_sandbox_123"
	failed.ProviderHandle = ProviderHandle{Provider: "unit-provider", SandboxID: "provider_sandbox_123"}
	store.sandboxes[failed.ID] = failed

	got, err := service.ReconcileStartupCleanup(context.Background(), workspace.DefaultID, failed.ID)
	if err == nil {
		t.Fatal("ReconcileStartupCleanup succeeded; want retryable release error")
	}
	if got == nil || got.CleanupStatus != CleanupStatusRetryableFailed || !got.CleanupRetryable ||
		got.CleanupAttemptCount != 20 || got.CleanupNextAttemptAt == nil {
		t.Fatalf("cleanup result = %+v; want recorded retryable twentieth attempt", got)
	}

	now = now.Add(31 * time.Second)
	got, err = service.ReconcileStartupCleanup(context.Background(), workspace.DefaultID, failed.ID)
	if err != nil {
		t.Fatalf("cap observation: %v", err)
	}
	if got == nil || got.CleanupStatus != CleanupStatusPermanentFailed || got.CleanupRetryable ||
		got.CleanupAttemptCount != 20 || got.CleanupNextAttemptAt != nil {
		t.Fatalf("cap observation result = %+v; want permanent failure without attempt 21", got)
	}
	if len(provider.releaseHandles) != 1 {
		t.Fatalf("release calls = %d; want no provider archive call at cap", len(provider.releaseHandles))
	}
}

func TestReconcileStartupCleanupAtCapTreatsMissingProviderMachineAsSuccess(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	provider.status = ProviderStatus{Availability: ProviderMissing, SandboxStatus: StatusReleased}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime.Add(10 * time.Minute) }),
		WithCleanupMaxAttempts(20),
	)
	failed := creatingRecordingSandbox()
	failed.Status = StatusFailed
	failed.StartupFailureReason = "startup_interrupted"
	failed.CleanupStatus = CleanupStatusPending
	failed.CleanupAttemptCount = 20
	failed.MachineWasUsable = true
	failed.ProviderSandboxID = ""
	failed.ProviderHandle = ProviderHandle{}
	store.sandboxes[failed.ID] = failed

	got, err := service.ReconcileStartupCleanup(context.Background(), workspace.DefaultID, failed.ID)
	if err != nil {
		t.Fatalf("ReconcileStartupCleanup: %v", err)
	}
	if got == nil || got.Status != StatusReleased || got.CleanupStatus != CleanupStatusReleased ||
		got.CleanupAttemptCount != 20 {
		t.Fatalf("cap observation result = %+v; want provider-missing release without another attempt", got)
	}
	if len(provider.releaseHandles) != 0 {
		t.Fatalf("release calls = %d; want read-only cap observation", len(provider.releaseHandles))
	}
}

func TestReconcileStartupCleanupAtCapDeadlineLeavesLeaseForReclaim(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	now := time.Now().UTC()
	provider.statusHook = func(ctx context.Context) (ProviderStatus, error) {
		<-ctx.Done()
		return ProviderStatus{}, ctx.Err()
	}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return now }),
		WithCleanupLeaseDuration(CleanupLeaseCompletionWriteMargin+50*time.Millisecond),
		WithCleanupMaxAttempts(20),
	)
	failed := creatingRecordingSandbox()
	failed.Status = StatusFailed
	failed.StartupFailureReason = "startup_interrupted"
	failed.CleanupStatus = CleanupStatusPending
	failed.CleanupAttemptCount = 20
	failed.ProviderSandboxID = ""
	failed.ProviderHandle = ProviderHandle{}
	store.sandboxes[failed.ID] = failed

	got, err := service.ReconcileStartupCleanup(context.Background(), workspace.DefaultID, failed.ID)
	if err == nil {
		t.Fatal("ReconcileStartupCleanup succeeded; want provider observation deadline")
	}
	if got == nil || got.CleanupStatus != CleanupStatusInProgress ||
		got.CleanupAttemptCount != 20 || got.CleanupLeaseToken == "" || got.CleanupLeaseExpiresAt == nil {
		t.Fatalf("cap deadline result = %+v; want abandoned observation lease for expiry reclaim", got)
	}
	firstLeaseToken := got.CleanupLeaseToken
	now = got.CleanupLeaseExpiresAt.Add(time.Nanosecond)
	provider.statusHook = nil
	provider.status = ProviderStatus{Availability: ProviderMissing, SandboxStatus: StatusReleased}

	reclaimed, err := service.ReconcileStartupCleanup(context.Background(), workspace.DefaultID, failed.ID)
	if err != nil {
		t.Fatalf("reclaim cap observation: %v", err)
	}
	if reclaimed == nil || reclaimed.Status != StatusReleased ||
		reclaimed.CleanupStatus != CleanupStatusReleased ||
		reclaimed.CleanupAttemptCount != 20 || reclaimed.CleanupLeaseToken != "" {
		t.Fatalf("reclaimed cap observation = %+v; want provider-missing release", reclaimed)
	}
	if store.cleanupLeaseSequence != 2 || firstLeaseToken == "" {
		t.Fatalf("cleanup lease sequence = %d, first token = %q; want a fresh reclaim lease", store.cleanupLeaseSequence, firstLeaseToken)
	}
}

func TestReconcileStartupCleanupAtCapTreatsProviderTimeoutAsTerminalObservation(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	provider.statusHook = func(context.Context) (ProviderStatus, error) {
		return ProviderStatus{}, &ProviderError{
			Provider: "unit-provider",
			Stage:    StageStatus,
			Kind:     ProviderErrorTimeout,
		}
	}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime.Add(10 * time.Minute) }),
		WithCleanupMaxAttempts(20),
	)
	failed := creatingRecordingSandbox()
	failed.Status = StatusFailed
	failed.StartupFailureReason = "startup_interrupted"
	failed.CleanupStatus = CleanupStatusPending
	failed.CleanupAttemptCount = 20
	failed.ProviderSandboxID = ""
	failed.ProviderHandle = ProviderHandle{}
	store.sandboxes[failed.ID] = failed

	got, err := service.ReconcileStartupCleanup(context.Background(), workspace.DefaultID, failed.ID)
	if err != nil {
		t.Fatalf("ReconcileStartupCleanup: %v", err)
	}
	if got == nil || got.CleanupStatus != CleanupStatusPermanentFailed ||
		got.CleanupErrorKind != string(ProviderErrorTimeout) ||
		got.CleanupAttemptCount != 20 || got.CleanupLeaseToken != "" {
		t.Fatalf("cap provider-timeout result = %+v; want terminal observation without attempt 21", got)
	}
}

func TestReconcileStartupCleanupImposesProviderDeadlineBeforeLeaseExpiry(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	now := time.Now().UTC()
	var observedDeadline time.Time
	provider.releaseHook = func(ctx context.Context) error {
		var ok bool
		observedDeadline, ok = ctx.Deadline()
		if !ok {
			return errors.New("cleanup provider call has no deadline")
		}
		<-ctx.Done()
		// A provider must not be able to turn an expired lease into success by
		// returning nil after the imposed deadline.
		return nil
	}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return now }),
		WithCleanupLeaseDuration(CleanupLeaseCompletionWriteMargin+50*time.Millisecond),
	)
	failed := creatingRecordingSandbox()
	failed.Status = StatusFailed
	failed.StartupFailureReason = "startup_interrupted"
	failed.CleanupStatus = CleanupStatusPending
	failed.ProviderSandboxID = "provider_sandbox_123"
	failed.ProviderHandle = ProviderHandle{Provider: "unit-provider", SandboxID: "provider_sandbox_123"}
	store.sandboxes[failed.ID] = failed

	startedAt := time.Now()
	got, err := service.ReconcileStartupCleanup(context.Background(), workspace.DefaultID, failed.ID)
	if err == nil {
		t.Fatal("ReconcileStartupCleanup succeeded; want imposed provider deadline")
	}
	if elapsed := time.Since(startedAt); elapsed > 500*time.Millisecond {
		t.Fatalf("cleanup provider call elapsed %s; want cutoff before lease expiry", elapsed)
	}
	if observedDeadline.IsZero() {
		t.Fatal("cleanup provider did not observe a deadline")
	}
	if want := now.Add(50 * time.Millisecond); !observedDeadline.Equal(want) {
		t.Fatalf("provider deadline = %s; want persisted lease deadline %s", observedDeadline, want)
	}
	if got == nil || got.CleanupStatus != CleanupStatusInProgress ||
		got.CleanupAttemptCount != 1 || got.CleanupLeaseToken == "" {
		t.Fatalf("deadline result = %+v; want abandoned leased attempt for expiry reclaim", got)
	}
	if slices.Contains(store.events, "mark_cleanup_attempt") {
		t.Fatalf("events = %v; provider deadline must not spend the completion-write margin", store.events)
	}
}

func TestReconcileStartupCleanupCompletesBeforeReservedWriteMargin(t *testing.T) {
	events := []string{}
	store := newRecordingStore()
	store.eventsRef = &events
	provider := newSuccessfulRecordingProvider()
	provider.eventsRef = &events
	now := time.Now().UTC()
	var observedDeadline time.Time
	provider.releaseHook = func(ctx context.Context) error {
		var ok bool
		observedDeadline, ok = ctx.Deadline()
		if !ok {
			return errors.New("cleanup provider call has no deadline")
		}
		return nil
	}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return now }),
		WithCleanupLeaseDuration(CleanupLeaseCompletionWriteMargin+2*time.Second),
	)
	failed := creatingRecordingSandbox()
	failed.Status = StatusFailed
	failed.StartupFailureReason = "startup_interrupted"
	failed.CleanupStatus = CleanupStatusPending
	failed.ProviderSandboxID = "provider_sandbox_123"
	failed.ProviderHandle = ProviderHandle{Provider: "unit-provider", SandboxID: "provider_sandbox_123"}
	store.sandboxes[failed.ID] = failed

	got, err := service.ReconcileStartupCleanup(context.Background(), workspace.DefaultID, failed.ID)
	if err != nil {
		t.Fatalf("ReconcileStartupCleanup: %v", err)
	}
	if got == nil || got.CleanupStatus != CleanupStatusReleased || got.CleanupLeaseToken != "" {
		t.Fatalf("cleanup result = %+v; want completed lease", got)
	}
	if want := now.Add(2 * time.Second); !observedDeadline.Equal(want) {
		t.Fatalf("provider deadline = %s; want persisted lease deadline %s", observedDeadline, want)
	}
	if want := now.Add(CleanupLeaseCompletionWriteMargin + 2*time.Second); !store.markCleanupDeadline.Equal(want) {
		t.Fatalf("completion deadline = %s; want lease expiry %s", store.markCleanupDeadline, want)
	}
	if !reflect.DeepEqual(events, []string{"claim_startup_cleanup", "release", "mark_cleanup_attempt"}) {
		t.Fatalf("events = %v; want provider call followed by durable completion", events)
	}
}

func TestReconcileStartupCleanupNameProbeAndReleaseSharePersistedLeaseDeadline(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	now := time.Now().UTC()
	provider.status = ProviderStatus{Availability: ProviderAvailable, SandboxStatus: StatusStopped}
	var releaseDeadline time.Time
	provider.releaseHook = func(ctx context.Context) error {
		releaseDeadline, _ = ctx.Deadline()
		return nil
	}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return now }),
		WithCleanupLeaseDuration(CleanupLeaseCompletionWriteMargin+2*time.Second),
	)
	failed := creatingRecordingSandbox()
	failed.Status = StatusFailed
	failed.StartupFailureReason = "startup_interrupted"
	failed.CleanupStatus = CleanupStatusPending
	failed.ProviderSandboxID = ""
	failed.ProviderHandle = ProviderHandle{}
	store.sandboxes[failed.ID] = failed

	got, err := service.ReconcileStartupCleanup(context.Background(), workspace.DefaultID, failed.ID)
	if err != nil {
		t.Fatalf("ReconcileStartupCleanup: %v", err)
	}
	if got == nil || got.CleanupStatus != CleanupStatusReleased {
		t.Fatalf("cleanup result = %+v; want released", got)
	}
	providerDeadline := now.Add(2 * time.Second)
	if !store.saveHandleDeadline.Equal(providerDeadline) || !releaseDeadline.Equal(providerDeadline) {
		t.Fatalf("provider path deadlines = save %s release %s; want %s", store.saveHandleDeadline, releaseDeadline, providerDeadline)
	}
	completionDeadline := now.Add(CleanupLeaseCompletionWriteMargin + 2*time.Second)
	if !store.markCleanupDeadline.Equal(completionDeadline) {
		t.Fatalf("completion deadline = %s; want %s", store.markCleanupDeadline, completionDeadline)
	}
}

func TestReconcileStartupCleanupPreservesOriginalStartupFailureReason(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime.Add(10 * time.Minute) }),
		WithCleanupRetryBackoff(30*time.Second),
	)
	originalFailedAt := fixedSandboxTime.Add(-2 * time.Minute)
	failed := creatingRecordingSandbox()
	failed.Status = StatusFailed
	failed.StartupFailureReason = string(SandboxErrorMountFailed)
	failed.FailedAt = &originalFailedAt
	failed.CleanupStatus = CleanupStatusRetryableFailed
	failed.CleanupNextAttemptAt = timePtr(fixedSandboxTime.Add(9 * time.Minute))
	failed.ProviderSandboxID = "provider_sandbox_123"
	failed.ProviderHandle = ProviderHandle{Provider: "unit-provider", SandboxID: "provider_sandbox_123"}
	store.sandboxes[failed.ID] = failed

	got, err := service.ReconcileStartupCleanup(context.Background(), workspace.DefaultID, failed.ID)
	if err != nil {
		t.Fatalf("ReconcileStartupCleanup: %v", err)
	}
	if got.StartupFailureReason != string(SandboxErrorMountFailed) || got.FailedAt == nil || !got.FailedAt.Equal(originalFailedAt) {
		t.Fatalf("startup fields = reason %q failed_at %v; want preserved mount failure", got.StartupFailureReason, got.FailedAt)
	}
	if got.CleanupStatus != CleanupStatusReleased || got.CleanupAttemptCount != 1 {
		t.Fatalf("cleanup fields = %+v; want cleanup retry success", got)
	}
}

func TestReconcileStartupCleanupDoesNotDeleteSessionResourceCopiesAfterProviderNotFound(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	provider.releaseErr = &ProviderError{Provider: "unit-provider", Stage: StageReleaseSandbox, Kind: ProviderErrorNotFound}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime.Add(10 * time.Minute) }),
		WithCleanupRetryBackoff(30*time.Second),
	)
	failed := creatingRecordingSandbox()
	failed.Status = StatusFailed
	failed.StartupFailureReason = "startup_interrupted"
	failed.CleanupStatus = CleanupStatusRetryableFailed
	failed.CleanupNextAttemptAt = timePtr(fixedSandboxTime.Add(9 * time.Minute))
	failed.MachineWasUsable = true
	failed.ProviderSandboxID = "provider_sandbox_123"
	failed.ProviderHandle = ProviderHandle{Provider: "unit-provider", SandboxID: "provider_sandbox_123"}
	store.sandboxes[failed.ID] = failed

	got, err := service.ReconcileStartupCleanup(context.Background(), workspace.DefaultID, failed.ID)
	if err != nil {
		t.Fatalf("ReconcileStartupCleanup: %v", err)
	}
	if got == nil || got.Status != StatusReleased || got.CleanupStatus != CleanupStatusReleased ||
		got.CleanupRetryable || got.CleanupNextAttemptAt != nil {
		t.Fatalf("cleanup result = %+v; want provider absence recorded as released", got)
	}
}

func TestReconcileStartupCleanupNameProbeMissingReleasesUsableMachine(t *testing.T) {
	store := newRecordingStore()
	provider := newSuccessfulRecordingProvider()
	provider.status = ProviderStatus{Availability: ProviderMissing, SandboxStatus: StatusReleased}
	service := NewService(store, provider,
		WithProviderName("unit-provider"),
		WithClock(func() time.Time { return fixedSandboxTime.Add(10 * time.Minute) }),
	)
	failed := creatingRecordingSandbox()
	failed.Status = StatusFailed
	failed.StartupFailureReason = "startup_interrupted"
	failed.CleanupStatus = CleanupStatusPending
	failed.MachineWasUsable = true
	failed.ProviderSandboxID = ""
	failed.ProviderHandle = ProviderHandle{}
	store.sandboxes[failed.ID] = failed

	got, err := service.ReconcileStartupCleanup(context.Background(), workspace.DefaultID, failed.ID)
	if err != nil {
		t.Fatalf("ReconcileStartupCleanup: %v", err)
	}
	if got == nil || got.Status != StatusReleased || got.CleanupStatus != CleanupStatusReleased {
		t.Fatalf("cleanup result = %+v; want missing name probe to settle released", got)
	}
	if len(provider.releaseHandles) != 0 {
		t.Fatalf("release calls = %d; want none after missing name probe", len(provider.releaseHandles))
	}
}

func credentialBearingURLForTest() string {
	return "https://user:p" + "ass@example.test/api" //nolint:gosec // test fixture intentionally exercises credential URL rejection.
}

var fixedSandboxTime = time.Date(2099, 5, 22, 10, 0, 0, 0, time.UTC)

func newTestService(store *recordingStore, provider *recordingProvider) *Service {
	return NewService(store, provider,
		WithProviderName("unit-provider"),
		WithIDStrategy(func() string { return "sandbox_test" }),
		WithClock(func() time.Time { return fixedSandboxTime }),
	)
}

func minimalCreateRequest() CreateForSessionRequest {
	return CreateForSessionRequest{
		WorkspaceID:         workspace.DefaultID,
		SessionID:           "sesn_test",
		EnvironmentID:       "env_test",
		ProviderArtifactRef: "artifact_env_test",
		Network:             NetworkSetup{Type: "unrestricted"},
	}
}

func newSuccessfulRecordingProvider() *recordingProvider {
	return &recordingProvider{
		handle: ProviderHandle{
			Provider:  "unit-provider",
			SandboxID: "provider_sandbox_123",
			Metadata:  map[string]string{"region": "iad"},
		},
	}
}

type recordingProvider struct {
	handle     ProviderHandle
	status     ProviderStatus
	statusHook func(context.Context) (ProviderStatus, error)

	createErr        error
	startErr         error
	createHook       func()
	healthErr        error
	networkErr       error
	baseDirectoryErr error
	releaseErr       error
	releaseErrors    []error
	releaseHook      func(context.Context) error

	calls                []string
	createRequests       []CreateSandboxRequest
	startHandles         []ProviderHandle
	healthHandles        []ProviderHandle
	networkHandles       []ProviderHandle
	baseDirectoryHandles []ProviderHandle
	networkSetups        []NetworkSetup
	releaseHandles       []ProviderHandle
	releaseReasons       []ReleaseReason
	eventsRef            *[]string
}

func (p *recordingProvider) appendEvent(event string) {
	p.calls = append(p.calls, event)
	if p.eventsRef != nil {
		*p.eventsRef = append(*p.eventsRef, event)
	}
}

func (p *recordingProvider) CreateSandbox(ctx context.Context, request CreateSandboxRequest) (ProviderHandle, error) {
	p.appendEvent("create")
	p.createRequests = append(p.createRequests, request)
	if p.createErr != nil {
		return ProviderHandle{}, p.createErr
	}
	if p.createHook != nil {
		p.createHook()
	}
	return p.handle, nil
}

func (p *recordingProvider) StartSandbox(ctx context.Context, handle ProviderHandle) error {
	p.appendEvent("start")
	p.startHandles = append(p.startHandles, handle)
	return p.startErr
}

func (p *recordingProvider) CheckBaseTemplateHealth(ctx context.Context, handle ProviderHandle) error {
	p.appendEvent("health")
	p.healthHandles = append(p.healthHandles, handle)
	return p.healthErr
}

func (p *recordingProvider) ApplyNetworkPolicy(ctx context.Context, handle ProviderHandle, network NetworkSetup) error {
	p.appendEvent("network")
	p.networkHandles = append(p.networkHandles, handle)
	p.networkSetups = append(p.networkSetups, cloneNetworkSetup(network))
	return p.networkErr
}

func (p *recordingProvider) PrepareBaseDirectories(ctx context.Context, handle ProviderHandle) error {
	p.appendEvent("base_directories")
	p.baseDirectoryHandles = append(p.baseDirectoryHandles, handle)
	return p.baseDirectoryErr
}

func (p *recordingProvider) GetStatus(ctx context.Context, handle ProviderHandle) (ProviderStatus, error) {
	p.appendEvent("status")
	if p.statusHook != nil {
		return p.statusHook(ctx)
	}
	if p.status.Availability != "" {
		return p.status, nil
	}
	return ProviderStatus{Availability: ProviderAvailable, SandboxStatus: StatusActive}, nil
}

func (p *recordingProvider) ReleaseSandbox(ctx context.Context, handle ProviderHandle, reason ReleaseReason) error {
	p.appendEvent("release")
	p.releaseHandles = append(p.releaseHandles, handle)
	p.releaseReasons = append(p.releaseReasons, reason)
	if p.releaseHook != nil {
		return p.releaseHook(ctx)
	}
	if len(p.releaseErrors) > 0 {
		err := p.releaseErrors[0]
		p.releaseErrors = p.releaseErrors[1:]
		return err
	}
	return p.releaseErr
}

func newRecordingStore() *recordingStore {
	return &recordingStore{sandboxes: map[string]*Sandbox{}, deletedSessions: map[string]bool{}}
}

type recordingStore struct {
	sandboxes             map[string]*Sandbox
	deletedSessions       map[string]bool
	events                []string
	eventsRef             *[]string
	postCreateDisposition postCreatePreparationDisposition
	postCreateSettlement  func()

	createErr             error
	saveHandleErr         error
	markActiveErr         error
	findLiveErr           error
	markReleasingErr      error
	markReleasedErr       error
	markFailedErr         error
	prepareReplacementErr error
	cleanupLeaseSequence  int
	saveHandleDeadline    time.Time
	markCleanupDeadline   time.Time
}

func (s *recordingStore) SaveProviderHandleForSessionPreparation(ctx context.Context, ws workspace.ID, _ string, _ string, sandboxID string, handle ProviderHandle, updatedAt time.Time) (*Sandbox, postCreatePreparationDisposition, error) {
	if s.postCreateDisposition == postCreatePreparationDeleted || s.postCreateDisposition == postCreatePreparationStale {
		return nil, s.postCreateDisposition, nil
	}
	got, err := s.SaveProviderHandle(ctx, ws, sandboxID, handle, updatedAt)
	return got, postCreatePreparationCurrent, err
}

func (s *recordingStore) SettleDeletedSessionPreparationAfterCreate(_ context.Context, ws workspace.ID, _ string, _ string, sandboxID string, failedAt time.Time) error {
	current, ok := s.sandboxes[sandboxID]
	if !ok || current.WorkspaceID != ws {
		return &NotFoundError{Message: "sandbox not found"}
	}
	current.Status = StatusFailed
	current.FailedAt = &failedAt
	current.StartupFailureReason = sessionDeletedPreparationFailureReason
	current.CleanupStatus = CleanupStatusReleased
	current.CleanupMethod = "delete"
	current.CleanupLastAttemptAt = &failedAt
	current.CleanupAttemptCount = 1
	current.UpdatedAt = failedAt
	if s.postCreateSettlement != nil {
		s.postCreateSettlement()
	}
	return nil
}

func (s *recordingStore) appendEvent(event string) {
	s.events = append(s.events, event)
	if s.eventsRef != nil {
		*s.eventsRef = append(*s.eventsRef, event)
	}
}

func (s *recordingStore) CreateSandbox(ctx context.Context, sandbox *Sandbox) error {
	s.appendEvent("create")
	if s.createErr != nil {
		return s.createErr
	}
	stored := cloneSandbox(sandbox)
	if stored.Status == StatusActive {
		stored.MachineWasUsable = true
	}
	s.sandboxes[sandbox.ID] = stored
	return nil
}

func (s *recordingStore) SaveProviderHandle(ctx context.Context, ws workspace.ID, sandboxID string, handle ProviderHandle, updatedAt time.Time) (*Sandbox, error) {
	s.appendEvent("save_handle")
	s.saveHandleDeadline, _ = ctx.Deadline()
	if s.saveHandleErr != nil {
		return nil, s.saveHandleErr
	}
	current, ok := s.sandboxes[sandboxID]
	if !ok || current.WorkspaceID != ws {
		return nil, &NotFoundError{Message: "sandbox not found"}
	}
	current.Provider = handle.Provider
	current.ProviderSandboxID = handle.SandboxID
	current.ProviderMetadata = cloneStringMap(handle.Metadata)
	current.ProviderHandle = cloneProviderHandle(handle)
	current.UpdatedAt = updatedAt
	return cloneSandbox(current), nil
}

func (s *recordingStore) MarkActive(ctx context.Context, ws workspace.ID, sandboxID string, updatedAt time.Time) (*Sandbox, error) {
	s.appendEvent("mark_active")
	if s.markActiveErr != nil {
		return nil, s.markActiveErr
	}
	current, ok := s.sandboxes[sandboxID]
	if !ok || current.WorkspaceID != ws {
		return nil, &NotFoundError{Message: "sandbox not found"}
	}
	current.Status = StatusActive
	current.MachineWasUsable = true
	current.UpdatedAt = updatedAt
	current.StatusRefreshedAt = &updatedAt
	current.FailedAt = nil
	current.StartupFailureReason = ""
	current.CleanupStatus = CleanupStatusNone
	current.CleanupMethod = ""
	current.CleanupErrorKind = ""
	current.CleanupRetryable = false
	current.CleanupLastAttemptAt = nil
	current.CleanupNextAttemptAt = nil
	return cloneSandbox(current), nil
}

func (s *recordingStore) MarkReleasingForDelete(_ context.Context, ws workspace.ID, sandboxID string, updatedAt time.Time) (*Sandbox, error) {
	s.appendEvent("mark_releasing")
	current, ok := s.sandboxes[sandboxID]
	if !ok || current.WorkspaceID != ws {
		return nil, &NotFoundError{Message: "sandbox not found"}
	}
	current.Status = StatusReleasing
	current.UpdatedAt = updatedAt
	return cloneSandbox(current), nil
}

func (s *recordingStore) MarkReleasedForDeleteProviderMissing(_ context.Context, ws workspace.ID, sandboxID string, releasedAt time.Time) (*Sandbox, error) {
	s.appendEvent("mark_released_for_delete_provider_missing")
	current, ok := s.sandboxes[sandboxID]
	if !ok || current.WorkspaceID != ws || current.ProviderSandboxID != "" || (current.Status != StatusFailed && current.Status != StatusReleased) {
		return nil, &NotFoundError{Message: "sandbox not found"}
	}
	current.Status = StatusReleased
	if current.ReleasedAt == nil {
		current.ReleasedAt = &releasedAt
	}
	current.UpdatedAt = releasedAt
	return cloneSandbox(current), nil
}

func (s *recordingStore) PrepareSandboxReplacement(_ context.Context, ws workspace.ID, sandboxID string, environmentID string, environmentGeneration int64, updatedAt time.Time) (*Sandbox, error) {
	s.appendEvent("prepare_replacement")
	if s.prepareReplacementErr != nil {
		return nil, s.prepareReplacementErr
	}
	current, ok := s.sandboxes[sandboxID]
	if !ok || current.WorkspaceID != ws {
		return nil, &NotFoundError{Message: "sandbox not found"}
	}
	current.Status = StatusCreating
	current.EnvironmentID = environmentID
	current.EnvironmentGeneration = environmentGeneration
	current.ProviderSandboxID = ""
	current.ProviderHandle = ProviderHandle{}
	current.ProviderMetadata = nil
	current.ReleasedAt = nil
	current.StatusRefreshedAt = nil
	current.MachineWasUsable = false
	current.FailedAt = nil
	current.StartupFailureReason = ""
	current.CleanupStatus = CleanupStatusNone
	current.CleanupMethod = ""
	current.CleanupErrorKind = ""
	current.CleanupRetryable = false
	current.CleanupLastAttemptAt = nil
	current.CleanupNextAttemptAt = nil
	current.CleanupAttemptCount = 0
	current.CleanupLeaseToken = ""
	current.CleanupLeaseExpiresAt = nil
	current.UpdatedAt = updatedAt
	return cloneSandbox(current), nil
}

func (s *recordingStore) MarkStatusRefreshed(ctx context.Context, ws workspace.ID, sandboxID string, refreshedAt time.Time) (*Sandbox, error) {
	s.appendEvent("mark_status_refreshed")
	current, ok := s.sandboxes[sandboxID]
	if !ok || current.WorkspaceID != ws {
		return nil, &NotFoundError{Message: "sandbox not found"}
	}
	current.StatusRefreshedAt = &refreshedAt
	current.UpdatedAt = refreshedAt
	return cloneSandbox(current), nil
}

func (s *recordingStore) RefreshSandboxState(ctx context.Context, ws workspace.ID, sandboxID string, status Status, refreshedAt time.Time) (*Sandbox, error) {
	s.appendEvent("refresh_state")
	current, ok := s.sandboxes[sandboxID]
	if !ok || current.WorkspaceID != ws ||
		(current.Status != StatusCreating && current.Status != StatusActive &&
			current.Status != StatusStopped && current.Status != StatusArchived &&
			current.Status != StatusResuming) {
		return nil, &NotFoundError{Message: "sandbox not found"}
	}
	current.Status = status
	current.StatusRefreshedAt = &refreshedAt
	current.UpdatedAt = refreshedAt
	switch status {
	case StatusActive:
		current.MachineWasUsable = true
		current.FailedAt = nil
		current.StartupFailureReason = ""
		current.CleanupStatus = CleanupStatusNone
		current.CleanupMethod = ""
		current.CleanupErrorKind = ""
		current.CleanupRetryable = false
		current.CleanupLastAttemptAt = nil
		current.CleanupNextAttemptAt = nil
	case StatusReleased:
		current.ReleasedAt = &refreshedAt
	case StatusFailed:
		current.FailedAt = &refreshedAt
	}
	return cloneSandbox(current), nil
}

func (s *recordingStore) MarkArchived(_ context.Context, ws workspace.ID, sandboxID string, archivedAt time.Time) (*Sandbox, error) {
	s.appendEvent("mark_archived")
	current, ok := s.sandboxes[sandboxID]
	if !ok || current.WorkspaceID != ws || current.Status != StatusReleasing {
		return nil, &NotFoundError{Message: "sandbox not found"}
	}
	current.Status = StatusArchived
	current.StatusRefreshedAt = &archivedAt
	current.UpdatedAt = archivedAt
	return cloneSandbox(current), nil
}

func (s *recordingStore) InspectStaleStartup(_ context.Context, ws workspace.ID, sandboxID string, staleBefore time.Time) (StaleStartupInspection, error) {
	s.appendEvent("inspect_stale_startup")
	current, ok := s.sandboxes[sandboxID]
	if !ok || current.WorkspaceID != ws || (current.Status != StatusCreating && current.Status != StatusResuming) || current.UpdatedAt.After(staleBefore) {
		return StaleStartupInspection{}, &NotFoundError{Message: "stale startup sandbox not found"}
	}
	return StaleStartupInspection{Sandbox: cloneSandbox(current), SessionDeleted: s.deletedSessions[current.SessionID]}, nil
}

func (s *recordingStore) RefreshStaleStartup(_ context.Context, ws workspace.ID, sandboxID string, staleBefore time.Time, update StaleStartupRefreshUpdate) (*Sandbox, error) {
	s.appendEvent("refresh_stale_startup")
	current, ok := s.sandboxes[sandboxID]
	if !ok || current.WorkspaceID != ws || (current.Status != StatusCreating && current.Status != StatusResuming) || current.UpdatedAt.After(staleBefore) {
		return nil, &NotFoundError{Message: "stale startup sandbox not found"}
	}
	if update.AdoptedProviderHandle != nil {
		current.Provider = update.AdoptedProviderHandle.Provider
		current.ProviderSandboxID = update.AdoptedProviderHandle.SandboxID
		current.ProviderMetadata = cloneStringMap(update.AdoptedProviderHandle.Metadata)
		current.ProviderHandle = cloneProviderHandle(*update.AdoptedProviderHandle)
	}
	current.Status = update.Status
	switch update.Status {
	case StatusActive:
		current.MachineWasUsable = true
	case StatusReleased:
		current.CleanupStatus = CleanupStatusReleased
		current.CleanupErrorKind = string(ProviderErrorNotFound)
		current.CleanupRetryable = false
		current.CleanupNextAttemptAt = nil
		current.CleanupLeaseToken = ""
		current.CleanupLeaseExpiresAt = nil
		current.ReleasedAt = timePtr(update.RefreshedAt)
	}
	current.StatusRefreshedAt = &update.RefreshedAt
	current.UpdatedAt = update.RefreshedAt
	return cloneSandbox(current), nil
}

func (s *recordingStore) ClaimStaleCreating(ctx context.Context, ws workspace.ID, sandboxID string, staleBefore time.Time, update StartupFailureUpdate) (*Sandbox, error) {
	s.appendEvent("claim_stale_creating")
	current, ok := s.sandboxes[sandboxID]
	if !ok || current.WorkspaceID != ws || (current.Status != StatusCreating && current.Status != StatusResuming) || current.UpdatedAt.After(staleBefore) {
		return nil, &NotFoundError{Message: "stale creating sandbox not found"}
	}
	if update.AdoptedProviderHandle != nil {
		current.Provider = update.AdoptedProviderHandle.Provider
		current.ProviderSandboxID = update.AdoptedProviderHandle.SandboxID
		current.ProviderMetadata = cloneStringMap(update.AdoptedProviderHandle.Metadata)
		current.ProviderHandle = cloneProviderHandle(*update.AdoptedProviderHandle)
	}
	current.Status = StatusFailed
	current.FailedAt = &update.FailedAt
	current.UpdatedAt = update.FailedAt
	current.StartupFailureReason = update.StartupFailureReason
	current.CleanupStatus = update.CleanupStatus
	current.CleanupMethod = ""
	current.CleanupErrorKind = ""
	current.CleanupRetryable = false
	current.CleanupLastAttemptAt = nil
	current.CleanupNextAttemptAt = nil
	return cloneSandbox(current), nil
}

func (s *recordingStore) ClaimDueStartupCleanup(ctx context.Context, ws workspace.ID, sandboxID string, now time.Time, leaseDuration time.Duration, maxAttempts int) (*StartupCleanupClaim, error) {
	s.appendEvent("claim_startup_cleanup")
	current, ok := s.sandboxes[sandboxID]
	if !ok ||
		current.WorkspaceID != ws ||
		current.Status != StatusFailed ||
		current.StartupFailureReason == "" ||
		(current.CleanupStatus != CleanupStatusPending &&
			(current.CleanupStatus != CleanupStatusRetryableFailed ||
				(current.CleanupNextAttemptAt != nil && current.CleanupNextAttemptAt.After(now))) &&
			(current.CleanupStatus != CleanupStatusInProgress ||
				current.CleanupLeaseExpiresAt == nil || current.CleanupLeaseExpiresAt.After(now))) {
		return nil, &NotFoundError{Message: "due startup cleanup not found"}
	}
	s.cleanupLeaseSequence++
	current.CleanupStatus = CleanupStatusInProgress
	current.CleanupLeaseToken = fmt.Sprintf("lease_test_%d", s.cleanupLeaseSequence)
	current.CleanupLeaseExpiresAt = timePtr(now.Add(leaseDuration))
	current.CleanupRetryable = false
	current.CleanupNextAttemptAt = nil
	attemptReserved := current.CleanupAttemptCount < maxAttempts
	if attemptReserved {
		current.CleanupAttemptCount++
		current.CleanupLastAttemptAt = timePtr(now)
	}
	current.UpdatedAt = now
	return &StartupCleanupClaim{
		Sandbox:                 cloneSandbox(current),
		ProviderAttemptReserved: attemptReserved,
	}, nil
}

func (s *recordingStore) MarkStartupCleanupAttempt(ctx context.Context, ws workspace.ID, sandboxID string, update CleanupAttemptUpdate) (*Sandbox, error) {
	s.appendEvent("mark_cleanup_attempt")
	s.markCleanupDeadline, _ = ctx.Deadline()
	current, ok := s.sandboxes[sandboxID]
	if !ok || current.WorkspaceID != ws {
		return nil, &NotFoundError{Message: "startup cleanup not found"}
	}
	if current.CleanupStatus == CleanupStatusInProgress && current.CleanupLeaseToken != update.CleanupLeaseToken {
		return cloneSandbox(current), nil
	}
	if cleanupStatusTerminal(current.CleanupStatus) ||
		current.Status == StatusArchived || current.Status == StatusReleasing || current.Status == StatusReleased {
		return cloneSandbox(current), nil
	}
	if current.Status != StatusFailed || current.StartupFailureReason == "" || current.CleanupStatus != CleanupStatusInProgress {
		return nil, &NotFoundError{Message: "startup cleanup not found"}
	}
	current.UpdatedAt = update.AttemptedAt
	current.CleanupStatus = update.CleanupStatus
	current.CleanupMethod = update.CleanupMethod
	current.CleanupErrorKind = update.CleanupErrorKind
	current.CleanupRetryable = update.CleanupRetryable
	current.CleanupNextAttemptAt = update.CleanupNextAttemptAt
	current.CleanupLeaseToken = ""
	current.CleanupLeaseExpiresAt = nil
	if update.CleanupStatus == CleanupStatusReleased && update.CleanupErrorKind == string(ProviderErrorNotFound) {
		current.Status = StatusReleased
		current.ReleasedAt = timePtr(update.AttemptedAt)
	} else if current.MachineWasUsable && update.CleanupStatus == CleanupStatusReleased {
		current.Status = StatusArchived
	}
	return cloneSandbox(current), nil
}

func (s *recordingStore) FindLiveBySessionID(ctx context.Context, ws workspace.ID, sessionID string) (*Sandbox, error) {
	s.appendEvent("find_live")
	if s.findLiveErr != nil {
		return nil, s.findLiveErr
	}
	for _, sandbox := range s.sandboxes {
		if sandbox.WorkspaceID == ws && sandbox.SessionID == sessionID && sandbox.Status.IsLive() {
			return cloneSandbox(sandbox), nil
		}
	}
	return nil, &NotFoundError{Message: "live sandbox not found"}
}

func (s *recordingStore) FindLatestBySessionID(ctx context.Context, ws workspace.ID, sessionID string) (*Sandbox, error) {
	s.appendEvent("find_latest")
	var latest *Sandbox
	for _, sandbox := range s.sandboxes {
		if sandbox.WorkspaceID == ws && sandbox.SessionID == sessionID {
			if latest == nil || sandbox.UpdatedAt.After(latest.UpdatedAt) {
				latest = sandbox
			}
		}
	}
	if latest == nil {
		return nil, &NotFoundError{Message: "sandbox not found"}
	}
	return cloneSandbox(latest), nil
}

func (s *recordingStore) MarkReleasing(ctx context.Context, ws workspace.ID, sandboxID string, updatedAt time.Time) (*Sandbox, error) {
	s.appendEvent("mark_releasing")
	if s.markReleasingErr != nil {
		return nil, s.markReleasingErr
	}
	current, ok := s.sandboxes[sandboxID]
	if !ok || current.WorkspaceID != ws {
		return nil, &NotFoundError{Message: "sandbox not found"}
	}
	current.Status = StatusReleasing
	current.UpdatedAt = updatedAt
	return cloneSandbox(current), nil
}

func (s *recordingStore) MarkReleased(ctx context.Context, ws workspace.ID, sandboxID string, releasedAt time.Time) (*Sandbox, error) {
	s.appendEvent("mark_released")
	if s.markReleasedErr != nil {
		return nil, s.markReleasedErr
	}
	current, ok := s.sandboxes[sandboxID]
	if !ok || current.WorkspaceID != ws {
		return nil, &NotFoundError{Message: "sandbox not found"}
	}
	current.Status = StatusReleased
	current.ReleasedAt = &releasedAt
	current.UpdatedAt = releasedAt
	return cloneSandbox(current), nil
}

func (s *recordingStore) MarkFailed(ctx context.Context, ws workspace.ID, sandboxID string, failedAt time.Time) (*Sandbox, error) {
	s.appendEvent("mark_failed")
	if s.markFailedErr != nil {
		return nil, s.markFailedErr
	}
	current, ok := s.sandboxes[sandboxID]
	if !ok || current.WorkspaceID != ws {
		return nil, &NotFoundError{Message: "sandbox not found"}
	}
	current.Status = StatusFailed
	current.FailedAt = &failedAt
	current.UpdatedAt = failedAt
	return cloneSandbox(current), nil
}

func (s *recordingStore) MarkStartupFailed(ctx context.Context, ws workspace.ID, sandboxID string, update StartupFailureUpdate) (*Sandbox, error) {
	s.appendEvent("mark_failed")
	if s.markFailedErr != nil {
		return nil, s.markFailedErr
	}
	current, ok := s.sandboxes[sandboxID]
	if !ok || current.WorkspaceID != ws {
		return nil, &NotFoundError{Message: "sandbox not found"}
	}
	if current.Status == StatusFailed || current.Status == StatusArchived ||
		current.Status == StatusReleasing || current.Status == StatusReleased {
		return cloneSandbox(current), nil
	}
	current.Status = StatusFailed
	current.FailedAt = &update.FailedAt
	current.UpdatedAt = update.FailedAt
	current.StartupFailureReason = update.StartupFailureReason
	current.CleanupStatus = update.CleanupStatus
	current.CleanupMethod = update.CleanupMethod
	current.CleanupErrorKind = update.CleanupErrorKind
	current.CleanupRetryable = update.CleanupRetryable
	if update.CleanupStatus == CleanupStatusReleased && update.CleanupErrorKind == string(ProviderErrorNotFound) {
		current.Status = StatusReleased
		current.ReleasedAt = timePtr(update.FailedAt)
	} else if current.MachineWasUsable && update.CleanupStatus == CleanupStatusReleased {
		current.Status = StatusArchived
	}
	if update.CleanupAttempted {
		current.CleanupLastAttemptAt = &update.FailedAt
		current.CleanupAttemptCount++
	}
	current.CleanupNextAttemptAt = update.CleanupNextAttemptAt
	return cloneSandbox(current), nil
}

func activeRecordingSandbox() *Sandbox {
	return &Sandbox{
		ID:                "sandbox_test",
		WorkspaceID:       workspace.DefaultID,
		SessionID:         "sesn_test",
		Status:            StatusActive,
		Provider:          "unit-provider",
		ProviderSandboxID: "provider_sandbox_123",
		ProviderMetadata:  map[string]string{"region": "iad"},
		ProviderHandle: ProviderHandle{
			Provider:  "unit-provider",
			SandboxID: "provider_sandbox_123",
			Metadata:  map[string]string{"region": "iad"},
		},
		MachineWasUsable: true,
		CreatedAt:        fixedSandboxTime,
		UpdatedAt:        fixedSandboxTime,
	}
}

func creatingRecordingSandbox() *Sandbox {
	return &Sandbox{
		ID:          "sandbox_test",
		WorkspaceID: workspace.DefaultID,
		SessionID:   "sesn_test",
		Status:      StatusCreating,
		Provider:    "unit-provider",
		CreatedAt:   fixedSandboxTime,
		UpdatedAt:   fixedSandboxTime,
	}
}

func timePtr(value time.Time) *time.Time {
	return &value
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func assertProviderReleasedStoredHandle(t *testing.T, provider *recordingProvider, want ProviderHandle) {
	t.Helper()
	if len(provider.releaseHandles) != 1 || !reflect.DeepEqual(provider.releaseHandles[0], want) {
		t.Fatalf("release handles = %#v; want exactly stored handle %#v", provider.releaseHandles, want)
	}
}
