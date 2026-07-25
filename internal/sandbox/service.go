package sandbox

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type Service struct {
	store                Store
	provider             LifecycleProvider
	providerName         string
	resourcePreparer     SessionResourcePreparer
	memorySnapshotReader MemorySnapshotReader
	memoryMutationLocker MemoryStoreMutationLocker
	memoryMaterializer   MemoryStoreMaterializer
	preparationStore     SessionPreparationStore
	gitTicketRotator     GitTicketRotator
	gitHubMaterializer   GitHubRepositoryMaterializer
	gitProxyHost         string
	gitTicketRandom      io.Reader
	statusFreshnessTTL   time.Duration
	staleStartupAfter    time.Duration
	cleanupRetryBackoff  time.Duration
	cleanupLeaseDuration time.Duration
	cleanupMaxAttempts   int
	clock                func() time.Time
	idStrategy           func() string
}

type Option func(*Service)

func WithProviderName(providerName string) Option {
	return func(s *Service) { s.providerName = providerName }
}

func WithClock(clock func() time.Time) Option {
	return func(s *Service) { s.clock = clock }
}

func WithIDStrategy(strategy func() string) Option {
	return func(s *Service) { s.idStrategy = strategy }
}

func WithStatusFreshnessTTL(ttl time.Duration) Option {
	return func(s *Service) { s.statusFreshnessTTL = ttl }
}

func WithStaleStartupThreshold(threshold time.Duration) Option {
	return func(s *Service) { s.staleStartupAfter = threshold }
}

func WithCleanupRetryBackoff(backoff time.Duration) Option {
	return func(s *Service) { s.cleanupRetryBackoff = backoff }
}

func WithCleanupLeaseDuration(duration time.Duration) Option {
	return func(s *Service) { s.cleanupLeaseDuration = duration }
}

func WithCleanupMaxAttempts(maxAttempts int) Option {
	return func(s *Service) { s.cleanupMaxAttempts = maxAttempts }
}

func WithSessionResourcePreparer(preparer SessionResourcePreparer) Option {
	return func(s *Service) { s.resourcePreparer = preparer }
}

func WithMemoryProjection(reader MemorySnapshotReader, materializer MemoryStoreMaterializer) Option {
	return func(s *Service) {
		s.memorySnapshotReader = reader
		if locker, ok := reader.(MemoryStoreMutationLocker); ok {
			s.memoryMutationLocker = locker
		}
		s.memoryMaterializer = materializer
	}
}

func WithSessionPreparationStore(store SessionPreparationStore) Option {
	return func(s *Service) { s.preparationStore = store }
}

func NewService(store Store, provider LifecycleProvider, options ...Option) *Service {
	service := &Service{
		store:                store,
		provider:             provider,
		cleanupLeaseDuration: 120 * time.Second,
		cleanupMaxAttempts:   20,
		clock:                func() time.Time { return storage.Now() },
		idStrategy:           func() string { return id.New("sandbox_") },
	}
	for _, option := range options {
		option(service)
	}
	if service.providerName != "" {
		if err := ValidateProviderName(service.providerName); err != nil {
			panic("sandbox: invalid provider name")
		}
	}
	return service
}

func (s *Service) CreateForSession(ctx context.Context, request CreateForSessionRequest) (*Sandbox, error) {
	created, err := s.CreateSandboxRecordForSession(ctx, request)
	if err != nil {
		return nil, err
	}
	return s.StartCreatedSandbox(ctx, created, request)
}

func (s *Service) CreateSandboxRecordForSession(ctx context.Context, request CreateForSessionRequest) (*Sandbox, error) {
	return s.CreateSandboxRecordForSessionWithStore(ctx, s.store, request)
}

func (s *Service) CreateSandboxRecordForSessionWithStore(ctx context.Context, store Store, request CreateForSessionRequest) (*Sandbox, error) {
	if err := s.validateConfigured(); err != nil {
		return nil, err
	}
	if store == nil {
		return nil, &SandboxError{Code: SandboxErrorProviderUnconfigured, Message: "sandbox provider is not configured"}
	}
	if request.WorkspaceID == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	if request.SessionID == "" {
		return nil, &ValidationError{Message: "session_id is required"}
	}
	if request.EnvironmentID == "" {
		return nil, &ValidationError{Message: "environment_id is required"}
	}
	if request.ProviderArtifactRef == "" {
		return nil, &ValidationError{Message: "provider_artifact_ref is required"}
	}

	now := s.clock().UTC()
	sandboxID := request.SandboxID
	if sandboxID == "" {
		sandboxID = s.idStrategy()
	}
	record := &Sandbox{
		ID:                    sandboxID,
		WorkspaceID:           request.WorkspaceID,
		SessionID:             request.SessionID,
		EnvironmentID:         request.EnvironmentID,
		EnvironmentGeneration: request.EnvironmentGeneration,
		Status:                StatusCreating,
		Provider:              s.providerName,
		CreatedAt:             now,
		UpdatedAt:             now,
	}
	if err := store.CreateSandbox(ctx, record); err != nil {
		return nil, &SandboxError{Code: SandboxErrorCreateFailed, Message: "sandbox startup failed", Cause: err}
	}
	return cloneSandbox(record), nil
}

func (s *Service) StartCreatedSandbox(ctx context.Context, created *Sandbox, request CreateForSessionRequest) (*Sandbox, error) {
	started, _, err := s.startCreatedSandbox(ctx, created, request)
	return started, err
}

type postCreatePreparationDisposition string

const (
	postCreatePreparationCurrent postCreatePreparationDisposition = "current"
	postCreatePreparationStale   postCreatePreparationDisposition = "stale"
	postCreatePreparationDeleted postCreatePreparationDisposition = "deleted"
)

type postCreatePreparationStore interface {
	SaveProviderHandleForSessionPreparation(context.Context, workspace.ID, string, string, string, ProviderHandle, time.Time) (*Sandbox, postCreatePreparationDisposition, error)
	SettleDeletedSessionPreparationAfterCreate(context.Context, workspace.ID, string, string, string, time.Time) error
}

func (s *Service) startCreatedSandbox(ctx context.Context, created *Sandbox, request CreateForSessionRequest) (*Sandbox, ResourceSetup, error) {
	if err := s.validateConfigured(); err != nil {
		return nil, ResourceSetup{}, err
	}
	if created == nil {
		return nil, ResourceSetup{}, &ValidationError{Message: "sandbox is required"}
	}
	if created.WorkspaceID == "" || created.SessionID == "" || created.ID == "" {
		return nil, ResourceSetup{}, &ValidationError{Message: "sandbox record is incomplete"}
	}
	if request.WorkspaceID == "" {
		request.WorkspaceID = created.WorkspaceID
	}
	if request.SessionID == "" {
		request.SessionID = created.SessionID
	}
	if request.WorkspaceID != created.WorkspaceID || request.SessionID != created.SessionID {
		return nil, ResourceSetup{}, &ValidationError{Message: "sandbox record does not match session startup request"}
	}
	if request.EnvironmentID == "" {
		return nil, ResourceSetup{}, &ValidationError{Message: "environment_id is required"}
	}
	setup := SandboxSetup{
		WorkspaceID:         request.WorkspaceID,
		SessionID:           request.SessionID,
		SandboxID:           created.ID,
		EnvironmentID:       request.EnvironmentID,
		ProviderArtifactRef: request.ProviderArtifactRef,
		Network:             cloneNetworkSetup(request.Network),
		Resources:           coldResourceSetup(request.Resources),
	}
	handle, err := s.provider.CreateSandbox(ctx, CreateSandboxRequest{Setup: setup})
	if err != nil {
		return nil, ResourceSetup{}, s.createStageError(ctx, request.WorkspaceID, request.SessionID, created.ID, ProviderHandle{}, created.CleanupAttemptCount, SandboxErrorCreateFailed, err)
	}
	handle = s.completeProviderHandle(handle)
	if err := ValidateProviderHandle(handle); err != nil {
		return nil, ResourceSetup{}, s.createStageError(ctx, request.WorkspaceID, request.SessionID, created.ID, handle, created.CleanupAttemptCount, SandboxErrorCreateFailed, err)
	}
	handleSaved := false
	if request.PreparationAttemptID != "" {
		if store, ok := s.store.(postCreatePreparationStore); ok {
			saved, disposition, saveErr := store.SaveProviderHandleForSessionPreparation(
				ctx, request.WorkspaceID, request.SessionID, request.PreparationAttemptID, created.ID, handle, s.clock().UTC(),
			)
			if saveErr != nil {
				_ = s.provider.ReleaseSandbox(ctx, handle, ReleaseReasonDelete)
				return nil, ResourceSetup{}, saveErr
			}
			switch disposition {
			case postCreatePreparationCurrent:
				created = saved
				handleSaved = true
			case postCreatePreparationDeleted:
				if releaseErr := s.provider.ReleaseSandbox(ctx, handle, ReleaseReasonDelete); releaseErr != nil && !isReleaseSandboxNotFound(releaseErr) {
					return nil, ResourceSetup{}, releaseErr
				}
				if settleErr := store.SettleDeletedSessionPreparationAfterCreate(
					ctx, request.WorkspaceID, request.SessionID, request.PreparationAttemptID, created.ID, s.clock().UTC(),
				); settleErr != nil {
					return nil, ResourceSetup{}, settleErr
				}
				return nil, ResourceSetup{}, errDeletedSessionAfterProviderCreate
			case postCreatePreparationStale:
				if releaseErr := s.provider.ReleaseSandbox(ctx, handle, ReleaseReasonDelete); releaseErr != nil && !isReleaseSandboxNotFound(releaseErr) {
					return nil, ResourceSetup{}, releaseErr
				}
				return nil, ResourceSetup{}, ErrStalePreparationAttempt
			default:
				_ = s.provider.ReleaseSandbox(ctx, handle, ReleaseReasonDelete)
				return nil, ResourceSetup{}, errors.New("session preparation post-create disposition is invalid")
			}
		}
	}
	if request.PreparationAttemptID != "" && !handleSaved {
		if checker, ok := s.preparationStore.(interface {
			SessionPreparationStillCurrent(context.Context, workspace.ID, string, string) (bool, error)
		}); ok {
			current, checkErr := checker.SessionPreparationStillCurrent(ctx, request.WorkspaceID, request.SessionID, request.PreparationAttemptID)
			if checkErr != nil {
				_ = s.provider.ReleaseSandbox(ctx, handle, ReleaseReasonDelete)
				return nil, ResourceSetup{}, checkErr
			}
			if !current {
				if releaseErr := s.provider.ReleaseSandbox(ctx, handle, ReleaseReasonDelete); releaseErr != nil && !isReleaseSandboxNotFound(releaseErr) {
					return nil, ResourceSetup{}, releaseErr
				}
				return nil, ResourceSetup{}, ErrStalePreparationAttempt
			}
		}
	}
	if !handleSaved {
		if _, err := s.store.SaveProviderHandle(ctx, request.WorkspaceID, created.ID, handle, s.clock().UTC()); err != nil {
			return nil, ResourceSetup{}, s.createStageError(ctx, request.WorkspaceID, request.SessionID, created.ID, handle, created.CleanupAttemptCount, SandboxErrorCreateFailed, err)
		}
	}
	if err := s.provider.CheckBaseTemplateHealth(ctx, handle); err != nil {
		return nil, ResourceSetup{}, s.createStageError(ctx, request.WorkspaceID, request.SessionID, created.ID, handle, created.CleanupAttemptCount, SandboxErrorBaseTemplateFailed, err)
	}
	active, err := s.store.MarkActive(ctx, request.WorkspaceID, created.ID, s.clock().UTC())
	if err != nil {
		return nil, ResourceSetup{}, s.createStageError(ctx, request.WorkspaceID, request.SessionID, created.ID, handle, created.CleanupAttemptCount, SandboxErrorCreateFailed, err)
	}
	setup.ResourceCleanup = request.ResourceCleanup
	preparedResources, err := s.prepareResources(ctx, setup, handle)
	if err != nil {
		if errors.Is(err, ErrStalePreparationAttempt) {
			return nil, ResourceSetup{}, err
		}
		if retryableSessionPreparationError(err) {
			return nil, ResourceSetup{}, &SandboxError{Code: SandboxErrorMountFailed, Message: "sandbox resource preparation failed", Cause: err}
		}
		return nil, ResourceSetup{}, s.createStageError(ctx, request.WorkspaceID, request.SessionID, created.ID, handle, created.CleanupAttemptCount, SandboxErrorMountFailed, err)
	}
	return active, preparedResources, nil
}

func (s *Service) prepareResources(ctx context.Context, setup SandboxSetup, handle ProviderHandle) (ResourceSetup, error) {
	if err := validateSessionResourceMountPaths(setup.Resources); err != nil {
		return ResourceSetup{}, err
	}
	if hasDeletedSessionResources(setup.Resources) && setup.ResourceCleanup == nil {
		return ResourceSetup{}, &ValidationError{Message: "session resource cleanup coordinator is required"}
	}
	if err := s.provider.PrepareBaseDirectories(ctx, handle); err != nil {
		return ResourceSetup{}, err
	}
	if err := s.removeDeletedGitHubRepositories(ctx, handle, setup.Resources.DeletedGitHubRepositories, setup.ResourceCleanup); err != nil {
		return ResourceSetup{}, err
	}
	originalSetup := setup
	originalSetup.Resources = cloneResourceSetup(setup.Resources)
	preparedResources, err := s.prepareSessionResources(ctx, setup, handle)
	if err != nil {
		return ResourceSetup{}, err
	}
	setup.Resources = preparedResources
	if err := s.materializeMemoryProjections(ctx, setup.WorkspaceID, handle, setup.Resources, setup.ResourceCleanup); err != nil {
		if errors.Is(err, ErrStalePreparationAttempt) {
			return ResourceSetup{}, err
		}
		return ResourceSetup{}, errors.Join(err, s.compensateSessionResourcePreparation(ctx, originalSetup, handle))
	}
	if err := s.prepareGitHubRepositories(ctx, setup.WorkspaceID, setup.SessionID, handle, setup.Resources); err != nil {
		return ResourceSetup{}, errors.Join(err, s.compensateSessionResourcePreparation(ctx, originalSetup, handle))
	}
	return preparedResources, nil
}

func hasDeletedSessionResources(resources ResourceSetup) bool {
	return len(resources.DeletedFiles) > 0 || len(resources.DeletedMemoryStores) > 0 || len(resources.DeletedGitHubRepositories) > 0
}

func coldResourceSetup(resources ResourceSetup) ResourceSetup {
	cloned := cloneResourceSetup(resources)
	cloned.ResourceCredExpiresAt = nil
	return cloned
}

func (s *Service) ReconcileStaleCreating(ctx context.Context, ws workspace.ID, sandboxID string) (*Sandbox, error) {
	if err := s.validateConfigured(); err != nil {
		return nil, err
	}
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	if sandboxID == "" {
		return nil, &ValidationError{Message: "sandbox_id is required"}
	}
	if s.staleStartupAfter <= 0 {
		return nil, &ValidationError{Message: "stale startup threshold is required"}
	}
	now := s.clock().UTC()
	staleBefore := now.Add(-s.staleStartupAfter)
	inspection, err := s.store.InspectStaleStartup(ctx, ws, sandboxID, staleBefore)
	if err != nil {
		var notFound *NotFoundError
		if errors.As(err, &notFound) {
			return nil, nil
		}
		return nil, &SandboxError{Code: SandboxErrorCreateFailed, Message: "sandbox reconciliation failed", Cause: err}
	}
	if inspection.Sandbox == nil {
		return nil, nil
	}
	if inspection.SessionDeleted {
		return s.claimInterruptedStartup(ctx, inspection.Sandbox, staleBefore, now, CleanupStatusPending, nil, false)
	}

	current := inspection.Sandbox
	handle := handleFromSandbox(current)
	var adopted *ProviderHandle
	if handle.SandboxID == "" {
		nameHandle := ProviderHandle{Provider: s.providerName, SandboxID: current.ID}
		handle = nameHandle
		adopted = &nameHandle
	}
	status, statusErr := s.provider.GetStatus(ctx, handle)
	if statusErr != nil {
		if isProviderNotFound(statusErr) {
			return s.settleMissingStaleStartup(ctx, current, staleBefore, now)
		}
		return nil, &SandboxError{Code: SandboxErrorCreateFailed, Message: "sandbox reconciliation probe failed", Cause: statusErr}
	}
	if err := ValidateProviderStatus(status); err != nil {
		return nil, &SandboxError{Code: SandboxErrorCreateFailed, Message: "sandbox reconciliation probe failed", Cause: err}
	}
	if status.Availability == ProviderMissing {
		return s.settleMissingStaleStartup(ctx, current, staleBefore, now)
	}
	providerState := status.SandboxStatus
	if providerState == "" {
		providerState = sandboxStatusFromProviderAvailability(status.Availability)
	}
	if adopted != nil && status.Metadata != nil {
		adopted.Metadata = cloneStringMap(status.Metadata)
	}
	switch providerState {
	case StatusCreating, StatusResuming, StatusActive:
		refreshed, refreshErr := s.store.RefreshStaleStartup(ctx, ws, sandboxID, staleBefore, StaleStartupRefreshUpdate{
			RefreshedAt: now, Status: providerState, AdoptedProviderHandle: adopted,
		})
		if refreshErr != nil {
			var notFound *NotFoundError
			if errors.As(refreshErr, &notFound) {
				recheck, inspectErr := s.store.InspectStaleStartup(ctx, ws, sandboxID, staleBefore)
				if inspectErr == nil && recheck.Sandbox != nil && recheck.SessionDeleted {
					return s.claimInterruptedStartup(ctx, recheck.Sandbox, staleBefore, now, CleanupStatusPending, nil, false)
				}
				if inspectErr != nil {
					var recheckNotFound *NotFoundError
					if !errors.As(inspectErr, &recheckNotFound) {
						return nil, &SandboxError{Code: SandboxErrorCreateFailed, Message: "sandbox reconciliation recheck failed", Cause: inspectErr}
					}
				}
				return nil, nil
			}
			return nil, &SandboxError{Code: SandboxErrorCreateFailed, Message: "sandbox reconciliation refresh failed", Cause: refreshErr}
		}
		return refreshed, nil
	default:
		return s.claimInterruptedStartup(ctx, current, staleBefore, now, CleanupStatusPending, adopted, true)
	}
}

func (s *Service) settleMissingStaleStartup(ctx context.Context, current *Sandbox, staleBefore time.Time, now time.Time) (*Sandbox, error) {
	settled, err := s.store.RefreshStaleStartup(ctx, current.WorkspaceID, current.ID, staleBefore, StaleStartupRefreshUpdate{
		RefreshedAt: now,
		Status:      StatusReleased,
	})
	if err != nil {
		var notFound *NotFoundError
		if errors.As(err, &notFound) {
			return nil, nil
		}
		return nil, &SandboxError{Code: SandboxErrorCreateFailed, Message: "sandbox reconciliation failed", Cause: err}
	}
	return settled, nil
}

func (s *Service) claimInterruptedStartup(ctx context.Context, current *Sandbox, staleBefore time.Time, now time.Time, cleanupStatus CleanupStatus, adopted *ProviderHandle, runCleanup bool) (*Sandbox, error) {
	claimed, err := s.store.ClaimStaleCreating(ctx, current.WorkspaceID, current.ID, staleBefore, StartupFailureUpdate{
		FailedAt: now, StartupFailureReason: "startup_interrupted", CleanupStatus: cleanupStatus, AdoptedProviderHandle: adopted,
	})
	if err != nil {
		var notFound *NotFoundError
		if errors.As(err, &notFound) {
			return nil, nil
		}
		return nil, &SandboxError{Code: SandboxErrorCreateFailed, Message: "sandbox reconciliation failed", Cause: err}
	}
	if !runCleanup {
		return claimed, nil
	}
	return s.ReconcileStartupCleanup(ctx, claimed.WorkspaceID, claimed.ID)
}

func (s *Service) ReconcileStartupCleanup(ctx context.Context, ws workspace.ID, sandboxID string) (*Sandbox, error) {
	if err := s.validateConfigured(); err != nil {
		return nil, err
	}
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	if sandboxID == "" {
		return nil, &ValidationError{Message: "sandbox_id is required"}
	}
	claimed, err := s.store.ClaimDueStartupCleanup(
		ctx,
		ws,
		sandboxID,
		s.clock().UTC(),
		s.cleanupLeaseDuration,
		s.cleanupMaxAttempts,
	)
	if err != nil {
		var notFound *NotFoundError
		if errors.As(err, &notFound) {
			return nil, nil
		}
		return nil, &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox cleanup failed", Cause: err}
	}
	return s.cleanupInterruptedStartup(ctx, claimed)
}

func (s *Service) prepareSessionResources(ctx context.Context, setup SandboxSetup, handle ProviderHandle) (ResourceSetup, error) {
	if s.resourcePreparer == nil {
		return cloneResourceSetup(setup.Resources), nil
	}
	return s.resourcePreparer.PrepareSessionResources(ctx, setup, handle)
}

func (s *Service) compensateSessionResourcePreparation(ctx context.Context, setup SandboxSetup, handle ProviderHandle) error {
	if len(setup.Resources.Files) == 0 && len(setup.Resources.Skills) == 0 {
		return nil
	}
	compensator, ok := s.resourcePreparer.(SessionResourcePreparationCompensator)
	if !ok {
		return nil
	}
	return compensator.CompensateSessionResourcePreparation(ctx, setup, handle)
}

func (s *Service) materializeMemoryProjections(ctx context.Context, ws workspace.ID, handle ProviderHandle, resources ResourceSetup, resourceCleanup SessionResourceCleanupCoordinator) error {
	if len(resources.MemoryStores) == 0 && len(resources.DeletedMemoryStores) == 0 {
		return nil
	}
	return MaterializeMemoryProjections(ctx, s.memorySnapshotReader, s.memoryMaterializer, MemoryProjectionMaterializationRequest{
		WorkspaceID:       ws,
		ProviderSandboxID: handle.SandboxID,
		Resources:         resources,
		MutationLocker:    s.memoryMutationLocker,
		ResourceCleanup:   resourceCleanup,
	})
}

// ReleaseCreatedSandbox releases a provider handle returned from create when the
// durable sandbox row could not be committed.
func (s *Service) ReleaseCreatedSandbox(ctx context.Context, created *Sandbox, reason ReleaseReason) error {
	if err := s.validateConfigured(); err != nil {
		return err
	}
	if created == nil {
		return &ValidationError{Message: "sandbox is required"}
	}
	handle := created.ProviderHandle
	if handle.Provider == "" {
		handle.Provider = created.Provider
	}
	if handle.SandboxID == "" {
		handle.SandboxID = created.ProviderSandboxID
	}
	if handle.Metadata == nil {
		handle.Metadata = cloneStringMap(created.ProviderMetadata)
	}
	if err := ValidateProviderHandle(handle); err != nil {
		return &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox release failed", Cause: err}
	}
	if err := s.provider.ReleaseSandbox(ctx, handle, reason); err != nil {
		return &SandboxError{Code: sandboxCodeForProviderFailure(SandboxErrorReleaseFailed, err), Message: "sandbox release failed", Cause: err}
	}
	return nil
}

func (s *Service) ClaimReleaseForSession(ctx context.Context, ws workspace.ID, sessionID string, reason ReleaseReason) (*Sandbox, error) {
	if err := s.validateConfigured(); err != nil {
		return nil, err
	}
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	if sessionID == "" {
		return nil, &ValidationError{Message: "session_id is required"}
	}
	current, err := s.findReleaseCandidateForSession(ctx, ws, sessionID, reason)
	if err != nil {
		return nil, &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox release failed", Cause: err}
	}
	if current == nil {
		return nil, nil
	}
	releasing, err := s.claimSandboxRelease(ctx, ws, current, reason)
	if err != nil {
		return nil, &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox release failed", Cause: err}
	}
	return releasing, nil
}

func (s *Service) ReleaseClaimedSandbox(ctx context.Context, claimed *Sandbox, reason ReleaseReason) error {
	if err := s.validateConfigured(); err != nil {
		return err
	}
	if claimed == nil {
		return nil
	}
	handle := handleFromSandbox(claimed)
	if err := ValidateProviderHandle(handle); err != nil {
		return &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox release failed", Cause: err}
	}
	if err := s.provider.ReleaseSandbox(ctx, handle, reason); err != nil {
		if claimed.Status == StatusReleasing && isReleaseSandboxNotFound(err) {
			return nil
		}
		return &SandboxError{Code: sandboxCodeForProviderFailure(SandboxErrorReleaseFailed, err), Message: "sandbox release failed", Cause: err}
	}
	return nil
}

func (s *Service) ReleaseClaimedSandboxProvider(ctx context.Context, claimed *Sandbox, reason ReleaseReason) error {
	if err := s.validateConfigured(); err != nil {
		return err
	}
	if claimed == nil {
		return nil
	}
	handle := handleFromSandbox(claimed)
	if err := ValidateProviderHandle(handle); err != nil {
		return &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox release failed", Cause: err}
	}
	if err := s.provider.ReleaseSandbox(ctx, handle, reason); err != nil {
		if claimed.Status == StatusReleasing && isReleaseSandboxNotFound(err) {
			return nil
		}
		return &SandboxError{Code: sandboxCodeForProviderFailure(SandboxErrorReleaseFailed, err), Message: "sandbox release failed", Cause: err}
	}
	return nil
}

func (s *Service) ObserveClaimedRelease(ctx context.Context, claimed *Sandbox, reason ReleaseReason) (ProviderStatus, error) {
	if err := s.validateConfigured(); err != nil {
		return ProviderStatus{}, err
	}
	if claimed == nil {
		return ProviderStatus{Availability: ProviderMissing}, nil
	}
	handle := handleFromSandbox(claimed)
	if err := ValidateProviderHandle(handle); err != nil {
		return ProviderStatus{}, &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox release status check failed", Cause: err}
	}
	status, err := s.provider.GetStatus(ctx, handle)
	if err != nil {
		if claimed.Status == StatusReleasing && isProviderNotFound(err) {
			return ProviderStatus{Availability: ProviderMissing}, nil
		}
		return ProviderStatus{}, &SandboxError{Code: sandboxCodeForProviderFailure(SandboxErrorReleaseFailed, err), Message: "sandbox release status check failed", Cause: err}
	}
	if err := ValidateProviderStatus(status); err != nil {
		return ProviderStatus{}, &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox release status check failed", Cause: err}
	}
	return status, nil
}

func (s *Service) CompleteProviderReleasedSandboxWithStore(ctx context.Context, store Store, claimed *Sandbox, reason ReleaseReason) error {
	if err := s.validateConfigured(); err != nil {
		return err
	}
	if claimed == nil {
		return nil
	}
	if err := s.completeSandboxReleaseState(ctx, store, claimed, reason); err != nil {
		return &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox release failed", Cause: err}
	}
	return nil
}

func (s *Service) CompleteClaimedReleaseWithStore(ctx context.Context, store Store, claimed *Sandbox, reason ReleaseReason) error {
	if err := s.validateConfigured(); err != nil {
		return err
	}
	if claimed == nil {
		return nil
	}
	if err := s.completeSandboxReleaseState(ctx, store, claimed, reason); err != nil {
		return &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox release failed", Cause: err}
	}
	return nil
}

func (s *Service) ReleaseForSession(ctx context.Context, ws workspace.ID, sessionID string, reason ReleaseReason) error {
	if err := s.validateConfigured(); err != nil {
		return err
	}
	if ws == "" {
		return &ValidationError{Message: "workspace_id is required"}
	}
	if sessionID == "" {
		return &ValidationError{Message: "session_id is required"}
	}

	current, err := s.findReleaseCandidateForSession(ctx, ws, sessionID, reason)
	if err != nil {
		return &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox release failed", Cause: err}
	}
	if current == nil {
		return nil
	}
	releasing, err := s.claimSandboxRelease(ctx, ws, current, reason)
	if err != nil {
		return &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox release failed", Cause: err}
	}
	handle := handleFromSandbox(releasing)
	if err := ValidateProviderHandle(handle); err != nil {
		return &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox release failed", Cause: err}
	}
	if err := s.provider.ReleaseSandbox(ctx, handle, reason); err != nil {
		if releasing.Status != StatusReleasing || !isReleaseSandboxNotFound(err) {
			return &SandboxError{Code: sandboxCodeForProviderFailure(SandboxErrorReleaseFailed, err), Message: "sandbox release failed", Cause: err}
		}
	}
	if err := s.completeSandboxReleaseState(ctx, s.store, releasing, reason); err != nil {
		return &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox release failed", Cause: err}
	}
	return nil
}

func (s *Service) findReleaseCandidateForSession(ctx context.Context, ws workspace.ID, sessionID string, reason ReleaseReason) (*Sandbox, error) {
	current, err := s.store.FindLiveBySessionID(ctx, ws, sessionID)
	if err == nil {
		return current, nil
	}
	var notFound *NotFoundError
	if !errors.As(err, &notFound) {
		return nil, err
	}
	latest, latestErr := s.store.FindLatestBySessionID(ctx, ws, sessionID)
	if latestErr != nil {
		var latestNotFound *NotFoundError
		if errors.As(latestErr, &latestNotFound) {
			return nil, nil
		}
		return nil, latestErr
	}
	if reason == ReleaseReasonDelete {
		handle := handleFromSandbox(latest)
		if handle.SandboxID != "" {
			return latest, nil
		}
		nameHandle := ProviderHandle{Provider: s.providerName, SandboxID: latest.ID}
		status, statusErr := s.provider.GetStatus(ctx, nameHandle)
		if statusErr != nil {
			if isProviderNotFound(statusErr) {
				_, settleErr := s.store.MarkReleasedForDeleteProviderMissing(ctx, ws, latest.ID, s.clock().UTC())
				return nil, settleErr
			}
			return nil, statusErr
		}
		if status.Availability == ProviderMissing {
			_, settleErr := s.store.MarkReleasedForDeleteProviderMissing(ctx, ws, latest.ID, s.clock().UTC())
			return nil, settleErr
		}
		found, saveErr := s.store.SaveProviderHandle(ctx, ws, latest.ID, nameHandle, s.clock().UTC())
		if saveErr != nil {
			return nil, saveErr
		}
		return found, nil
	}
	if failedSandboxNeedsCleanup(latest) {
		return latest, nil
	}
	return nil, nil
}

func (s *Service) claimSandboxRelease(ctx context.Context, ws workspace.ID, current *Sandbox, reason ReleaseReason) (*Sandbox, error) {
	if reason == ReleaseReasonDelete {
		if store, ok := s.store.(interface {
			MarkReleasingForDelete(context.Context, workspace.ID, string, time.Time) (*Sandbox, error)
		}); ok {
			return store.MarkReleasingForDelete(ctx, ws, current.ID, s.clock().UTC())
		}
		return s.store.RefreshSandboxState(ctx, ws, current.ID, StatusReleasing, s.clock().UTC())
	}
	return s.store.MarkReleasing(ctx, ws, current.ID, s.clock().UTC())
}

func (s *Service) completeSandboxReleaseState(ctx context.Context, store Store, claimed *Sandbox, reason ReleaseReason) error {
	if reason == ReleaseReasonDelete {
		_, err := store.MarkReleased(ctx, claimed.WorkspaceID, claimed.ID, s.clock().UTC())
		return err
	}
	_, err := store.MarkArchived(ctx, claimed.WorkspaceID, claimed.ID, s.clock().UTC())
	return err
}

func failedSandboxNeedsCleanup(current *Sandbox) bool {
	if current == nil || current.Status != StatusFailed {
		return false
	}
	return current.CleanupStatus == CleanupStatusPending || current.CleanupStatus == CleanupStatusRetryableFailed
}

func handleFromSandbox(sandbox *Sandbox) ProviderHandle {
	handle := sandbox.ProviderHandle
	if handle.Provider == "" {
		handle.Provider = sandbox.Provider
	}
	if handle.SandboxID == "" {
		handle.SandboxID = sandbox.ProviderSandboxID
	}
	if handle.Metadata == nil {
		handle.Metadata = cloneStringMap(sandbox.ProviderMetadata)
	}
	return handle
}

func (s *Service) validateConfigured() error {
	if s == nil || s.store == nil || s.provider == nil || s.providerName == "" {
		return &SandboxError{Code: SandboxErrorProviderUnconfigured, Message: "sandbox provider is not configured"}
	}
	if s.cleanupLeaseDuration <= CleanupLeaseCompletionWriteMargin {
		return &ValidationError{Message: "cleanup lease duration must exceed the completion-write margin"}
	}
	if s.cleanupMaxAttempts <= 0 {
		return &ValidationError{Message: "cleanup max attempts must be positive"}
	}
	return nil
}

func (s *Service) completeProviderHandle(handle ProviderHandle) ProviderHandle {
	out := cloneProviderHandle(handle)
	out.Provider = s.providerName
	return out
}

func (s *Service) createStageError(ctx context.Context, ws workspace.ID, sessionID string, sandboxID string, handle ProviderHandle, cleanupAttemptCount int, code SandboxErrorCode, cause error) error {
	sandboxErr := &SandboxError{Code: sandboxCodeForProviderFailure(code, cause), Message: "sandbox startup failed", Cause: cause}
	failure := StartupFailureUpdate{
		FailedAt:             s.clock().UTC(),
		StartupFailureReason: string(code),
		CleanupStatus:        CleanupStatusPending,
	}
	if handle.SandboxID == "" || cleanupAttemptCount >= s.cleanupMaxAttempts {
		if _, err := s.store.MarkStartupFailed(ctx, ws, sandboxID, failure); err != nil {
			sandboxErr.CleanupError = err
		}
		return sandboxErr
	}
	failure.CleanupStatus = CleanupStatusReleased
	failure.CleanupMethod = string(ReleaseReasonCleanup)
	failure.CleanupAttempted = true
	if err := s.provider.ReleaseSandbox(ctx, handle, ReleaseReasonCleanup); err != nil {
		failure.CleanupStatus = cleanupStatusForReleaseError(err)
		failure.CleanupErrorKind = providerErrorKind(err)
		failure.CleanupRetryable = cleanupRetryable(err)
		failure.CleanupNextAttemptAt = s.nextCleanupAttempt(failure.FailedAt, failure.CleanupRetryable)
		sandboxErr.CleanupError = err
	}
	if _, err := s.store.MarkStartupFailed(ctx, ws, sandboxID, failure); err != nil {
		if sandboxErr.CleanupError == nil {
			sandboxErr.CleanupError = err
		}
	}
	return sandboxErr
}

func (s *Service) cleanupInterruptedStartup(ctx context.Context, claim *StartupCleanupClaim) (*Sandbox, error) {
	if claim == nil || claim.Sandbox == nil {
		return nil, nil
	}
	claimed := claim.Sandbox
	if claimed.CleanupLeaseToken == "" || claimed.CleanupLeaseExpiresAt == nil {
		return nil, &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox cleanup lease is incomplete"}
	}
	providerDeadline := claimed.CleanupLeaseExpiresAt.Add(-CleanupLeaseCompletionWriteMargin)
	providerCtx, cancel := context.WithDeadline(ctx, providerDeadline)
	defer cancel()
	completionCtx, cancelCompletion := context.WithDeadline(ctx, *claimed.CleanupLeaseExpiresAt)
	defer cancelCompletion()

	if !claim.ProviderAttemptReserved {
		return s.observeStartupCleanupAtCap(completionCtx, providerCtx, claimed)
	}

	handle := handleFromSandbox(claimed)
	attempt := CleanupAttemptUpdate{
		AttemptedAt:       s.clock().UTC(),
		CleanupLeaseToken: claimed.CleanupLeaseToken,
		CleanupStatus:     CleanupStatusPending,
	}
	if handle.SandboxID == "" {
		nameHandle := ProviderHandle{Provider: s.providerName, SandboxID: claimed.ID}
		status, statusErr := s.provider.GetStatus(providerCtx, nameHandle)
		if providerCtx.Err() != nil {
			return claimed, &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox cleanup provider deadline reached", Cause: providerCtx.Err()}
		}
		if statusErr != nil {
			if isProviderNotFound(statusErr) {
				attempt.CleanupStatus = CleanupStatusReleased
				attempt.CleanupErrorKind = string(ProviderErrorNotFound)
				return s.store.MarkStartupCleanupAttempt(completionCtx, claimed.WorkspaceID, claimed.ID, attempt)
			}
			attempt.CleanupStatus = cleanupStatusForReleaseError(statusErr)
			attempt.CleanupErrorKind = providerErrorKind(statusErr)
			attempt.CleanupRetryable = cleanupRetryable(statusErr)
			attempt.CleanupNextAttemptAt = s.nextCleanupAttempt(attempt.AttemptedAt, attempt.CleanupRetryable)
			got, markErr := s.store.MarkStartupCleanupAttempt(completionCtx, claimed.WorkspaceID, claimed.ID, attempt)
			if markErr != nil {
				return got, &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox reconciliation settlement failed", Cause: markErr}
			}
			return got, &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox reconciliation failed", Cause: statusErr}
		}
		if status.Availability == ProviderMissing {
			attempt.CleanupStatus = CleanupStatusReleased
			attempt.CleanupErrorKind = string(ProviderErrorNotFound)
			return s.store.MarkStartupCleanupAttempt(completionCtx, claimed.WorkspaceID, claimed.ID, attempt)
		}
		handle = nameHandle
		if _, err := s.store.SaveProviderHandle(providerCtx, claimed.WorkspaceID, claimed.ID, handle, attempt.AttemptedAt); err != nil {
			return nil, &SandboxError{Code: SandboxErrorCreateFailed, Message: "sandbox reconciliation failed", Cause: err}
		}
	}
	attempt.CleanupStatus = CleanupStatusReleased
	attempt.CleanupMethod = string(ReleaseReasonCleanup)
	var cleanupErr error
	releaseErr := s.provider.ReleaseSandbox(providerCtx, handle, ReleaseReasonCleanup)
	if providerCtx.Err() != nil {
		return claimed, &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox cleanup provider deadline reached", Cause: providerCtx.Err()}
	}
	if releaseErr != nil {
		if isProviderNotFound(releaseErr) {
			attempt.CleanupStatus = CleanupStatusReleased
			attempt.CleanupErrorKind = string(ProviderErrorNotFound)
		} else {
			cleanupErr = releaseErr
			attempt.CleanupStatus = cleanupStatusForReleaseError(releaseErr)
			attempt.CleanupErrorKind = providerErrorKind(releaseErr)
			attempt.CleanupRetryable = cleanupRetryable(releaseErr)
			attempt.CleanupNextAttemptAt = s.nextCleanupAttempt(attempt.AttemptedAt, attempt.CleanupRetryable)
		}
	}
	got, err := s.store.MarkStartupCleanupAttempt(completionCtx, claimed.WorkspaceID, claimed.ID, attempt)
	if err != nil {
		if cleanupErr == nil {
			cleanupErr = err
		}
	}
	if cleanupErr != nil {
		return got, &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox cleanup failed", Cause: cleanupErr}
	}
	return got, nil
}

func (s *Service) observeStartupCleanupAtCap(ctx context.Context, providerCtx context.Context, claimed *Sandbox) (*Sandbox, error) {
	handle := handleFromSandbox(claimed)
	if handle.SandboxID == "" {
		handle = ProviderHandle{Provider: s.providerName, SandboxID: claimed.ID}
	}
	status, observeErr := s.provider.GetStatus(providerCtx, handle)
	if providerCtx.Err() != nil {
		return claimed, &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox cleanup provider deadline reached", Cause: providerCtx.Err()}
	}
	success := observeErr != nil && isProviderNotFound(observeErr)
	if observeErr == nil {
		success = status.Availability == ProviderMissing ||
			status.SandboxStatus == StatusArchived ||
			status.SandboxStatus == StatusReleased
	}
	update := CleanupAttemptUpdate{
		AttemptedAt:       s.clock().UTC(),
		CleanupLeaseToken: claimed.CleanupLeaseToken,
		CleanupStatus:     CleanupStatusPermanentFailed,
	}
	if success {
		update.CleanupStatus = CleanupStatusReleased
		if (observeErr != nil && isProviderNotFound(observeErr)) ||
			(observeErr == nil && (status.Availability == ProviderMissing || status.SandboxStatus == StatusReleased)) {
			update.CleanupErrorKind = string(ProviderErrorNotFound)
		}
	} else if observeErr != nil {
		update.CleanupErrorKind = providerErrorKind(observeErr)
	}
	got, err := s.store.MarkStartupCleanupAttempt(ctx, claimed.WorkspaceID, claimed.ID, update)
	if err != nil {
		return got, &SandboxError{Code: SandboxErrorReleaseFailed, Message: "sandbox cleanup cap observation settlement failed", Cause: err}
	}
	return got, nil
}

func (s *Service) nextCleanupAttempt(failedAt time.Time, retryable bool) *time.Time {
	if !retryable || s.cleanupRetryBackoff <= 0 {
		return nil
	}
	next := failedAt.Add(s.cleanupRetryBackoff)
	return &next
}

func cleanupStatusForReleaseError(err error) CleanupStatus {
	var providerErr *ProviderError
	if errors.As(err, &providerErr) {
		if providerErr.Kind == ProviderErrorNotFound {
			return CleanupStatusReleased
		}
		if providerErr.Retryable || providerErr.Kind == ProviderErrorTimeout || providerErr.Kind == ProviderErrorUnavailable {
			return CleanupStatusRetryableFailed
		}
	}
	return CleanupStatusPermanentFailed
}

func providerErrorKind(err error) string {
	var providerErr *ProviderError
	if errors.As(err, &providerErr) {
		return string(providerErr.Kind)
	}
	return string(ProviderErrorUnknown)
}

func cleanupRetryable(err error) bool {
	var providerErr *ProviderError
	if errors.As(err, &providerErr) {
		return providerErr.Retryable || providerErr.Kind == ProviderErrorTimeout || providerErr.Kind == ProviderErrorUnavailable
	}
	return false
}

func sandboxCodeForProviderFailure(fallback SandboxErrorCode, err error) SandboxErrorCode {
	var sandboxErr *SandboxError
	if errors.As(err, &sandboxErr) && sandboxErr.Code != "" {
		return sandboxErr.Code
	}
	var providerErr *ProviderError
	if errors.As(err, &providerErr) && providerErr.Kind == ProviderErrorConfigInvalid {
		return SandboxErrorProviderConfigInvalid
	}
	return fallback
}

func isReleaseSandboxNotFound(err error) bool {
	var providerErr *ProviderError
	return errors.As(err, &providerErr) &&
		providerErr.Stage == StageReleaseSandbox &&
		providerErr.Kind == ProviderErrorNotFound
}

func isProviderNotFound(err error) bool {
	var providerErr *ProviderError
	return errors.As(err, &providerErr) && providerErr.Kind == ProviderErrorNotFound
}
