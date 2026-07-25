package sandbox

import (
	"context"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	SessionPrepareStatusReady            = "ready"
	SessionPrepareStatusFailed           = "failed"
	SessionPrepareStatusNoop             = "noop"
	SessionPrepareStatusWaitingOnMachine = "waiting_on_machine"
)

var ErrStalePreparationAttempt = errors.New("stale session preparation attempt")

var errDeletedSessionAfterProviderCreate = errors.New("session deleted after provider create")
var errSessionPreparationWaitingOnMachine = errors.New("session preparation is waiting on machine cleanup")

const sessionDeletedPreparationFailureReason = "session_deleted"

type SessionPrepareRequest struct {
	WorkspaceID          workspace.ID
	SessionID            string
	PreparationAttemptID string
}

type SessionPrepareResult struct {
	Status        string
	FailureReason string
}

type SessionPreparationStore interface {
	ClaimSessionPreparation(context.Context, workspace.ID, string, string, time.Time) (SessionPreparation, bool, error)
	ListSessionPreparationResources(context.Context, workspace.ID, string) (ResourceSetup, error)
	CheckSessionPreparationResourceCleanup(context.Context, workspace.ID, string, string, string) (bool, error)
	DetachSessionPreparationResource(context.Context, workspace.ID, string, string, string, time.Time) error
	MarkSessionPreparationReady(context.Context, workspace.ID, string, string, SessionPreparationReadyUpdate) error
	MarkSessionPreparationFailed(context.Context, workspace.ID, string, string, SessionPreparationFailureUpdate) error
}

type SessionPreparation struct {
	WorkspaceID           workspace.ID
	SessionID             string
	PreparationAttemptID  string
	EnvironmentID         string
	EnvironmentGeneration int64
	ProviderArtifactRef   string
	Network               NetworkSetup
	SandboxID             string
	Status                string
	FailureReason         string
	ResourceCredExpiresAt *time.Time
	ResourceRootsJSON     string
	IsCurrent             bool
	CreatedAt             time.Time
}

type SessionPreparationReadyUpdate struct {
	ReadyAt               time.Time
	ResourceCredExpiresAt *time.Time
	ResourceRootsJSON     string
}

type SessionPreparationFailureUpdate struct {
	FailedAt           time.Time
	FailureStage       string
	LastErrorKind      string
	FailureReason      string
	FailureResourceID  string
	FailureResourceURL string
	Retryable          bool
}

func (s *Service) PrepareSession(ctx context.Context, request SessionPrepareRequest) (SessionPrepareResult, error) {
	if err := s.validateConfigured(); err != nil {
		return SessionPrepareResult{}, err
	}
	if s.preparationStore == nil {
		return SessionPrepareResult{}, &ValidationError{Message: "session preparation store is required"}
	}
	if request.WorkspaceID == "" {
		return SessionPrepareResult{}, &ValidationError{Message: "workspace_id is required"}
	}
	if request.SessionID == "" {
		return SessionPrepareResult{}, &ValidationError{Message: "session_id is required"}
	}
	if request.PreparationAttemptID == "" {
		return SessionPrepareResult{}, &ValidationError{Message: "preparation_attempt_id is required"}
	}
	now := storage.Now()
	if s.clock != nil {
		now = s.clock().UTC()
	}
	preparation, claimed, err := s.preparationStore.ClaimSessionPreparation(ctx, request.WorkspaceID, request.SessionID, request.PreparationAttemptID, now)
	if err != nil {
		return SessionPrepareResult{}, err
	}
	if !claimed {
		if preparation.IsCurrent && preparation.Status == SessionPrepareStatusFailed {
			if preparation.FailureReason == sessionDeletedPreparationFailureReason {
				return SessionPrepareResult{Status: SessionPrepareStatusFailed, FailureReason: preparation.FailureReason}, nil
			}
			if err := s.cleanupFailedSessionPreparationSandbox(ctx, request, preparation.FailureReason, now, false); err != nil {
				return SessionPrepareResult{}, err
			}
			return SessionPrepareResult{Status: SessionPrepareStatusFailed, FailureReason: preparation.FailureReason}, nil
		}
		return SessionPrepareResult{Status: SessionPrepareStatusNoop}, nil
	}
	resources, err := s.preparationStore.ListSessionPreparationResources(ctx, request.WorkspaceID, request.SessionID)
	if err != nil {
		return SessionPrepareResult{}, err
	}
	if preparation.ResourceCredExpiresAt != nil {
		expiresAt := preparation.ResourceCredExpiresAt.UTC()
		resources.ResourceCredExpiresAt = &expiresAt
	}
	resources.ResourceRootsJSON = preparation.ResourceRootsJSON
	resources.PreparationAttemptCreatedAt = preparation.CreatedAt
	createRequest := CreateForSessionRequest{
		WorkspaceID:           request.WorkspaceID,
		SessionID:             request.SessionID,
		SandboxID:             preparation.SandboxID,
		EnvironmentID:         preparation.EnvironmentID,
		EnvironmentGeneration: preparation.EnvironmentGeneration,
		PreparationAttemptID:  request.PreparationAttemptID,
		ProviderArtifactRef:   preparation.ProviderArtifactRef,
		Network:               preparation.Network,
		Resources:             resources,
		ResourceCleanup: &sessionPreparationResourceCleanupCoordinator{
			store:                s.preparationStore,
			workspaceID:          request.WorkspaceID,
			sessionID:            request.SessionID,
			preparationAttemptID: request.PreparationAttemptID,
			clock:                s.clock,
		},
	}
	result, preparedResources, err := s.prepareClaimedSession(ctx, createRequest)
	if err != nil {
		if errors.Is(err, errSessionPreparationWaitingOnMachine) {
			return SessionPrepareResult{Status: SessionPrepareStatusWaitingOnMachine}, nil
		}
		if errors.Is(err, errDeletedSessionAfterProviderCreate) {
			return SessionPrepareResult{Status: SessionPrepareStatusFailed, FailureReason: sessionDeletedPreparationFailureReason}, nil
		}
		if errors.Is(err, ErrStalePreparationAttempt) {
			return SessionPrepareResult{Status: SessionPrepareStatusNoop}, nil
		}
		if retryableSessionPreparationError(err) {
			return SessionPrepareResult{}, err
		}
		failure := sessionPreparationFailureForError(err, now)
		if markErr := s.preparationStore.MarkSessionPreparationFailed(ctx, request.WorkspaceID, request.SessionID, request.PreparationAttemptID, failure); markErr != nil {
			if errors.Is(markErr, ErrStalePreparationAttempt) {
				return SessionPrepareResult{Status: SessionPrepareStatusNoop}, nil
			}
			return SessionPrepareResult{}, markErr
		}
		allowInlineCleanup := true
		var sandboxErr *SandboxError
		if errors.As(err, &sandboxErr) && sandboxErr.CleanupError != nil {
			allowInlineCleanup = false
		}
		if cleanupErr := s.cleanupFailedSessionPreparationSandbox(ctx, request, failure.FailureReason, now, allowInlineCleanup); cleanupErr != nil {
			return SessionPrepareResult{}, cleanupErr
		}
		return SessionPrepareResult{Status: SessionPrepareStatusFailed, FailureReason: failure.FailureReason}, nil
	}
	if result == nil {
		return SessionPrepareResult{}, errors.New("session preparation returned no sandbox")
	}
	readyUpdate := SessionPreparationReadyUpdate{
		ReadyAt:               now,
		ResourceCredExpiresAt: preparedResources.ResourceCredExpiresAt,
		ResourceRootsJSON:     preparedResources.ResourceRootsJSON,
	}
	if err := s.preparationStore.MarkSessionPreparationReady(ctx, request.WorkspaceID, request.SessionID, request.PreparationAttemptID, readyUpdate); err != nil {
		if errors.Is(err, ErrStalePreparationAttempt) {
			return SessionPrepareResult{Status: SessionPrepareStatusNoop}, nil
		}
		return SessionPrepareResult{}, err
	}
	return SessionPrepareResult{Status: SessionPrepareStatusReady}, nil
}

type sessionPreparationResourceCleanupCoordinator struct {
	store                SessionPreparationStore
	workspaceID          workspace.ID
	sessionID            string
	preparationAttemptID string
	clock                func() time.Time
}

func (c *sessionPreparationResourceCleanupCoordinator) CleanupSessionResource(ctx context.Context, resourceID string, remove func(context.Context) error) error {
	pending, err := c.store.CheckSessionPreparationResourceCleanup(ctx, c.workspaceID, c.sessionID, c.preparationAttemptID, resourceID)
	if err != nil {
		return err
	}
	if !pending {
		return nil
	}
	if remove == nil {
		return &ValidationError{Message: "session resource removal is required"}
	}
	if err := remove(ctx); err != nil {
		return err
	}
	detachedAt := storage.Now()
	if c.clock != nil {
		detachedAt = c.clock().UTC()
	}
	return c.store.DetachSessionPreparationResource(ctx, c.workspaceID, c.sessionID, c.preparationAttemptID, resourceID, detachedAt)
}

func (s *Service) FailSessionPreparationAfterRetryExhaustion(ctx context.Context, request SessionPrepareRequest, failureReason string) (SessionPrepareResult, error) {
	if err := s.validateConfigured(); err != nil {
		return SessionPrepareResult{}, err
	}
	if s.preparationStore == nil {
		return SessionPrepareResult{}, errors.New("session preparation store is required")
	}
	if failureReason == "" {
		failureReason = "session_prepare_error"
	}
	now := s.clock().UTC()
	failure := SessionPreparationFailureUpdate{
		FailedAt:      now,
		FailureStage:  "session_prepare",
		LastErrorKind: failureReason,
		FailureReason: failureReason,
		Retryable:     false,
	}
	if err := s.preparationStore.MarkSessionPreparationFailed(ctx, request.WorkspaceID, request.SessionID, request.PreparationAttemptID, failure); err != nil {
		if errors.Is(err, ErrStalePreparationAttempt) {
			return SessionPrepareResult{Status: SessionPrepareStatusNoop}, nil
		}
		return SessionPrepareResult{}, err
	}
	if err := s.cleanupFailedSessionPreparationSandbox(ctx, request, failureReason, now, true); err != nil {
		return SessionPrepareResult{}, err
	}
	return SessionPrepareResult{Status: SessionPrepareStatusFailed, FailureReason: failureReason}, nil
}

func (s *Service) cleanupFailedSessionPreparationSandbox(ctx context.Context, request SessionPrepareRequest, failureReason string, now time.Time, allowInlineCleanup bool) error {
	current, err := s.store.FindLatestBySessionID(ctx, request.WorkspaceID, request.SessionID)
	if err != nil {
		var notFound *NotFoundError
		if errors.As(err, &notFound) {
			return nil
		}
		return err
	}
	if current == nil {
		return nil
	}
	if current.Status == StatusArchived || current.Status == StatusReleased || current.Status == StatusReleasing {
		return nil
	}
	if current.Status == StatusFailed {
		if current.CleanupStatus == CleanupStatusPending ||
			current.CleanupStatus == CleanupStatusInProgress ||
			current.CleanupStatus == CleanupStatusRetryableFailed {
			_, err := s.ReconcileStartupCleanup(ctx, request.WorkspaceID, current.ID)
			return err
		}
		return nil
	}
	handle := handleFromSandbox(current)
	update := StartupFailureUpdate{
		FailedAt:             now,
		StartupFailureReason: failureReason,
		CleanupStatus:        CleanupStatusPending,
	}
	if allowInlineCleanup && handle.SandboxID != "" && current.CleanupAttemptCount < s.cleanupMaxAttempts {
		update.CleanupStatus = CleanupStatusReleased
		update.CleanupMethod = string(ReleaseReasonCleanup)
		update.CleanupAttempted = true
		if err := s.provider.ReleaseSandbox(ctx, handle, ReleaseReasonCleanup); err != nil {
			update.CleanupStatus = cleanupStatusForReleaseError(err)
			update.CleanupErrorKind = providerErrorKind(err)
			update.CleanupRetryable = cleanupRetryable(err)
			update.CleanupNextAttemptAt = s.nextCleanupAttempt(now, update.CleanupRetryable)
		}
	}
	if _, err := s.store.MarkStartupFailed(ctx, request.WorkspaceID, current.ID, update); err != nil {
		return err
	}
	if !allowInlineCleanup {
		_, err := s.ReconcileStartupCleanup(ctx, request.WorkspaceID, current.ID)
		return err
	}
	return nil
}

// prepareClaimedSession routes a freshly claimed preparation attempt onto the
// existing sandboxes row for its session. Cold return decides wake-vs-replace as
// ONE closed, ORDERED decision table — every branch is a checkable durable fact or
// an explicit provider answer, with no discretionary fallback, and earlier rows
// win. The table is split across resumeSandboxAndPrepare, replaceSandboxAndPrepare,
// and sandboxEnvironmentIdentityChanged:
//
//	#  condition (evaluated in order)                                         route
//	-  ---------------------------------------------------------------------  --------------------------------------------
//	1  sandboxes.status = released (reached only via FindLatestBySessionID,   REPLACE (replaceSandboxAndPrepare)
//	   since a released row is not live) — takes precedence over the env check
//	2  live row whose environment identity != the attempt's freshly resolved  REPLACE
//	   identity (sandboxEnvironmentIdentityChanged: environment_id or
//	   environment_generation differs)
//	3  stopped/archived/resuming, identity matches, StartSandbox succeeds      WAKE same machine (resumeSandboxAndPrepare),
//	                                                                          then incremental preparation reconciles drift
//	4  stopped/archived, identity matches, StartSandbox answers provider-      settle row released, then REPLACE
//	   missing or a non-retryable start error (a retryable start error
//	   retries in place)
//	5  failed row with machine_was_usable TRUE and cleanup_status in           DEFER: emit waiting_on_machine
//	   {pending,in_progress,retryable_failed} (post-failure checkpoint-archive  (errSessionPreparationWaitingOnMachine); the
//	   still in flight or leased)                                              job waits attempt-neutrally until the archive
//	                                                                          lands and row 3 wakes it. permanent_failed and
//	                                                                          every fact-FALSE failed row keep the non-
//	                                                                          retryable failed-state rejection.
//
// An already-active row whose environment identity matches skips wake/replace and
// runs incremental preparation directly.
//
// COLD-RETURN RE-PREPARATION — cross-transaction rules this router relies on:
//   - A cold return is driven by Bridge resetting a ready/released session back to
//     pending under a FRESH preparation_attempt_id whose dedupe key never collides
//     with the superseded attempt, so a stale in-flight job is never mistaken for
//     the reset attempt.
//   - A leased job whose attempt has been superseded settles as a no-op:
//     ErrStalePreparationAttempt from any store write maps to SessionPrepareStatusNoop.
//   - The deleted-session tombstone gate blocks any wake or replace and is re-checked
//     twice: at claim time (ClaimSessionPreparation joins sessions on
//     lifecycle_state <> 'deleted', so a deleted session yields no claim) and again
//     after a provider create (errDeletedSessionAfterProviderCreate ->
//     SettleDeletedSessionPreparationAfterCreate releases the just-created machine,
//     reason=delete, and settles the attempt failed with reason session_deleted).
//
// UPDATE-WITH: resumeSandboxAndPrepare, replaceSandboxAndPrepare,
// sandboxEnvironmentIdentityChanged (this file); ClaimSessionPreparation and
// SettleDeletedSessionPreparationAfterCreate in internal/sandbox/postgresql_store.go.
func (s *Service) prepareClaimedSession(ctx context.Context, request CreateForSessionRequest) (*Sandbox, ResourceSetup, error) {
	current, err := s.store.FindLiveBySessionID(ctx, request.WorkspaceID, request.SessionID)
	if err == nil && current != nil && current.ID == request.SandboxID && sandboxEnvironmentIdentityChanged(current, request) {
		return s.replaceSandboxAndPrepare(ctx, current, request)
	}
	if err == nil && current != nil && current.ID == request.SandboxID && current.Status == StatusActive {
		handle := handleFromSandbox(current)
		refreshed, err := s.refreshCurrentSandboxProviderState(ctx, request.WorkspaceID, current.ID, handle)
		if err != nil {
			return nil, ResourceSetup{}, err
		}
		switch refreshed.Status {
		case StatusActive:
		case StatusStopped, StatusArchived, StatusResuming:
			return s.resumeSandboxAndPrepare(ctx, refreshed, request)
		case StatusReleased:
			return s.replaceSandboxAndPrepare(ctx, refreshed, request)
		case StatusFailed:
			return nil, ResourceSetup{}, &ProviderError{
				Provider:    handle.Provider,
				Stage:       StageStatus,
				Kind:        ProviderErrorUnknown,
				Retryable:   false,
				SafeMessage: "sandbox provider reported failed state",
			}
		default:
			return nil, ResourceSetup{}, &ProviderError{
				Provider:    handle.Provider,
				Stage:       StageStatus,
				Kind:        ProviderErrorUnavailable,
				Retryable:   true,
				SafeMessage: "sandbox provider state is not ready",
			}
		}
		setup := SandboxSetup{
			WorkspaceID:         request.WorkspaceID,
			SessionID:           request.SessionID,
			SandboxID:           request.SandboxID,
			EnvironmentID:       request.EnvironmentID,
			ProviderArtifactRef: request.ProviderArtifactRef,
			Resources:           cloneResourceSetup(request.Resources),
			ResourceCleanup:     request.ResourceCleanup,
		}
		preparedResources, err := s.prepareResources(ctx, setup, handle)
		if err != nil {
			return nil, ResourceSetup{}, err
		}
		return refreshed, preparedResources, nil
	}
	if err != nil {
		var notFound *NotFoundError
		if !errors.As(err, &notFound) {
			return nil, ResourceSetup{}, err
		}
	}
	latest, latestErr := s.store.FindLatestBySessionID(ctx, request.WorkspaceID, request.SessionID)
	if latestErr == nil && latest != nil && latest.ID == request.SandboxID {
		switch latest.Status {
		case StatusCreating:
			return s.startCreatedSandbox(ctx, latest, request)
		case StatusFailed:
			if latest.MachineWasUsable && (latest.CleanupStatus == CleanupStatusPending || latest.CleanupStatus == CleanupStatusInProgress || latest.CleanupStatus == CleanupStatusRetryableFailed) {
				return nil, ResourceSetup{}, errSessionPreparationWaitingOnMachine
			}
			return nil, ResourceSetup{}, &ProviderError{
				Provider:    handleFromSandbox(latest).Provider,
				Stage:       StageStatus,
				Kind:        ProviderErrorUnknown,
				Retryable:   false,
				SafeMessage: "sandbox is in failed state",
			}
		case StatusStopped, StatusArchived, StatusResuming:
			return s.resumeSandboxAndPrepare(ctx, latest, request)
		case StatusReleased:
			return s.replaceSandboxAndPrepare(ctx, latest, request)
		}
	}
	if latestErr != nil {
		var notFound *NotFoundError
		if !errors.As(latestErr, &notFound) {
			return nil, ResourceSetup{}, latestErr
		}
	}
	created, err := s.CreateSandboxRecordForSession(ctx, request)
	if err != nil {
		return nil, ResourceSetup{}, err
	}
	return s.startCreatedSandbox(ctx, created, request)
}

func (s *Service) refreshCurrentSandboxProviderState(ctx context.Context, ws workspace.ID, sandboxID string, handle ProviderHandle) (*Sandbox, error) {
	status, err := s.provider.GetStatus(ctx, handle)
	if err != nil {
		if isProviderNotFound(err) {
			return s.store.RefreshSandboxState(ctx, ws, sandboxID, StatusReleased, s.clock().UTC())
		}
		return nil, err
	}
	if err := ValidateProviderStatus(status); err != nil {
		return nil, err
	}
	mapped := status.SandboxStatus
	if mapped == "" {
		mapped = sandboxStatusFromProviderAvailability(status.Availability)
	}
	return s.store.RefreshSandboxState(ctx, ws, sandboxID, mapped, s.clock().UTC())
}

func sandboxStatusFromProviderAvailability(availability ProviderAvailability) Status {
	switch availability {
	case ProviderAvailable:
		return StatusActive
	case ProviderMissing:
		return StatusReleased
	case ProviderUnknown:
		return StatusFailed
	default:
		return StatusResuming
	}
}

func (s *Service) resumeSandboxAndPrepare(ctx context.Context, current *Sandbox, request CreateForSessionRequest) (*Sandbox, ResourceSetup, error) {
	if current == nil {
		return nil, ResourceSetup{}, &ValidationError{Message: "sandbox is required"}
	}
	handle := handleFromSandbox(current)
	if _, err := s.store.RefreshSandboxState(ctx, request.WorkspaceID, current.ID, StatusResuming, s.clock().UTC()); err != nil {
		return nil, ResourceSetup{}, err
	}
	if err := s.provider.StartSandbox(ctx, handle); err != nil {
		if isProviderNotFound(err) || !cleanupRetryable(err) {
			released, settleErr := s.store.RefreshSandboxState(ctx, request.WorkspaceID, current.ID, StatusReleased, s.clock().UTC())
			if settleErr != nil {
				return nil, ResourceSetup{}, settleErr
			}
			return s.replaceSandboxAndPrepare(ctx, released, request)
		}
		return nil, ResourceSetup{}, err
	}
	active, err := s.store.MarkActive(ctx, request.WorkspaceID, current.ID, s.clock().UTC())
	if err != nil {
		return nil, ResourceSetup{}, err
	}
	setup := SandboxSetup{
		WorkspaceID:         request.WorkspaceID,
		SessionID:           request.SessionID,
		SandboxID:           request.SandboxID,
		EnvironmentID:       request.EnvironmentID,
		ProviderArtifactRef: request.ProviderArtifactRef,
		Resources:           coldResourceSetup(request.Resources),
		ResourceCleanup:     request.ResourceCleanup,
	}
	preparedResources, err := s.prepareResources(ctx, setup, handle)
	if err != nil {
		return nil, ResourceSetup{}, err
	}
	return active, preparedResources, nil
}

func sandboxEnvironmentIdentityChanged(current *Sandbox, request CreateForSessionRequest) bool {
	if current == nil || request.EnvironmentID == "" || request.EnvironmentGeneration <= 0 {
		return false
	}
	return current.EnvironmentID != request.EnvironmentID || current.EnvironmentGeneration != request.EnvironmentGeneration
}

func (s *Service) replaceSandboxAndPrepare(ctx context.Context, current *Sandbox, request CreateForSessionRequest) (*Sandbox, ResourceSetup, error) {
	if current == nil {
		return nil, ResourceSetup{}, &ValidationError{Message: "sandbox is required"}
	}
	handle := handleFromSandbox(current)
	if handle.SandboxID == "" {
		handle = ProviderHandle{Provider: s.providerName, SandboxID: current.ID}
	}
	if err := s.provider.ReleaseSandbox(ctx, handle, ReleaseReasonDelete); err != nil && !isReleaseSandboxNotFound(err) {
		return nil, ResourceSetup{}, err
	}
	replacementStore, ok := s.store.(interface {
		PrepareSandboxReplacement(context.Context, workspace.ID, string, string, int64, time.Time) (*Sandbox, error)
	})
	if !ok {
		return nil, ResourceSetup{}, &ValidationError{Message: "sandbox replacement store is required"}
	}
	replacement, err := replacementStore.PrepareSandboxReplacement(ctx, request.WorkspaceID, current.ID, request.EnvironmentID, request.EnvironmentGeneration, s.clock().UTC())
	if err != nil {
		return nil, ResourceSetup{}, err
	}
	return s.startCreatedSandbox(ctx, replacement, request)
}

func sessionPreparationFailureForError(err error, now time.Time) SessionPreparationFailureUpdate {
	reason := "sandbox_preparation_failed"
	errorKind := "sandbox_preparation_failed"
	if IsGitHubCredentialRequired(err) {
		reason = GitHubCredentialRequiredReason
		errorKind = GitHubCredentialRequiredReason
	} else if IsGitHubRepositoryUnavailable(err) {
		reason = GitHubRepositoryUnavailableReason
		errorKind = GitHubRepositoryUnavailableReason
	} else if kind := preparationFailureKindForError(err); kind != "" {
		reason = kind
		errorKind = kind
	}
	resourceID, resourceURL := gitHubPreparationFailureIdentity(err)
	return SessionPreparationFailureUpdate{
		FailedAt:           now,
		FailureStage:       "resource_preparation",
		LastErrorKind:      errorKind,
		FailureReason:      reason,
		FailureResourceID:  resourceID,
		FailureResourceURL: resourceURL,
		Retryable:          false,
	}
}

func retryableSessionPreparationError(err error) bool {
	if err == nil {
		return false
	}
	if IsGitHubCredentialRequired(err) {
		return false
	}
	if IsGitHubRepositoryUnavailable(err) {
		return false
	}
	if preparationFailureKindForError(err) != "" {
		return false
	}
	var validation *ValidationError
	if errors.As(err, &validation) {
		return false
	}
	var conflict *ConflictError
	if errors.As(err, &conflict) {
		return false
	}
	var notFound *NotFoundError
	if errors.As(err, &notFound) {
		return false
	}
	var providerErr *ProviderError
	if errors.As(err, &providerErr) {
		if providerErr.Retryable || providerErr.Kind == ProviderErrorTimeout || providerErr.Kind == ProviderErrorUnavailable {
			return true
		}
		return false
	}
	var sandboxErr *SandboxError
	if errors.As(err, &sandboxErr) {
		switch sandboxErr.Code {
		case SandboxErrorProviderConfigInvalid, SandboxErrorProviderUnconfigured:
			return false
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return true
}

type preparationFailureKind interface {
	PreparationFailureKind() string
}

func preparationFailureKindForError(err error) string {
	var kinded preparationFailureKind
	if errors.As(err, &kinded) {
		return kinded.PreparationFailureKind()
	}
	return ""
}
