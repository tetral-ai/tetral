package tetralsandbox

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	sandboxrelease "github.com/tetral-ai/tetral/internal/sandbox/release"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

type SandboxLifecycleJob struct {
	QueueJob         *queuev1.QueueJob
	JobID            string
	LeaseToken       string
	LeaseOwner       string
	LeaseExpiresAt   time.Time
	AttemptCount     int
	MaxAttempts      int
	WorkspaceID      string
	SessionID        string
	LogicalSandboxID string
	OperationID      string
}

type SandboxActivationWork struct {
	Job           SandboxLifecycleJob
	Kind          ActivationKind
	Provider      string
	CurrentHandle sandbox.ProviderHandle
	StableName    string
	Labels        map[string]string
	MayCreate     bool
	Setup         sandbox.SandboxSetup
}

type SandboxMaterializationWork struct {
	Job                         SandboxLifecycleJob
	Provider                    string
	Handle                      sandbox.ProviderHandle
	BindingRevision             int64
	TargetEnvironmentGeneration int64
	TargetResourceRevision      int64
	Setup                       sandbox.SandboxSetup
}

type SandboxReleaseReason = sandboxrelease.Reason

const (
	SandboxReleaseSessionDelete  = sandboxrelease.SessionDelete
	SandboxReleaseReplacedHandle = sandboxrelease.ReplacedHandle
)

func lifecycleFinalizerError(disposition SandboxLifecycleDisposition, err error) error {
	if err != nil {
		return err
	}
	if disposition == SandboxLifecycleLostAuthority {
		return errQueueLeaseLost
	}
	return nil
}

type SandboxReleaseWork struct {
	Job         SandboxLifecycleJob
	Provider    string
	Handle      sandbox.ProviderHandle
	Reason      SandboxReleaseReason
	ObserveOnly bool
}

type SandboxLifecycleDisposition string

const (
	SandboxLifecycleApplied       SandboxLifecycleDisposition = "applied"
	SandboxLifecycleNotApplicable SandboxLifecycleDisposition = "not_applicable"
	SandboxLifecycleLostAuthority SandboxLifecycleDisposition = "lost_authority"
)

type SandboxLifecycleStore interface {
	ClaimActivation(context.Context, SandboxLifecycleJob, time.Time) (SandboxActivationWork, SandboxLifecycleDisposition, error)
	ReplaceMissingActivation(context.Context, SandboxActivationWork, time.Time) (SandboxActivationWork, SandboxLifecycleDisposition, error)
	CompleteActivation(context.Context, SandboxActivationWork, sandbox.ProviderHandle, time.Time) (SandboxLifecycleDisposition, error)
	ObserveUnknownActivation(context.Context, SandboxActivationWork, string, time.Time) (SandboxLifecycleDisposition, error)
	FailActivation(context.Context, SandboxActivationWork, ProviderEffectBoundary, ProviderDisposition, string, string, time.Time) (SandboxLifecycleDisposition, error)
	ClaimMaterialization(context.Context, SandboxLifecycleJob, time.Time) (SandboxMaterializationWork, SandboxLifecycleDisposition, error)
	WaitMaterializationForActivation(context.Context, SandboxMaterializationWork, ExecutionReadiness, time.Time) (SandboxLifecycleDisposition, error)
	CompleteMaterialization(context.Context, SandboxMaterializationWork, MaterializationResult, time.Time) error
	FailMaterialization(context.Context, SandboxMaterializationWork, ProviderEffectBoundary, ProviderDisposition, string, string, time.Time) error
	ClaimRelease(context.Context, SandboxLifecycleJob, time.Time) (SandboxReleaseWork, SandboxLifecycleDisposition, error)
	ParkBlockedRelease(context.Context, SandboxLifecycleJob, time.Time) (SandboxLifecycleDisposition, error)
	AuthorizeRelease(context.Context, SandboxReleaseWork, time.Time) (bool, error)
	RearmRelease(context.Context, SandboxReleaseWork, time.Time) error
	CompleteRelease(context.Context, SandboxReleaseWork, time.Time) error
	ObserveUnknownRelease(context.Context, SandboxReleaseWork, string, time.Time) error
	FailRelease(context.Context, SandboxReleaseWork, ProviderEffectBoundary, ProviderDisposition, string, string, time.Time) error
	FinalizeInvalidLifecycle(context.Context, *queuev1.QueueJob, string, time.Time) (SandboxLifecycleDisposition, error)
	FinalizeExhaustedLifecycle(context.Context, *queuev1.QueueJob, string, time.Time) (SandboxLifecycleDisposition, error)
}

type SandboxLifecycleRunnerConfig struct {
	WorkspaceID       string
	LeaseOwner        string
	MaxJobs           int
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
}

type SandboxActivationJobRunner struct {
	Queue     SandboxQueueClient
	Store     SandboxLifecycleStore
	Providers *ProviderRegistry
	Config    SandboxLifecycleRunnerConfig
	Clock     func() time.Time
	Logger    *slog.Logger
}

func (r *SandboxActivationJobRunner) RunOnce(ctx context.Context) error {
	_, err := r.RunOnceWithActivity(ctx)
	return err
}

func (r *SandboxActivationJobRunner) RunOnceWithActivity(ctx context.Context) (bool, error) {
	if r == nil || r.Queue == nil || r.Store == nil || r.Providers == nil {
		return false, errors.New("sandbox activation runner dependencies are required")
	}
	cfg, err := normalizeSandboxLifecycleRunnerConfig(r.Config)
	if err != nil {
		return false, err
	}
	leaseSentAt := time.Now()
	lease, err := r.Queue.Lease(ctx, &queuev1.LeaseRequest{
		WorkspaceId: cfg.WorkspaceID, Kinds: []string{queue.KindSandboxActivate},
		LeaseOwner: cfg.LeaseOwner, MaxJobs: int32(cfg.MaxJobs), LeaseDurationMs: cfg.LeaseDuration.Milliseconds(),
	})
	if err != nil {
		return false, err
	}
	for _, queueJob := range lease.GetJobs() {
		if err := r.processJob(ctx, queueJob, cfg, leaseSentAt.Add(wireRoundedQueueLeaseDuration(cfg.LeaseDuration))); err != nil {
			return len(lease.GetJobs()) > 0, err
		}
	}
	return len(lease.GetJobs()) > 0, nil
}

func (r *SandboxActivationJobRunner) processJob(ctx context.Context, queueJob *queuev1.QueueJob, cfg SandboxLifecycleRunnerConfig, localExpiry time.Time) (resultErr error) {
	attemptStarted := time.Now()
	workCtx, stopHeartbeat, err := startQueueLeaseGuard(ctx, r.Queue, queueJob, localExpiry, cfg.HeartbeatInterval, cfg.LeaseDuration)
	if err != nil {
		return err
	}
	defer func() {
		if heartbeatErr := stopHeartbeat(); resultErr == nil && heartbeatErr != nil {
			resultErr = heartbeatErr
		}
	}()
	ctx = workCtx
	ctx = withActivationAttemptStarted(ctx, attemptStarted)
	jobIdentity := SandboxLifecycleJob{
		JobID: queueJob.GetId(), WorkspaceID: queueJob.GetWorkspaceId(),
		AttemptCount: int(queueJob.GetAttemptCount()), MaxAttempts: int(queueJob.GetMaxAttempts()),
	}
	defer func() {
		if writer := queueAuthorityLossWriter(resultErr); writer != "" {
			logSandboxQueueAuthorityLost(r.Logger, jobIdentity, queue.KindSandboxActivate, writer)
		}
	}()
	if queueJob.GetMaxAttempts() <= 0 {
		if err := lifecycleFinalizerError(r.Store.FinalizeInvalidLifecycle(ctx, queueJob, queue.KindSandboxActivate, r.now())); err != nil {
			if errors.Is(err, errQueueLeaseLost) {
				return queueAuthorityLostBy("sandbox_lifecycle_finalize", err)
			}
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		err := transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "sandbox_queue_integrity_error", ErrorMessage: "sandbox activation job has no attempt budget",
		}))
		if err == nil {
			logSandboxActivationAttemptCompleted(ctx, r.Logger, jobIdentity, "terminal_failure", "sandbox_queue_integrity_error", "sandbox activation job has no attempt budget")
		}
		return err
	}
	if queueJob.GetAttemptCount() > queueJob.GetMaxAttempts() {
		if err := lifecycleFinalizerError(r.Store.FinalizeExhaustedLifecycle(ctx, queueJob, queue.KindSandboxActivate, r.now())); err != nil {
			if errors.Is(err, errQueueLeaseLost) {
				return queueAuthorityLostBy("sandbox_lifecycle_finalize", err)
			}
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		err := transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "sandbox_activation_attempts_exhausted", ErrorMessage: "sandbox activation attempt budget exhausted",
		}))
		if err == nil {
			logSandboxActivationAttemptCompleted(ctx, r.Logger, jobIdentity, "terminal_failure", "sandbox_activation_attempts_exhausted", "sandbox activation attempt budget exhausted")
		}
		return err
	}
	job, err := DecodeSandboxLifecycleJob(queueJob, queue.KindSandboxActivate)
	if err != nil {
		if err := lifecycleFinalizerError(r.Store.FinalizeInvalidLifecycle(ctx, queueJob, queue.KindSandboxActivate, r.now())); err != nil {
			if errors.Is(err, errQueueLeaseLost) {
				return queueAuthorityLostBy("sandbox_lifecycle_finalize", err)
			}
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		err := transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "invalid_sandbox_activate_payload", ErrorMessage: "sandbox activation queue payload is invalid",
		}))
		if err == nil {
			logSandboxActivationAttemptCompleted(ctx, r.Logger, jobIdentity, "terminal_failure", "invalid_sandbox_activate_payload", "sandbox activation queue payload is invalid")
		}
		return err
	}
	jobIdentity = job
	work, disposition, err := r.Store.ClaimActivation(ctx, job, r.now())
	if err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return queueAuthorityLostBy("sandbox_activation_claim", err)
		}
		return r.retry(ctx, job, "sandbox_activation_store_error")
	}
	if disposition == SandboxLifecycleLostAuthority {
		return queueAuthorityLostBy("sandbox_activation_claim", errQueueLeaseLost)
	}
	if disposition == SandboxLifecycleNotApplicable {
		return r.ack(ctx, job)
	}
	adapter, ok := r.Providers.Resolve(work.Provider)
	if !ok {
		return r.fail(ctx, job, work, ProviderProvedNotStarted, ProviderTerminal, "provider_not_registered", "sandbox provider is not registered")
	}
	return r.activate(ctx, job, work, adapter)
}

func (r *SandboxActivationJobRunner) activate(ctx context.Context, job SandboxLifecycleJob, work SandboxActivationWork, adapter ProviderAdapter) error {
	switch work.Kind {
	case ActivationCreate, ActivationReplace:
		resolution := adapter.ResolveActivation(ctx, ActivationResolutionRequest{StableName: work.StableName, Labels: work.Labels})
		if resolution.Failed() {
			resolved := "outcome_unknown"
			if resolution.ErrorKind == "sandbox_identity_mismatch" {
				resolved = "identity_mismatch"
			}
			logSandboxActivationResolved(r.Logger, job, resolved, "")
			return r.handleActivationFailure(ctx, job, work, resolution.EffectBoundary, resolution.Disposition, resolution.ErrorKind, resolution.SafeMessage)
		}
		if resolution.Value.Found {
			logSandboxActivationResolved(r.Logger, job, "owned_found", finiteDaytonaProviderState(resolution.Value.Handle.Metadata["daytona_state"]))
			inspection := adapter.InspectForExecution(ctx, resolution.Value.Handle.SandboxID)
			if inspection.Failed() {
				return r.handleActivationFailure(ctx, job, work, inspection.EffectBoundary, inspection.Disposition, inspection.ErrorKind, inspection.SafeMessage)
			}
			if inspection.Value == ExecutionReady {
				return r.completeReadyActivation(ctx, job, work, resolution.Value.Handle)
			}
			if inspection.Value != ExecutionNeedsActivation {
				return r.retry(ctx, job, "sandbox_activation_not_ready")
			}
			outcome := adapter.Activate(ctx, ActivationRequest{Kind: ActivationStart, Setup: work.Setup, CurrentHandle: resolution.Value.Handle})
			if outcome.Failed() {
				return r.handleActivationFailure(ctx, job, work, outcome.EffectBoundary, outcome.Disposition, outcome.ErrorKind, outcome.SafeMessage)
			}
			return r.confirmActivationReady(ctx, job, work, adapter, outcome.Value)
		}
		logSandboxActivationResolved(r.Logger, job, "absent", "")
		if !work.MayCreate {
			return r.retry(ctx, job, "activation_observation_not_visible")
		}
		if job.AttemptCount > sandboxActivationSubmissionMaxAttempts {
			return r.fail(ctx, job, work, ProviderProvedNotStarted, ProviderTerminal, "sandbox_activation_attempts_exhausted", "sandbox activation could not be completed")
		}
	case ActivationStart:
		inspection := adapter.InspectForExecution(ctx, work.CurrentHandle.SandboxID)
		if inspection.Failed() {
			logSandboxActivationResolved(r.Logger, job, "outcome_unknown", "")
			return r.handleActivationFailure(ctx, job, work, inspection.EffectBoundary, inspection.Disposition, inspection.ErrorKind, inspection.SafeMessage)
		}
		if inspection.Value == ExecutionReady {
			logSandboxActivationResolved(r.Logger, job, "owned_found", "started")
			disposition, err := r.Store.CompleteActivation(ctx, work, work.CurrentHandle, r.now())
			if err != nil {
				if errors.Is(err, errQueueLeaseLost) {
					return queueAuthorityLostBy("sandbox_activation_complete", err)
				}
				return r.retry(ctx, job, "sandbox_activation_store_error")
			}
			if disposition == SandboxLifecycleLostAuthority {
				return queueAuthorityLostBy("sandbox_activation_complete", errQueueLeaseLost)
			}
			return r.ack(ctx, job)
		}
		if inspection.Value != ExecutionNeedsActivation {
			replacement, disposition, err := r.Store.ReplaceMissingActivation(ctx, work, r.now())
			if err != nil {
				if errors.Is(err, errQueueLeaseLost) {
					return queueAuthorityLostBy("sandbox_activation_replace_missing", err)
				}
				return r.retry(ctx, job, "sandbox_activation_store_error")
			}
			if disposition == SandboxLifecycleLostAuthority {
				return queueAuthorityLostBy("sandbox_activation_replace_missing", errQueueLeaseLost)
			}
			if disposition == SandboxLifecycleNotApplicable {
				logSandboxActivationResolved(r.Logger, job, "absent", "")
				return r.ack(ctx, job)
			}
			if replacement.Kind == "" {
				logSandboxActivationResolved(r.Logger, job, "absent", "")
				return r.ack(ctx, job)
			}
			return r.activate(ctx, job, replacement, adapter)
		}
		logSandboxActivationResolved(r.Logger, job, "owned_found", "stopped")
	}
	outcome := adapter.Activate(ctx, ActivationRequest{Kind: work.Kind, Setup: work.Setup, CurrentHandle: work.CurrentHandle})
	if outcome.Failed() {
		return r.handleActivationFailure(ctx, job, work, outcome.EffectBoundary, outcome.Disposition, outcome.ErrorKind, outcome.SafeMessage)
	}
	return r.confirmActivationReady(ctx, job, work, adapter, outcome.Value)
}

func (r *SandboxActivationJobRunner) confirmActivationReady(ctx context.Context, job SandboxLifecycleJob, work SandboxActivationWork, adapter ProviderAdapter, handle sandbox.ProviderHandle) error {
	inspection := adapter.InspectForExecution(ctx, handle.SandboxID)
	if inspection.Failed() {
		return r.handleActivationFailure(ctx, job, work, inspection.EffectBoundary, inspection.Disposition, inspection.ErrorKind, inspection.SafeMessage)
	}
	if inspection.Value != ExecutionReady {
		return r.retry(ctx, job, "sandbox_activation_not_ready")
	}
	return r.completeReadyActivation(ctx, job, work, handle)
}

func (r *SandboxActivationJobRunner) completeReadyActivation(ctx context.Context, job SandboxLifecycleJob, work SandboxActivationWork, handle sandbox.ProviderHandle) error {
	disposition, err := r.Store.CompleteActivation(ctx, work, handle, r.now())
	if err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return queueAuthorityLostBy("sandbox_activation_complete", err)
		}
		return r.retry(ctx, job, "sandbox_activation_store_error")
	}
	if disposition == SandboxLifecycleLostAuthority {
		return queueAuthorityLostBy("sandbox_activation_complete", errQueueLeaseLost)
	}
	return r.ack(ctx, job)
}

func (r *SandboxActivationJobRunner) handleActivationFailure(ctx context.Context, job SandboxLifecycleJob, work SandboxActivationWork, boundary ProviderEffectBoundary, disposition ProviderDisposition, kind string, message string) error {
	kind = valueOrDefault(kind, "sandbox_activation_failed")
	if boundary == ProviderOutcomeUnknown && (work.Kind == ActivationCreate || work.Kind == ActivationReplace) {
		disposition, err := r.Store.ObserveUnknownActivation(ctx, work, kind, r.now())
		if err != nil {
			if errors.Is(err, errQueueLeaseLost) {
				return queueAuthorityLostBy("sandbox_activation_observe_unknown", err)
			}
			return err
		}
		if disposition == SandboxLifecycleLostAuthority {
			return queueAuthorityLostBy("sandbox_activation_observe_unknown", errQueueLeaseLost)
		}
		if disposition == SandboxLifecycleNotApplicable {
			return r.ack(ctx, job)
		}
		return r.retry(ctx, job, kind)
	}
	if boundary == ProviderOutcomeUnknown && work.Kind == ActivationStart {
		return r.retry(ctx, job, kind)
	}
	if boundary == ProviderProvedNotStarted && disposition == ProviderRetryable {
		return r.retry(ctx, job, kind)
	}
	return r.fail(ctx, job, work, boundary, disposition, kind, message)
}

func (r *SandboxActivationJobRunner) fail(ctx context.Context, job SandboxLifecycleJob, work SandboxActivationWork, boundary ProviderEffectBoundary, disposition ProviderDisposition, kind string, message string) error {
	lifecycleDisposition, err := r.Store.FailActivation(ctx, work, boundary, disposition, kind, message, r.now())
	if err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return queueAuthorityLostBy("sandbox_activation_fail", err)
		}
		return err
	}
	if lifecycleDisposition == SandboxLifecycleLostAuthority {
		return queueAuthorityLostBy("sandbox_activation_fail", errQueueLeaseLost)
	}
	if lifecycleDisposition == SandboxLifecycleNotApplicable {
		return r.ack(ctx, job)
	}
	if err := stopQueueLeaseGuard(ctx); err != nil {
		return err
	}
	err = transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
		WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
		ErrorKind: kind, ErrorMessage: "sandbox activation failed",
	}))
	if err == nil {
		logSandboxActivationAttemptCompleted(ctx, r.Logger, job, "terminal_failure", kind, "sandbox activation failed")
	}
	return err
}

func (r *SandboxActivationJobRunner) retry(ctx context.Context, job SandboxLifecycleJob, kind string) error {
	if job.AttemptCount >= job.MaxAttempts {
		if err := lifecycleFinalizerError(r.Store.FinalizeExhaustedLifecycle(ctx, job.QueueJob, queue.KindSandboxActivate, r.now())); err != nil {
			if errors.Is(err, errQueueLeaseLost) {
				return queueAuthorityLostBy("sandbox_lifecycle_finalize", err)
			}
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		err := transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
			ErrorKind: "sandbox_activation_attempts_exhausted", ErrorMessage: "sandbox activation attempt budget exhausted",
		}))
		if err == nil {
			logSandboxActivationAttemptCompleted(ctx, r.Logger, job, "terminal_failure", "sandbox_activation_attempts_exhausted", "sandbox activation attempt budget exhausted")
		}
		return err
	}
	if err := stopQueueLeaseGuard(ctx); err != nil {
		return err
	}
	err := transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{
		WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
		ErrorKind: kind, ErrorMessage: "sandbox activation will be retried",
	}))
	if err == nil {
		// The Queue owns attempt custody and budget. Observability preserves the
		// normalized cause and the leased N/M values without affecting Retry.
		logSandboxActivationAttemptCompleted(ctx, r.Logger, job, "retry", kind, "sandbox activation will be retried")
	}
	return err
}

func (r *SandboxActivationJobRunner) ack(ctx context.Context, job SandboxLifecycleJob) error {
	if err := stopQueueLeaseGuard(ctx); err != nil {
		return err
	}
	err := transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken}))
	if err == nil {
		logSandboxActivationAttemptCompleted(ctx, r.Logger, job, "success", "", "")
	}
	return err
}

func finiteDaytonaProviderState(state string) string {
	switch state {
	case "started", "stopped", "archived", "paused", "destroyed", "deleted", "creating", "pending_build", "building_snapshot", "pulling_snapshot", "resizing", "snapshotting", "forking", "restoring", "starting", "stopping", "archiving", "destroying", "pausing", "resuming", "deleting":
		return state
	default:
		return ""
	}
}

func (r *SandboxActivationJobRunner) now() time.Time {
	if r != nil && r.Clock != nil {
		return r.Clock().UTC()
	}
	return storage.Now()
}

type SandboxMaterializationJobRunner struct {
	Queue     SandboxQueueClient
	Store     SandboxLifecycleStore
	Providers *ProviderRegistry
	Config    SandboxLifecycleRunnerConfig
	Clock     func() time.Time
	Logger    *slog.Logger
}

func (r *SandboxMaterializationJobRunner) RunOnce(ctx context.Context) error {
	_, err := r.RunOnceWithActivity(ctx)
	return err
}

func (r *SandboxMaterializationJobRunner) RunOnceWithActivity(ctx context.Context) (bool, error) {
	if r == nil || r.Queue == nil || r.Store == nil || r.Providers == nil {
		return false, errors.New("sandbox materialization runner dependencies are required")
	}
	cfg, err := normalizeSandboxLifecycleRunnerConfig(r.Config)
	if err != nil {
		return false, err
	}
	leaseSentAt := time.Now()
	lease, err := r.Queue.Lease(ctx, &queuev1.LeaseRequest{
		WorkspaceId: cfg.WorkspaceID, Kinds: []string{queue.KindSandboxMaterialize},
		LeaseOwner: cfg.LeaseOwner, MaxJobs: int32(cfg.MaxJobs), LeaseDurationMs: cfg.LeaseDuration.Milliseconds(),
	})
	if err != nil {
		return false, err
	}
	for _, queueJob := range lease.GetJobs() {
		if err := r.processJob(ctx, queueJob, cfg, leaseSentAt.Add(wireRoundedQueueLeaseDuration(cfg.LeaseDuration))); err != nil {
			return len(lease.GetJobs()) > 0, err
		}
	}
	return len(lease.GetJobs()) > 0, nil
}

func (r *SandboxMaterializationJobRunner) processJob(ctx context.Context, queueJob *queuev1.QueueJob, cfg SandboxLifecycleRunnerConfig, localExpiry time.Time) (resultErr error) {
	workCtx, stopHeartbeat, err := startQueueLeaseGuard(ctx, r.Queue, queueJob, localExpiry, cfg.HeartbeatInterval, cfg.LeaseDuration)
	if err != nil {
		return err
	}
	defer func() {
		if heartbeatErr := stopHeartbeat(); resultErr == nil && heartbeatErr != nil {
			resultErr = heartbeatErr
		}
	}()
	ctx = workCtx
	jobIdentity := SandboxLifecycleJob{JobID: queueJob.GetId(), WorkspaceID: queueJob.GetWorkspaceId()}
	defer func() {
		if writer := queueAuthorityLossWriter(resultErr); writer != "" {
			logSandboxQueueAuthorityLost(r.Logger, jobIdentity, queue.KindSandboxMaterialize, writer)
		}
	}()
	if queueJob.GetMaxAttempts() <= 0 {
		if err := lifecycleFinalizerError(r.Store.FinalizeInvalidLifecycle(ctx, queueJob, queue.KindSandboxMaterialize, r.now())); err != nil {
			if errors.Is(err, errQueueLeaseLost) {
				return queueAuthorityLostBy("sandbox_lifecycle_finalize", err)
			}
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "sandbox_queue_integrity_error", ErrorMessage: "sandbox materialization job has no attempt budget",
		}))
	}
	if queueJob.GetAttemptCount() > queueJob.GetMaxAttempts() {
		if err := lifecycleFinalizerError(r.Store.FinalizeExhaustedLifecycle(ctx, queueJob, queue.KindSandboxMaterialize, r.now())); err != nil {
			if errors.Is(err, errQueueLeaseLost) {
				return queueAuthorityLostBy("sandbox_lifecycle_finalize", err)
			}
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "sandbox_materialization_attempts_exhausted", ErrorMessage: "sandbox materialization attempt budget exhausted",
		}))
	}
	job, err := DecodeSandboxLifecycleJob(queueJob, queue.KindSandboxMaterialize)
	if err != nil {
		if err := lifecycleFinalizerError(r.Store.FinalizeInvalidLifecycle(ctx, queueJob, queue.KindSandboxMaterialize, r.now())); err != nil {
			if errors.Is(err, errQueueLeaseLost) {
				return queueAuthorityLostBy("sandbox_lifecycle_finalize", err)
			}
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "invalid_sandbox_materialize_payload", ErrorMessage: "sandbox materialization queue payload is invalid",
		}))
	}
	jobIdentity = job
	work, disposition, err := r.Store.ClaimMaterialization(ctx, job, r.now())
	if err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return queueAuthorityLostBy("sandbox_materialization_claim", err)
		}
		return r.retry(ctx, job, "sandbox_materialization_store_error")
	}
	if disposition == SandboxLifecycleLostAuthority {
		return queueAuthorityLostBy("sandbox_materialization_claim", errQueueLeaseLost)
	}
	if disposition == SandboxLifecycleNotApplicable {
		return r.ack(ctx, job)
	}
	adapter, ok := r.Providers.Resolve(work.Provider)
	if !ok {
		return r.fail(ctx, job, work, ProviderProvedNotStarted, ProviderTerminal, "provider_not_registered", "sandbox provider is not registered")
	}
	inspection := adapter.InspectForExecution(ctx, work.Handle.SandboxID)
	if inspection.Failed() {
		if inspection.EffectBoundary == ProviderProvedNotStarted && inspection.Disposition == ProviderRetryable {
			return r.retry(ctx, job, valueOrDefault(inspection.ErrorKind, "sandbox_inspection_failed"))
		}
		return r.fail(ctx, job, work, inspection.EffectBoundary, inspection.Disposition, valueOrDefault(inspection.ErrorKind, "sandbox_inspection_failed"), inspection.SafeMessage)
	}
	if inspection.Value != ExecutionReady {
		disposition, err := r.Store.WaitMaterializationForActivation(ctx, work, inspection.Value, r.now())
		if err != nil {
			if errors.Is(err, errQueueLeaseLost) {
				return queueAuthorityLostBy("sandbox_materialization_wait_for_activation", err)
			}
			return err
		}
		if disposition == SandboxLifecycleLostAuthority {
			return queueAuthorityLostBy("sandbox_materialization_wait_for_activation", errQueueLeaseLost)
		}
		if disposition == SandboxLifecycleApplied {
			return nil
		}
		return r.ack(ctx, job)
	}
	outcome := adapter.MaterializeResources(ctx, MaterializationRequest{
		Setup: work.Setup, Handle: work.Handle, BindingRevision: work.BindingRevision,
		TargetEnvironmentGeneration: work.TargetEnvironmentGeneration,
		TargetResourceRevision:      work.TargetResourceRevision,
	})
	if outcome.Failed() {
		if outcome.EffectBoundary == ProviderProvedNotStarted && outcome.Disposition == ProviderRetryable {
			return r.retry(ctx, job, valueOrDefault(outcome.ErrorKind, "sandbox_materialization_failed"))
		}
		return r.fail(ctx, job, work, outcome.EffectBoundary, outcome.Disposition, valueOrDefault(outcome.ErrorKind, "sandbox_materialization_failed"), outcome.SafeMessage)
	}
	if err := r.Store.CompleteMaterialization(ctx, work, outcome.Value, r.now()); err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return queueAuthorityLostBy("sandbox_materialization_complete", err)
		}
		return r.retry(ctx, job, "sandbox_materialization_store_error")
	}
	return r.ack(ctx, job)
}

func (r *SandboxMaterializationJobRunner) retry(ctx context.Context, job SandboxLifecycleJob, kind string) error {
	if job.AttemptCount >= job.MaxAttempts {
		if err := lifecycleFinalizerError(r.Store.FinalizeExhaustedLifecycle(ctx, job.QueueJob, queue.KindSandboxMaterialize, r.now())); err != nil {
			if errors.Is(err, errQueueLeaseLost) {
				return queueAuthorityLostBy("sandbox_lifecycle_finalize", err)
			}
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
			ErrorKind: "sandbox_materialization_attempts_exhausted", ErrorMessage: "sandbox materialization attempt budget exhausted",
		}))
	}
	if err := stopQueueLeaseGuard(ctx); err != nil {
		return err
	}
	return transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{
		WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
		ErrorKind: kind, ErrorMessage: "sandbox materialization will be retried",
	}))
}

func (r *SandboxMaterializationJobRunner) fail(ctx context.Context, job SandboxLifecycleJob, work SandboxMaterializationWork, boundary ProviderEffectBoundary, disposition ProviderDisposition, kind string, message string) error {
	if err := r.Store.FailMaterialization(ctx, work, boundary, disposition, kind, message, r.now()); err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return queueAuthorityLostBy("sandbox_materialization_fail", err)
		}
		return err
	}
	if err := stopQueueLeaseGuard(ctx); err != nil {
		return err
	}
	return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
		WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
		ErrorKind: kind, ErrorMessage: "sandbox materialization failed",
	}))
}

func (r *SandboxMaterializationJobRunner) ack(ctx context.Context, job SandboxLifecycleJob) error {
	if err := stopQueueLeaseGuard(ctx); err != nil {
		return err
	}
	return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken}))
}

func (r *SandboxMaterializationJobRunner) now() time.Time {
	if r != nil && r.Clock != nil {
		return r.Clock().UTC()
	}
	return storage.Now()
}

type SandboxReleaseJobRunner struct {
	Queue     SandboxQueueClient
	Store     SandboxLifecycleStore
	Providers *ProviderRegistry
	Config    SandboxLifecycleRunnerConfig
	Clock     func() time.Time
	Logger    *slog.Logger
}

func (r *SandboxReleaseJobRunner) RunOnce(ctx context.Context) error {
	_, err := r.RunOnceWithActivity(ctx)
	return err
}

func (r *SandboxReleaseJobRunner) RunOnceWithActivity(ctx context.Context) (bool, error) {
	if r == nil || r.Queue == nil || r.Store == nil || r.Providers == nil {
		return false, errors.New("sandbox release runner dependencies are required")
	}
	cfg, err := normalizeSandboxLifecycleRunnerConfig(r.Config)
	if err != nil {
		return false, err
	}
	leaseSentAt := time.Now()
	lease, err := r.Queue.Lease(ctx, &queuev1.LeaseRequest{
		WorkspaceId: cfg.WorkspaceID, Kinds: []string{queue.KindSandboxRelease},
		LeaseOwner: cfg.LeaseOwner, MaxJobs: int32(cfg.MaxJobs), LeaseDurationMs: cfg.LeaseDuration.Milliseconds(),
	})
	if err != nil {
		return false, err
	}
	for _, queueJob := range lease.GetJobs() {
		if err := r.processJob(ctx, queueJob, cfg, leaseSentAt.Add(wireRoundedQueueLeaseDuration(cfg.LeaseDuration))); err != nil {
			return len(lease.GetJobs()) > 0, err
		}
	}
	return len(lease.GetJobs()) > 0, nil
}

func (r *SandboxReleaseJobRunner) processJob(ctx context.Context, queueJob *queuev1.QueueJob, cfg SandboxLifecycleRunnerConfig, localExpiry time.Time) (resultErr error) {
	workCtx, stopHeartbeat, err := startQueueLeaseGuard(ctx, r.Queue, queueJob, localExpiry, cfg.HeartbeatInterval, cfg.LeaseDuration)
	if err != nil {
		return err
	}
	defer func() {
		if heartbeatErr := stopHeartbeat(); resultErr == nil && heartbeatErr != nil {
			resultErr = heartbeatErr
		}
	}()
	ctx = workCtx
	jobIdentity := SandboxLifecycleJob{JobID: queueJob.GetId(), WorkspaceID: queueJob.GetWorkspaceId()}
	defer func() {
		if writer := queueAuthorityLossWriter(resultErr); writer != "" {
			logSandboxQueueAuthorityLost(r.Logger, jobIdentity, queue.KindSandboxRelease, writer)
		}
	}()
	if queueJob.GetMaxAttempts() <= 0 {
		if err := lifecycleFinalizerError(r.Store.FinalizeInvalidLifecycle(ctx, queueJob, queue.KindSandboxRelease, r.now())); err != nil {
			if errors.Is(err, errQueueLeaseLost) {
				return queueAuthorityLostBy("sandbox_lifecycle_finalize", err)
			}
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "sandbox_queue_integrity_error", ErrorMessage: "sandbox release job has no attempt budget",
		}))
	}
	if queueJob.GetAttemptCount() > queueJob.GetMaxAttempts() {
		if err := lifecycleFinalizerError(r.Store.FinalizeExhaustedLifecycle(ctx, queueJob, queue.KindSandboxRelease, r.now())); err != nil {
			if errors.Is(err, errQueueLeaseLost) {
				return queueAuthorityLostBy("sandbox_lifecycle_finalize", err)
			}
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "sandbox_release_attempts_exhausted", ErrorMessage: "sandbox release attempt budget exhausted",
		}))
	}
	job, err := DecodeSandboxLifecycleJob(queueJob, queue.KindSandboxRelease)
	if err != nil {
		if err := lifecycleFinalizerError(r.Store.FinalizeInvalidLifecycle(ctx, queueJob, queue.KindSandboxRelease, r.now())); err != nil {
			if errors.Is(err, errQueueLeaseLost) {
				return queueAuthorityLostBy("sandbox_lifecycle_finalize", err)
			}
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "invalid_sandbox_release_payload", ErrorMessage: "sandbox release queue payload is invalid",
		}))
	}
	jobIdentity = job
	work, disposition, err := r.Store.ClaimRelease(ctx, job, r.now())
	if err != nil {
		if errors.Is(err, errSandboxReleaseBlocked) {
			parkDisposition, parkErr := r.Store.ParkBlockedRelease(ctx, job, r.now())
			if parkErr != nil {
				if errors.Is(parkErr, errQueueLeaseLost) {
					return queueAuthorityLostBy("sandbox_release_park_blocked", parkErr)
				}
				return parkErr
			}
			if parkDisposition == SandboxLifecycleLostAuthority {
				return queueAuthorityLostBy("sandbox_release_park_blocked", errQueueLeaseLost)
			}
			if parkDisposition == SandboxLifecycleApplied {
				return nil
			}
			work, disposition, err = r.Store.ClaimRelease(ctx, job, r.now())
			if err != nil {
				if errors.Is(err, errQueueLeaseLost) {
					return queueAuthorityLostBy("sandbox_release_claim", err)
				}
				return err
			}
		} else {
			if errors.Is(err, errQueueLeaseLost) {
				return queueAuthorityLostBy("sandbox_release_claim", err)
			}
			return r.retry(ctx, job, "sandbox_release_store_error")
		}
	}
	if disposition == SandboxLifecycleLostAuthority {
		return queueAuthorityLostBy("sandbox_release_claim", errQueueLeaseLost)
	}
	if disposition == SandboxLifecycleNotApplicable {
		return r.ack(ctx, job)
	}
	adapter, ok := r.Providers.Resolve(work.Provider)
	if !ok {
		return r.fail(ctx, job, work, ProviderProvedNotStarted, ProviderTerminal, "provider_not_registered", "sandbox provider is not registered")
	}
	return r.release(ctx, job, work, adapter)
}

func (r *SandboxReleaseJobRunner) release(ctx context.Context, job SandboxLifecycleJob, work SandboxReleaseWork, adapter ProviderAdapter) error {
	inspection := adapter.InspectForRelease(ctx, work.Handle.SandboxID)
	if inspection.Failed() {
		if inspection.EffectBoundary == ProviderProvedNotStarted && inspection.Disposition == ProviderRetryable {
			return r.retryProvedNotStarted(ctx, job, work, valueOrDefault(inspection.ErrorKind, "sandbox_release_inspection_failed"))
		}
		return r.fail(ctx, job, work, inspection.EffectBoundary, inspection.Disposition, valueOrDefault(inspection.ErrorKind, "sandbox_release_inspection_failed"), inspection.SafeMessage)
	}
	if !inspection.Value {
		if err := r.Store.CompleteRelease(ctx, work, r.now()); err != nil {
			if errors.Is(err, errQueueLeaseLost) {
				return queueAuthorityLostBy("sandbox_release_complete", err)
			}
			return r.retry(ctx, job, "sandbox_release_store_error")
		}
		return r.ack(ctx, job)
	}
	if work.ObserveOnly {
		return r.retry(ctx, job, "sandbox_release_outcome_unknown")
	}
	authorized, err := r.Store.AuthorizeRelease(ctx, work, r.now())
	if err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return queueAuthorityLostBy("sandbox_release_authorize", err)
		}
		return r.retry(ctx, job, "sandbox_release_store_error")
	}
	if !authorized {
		return r.ack(ctx, job)
	}
	outcome := adapter.Release(ctx, ReleaseRequest{Handle: work.Handle})
	if outcome.Failed() {
		if outcome.EffectBoundary == ProviderProvedNotStarted && outcome.Disposition == ProviderRetryable {
			return r.retryProvedNotStarted(ctx, job, work, valueOrDefault(outcome.ErrorKind, "sandbox_release_failed"))
		}
		if outcome.EffectBoundary == ProviderSubmitted || outcome.EffectBoundary == ProviderOutcomeUnknown {
			kind := valueOrDefault(outcome.ErrorKind, "sandbox_release_outcome_unknown")
			if err := r.Store.ObserveUnknownRelease(ctx, work, kind, r.now()); err != nil {
				if errors.Is(err, errQueueLeaseLost) {
					return queueAuthorityLostBy("sandbox_release_observe_unknown", err)
				}
				return err
			}
			return r.retry(ctx, job, kind)
		}
		return r.fail(ctx, job, work, outcome.EffectBoundary, outcome.Disposition, valueOrDefault(outcome.ErrorKind, "sandbox_release_failed"), outcome.SafeMessage)
	}
	if err := r.Store.CompleteRelease(ctx, work, r.now()); err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return queueAuthorityLostBy("sandbox_release_complete", err)
		}
		return r.retry(ctx, job, "sandbox_release_store_error")
	}
	return r.ack(ctx, job)
}

func (r *SandboxReleaseJobRunner) retryProvedNotStarted(ctx context.Context, job SandboxLifecycleJob, work SandboxReleaseWork, kind string) error {
	if err := r.Store.RearmRelease(ctx, work, r.now()); err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return queueAuthorityLostBy("sandbox_release_rearm", err)
		}
		return err
	}
	return r.retry(ctx, job, kind)
}

func (r *SandboxReleaseJobRunner) retry(ctx context.Context, job SandboxLifecycleJob, kind string) error {
	if job.AttemptCount >= job.MaxAttempts {
		if err := lifecycleFinalizerError(r.Store.FinalizeExhaustedLifecycle(ctx, job.QueueJob, queue.KindSandboxRelease, r.now())); err != nil {
			if errors.Is(err, errQueueLeaseLost) {
				return queueAuthorityLostBy("sandbox_lifecycle_finalize", err)
			}
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
			ErrorKind: "sandbox_release_attempts_exhausted", ErrorMessage: "sandbox release attempt budget exhausted",
		}))
	}
	if err := stopQueueLeaseGuard(ctx); err != nil {
		return err
	}
	return transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{
		WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
		ErrorKind: kind, ErrorMessage: "sandbox release will be retried",
	}))
}

func (r *SandboxReleaseJobRunner) fail(ctx context.Context, job SandboxLifecycleJob, work SandboxReleaseWork, boundary ProviderEffectBoundary, disposition ProviderDisposition, kind string, message string) error {
	if err := r.Store.FailRelease(ctx, work, boundary, disposition, kind, message, r.now()); err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return queueAuthorityLostBy("sandbox_release_fail", err)
		}
		return err
	}
	if err := stopQueueLeaseGuard(ctx); err != nil {
		return err
	}
	return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
		WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
		ErrorKind: kind, ErrorMessage: "sandbox release failed",
	}))
}

func (r *SandboxReleaseJobRunner) ack(ctx context.Context, job SandboxLifecycleJob) error {
	if err := stopQueueLeaseGuard(ctx); err != nil {
		return err
	}
	return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken}))
}

func (r *SandboxReleaseJobRunner) now() time.Time {
	if r != nil && r.Clock != nil {
		return r.Clock().UTC()
	}
	return storage.Now()
}

func DecodeSandboxLifecycleJob(queueJob *queuev1.QueueJob, expectedKind string) (SandboxLifecycleJob, error) {
	if queueJob == nil || queueJob.GetKind() != expectedKind || !isSandboxLifecycleQueueKind(expectedKind) {
		return SandboxLifecycleJob{}, errors.New("queue job kind is not the expected sandbox lifecycle kind")
	}
	job, err := decodeSandboxLifecycleQueueIdentity(
		queueJob.GetId(), queueJob.GetWorkspaceId(), expectedKind,
		queueJob.GetPartitionKey(), queueJob.GetDedupeKey(), queueJob.GetPayloadJson(),
	)
	if err != nil {
		return SandboxLifecycleJob{}, err
	}
	if queueJob.GetLeaseToken() == "" {
		return SandboxLifecycleJob{}, errors.New("sandbox lifecycle lease token is missing")
	}
	job.LeaseToken = queueJob.GetLeaseToken()
	job.LeaseOwner = queueJob.GetLeasedBy()
	job.AttemptCount = int(queueJob.GetAttemptCount())
	job.MaxAttempts = int(queueJob.GetMaxAttempts())
	job.QueueJob = queueJob
	if raw := queueJob.GetLeasedUntil(); raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return SandboxLifecycleJob{}, errors.New("sandbox lifecycle lease expiry is invalid")
		}
		job.LeaseExpiresAt = parsed.UTC()
	}
	return job, nil
}

func decodeSandboxLifecycleQueueIdentity(jobID string, workspaceID string, expectedKind string, partitionKey string, dedupeKey string, payloadJSON string) (SandboxLifecycleJob, error) {
	if !isSandboxLifecycleQueueKind(expectedKind) {
		return SandboxLifecycleJob{}, errors.New("queue job kind is not the expected sandbox lifecycle kind")
	}
	var payload struct {
		WorkspaceID      string `json:"workspace_id"`
		SessionID        string `json:"session_id"`
		LogicalSandboxID string `json:"logical_sandbox_id"`
		OperationID      string `json:"operation_id"`
	}
	decoder := json.NewDecoder(strings.NewReader(payloadJSON))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return SandboxLifecycleJob{}, err
	}
	ws := workspace.ID(payload.WorkspaceID)
	if jobID == "" || payload.WorkspaceID == "" || payload.WorkspaceID != workspaceID ||
		payload.SessionID == "" || payload.LogicalSandboxID == "" || payload.OperationID == "" ||
		partitionKey != queue.FormatSandboxLifecyclePartitionKey(ws, payload.LogicalSandboxID) ||
		dedupeKey != queue.FormatSandboxLifecycleDedupeKey(expectedKind, ws, payload.LogicalSandboxID, payload.OperationID) {
		return SandboxLifecycleJob{}, errors.New("sandbox lifecycle queue identity is invalid")
	}
	return SandboxLifecycleJob{
		JobID: jobID, WorkspaceID: payload.WorkspaceID, SessionID: payload.SessionID,
		LogicalSandboxID: payload.LogicalSandboxID, OperationID: payload.OperationID,
	}, nil
}

func normalizeSandboxLifecycleRunnerConfig(cfg SandboxLifecycleRunnerConfig) (SandboxLifecycleRunnerConfig, error) {
	if cfg.LeaseOwner == "" {
		cfg.LeaseOwner = ServiceName
	}
	if cfg.MaxJobs <= 0 {
		cfg.MaxJobs = 1
	}
	if cfg.LeaseDuration <= 0 || cfg.HeartbeatInterval <= 0 || cfg.HeartbeatInterval >= cfg.LeaseDuration {
		return SandboxLifecycleRunnerConfig{}, errors.New("sandbox lifecycle timing configuration is invalid")
	}
	return cfg, nil
}

func isSandboxLifecycleQueueKind(kind string) bool {
	return kind == queue.KindSandboxActivate || kind == queue.KindSandboxMaterialize || kind == queue.KindSandboxRelease
}
