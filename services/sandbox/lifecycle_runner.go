package tetralsandbox

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
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

type SandboxLifecycleStore interface {
	ClaimActivation(context.Context, SandboxLifecycleJob, time.Time) (SandboxActivationWork, bool, error)
	ReplaceMissingActivation(context.Context, SandboxActivationWork, time.Time) (SandboxActivationWork, bool, error)
	CompleteActivation(context.Context, SandboxActivationWork, sandbox.ProviderHandle, time.Time) error
	ObserveUnknownActivation(context.Context, SandboxActivationWork, string, time.Time) error
	FailActivation(context.Context, SandboxActivationWork, ProviderEffectBoundary, ProviderDisposition, string, string, time.Time) error
	ClaimMaterialization(context.Context, SandboxLifecycleJob, time.Time) (SandboxMaterializationWork, bool, error)
	WaitMaterializationForActivation(context.Context, SandboxMaterializationWork, ExecutionReadiness, time.Time) error
	CompleteMaterialization(context.Context, SandboxMaterializationWork, MaterializationResult, time.Time) error
	FailMaterialization(context.Context, SandboxMaterializationWork, ProviderEffectBoundary, ProviderDisposition, string, string, time.Time) error
	FinalizeInvalidLifecycle(context.Context, *queuev1.QueueJob, string, time.Time) error
	FinalizeExhaustedLifecycle(context.Context, *queuev1.QueueJob, string, time.Time) error
}

type SandboxLifecycleRunnerConfig struct {
	WorkspaceID       string
	LeaseOwner        string
	MaxJobs           int
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
}

type SandboxActivationJobRunner struct {
	Queue     SessionPrepareQueueClient
	Store     SandboxLifecycleStore
	Providers *ProviderRegistry
	Config    SandboxLifecycleRunnerConfig
	Clock     func() time.Time
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
	lease, err := r.Queue.Lease(ctx, &queuev1.LeaseRequest{
		WorkspaceId: cfg.WorkspaceID, Kinds: []string{queue.KindSandboxActivate},
		LeaseOwner: cfg.LeaseOwner, MaxJobs: int32(cfg.MaxJobs), LeaseDurationMs: cfg.LeaseDuration.Milliseconds(),
	})
	if err != nil {
		return false, err
	}
	for _, queueJob := range lease.GetJobs() {
		if err := r.processJob(ctx, queueJob, cfg); err != nil {
			return len(lease.GetJobs()) > 0, err
		}
	}
	return len(lease.GetJobs()) > 0, nil
}

func (r *SandboxActivationJobRunner) processJob(ctx context.Context, queueJob *queuev1.QueueJob, cfg SandboxLifecycleRunnerConfig) error {
	if queueJob.GetMaxAttempts() <= 0 {
		if err := r.Store.FinalizeInvalidLifecycle(ctx, queueJob, queue.KindSandboxActivate, r.now()); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "sandbox_queue_integrity_error", ErrorMessage: "sandbox activation job has no attempt budget",
		}))
	}
	if queueJob.GetAttemptCount() > queueJob.GetMaxAttempts() {
		if err := r.Store.FinalizeExhaustedLifecycle(ctx, queueJob, queue.KindSandboxActivate, r.now()); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "sandbox_activation_attempts_exhausted", ErrorMessage: "sandbox activation attempt budget exhausted",
		}))
	}
	job, err := DecodeSandboxLifecycleJob(queueJob, queue.KindSandboxActivate)
	if err != nil {
		if err := r.Store.FinalizeInvalidLifecycle(ctx, queueJob, queue.KindSandboxActivate, r.now()); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "invalid_sandbox_activate_payload", ErrorMessage: "sandbox activation queue payload is invalid",
		}))
	}
	work, current, err := r.Store.ClaimActivation(ctx, job, r.now())
	if err != nil {
		return r.retry(ctx, job, "sandbox_activation_store_error")
	}
	if !current {
		return r.ack(ctx, job)
	}
	adapter, ok := r.Providers.Resolve(work.Provider)
	if !ok {
		return r.fail(ctx, job, work, ProviderProvedNotStarted, ProviderTerminal, "provider_not_registered", "sandbox provider is not registered")
	}
	workCtx, stopHeartbeat := startQueueLeaseGuard(ctx, r.Queue, job.WorkspaceID, job.JobID, job.LeaseToken, cfg.HeartbeatInterval, cfg.LeaseDuration)
	err = r.activate(workCtx, job, work, adapter)
	if heartbeatErr := stopHeartbeat(); heartbeatErr != nil {
		return heartbeatErr
	}
	return err
}

func (r *SandboxActivationJobRunner) activate(ctx context.Context, job SandboxLifecycleJob, work SandboxActivationWork, adapter ProviderAdapter) error {
	if work.Kind == ActivationCreate || work.Kind == ActivationReplace {
		resolution := adapter.ResolveActivation(ctx, ActivationResolutionRequest{StableName: work.StableName, Labels: work.Labels})
		if resolution.Failed() {
			return r.handleActivationFailure(ctx, job, work, resolution.EffectBoundary, resolution.Disposition, resolution.ErrorKind, resolution.SafeMessage)
		}
		if resolution.Value.Found {
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
		if !work.MayCreate {
			return r.fail(ctx, job, work, ProviderOutcomeUnknown, ProviderTerminal, "activation_outcome_unknown", "sandbox creation outcome could not be reconciled")
		}
		if job.AttemptCount > sandboxActivationSubmissionMaxAttempts {
			return r.fail(ctx, job, work, ProviderProvedNotStarted, ProviderTerminal, "sandbox_activation_attempts_exhausted", "sandbox activation could not be completed")
		}
	} else if work.Kind == ActivationStart {
		inspection := adapter.InspectForExecution(ctx, work.CurrentHandle.SandboxID)
		if inspection.Failed() {
			return r.handleActivationFailure(ctx, job, work, inspection.EffectBoundary, inspection.Disposition, inspection.ErrorKind, inspection.SafeMessage)
		}
		if inspection.Value == ExecutionReady {
			if err := r.Store.CompleteActivation(ctx, work, work.CurrentHandle, r.now()); err != nil {
				return r.retry(ctx, job, "sandbox_activation_store_error")
			}
			return r.ack(ctx, job)
		}
		if inspection.Value != ExecutionNeedsActivation {
			replacement, current, err := r.Store.ReplaceMissingActivation(ctx, work, r.now())
			if err != nil {
				return r.retry(ctx, job, "sandbox_activation_store_error")
			}
			if !current {
				return r.ack(ctx, job)
			}
			return r.activate(ctx, job, replacement, adapter)
		}
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
	if err := r.Store.CompleteActivation(ctx, work, handle, r.now()); err != nil {
		return r.retry(ctx, job, "sandbox_activation_store_error")
	}
	return r.ack(ctx, job)
}

func (r *SandboxActivationJobRunner) handleActivationFailure(ctx context.Context, job SandboxLifecycleJob, work SandboxActivationWork, boundary ProviderEffectBoundary, disposition ProviderDisposition, kind string, message string) error {
	kind = valueOrDefault(kind, "sandbox_activation_failed")
	if boundary == ProviderOutcomeUnknown && (work.Kind == ActivationCreate || work.Kind == ActivationReplace) {
		if err := r.Store.ObserveUnknownActivation(ctx, work, kind, r.now()); err != nil {
			return err
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
	if err := r.Store.FailActivation(ctx, work, boundary, disposition, kind, message, r.now()); err != nil {
		return err
	}
	return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
		WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
		ErrorKind: kind, ErrorMessage: "sandbox activation failed",
	}))
}

func (r *SandboxActivationJobRunner) retry(ctx context.Context, job SandboxLifecycleJob, kind string) error {
	if job.AttemptCount >= job.MaxAttempts {
		if err := r.Store.FinalizeExhaustedLifecycle(ctx, job.QueueJob, queue.KindSandboxActivate, r.now()); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
			ErrorKind: "sandbox_activation_attempts_exhausted", ErrorMessage: "sandbox activation attempt budget exhausted",
		}))
	}
	return transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{
		WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
		ErrorKind: kind, ErrorMessage: "sandbox activation will be retried",
	}))
}

func (r *SandboxActivationJobRunner) ack(ctx context.Context, job SandboxLifecycleJob) error {
	return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken}))
}

func (r *SandboxActivationJobRunner) now() time.Time {
	if r != nil && r.Clock != nil {
		return r.Clock().UTC()
	}
	return storage.Now()
}

type SandboxMaterializationJobRunner struct {
	Queue     SessionPrepareQueueClient
	Store     SandboxLifecycleStore
	Providers *ProviderRegistry
	Config    SandboxLifecycleRunnerConfig
	Clock     func() time.Time
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
	lease, err := r.Queue.Lease(ctx, &queuev1.LeaseRequest{
		WorkspaceId: cfg.WorkspaceID, Kinds: []string{queue.KindSandboxMaterialize},
		LeaseOwner: cfg.LeaseOwner, MaxJobs: int32(cfg.MaxJobs), LeaseDurationMs: cfg.LeaseDuration.Milliseconds(),
	})
	if err != nil {
		return false, err
	}
	for _, queueJob := range lease.GetJobs() {
		if err := r.processJob(ctx, queueJob, cfg); err != nil {
			return len(lease.GetJobs()) > 0, err
		}
	}
	return len(lease.GetJobs()) > 0, nil
}

func (r *SandboxMaterializationJobRunner) processJob(ctx context.Context, queueJob *queuev1.QueueJob, cfg SandboxLifecycleRunnerConfig) error {
	if queueJob.GetMaxAttempts() <= 0 {
		if err := r.Store.FinalizeInvalidLifecycle(ctx, queueJob, queue.KindSandboxMaterialize, r.now()); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "sandbox_queue_integrity_error", ErrorMessage: "sandbox materialization job has no attempt budget",
		}))
	}
	if queueJob.GetAttemptCount() > queueJob.GetMaxAttempts() {
		if err := r.Store.FinalizeExhaustedLifecycle(ctx, queueJob, queue.KindSandboxMaterialize, r.now()); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "sandbox_materialization_attempts_exhausted", ErrorMessage: "sandbox materialization attempt budget exhausted",
		}))
	}
	job, err := DecodeSandboxLifecycleJob(queueJob, queue.KindSandboxMaterialize)
	if err != nil {
		if err := r.Store.FinalizeInvalidLifecycle(ctx, queueJob, queue.KindSandboxMaterialize, r.now()); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "invalid_sandbox_materialize_payload", ErrorMessage: "sandbox materialization queue payload is invalid",
		}))
	}
	work, current, err := r.Store.ClaimMaterialization(ctx, job, r.now())
	if err != nil {
		return r.retry(ctx, job, "sandbox_materialization_store_error")
	}
	if !current {
		return r.ack(ctx, job)
	}
	adapter, ok := r.Providers.Resolve(work.Provider)
	if !ok {
		return r.fail(ctx, job, work, ProviderProvedNotStarted, ProviderTerminal, "provider_not_registered", "sandbox provider is not registered")
	}
	workCtx, stopHeartbeat := startQueueLeaseGuard(ctx, r.Queue, job.WorkspaceID, job.JobID, job.LeaseToken, cfg.HeartbeatInterval, cfg.LeaseDuration)
	inspection := adapter.InspectForExecution(workCtx, work.Handle.SandboxID)
	if inspection.Failed() {
		if heartbeatErr := stopHeartbeat(); heartbeatErr != nil {
			return heartbeatErr
		}
		if inspection.EffectBoundary == ProviderProvedNotStarted && inspection.Disposition == ProviderRetryable {
			return r.retry(ctx, job, valueOrDefault(inspection.ErrorKind, "sandbox_inspection_failed"))
		}
		return r.fail(ctx, job, work, inspection.EffectBoundary, inspection.Disposition, valueOrDefault(inspection.ErrorKind, "sandbox_inspection_failed"), inspection.SafeMessage)
	}
	if inspection.Value != ExecutionReady {
		if heartbeatErr := stopHeartbeat(); heartbeatErr != nil {
			return heartbeatErr
		}
		if err := r.Store.WaitMaterializationForActivation(ctx, work, inspection.Value, r.now()); err != nil {
			return err
		}
		return r.ack(ctx, job)
	}
	outcome := adapter.MaterializeResources(workCtx, MaterializationRequest{
		Setup: work.Setup, Handle: work.Handle, BindingRevision: work.BindingRevision,
		TargetEnvironmentGeneration: work.TargetEnvironmentGeneration,
		TargetResourceRevision:      work.TargetResourceRevision,
	})
	if heartbeatErr := stopHeartbeat(); heartbeatErr != nil {
		return heartbeatErr
	}
	if outcome.Failed() {
		if outcome.EffectBoundary == ProviderProvedNotStarted && outcome.Disposition == ProviderRetryable {
			return r.retry(ctx, job, valueOrDefault(outcome.ErrorKind, "sandbox_materialization_failed"))
		}
		return r.fail(ctx, job, work, outcome.EffectBoundary, outcome.Disposition, valueOrDefault(outcome.ErrorKind, "sandbox_materialization_failed"), outcome.SafeMessage)
	}
	if err := r.Store.CompleteMaterialization(ctx, work, outcome.Value, r.now()); err != nil {
		return r.retry(ctx, job, "sandbox_materialization_store_error")
	}
	return r.ack(ctx, job)
}

func (r *SandboxMaterializationJobRunner) retry(ctx context.Context, job SandboxLifecycleJob, kind string) error {
	if job.AttemptCount >= job.MaxAttempts {
		if err := r.Store.FinalizeExhaustedLifecycle(ctx, job.QueueJob, queue.KindSandboxMaterialize, r.now()); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
			ErrorKind: "sandbox_materialization_attempts_exhausted", ErrorMessage: "sandbox materialization attempt budget exhausted",
		}))
	}
	return transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{
		WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
		ErrorKind: kind, ErrorMessage: "sandbox materialization will be retried",
	}))
}

func (r *SandboxMaterializationJobRunner) fail(ctx context.Context, job SandboxLifecycleJob, work SandboxMaterializationWork, boundary ProviderEffectBoundary, disposition ProviderDisposition, kind string, message string) error {
	if err := r.Store.FailMaterialization(ctx, work, boundary, disposition, kind, message, r.now()); err != nil {
		return err
	}
	return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
		WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
		ErrorKind: kind, ErrorMessage: "sandbox materialization failed",
	}))
}

func (r *SandboxMaterializationJobRunner) ack(ctx context.Context, job SandboxLifecycleJob) error {
	return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken}))
}

func (r *SandboxMaterializationJobRunner) now() time.Time {
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
