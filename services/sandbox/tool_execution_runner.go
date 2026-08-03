package tetralsandbox

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

const (
	SandboxToolExecuteMaxAttempts         = 5
	SandboxBackgroundReconcileMaxAttempts = 5
	// Foreground command observation is a privileged provider call. A bounded
	// interval prevents a single execution from spinning against that API.
	sandboxForegroundObservationPollInterval = 500 * time.Millisecond
)

type SandboxExecutionRef struct {
	WorkspaceID     string
	SessionID       string
	SessionThreadID string
	ToolUseEventID  string
}

type SandboxBinding struct {
	LogicalSandboxID             string
	EnvironmentID                string
	EnvironmentGeneration        int64
	Provider                     string
	ProviderResourceID           string
	BindingRevision              int64
	MaterializedResourceRevision int64
	ResourceCredentialExpiresAt  *time.Time
	HelperVerifiedAt             *time.Time
	ReleaseRequestedAt           *time.Time
	ReleaseReason                string
	ResourceRootsJSON            string
	ProviderMetadataJSON         string
}

func (b SandboxBinding) ReleaseRequested() bool {
	return b.ReleaseRequestedAt != nil
}

type SandboxExecutionWork struct {
	Ref                          SandboxExecutionRef
	AttemptGeneration            int64
	State                        string
	Binding                      *SandboxBinding
	MaterializationReady         bool
	AuthorizedBindingRevision    int64
	AuthorizedProviderResourceID string
	PreparationDeadline          *time.Time
	ProviderCommandReference     string
	Invocation                   sandboxdriver.ToolInvocation
}

type SandboxExecutionJob struct {
	JobID             string
	LeaseToken        string
	Ref               SandboxExecutionRef
	AttemptGeneration int64
}

type SandboxExecutionSettlementKind string

const (
	SandboxExecutionCompleted      SandboxExecutionSettlementKind = "completed"
	SandboxExecutionFailed         SandboxExecutionSettlementKind = "failed"
	SandboxExecutionUnknownOutcome SandboxExecutionSettlementKind = "unknown_outcome"
)

type SandboxExecutionSettlement struct {
	Kind                     SandboxExecutionSettlementKind
	ResultJSON               string
	ResultDigest             string
	ProviderCommandReference string
	BackgroundTask           *sandboxdriver.BackgroundTask
	LogicalSandboxID         string
	Provider                 string
	BindingRevision          int64
	ResourceRootsJSON        string
	ErrorKind                string
	SafeMessage              string
}

type SandboxExecutionCoordinator interface {
	LoadExecution(context.Context, SandboxExecutionJob) (SandboxExecutionWork, bool, error)
	WaitForActivation(context.Context, SandboxExecutionWork, ExecutionReadiness) error
	WaitForMaterialization(context.Context, SandboxExecutionWork) error
	BeginPreparing(context.Context, SandboxExecutionWork, time.Time) (bool, error)
	AuthorizeRunning(context.Context, SandboxExecutionWork) (bool, error)
	RecordProviderCommandReference(context.Context, SandboxExecutionWork, string) (bool, error)
	SettleExecution(context.Context, SandboxExecutionWork, SandboxExecutionSettlement) error
	FinalizeInvalidExecution(context.Context, *queuev1.QueueJob) error
	FinalizeExhaustedExecution(context.Context, *queuev1.QueueJob) error
}

type SandboxMediaMaterializer interface {
	MaterializeResult(context.Context, SandboxExecutionRef, string, string, string, time.Time) (string, error)
	RecoverResult(context.Context, SandboxExecutionRef) (SandboxMediaRecovery, error)
}

type SandboxMediaRecovery struct {
	Found      bool
	Ready      bool
	ResultJSON string
}

type SandboxToolExecutionJobRunner struct {
	Queue       SandboxQueueClient
	Coordinator SandboxExecutionCoordinator
	Providers   *ProviderRegistry
	Media       SandboxMediaMaterializer
	Config      SandboxToolExecutionRunnerConfig
	Clock       func() time.Time
}

type SandboxToolExecutionRunnerConfig struct {
	WorkspaceID        string
	LeaseOwner         string
	MaxJobs            int
	LeaseDuration      time.Duration
	HeartbeatInterval  time.Duration
	PreparationTimeout time.Duration
	LateCommandMargin  time.Duration
}

func (r *SandboxToolExecutionJobRunner) RunOnce(ctx context.Context) error {
	_, err := r.RunOnceWithActivity(ctx)
	return err
}

func (r *SandboxToolExecutionJobRunner) RunOnceWithActivity(ctx context.Context) (bool, error) {
	if r == nil || r.Queue == nil {
		return false, errors.New("sandbox tool execution queue client is required")
	}
	if r.Coordinator == nil {
		return false, errors.New("sandbox execution coordinator is required")
	}
	if r.Providers == nil || r.Media == nil {
		return false, errors.New("sandbox provider registry and media materializer are required")
	}
	cfg, err := normalizeSandboxToolExecutionRunnerConfig(r.Config)
	if err != nil {
		return false, err
	}
	leaseSentAt := time.Now()
	lease, err := r.Queue.Lease(ctx, &queuev1.LeaseRequest{
		WorkspaceId: cfg.WorkspaceID, Kinds: []string{queue.KindSandboxToolExecute},
		LeaseOwner: cfg.LeaseOwner, MaxJobs: int32(cfg.MaxJobs), LeaseDurationMs: cfg.LeaseDuration.Milliseconds(),
	})
	if err != nil {
		return false, err
	}
	hadWork := len(lease.GetJobs()) > 0
	for _, job := range lease.GetJobs() {
		if err := r.processJob(ctx, job, cfg, leaseSentAt.Add(wireRoundedQueueLeaseDuration(cfg.LeaseDuration))); err != nil {
			return hadWork, err
		}
	}
	return hadWork, nil
}

// RunSandboxToolExecutionConsumerGroup composes the production Sandbox Tool
// execution runner with long-lived contenders sharing the process-wide worker
// pool. Each contender receives a worker slot before it can lease one job.
func RunSandboxToolExecutionConsumerGroup(
	ctx context.Context,
	contenders int,
	pool *WorkspaceConsumerPool,
	lister WorkspaceLister,
	pollInterval time.Duration,
	queueClient SandboxQueueClient,
	coordinator SandboxExecutionCoordinator,
	providers *ProviderRegistry,
	media SandboxMediaMaterializer,
	config SandboxToolExecutionRunnerConfig,
) error {
	return RunWorkspaceConsumerGroup(ctx, contenders, pool, lister, pollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
		workspaceConfig := config
		workspaceConfig.WorkspaceID = workspaceID.String()
		return (&SandboxToolExecutionJobRunner{
			Queue: queueClient, Coordinator: coordinator, Providers: providers, Media: media, Config: workspaceConfig,
		}).RunOnceWithActivity(cycleCtx)
	})
}

func (r *SandboxToolExecutionJobRunner) processJob(ctx context.Context, queueJob *queuev1.QueueJob, cfg SandboxToolExecutionRunnerConfig, localExpiry time.Time) (resultErr error) {
	workCtx, finishLease, err := startQueueLeaseGuard(ctx, r.Queue, queueJob, localExpiry, cfg.HeartbeatInterval, cfg.LeaseDuration)
	if err != nil {
		return err
	}
	defer func() {
		if leaseErr := finishLease(); resultErr == nil && leaseErr != nil {
			resultErr = leaseErr
		}
	}()
	ctx = workCtx
	if queueJob.GetMaxAttempts() <= 0 {
		if err := r.Coordinator.FinalizeInvalidExecution(ctx, queueJob); err != nil {
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "sandbox_queue_integrity_error", ErrorMessage: "sandbox execution job has no attempt budget",
		}))
	}
	if queueJob.GetAttemptCount() > queueJob.GetMaxAttempts() {
		if err := r.Coordinator.FinalizeExhaustedExecution(ctx, queueJob); err != nil {
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "sandbox_execution_attempts_exhausted", ErrorMessage: "sandbox execution attempt budget exhausted",
		}))
	}
	job, err := DecodeSandboxExecutionJob(queueJob)
	if err != nil {
		if err := r.Coordinator.FinalizeInvalidExecution(ctx, queueJob); err != nil {
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "invalid_sandbox_tool_execute_payload", ErrorMessage: "sandbox tool execution queue payload is invalid",
		}))
	}
	err = r.handleExecution(ctx, job, cfg)
	if err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return err
		}
		var retry *sandboxExecutionRetry
		if errors.As(err, &retry) {
			if queueJob.GetAttemptCount() >= queueJob.GetMaxAttempts() {
				if err := r.Coordinator.FinalizeExhaustedExecution(ctx, queueJob); err != nil {
					return err
				}
				if err := stopQueueLeaseGuard(ctx); err != nil {
					return err
				}
				return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
					WorkspaceId: job.Ref.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
					ErrorKind: "sandbox_execution_attempts_exhausted", ErrorMessage: "sandbox execution attempt budget exhausted",
				}))
			}
			if err := stopQueueLeaseGuard(ctx); err != nil {
				return err
			}
			return transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{
				WorkspaceId: job.Ref.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
				ErrorKind: retry.kind, ErrorMessage: retry.message,
			}))
		}
		return err
	}
	if err := stopQueueLeaseGuard(ctx); err != nil {
		return err
	}
	return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{
		WorkspaceId: job.Ref.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
	}))
}

func (r *SandboxToolExecutionJobRunner) handleExecution(ctx context.Context, job SandboxExecutionJob, cfg SandboxToolExecutionRunnerConfig) error {
	work, current, err := r.Coordinator.LoadExecution(ctx, job)
	if err != nil {
		return sandboxExecutionRetryUnlessAuthorityLost(err, "sandbox_execution_store_error", "sandbox execution state could not be loaded")
	}
	if !current {
		return nil
	}
	if work.State == "running" {
		if work.ProviderCommandReference == "" {
			recovery, err := r.Media.RecoverResult(ctx, work.Ref)
			if err != nil {
				return sandboxExecutionRetryUnlessAuthorityLost(err, "sandbox_media_recovery_error", "sandbox media custody could not be inspected")
			}
			if recovery.Found {
				if recovery.Ready {
					return r.Coordinator.SettleExecution(ctx, work, SandboxExecutionSettlement{
						Kind: SandboxExecutionCompleted, ResultJSON: recovery.ResultJSON,
					})
				}
				return r.Coordinator.SettleExecution(ctx, work, SandboxExecutionSettlement{
					Kind: SandboxExecutionFailed, ErrorKind: "transient_attachment_unavailable",
					SafeMessage: "sandbox media attachment handoff failed",
				})
			}
			return r.Coordinator.SettleExecution(ctx, work, SandboxExecutionSettlement{
				Kind:      SandboxExecutionUnknownOutcome,
				ErrorKind: "sandbox_execution_outcome_unknown", SafeMessage: "sandbox execution outcome is unknown",
			})
		}
		return r.observeRunningExecution(ctx, work)
	}
	if work.Binding == nil || work.Binding.ProviderResourceID == "" {
		return sandboxExecutionDependencyResult(r.Coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation))
	}
	adapter, ok := r.Providers.Resolve(work.Binding.Provider)
	if !ok {
		return r.settleProviderFailure(ctx, work, ProviderOutcome[ExecutionReadiness]{
			EffectBoundary: ProviderProvedNotStarted, Disposition: ProviderTerminal,
			ErrorKind: "provider_not_registered", SafeMessage: "sandbox provider is not registered",
		})
	}
	inspection := adapter.InspectForExecution(ctx, work.Binding.ProviderResourceID)
	if inspection.Failed() {
		if inspection.EffectBoundary == ProviderProvedNotStarted && inspection.Disposition == ProviderRetryable {
			return newSandboxExecutionRetry(valueOrDefault(inspection.ErrorKind, "provider_inspection_retryable"), "sandbox provider inspection will be retried")
		}
		return r.settleProviderFailure(ctx, work, inspection)
	}
	switch inspection.Value {
	case ExecutionNeedsActivation, ExecutionNeedsCreation:
		return sandboxExecutionDependencyResult(r.Coordinator.WaitForActivation(ctx, work, inspection.Value))
	case ExecutionReady:
	default:
		return r.settleProviderFailure(ctx, work, ProviderOutcome[ExecutionReadiness]{
			EffectBoundary: ProviderProvedNotStarted, Disposition: ProviderTerminal,
			ErrorKind: "provider_response_malformed", SafeMessage: "sandbox provider returned an invalid readiness result",
		})
	}
	if !work.MaterializationReady {
		return sandboxExecutionDependencyResult(r.Coordinator.WaitForMaterialization(ctx, work))
	}
	deadline := r.now().Add(cfg.PreparationTimeout)
	if work.State == "preparing" && work.PreparationDeadline != nil {
		deadline = work.PreparationDeadline.UTC()
	}
	prepared, err := r.Coordinator.BeginPreparing(ctx, work, deadline)
	if err != nil {
		if errors.Is(err, errSandboxExecutionReinspection) {
			return newSandboxExecutionRetry("sandbox_execution_reinspection", "sandbox execution state changed before preparation")
		}
		return sandboxExecutionRetryUnlessAuthorityLost(err, "sandbox_execution_store_error", "sandbox execution preparation could not be recorded")
	}
	if !prepared {
		return nil
	}
	work.Invocation.Target = sandboxdriver.ToolTarget{
		WorkspaceID: work.Ref.WorkspaceID, SessionID: work.Ref.SessionID,
		SessionThreadID: work.Ref.SessionThreadID, SandboxID: work.Binding.LogicalSandboxID,
		ProviderSandboxID: work.Binding.ProviderResourceID, ResourceRootsJSON: work.Binding.ResourceRootsJSON,
	}
	preparationCtx, cancelPreparation := context.WithDeadline(ctx, deadline)
	preparation := adapter.PrepareTool(preparationCtx, ToolExecutionRequest{
		Handle:     sandbox.ProviderHandle{Provider: work.Binding.Provider, SandboxID: work.Binding.ProviderResourceID},
		Invocation: work.Invocation,
	})
	cancelPreparation()
	if preparation.Failed() {
		if preparation.EffectBoundary == ProviderProvedNotStarted &&
			preparation.ErrorKind == string(sandbox.ProviderErrorNotFound) {
			return sandboxExecutionDependencyResult(r.Coordinator.WaitForActivation(ctx, work, ExecutionNeedsCreation))
		}
		if preparation.EffectBoundary == ProviderProvedNotStarted && preparation.Disposition == ProviderRetryable {
			return newSandboxExecutionRetry(valueOrDefault(preparation.ErrorKind, "sandbox_tool_preparation_retryable"), "sandbox tool preparation will be retried")
		}
		return r.Coordinator.SettleExecution(ctx, work, SandboxExecutionSettlement{
			Kind: SandboxExecutionFailed, ErrorKind: valueOrDefault(preparation.ErrorKind, "sandbox_tool_preparation_failed"), SafeMessage: preparation.SafeMessage,
		})
	}
	if preparation.Value.ImmediateResult != nil {
		return r.settleCompletedExecution(ctx, work, preparation.Value.ImmediateResult.ResultJSON, nil, "")
	}
	authorized, err := r.Coordinator.AuthorizeRunning(ctx, work)
	if err != nil {
		if errors.Is(err, errSandboxExecutionReinspection) {
			return newSandboxExecutionRetry("sandbox_execution_reinspection", "sandbox execution state changed before submission")
		}
		return sandboxExecutionRetryUnlessAuthorityLost(err, "sandbox_execution_store_error", "sandbox execution authorization could not be recorded")
	}
	if !authorized {
		return nil
	}
	outcome := adapter.ExecuteTool(ctx, ToolExecutionRequest{
		Handle:     sandbox.ProviderHandle{Provider: work.Binding.Provider, SandboxID: work.Binding.ProviderResourceID},
		Invocation: work.Invocation,
		Prepared:   preparation.Value.Prepared,
	})
	if outcome.Failed() {
		kind := SandboxExecutionFailed
		if outcome.EffectBoundary == ProviderOutcomeUnknown || outcome.EffectBoundary == ProviderSubmitted {
			kind = SandboxExecutionUnknownOutcome
		}
		return r.Coordinator.SettleExecution(ctx, work, SandboxExecutionSettlement{
			Kind: kind, ErrorKind: valueOrDefault(outcome.ErrorKind, "sandbox_execution_failed"), SafeMessage: outcome.SafeMessage,
		})
	}
	if outcome.Value.ForegroundObservation != nil {
		encodedReference, err := encodeSandboxToolObservationReference(work.Binding.Provider, *outcome.Value.ForegroundObservation)
		if err != nil {
			return r.Coordinator.SettleExecution(ctx, work, SandboxExecutionSettlement{
				Kind: SandboxExecutionUnknownOutcome, ErrorKind: "sandbox_execution_outcome_unknown",
				SafeMessage: "sandbox execution outcome is unknown",
			})
		}
		recorded, err := r.Coordinator.RecordProviderCommandReference(ctx, work, encodedReference)
		if err != nil {
			return sandboxExecutionRetryUnlessAuthorityLost(err, "sandbox_execution_store_error", "sandbox command reference could not be recorded")
		}
		if !recorded {
			return nil
		}
		work.ProviderCommandReference = encodedReference
		return r.observeRunningExecution(ctx, work)
	}
	backgroundTask := outcome.Value.BackgroundTask
	if backgroundTask != nil {
		owned := *backgroundTask
		owned.SourceToolUseEventID = work.Ref.ToolUseEventID
		backgroundTask = &owned
	}
	return r.settleCompletedExecution(ctx, work, outcome.Value.ResultJSON, backgroundTask, "")
}

type sandboxToolObservationReference struct {
	Provider    string                                     `json:"provider"`
	Observation sandboxdriver.ForegroundCommandObservation `json:"observation"`
}

func encodeSandboxToolObservationReference(provider string, observation sandboxdriver.ForegroundCommandObservation) (string, error) {
	if provider == "" || observation.Reference.Target.ProviderSandboxID == "" ||
		observation.Reference.Task.TaskID == "" || observation.Reference.Task.ProviderCommandID == "" {
		return "", errors.New("sandbox command observation reference is incomplete")
	}
	encoded, err := json.Marshal(sandboxToolObservationReference{Provider: provider, Observation: observation})
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func decodeSandboxToolObservationReference(encoded string) (sandboxToolObservationReference, error) {
	var reference sandboxToolObservationReference
	decoder := json.NewDecoder(strings.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&reference); err != nil {
		return sandboxToolObservationReference{}, err
	}
	if reference.Provider == "" || reference.Observation.Reference.Target.ProviderSandboxID == "" ||
		reference.Observation.Reference.Task.TaskID == "" || reference.Observation.Reference.Task.ProviderCommandID == "" {
		return sandboxToolObservationReference{}, errors.New("sandbox command observation reference is incomplete")
	}
	return reference, nil
}

func (r *SandboxToolExecutionJobRunner) observeRunningExecution(ctx context.Context, work SandboxExecutionWork) error {
	for {
		reference, err := decodeSandboxToolObservationReference(work.ProviderCommandReference)
		if err != nil {
			return r.Coordinator.SettleExecution(ctx, work, SandboxExecutionSettlement{
				Kind: SandboxExecutionUnknownOutcome, ErrorKind: "sandbox_execution_outcome_unknown",
				SafeMessage: "sandbox execution outcome is unknown",
			})
		}
		adapter, ok := r.Providers.Resolve(reference.Provider)
		if !ok {
			return r.Coordinator.SettleExecution(ctx, work, SandboxExecutionSettlement{
				Kind: SandboxExecutionUnknownOutcome, ProviderCommandReference: work.ProviderCommandReference,
				ErrorKind: "sandbox_execution_outcome_unknown", SafeMessage: "sandbox execution outcome is unknown",
			})
		}
		outcome := adapter.ObserveTool(ctx, reference.Observation)
		if outcome.Failed() {
			if outcome.Disposition == ProviderRetryable {
				return newSandboxExecutionRetry(valueOrDefault(outcome.ErrorKind, "sandbox_execution_observation_retryable"), "sandbox execution observation will be retried")
			}
			return r.Coordinator.SettleExecution(ctx, work, SandboxExecutionSettlement{
				Kind: SandboxExecutionUnknownOutcome, ProviderCommandReference: work.ProviderCommandReference,
				ErrorKind: "sandbox_execution_outcome_unknown", SafeMessage: "sandbox execution outcome is unknown",
			})
		}
		if outcome.Value.ForegroundObservation == nil {
			return r.settleCompletedExecution(ctx, work, outcome.Value.ResultJSON, nil, work.ProviderCommandReference)
		}
		encodedReference, err := encodeSandboxToolObservationReference(reference.Provider, *outcome.Value.ForegroundObservation)
		if err != nil {
			return r.Coordinator.SettleExecution(ctx, work, SandboxExecutionSettlement{
				Kind: SandboxExecutionUnknownOutcome, ProviderCommandReference: work.ProviderCommandReference,
				ErrorKind: "sandbox_execution_outcome_unknown", SafeMessage: "sandbox execution outcome is unknown",
			})
		}
		recorded, err := r.Coordinator.RecordProviderCommandReference(ctx, work, encodedReference)
		if err != nil {
			return sandboxExecutionRetryUnlessAuthorityLost(err, "sandbox_execution_store_error", "sandbox command observation could not be recorded")
		}
		if !recorded {
			return nil
		}
		work.ProviderCommandReference = encodedReference
		if err := r.waitForObservation(ctx, sandboxForegroundObservationPollInterval); err != nil {
			return err
		}
	}
}

func (r *SandboxToolExecutionJobRunner) waitForObservation(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *SandboxToolExecutionJobRunner) settleCompletedExecution(ctx context.Context, work SandboxExecutionWork, resultJSON string, backgroundTask *sandboxdriver.BackgroundTask, providerCommandReference string) error {
	materialized, err := r.Media.MaterializeResult(ctx, work.Ref, work.Invocation.ToolName, work.Invocation.InputJSON, resultJSON, r.now())
	if err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return err
		}
		return r.Coordinator.SettleExecution(ctx, work, SandboxExecutionSettlement{
			Kind: SandboxExecutionFailed, ErrorKind: "transient_attachment_unavailable",
			SafeMessage: "sandbox media attachment handoff failed", ProviderCommandReference: providerCommandReference,
		})
	}
	return r.Coordinator.SettleExecution(ctx, work, SandboxExecutionSettlement{
		Kind: SandboxExecutionCompleted, ResultJSON: materialized,
		BackgroundTask: backgroundTask, ProviderCommandReference: providerCommandReference,
	})
}

func sandboxExecutionDependencyResult(err error) error {
	if errors.Is(err, errSandboxExecutionReinspection) {
		return newSandboxExecutionRetry("sandbox_execution_reinspection", "sandbox execution state changed before its dependency was recorded")
	}
	return err
}

func (r *SandboxToolExecutionJobRunner) settleProviderFailure(ctx context.Context, work SandboxExecutionWork, outcome ProviderOutcome[ExecutionReadiness]) error {
	return r.Coordinator.SettleExecution(ctx, work, SandboxExecutionSettlement{
		Kind: SandboxExecutionFailed, ErrorKind: valueOrDefault(outcome.ErrorKind, "sandbox_provider_unavailable"), SafeMessage: outcome.SafeMessage,
	})
}

func (r *SandboxToolExecutionJobRunner) now() time.Time {
	if r != nil && r.Clock != nil {
		return r.Clock().UTC()
	}
	return storage.Now()
}

func DecodeSandboxExecutionJob(queueJob *queuev1.QueueJob) (SandboxExecutionJob, error) {
	if queueJob == nil || queueJob.GetKind() != queue.KindSandboxToolExecute {
		return SandboxExecutionJob{}, errors.New("queue job kind is not sandbox_tool_execute")
	}
	job, err := decodeSandboxExecutionQueueIdentity(
		queueJob.GetId(), queueJob.GetWorkspaceId(), queueJob.GetPartitionKey(),
		queueJob.GetDedupeKey(), queueJob.GetPayloadJson(),
	)
	if err != nil {
		return SandboxExecutionJob{}, err
	}
	if queueJob.GetLeaseToken() == "" {
		return SandboxExecutionJob{}, errors.New("sandbox execution lease token is missing")
	}
	job.LeaseToken = queueJob.GetLeaseToken()
	return job, nil
}

func decodeSandboxExecutionQueueIdentity(jobID string, workspaceID string, partitionKey string, dedupeKey string, payloadJSON string) (SandboxExecutionJob, error) {
	var payload struct {
		WorkspaceID     string `json:"workspace_id"`
		SessionID       string `json:"session_id"`
		SessionThreadID string `json:"session_thread_id"`
		ToolUseEventID  string `json:"tool_use_event_id"`
	}
	decoder := json.NewDecoder(strings.NewReader(payloadJSON))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return SandboxExecutionJob{}, err
	}
	separator := strings.LastIndexByte(dedupeKey, ':')
	if separator < 0 {
		return SandboxExecutionJob{}, errors.New("sandbox execution dedupe generation is missing")
	}
	generation, err := strconv.ParseInt(dedupeKey[separator+1:], 10, 64)
	if err != nil || generation <= 0 {
		return SandboxExecutionJob{}, errors.New("sandbox execution dedupe generation is invalid")
	}
	ref := SandboxExecutionRef{WorkspaceID: payload.WorkspaceID, SessionID: payload.SessionID, SessionThreadID: payload.SessionThreadID, ToolUseEventID: payload.ToolUseEventID}
	if jobID == "" || ref.WorkspaceID == "" || ref.WorkspaceID != workspaceID ||
		ref.SessionID == "" || ref.SessionThreadID == "" || ref.ToolUseEventID == "" ||
		partitionKey != queue.FormatSandboxExecutionPartitionKey(queueWorkspaceID(ref.WorkspaceID), ref.SessionID, ref.SessionThreadID, ref.ToolUseEventID) ||
		dedupeKey != queue.FormatSandboxToolExecuteDedupeKey(queueWorkspaceID(ref.WorkspaceID), ref.SessionID, ref.SessionThreadID, ref.ToolUseEventID, generation) {
		return SandboxExecutionJob{}, errors.New("sandbox execution queue identity is invalid")
	}
	return SandboxExecutionJob{JobID: jobID, Ref: ref, AttemptGeneration: generation}, nil
}

func queueWorkspaceID(value string) workspace.ID { return workspace.ID(value) }

func normalizeSandboxToolExecutionRunnerConfig(cfg SandboxToolExecutionRunnerConfig) (SandboxToolExecutionRunnerConfig, error) {
	if cfg.LeaseOwner == "" {
		cfg.LeaseOwner = ServiceName
	}
	if cfg.MaxJobs <= 0 {
		cfg.MaxJobs = 1
	}
	if cfg.LeaseDuration <= 0 || cfg.HeartbeatInterval <= 0 || cfg.PreparationTimeout <= 0 || cfg.LateCommandMargin < 0 {
		return SandboxToolExecutionRunnerConfig{}, errors.New("sandbox tool execution timing configuration is invalid")
	}
	if cfg.LeaseDuration-cfg.HeartbeatInterval < cfg.PreparationTimeout+cfg.LateCommandMargin {
		return SandboxToolExecutionRunnerConfig{}, errors.New("sandbox tool execution lease does not cover preparation deadline and late-command margin")
	}
	return cfg, nil
}

type sandboxExecutionRetry struct {
	kind    string
	message string
}

func (e *sandboxExecutionRetry) Error() string { return e.message }

func newSandboxExecutionRetry(kind string, message string) error {
	return &sandboxExecutionRetry{kind: kind, message: message}
}

func sandboxExecutionRetryUnlessAuthorityLost(err error, kind string, message string) error {
	if errors.Is(err, errQueueLeaseLost) {
		return err
	}
	return newSandboxExecutionRetry(kind, message)
}
