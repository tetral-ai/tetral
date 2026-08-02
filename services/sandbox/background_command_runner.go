package tetralsandbox

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

const (
	SandboxBackgroundCommandMaxAttempts = 5
	SandboxBackgroundPollInterval       = time.Second
	SandboxBackgroundOutageRetryBackoff = 30 * time.Second
)

type BackgroundCommandAdapter interface {
	PollBackground(context.Context, sandboxdriver.CommandReference) ProviderOutcome[sandboxdriver.CommandResult]
	SendBackgroundInput(context.Context, sandboxdriver.CommandInput) ProviderOutcome[sandboxdriver.CommandResult]
	CancelBackground(context.Context, sandboxdriver.CommandCancel) ProviderOutcome[sandboxdriver.CommandResult]
}

type SandboxBackgroundTaskWork struct {
	WorkspaceID         string
	SessionID           string
	SessionThreadID     string
	TaskID              string
	Provider            string
	ReconcileGeneration int64
	ReleaseOperationID  string
	Reference           sandboxdriver.CommandReference
}

type SandboxBackgroundReconcileJob struct {
	JobID               string
	LeaseToken          string
	WorkspaceID         string
	SessionID           string
	TaskID              string
	ReconcileGeneration int64
}

type SandboxBackgroundOperationKind string

const (
	SandboxBackgroundOperationPoll   SandboxBackgroundOperationKind = "poll"
	SandboxBackgroundOperationStdin  SandboxBackgroundOperationKind = "stdin"
	SandboxBackgroundOperationCancel SandboxBackgroundOperationKind = "cancel"
)

const (
	SandboxBackgroundOperationPending   = "pending"
	SandboxBackgroundOperationSubmitted = "submitted"
	SandboxBackgroundOperationTerminal  = "terminal"
)

type SandboxBackgroundCommandJob struct {
	JobID       string
	LeaseToken  string
	WorkspaceID string
	SessionID   string
	TaskID      string
	RequestID   string
}

type SandboxBackgroundOperationWork struct {
	Task            SandboxBackgroundTaskWork
	RequestID       string
	ToolUseEventID  string
	Kind            SandboxBackgroundOperationKind
	State           string
	InputJSON       string
	MaxOutputTokens int
}

type SandboxBackgroundCommandStore interface {
	LoadReconcile(context.Context, SandboxBackgroundReconcileJob) (SandboxBackgroundTaskWork, bool, error)
	AdvanceReconcile(context.Context, SandboxBackgroundTaskWork, sandboxdriver.CommandResult, time.Time, time.Time) error
	SettleTask(context.Context, SandboxBackgroundTaskWork, sandboxdriver.CommandResult, time.Time) error
	FinalizeReconcileExhaustion(context.Context, *queuev1.QueueJob, time.Time) error
	LoadCommand(context.Context, SandboxBackgroundCommandJob, time.Time) (SandboxBackgroundOperationWork, bool, error)
	MarkCommandSubmitted(context.Context, SandboxBackgroundOperationWork, time.Time) (bool, error)
	SettleCommand(context.Context, SandboxBackgroundOperationWork, string, sandboxdriver.CommandResult, time.Time) error
	FinalizeCommandExhaustion(context.Context, *queuev1.QueueJob, time.Time) error
}

type SandboxBackgroundRunnerConfig struct {
	WorkspaceID       string
	LeaseOwner        string
	MaxJobs           int
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
}

type SandboxBackgroundReconcileJobRunner struct {
	Queue     SandboxQueueClient
	Store     SandboxBackgroundCommandStore
	Providers *ProviderRegistry
	Config    SandboxBackgroundRunnerConfig
	Clock     func() time.Time
}

func (r *SandboxBackgroundReconcileJobRunner) RunOnce(ctx context.Context) error {
	_, err := r.RunOnceWithActivity(ctx)
	return err
}

func (r *SandboxBackgroundReconcileJobRunner) RunOnceWithActivity(ctx context.Context) (bool, error) {
	if r == nil || r.Queue == nil || r.Store == nil || r.Providers == nil {
		return false, errors.New("sandbox background reconcile dependencies are required")
	}
	cfg, err := normalizeSandboxBackgroundRunnerConfig(r.Config)
	if err != nil {
		return false, err
	}
	leaseSentAt := time.Now()
	lease, err := r.Queue.Lease(ctx, &queuev1.LeaseRequest{
		WorkspaceId: cfg.WorkspaceID, Kinds: []string{queue.KindSandboxBackgroundReconcile},
		LeaseOwner: cfg.LeaseOwner, MaxJobs: int32(cfg.MaxJobs), LeaseDurationMs: cfg.LeaseDuration.Milliseconds(),
	})
	if err != nil {
		return false, err
	}
	for _, job := range lease.GetJobs() {
		if err := r.processJob(ctx, job, cfg, leaseSentAt.Add(wireRoundedQueueLeaseDuration(cfg.LeaseDuration))); err != nil {
			return len(lease.GetJobs()) > 0, err
		}
	}
	return len(lease.GetJobs()) > 0, nil
}

func (r *SandboxBackgroundReconcileJobRunner) processJob(ctx context.Context, queueJob *queuev1.QueueJob, cfg SandboxBackgroundRunnerConfig, localExpiry time.Time) (resultErr error) {
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
	if queueJob.GetAttemptCount() > queueJob.GetMaxAttempts() || queueJob.GetMaxAttempts() <= 0 {
		if err := r.Store.FinalizeReconcileExhaustion(ctx, queueJob, r.now()); err != nil {
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return deadLetterBackgroundJob(ctx, r.Queue, queueJob, "sandbox_background_reconcile_exhausted")
	}
	job, err := DecodeSandboxBackgroundReconcileJob(queueJob)
	if err != nil {
		if err := r.Store.FinalizeReconcileExhaustion(ctx, queueJob, r.now()); err != nil {
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return deadLetterBackgroundJob(ctx, r.Queue, queueJob, "invalid_sandbox_background_reconcile_payload")
	}
	err = r.reconcile(ctx, job)
	if err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return err
		}
		var retry *sandboxExecutionRetry
		if errors.As(err, &retry) {
			if queueJob.GetAttemptCount() >= queueJob.GetMaxAttempts() {
				if err := r.Store.FinalizeReconcileExhaustion(ctx, queueJob, r.now()); err != nil {
					return err
				}
				if err := stopQueueLeaseGuard(ctx); err != nil {
					return err
				}
				return deadLetterBackgroundJob(ctx, r.Queue, queueJob, "sandbox_background_reconcile_exhausted")
			}
			if err := stopQueueLeaseGuard(ctx); err != nil {
				return err
			}
			return transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken, ErrorKind: retry.kind, ErrorMessage: retry.message}))
		}
		return err
	}
	if err := stopQueueLeaseGuard(ctx); err != nil {
		return err
	}
	return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken}))
}

func (r *SandboxBackgroundReconcileJobRunner) reconcile(ctx context.Context, job SandboxBackgroundReconcileJob) error {
	task, current, err := r.Store.LoadReconcile(ctx, job)
	if err != nil {
		return sandboxExecutionRetryUnlessAuthorityLost(err, "sandbox_background_store_error", "background task could not be loaded")
	}
	if !current {
		return nil
	}
	adapter, ok := r.Providers.Resolve(task.Provider)
	if !ok {
		return r.Store.SettleTask(ctx, task, backgroundTaskFailure("sandbox_provider_unavailable", "sandbox provider is unavailable"), r.now())
	}
	background, ok := adapter.(BackgroundCommandAdapter)
	if !ok {
		return r.Store.SettleTask(ctx, task, backgroundTaskFailure("sandbox_provider_unavailable", "sandbox background command adapter is unavailable"), r.now())
	}
	outcome := background.PollBackground(ctx, task.Reference)
	if outcome.Failed() {
		if outcome.Disposition == ProviderRetryable {
			return newSandboxExecutionRetry(valueOrDefault(outcome.ErrorKind, "sandbox_background_observation_retryable"), "background task observation will be retried")
		}
		return r.Store.SettleTask(ctx, task, backgroundTaskFailure(valueOrDefault(outcome.ErrorKind, "sandbox_background_observation_failed"), outcome.SafeMessage), r.now())
	}
	if outcome.Value.TerminalStatus == "" {
		now := r.now()
		return r.Store.AdvanceReconcile(ctx, task, outcome.Value, now, now.Add(SandboxBackgroundPollInterval))
	}
	return r.Store.SettleTask(ctx, task, outcome.Value, r.now())
}

func (r *SandboxBackgroundReconcileJobRunner) now() time.Time {
	if r != nil && r.Clock != nil {
		return r.Clock().UTC()
	}
	return storage.Now()
}

type SandboxBackgroundCommandJobRunner struct {
	Queue     SandboxQueueClient
	Store     SandboxBackgroundCommandStore
	Providers *ProviderRegistry
	Config    SandboxBackgroundRunnerConfig
	Clock     func() time.Time
}

func (r *SandboxBackgroundCommandJobRunner) RunOnce(ctx context.Context) error {
	_, err := r.RunOnceWithActivity(ctx)
	return err
}

func (r *SandboxBackgroundCommandJobRunner) RunOnceWithActivity(ctx context.Context) (bool, error) {
	if r == nil || r.Queue == nil || r.Store == nil || r.Providers == nil {
		return false, errors.New("sandbox background command dependencies are required")
	}
	cfg, err := normalizeSandboxBackgroundRunnerConfig(r.Config)
	if err != nil {
		return false, err
	}
	leaseSentAt := time.Now()
	lease, err := r.Queue.Lease(ctx, &queuev1.LeaseRequest{
		WorkspaceId: cfg.WorkspaceID, Kinds: []string{queue.KindSandboxBackgroundCommand},
		LeaseOwner: cfg.LeaseOwner, MaxJobs: int32(cfg.MaxJobs), LeaseDurationMs: cfg.LeaseDuration.Milliseconds(),
	})
	if err != nil {
		return false, err
	}
	for _, job := range lease.GetJobs() {
		if err := r.processJob(ctx, job, cfg, leaseSentAt.Add(wireRoundedQueueLeaseDuration(cfg.LeaseDuration))); err != nil {
			return len(lease.GetJobs()) > 0, err
		}
	}
	return len(lease.GetJobs()) > 0, nil
}

func (r *SandboxBackgroundCommandJobRunner) processJob(ctx context.Context, queueJob *queuev1.QueueJob, cfg SandboxBackgroundRunnerConfig, localExpiry time.Time) (resultErr error) {
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
	if queueJob.GetAttemptCount() > queueJob.GetMaxAttempts() || queueJob.GetMaxAttempts() <= 0 {
		if err := r.Store.FinalizeCommandExhaustion(ctx, queueJob, r.now()); err != nil {
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return deadLetterBackgroundJob(ctx, r.Queue, queueJob, "sandbox_background_command_exhausted")
	}
	job, err := DecodeSandboxBackgroundCommandJob(queueJob)
	if err != nil {
		if err := r.Store.FinalizeCommandExhaustion(ctx, queueJob, r.now()); err != nil {
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return deadLetterBackgroundJob(ctx, r.Queue, queueJob, "invalid_sandbox_background_command_payload")
	}
	err = r.execute(ctx, job)
	if err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return err
		}
		var retry *sandboxExecutionRetry
		if errors.As(err, &retry) {
			if queueJob.GetAttemptCount() >= queueJob.GetMaxAttempts() {
				if err := r.Store.FinalizeCommandExhaustion(ctx, queueJob, r.now()); err != nil {
					return err
				}
				if err := stopQueueLeaseGuard(ctx); err != nil {
					return err
				}
				return deadLetterBackgroundJob(ctx, r.Queue, queueJob, "sandbox_background_command_exhausted")
			}
			if err := stopQueueLeaseGuard(ctx); err != nil {
				return err
			}
			return transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken, ErrorKind: retry.kind, ErrorMessage: retry.message}))
		}
		return err
	}
	if err := stopQueueLeaseGuard(ctx); err != nil {
		return err
	}
	return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken}))
}

func (r *SandboxBackgroundCommandJobRunner) execute(ctx context.Context, job SandboxBackgroundCommandJob) error {
	work, current, err := r.Store.LoadCommand(ctx, job, r.now())
	if err != nil {
		return sandboxExecutionRetryUnlessAuthorityLost(err, "sandbox_background_store_error", "background command could not be loaded")
	}
	if !current {
		return nil
	}
	if work.State == SandboxBackgroundOperationSubmitted && work.Kind != SandboxBackgroundOperationPoll {
		return r.Store.SettleCommand(ctx, work, "unknown_outcome", backgroundTaskFailure("sandbox_background_outcome_unknown", "background command outcome is unknown"), r.now())
	}
	adapter, ok := r.Providers.Resolve(work.Task.Provider)
	if !ok {
		return r.Store.SettleCommand(ctx, work, "failed", backgroundTaskFailure("sandbox_provider_unavailable", "sandbox provider is unavailable"), r.now())
	}
	background, ok := adapter.(BackgroundCommandAdapter)
	if !ok {
		return r.Store.SettleCommand(ctx, work, "failed", backgroundTaskFailure("sandbox_provider_unavailable", "sandbox background command adapter is unavailable"), r.now())
	}
	if work.Kind != SandboxBackgroundOperationPoll {
		submitted, err := r.Store.MarkCommandSubmitted(ctx, work, r.now())
		if err != nil {
			return sandboxExecutionRetryUnlessAuthorityLost(err, "sandbox_background_store_error", "background command submission could not be recorded")
		}
		if !submitted {
			return nil
		}
		work.State = SandboxBackgroundOperationSubmitted
	}
	var outcome ProviderOutcome[sandboxdriver.CommandResult]
	switch work.Kind {
	case SandboxBackgroundOperationPoll:
		reference := work.Task.Reference
		reference.MaxOutputTokens = work.MaxOutputTokens
		outcome = background.PollBackground(ctx, reference)
	case SandboxBackgroundOperationStdin:
		outcome = background.SendBackgroundInput(ctx, sandboxdriver.CommandInput{CommandReference: work.Task.Reference, InputJSON: work.InputJSON})
	case SandboxBackgroundOperationCancel:
		outcome = background.CancelBackground(ctx, sandboxdriver.CommandCancel{CommandReference: work.Task.Reference, Reason: backgroundCancelReason(work.InputJSON)})
	default:
		return r.Store.SettleCommand(ctx, work, "failed", backgroundTaskFailure("sandbox_background_request_invalid", "background command kind is invalid"), r.now())
	}
	if outcome.Failed() {
		if work.Kind == SandboxBackgroundOperationPoll && outcome.Disposition == ProviderRetryable {
			return newSandboxExecutionRetry(valueOrDefault(outcome.ErrorKind, "sandbox_background_observation_retryable"), "background command observation will be retried")
		}
		disposition := "failed"
		if work.State == SandboxBackgroundOperationSubmitted {
			disposition = "unknown_outcome"
		}
		return r.Store.SettleCommand(ctx, work, disposition, backgroundTaskFailure(valueOrDefault(outcome.ErrorKind, "sandbox_background_unavailable"), outcome.SafeMessage), r.now())
	}
	return r.Store.SettleCommand(ctx, work, "completed", outcome.Value, r.now())
}

func (r *SandboxBackgroundCommandJobRunner) now() time.Time {
	if r != nil && r.Clock != nil {
		return r.Clock().UTC()
	}
	return storage.Now()
}

func DecodeSandboxBackgroundReconcileJob(job *queuev1.QueueJob) (SandboxBackgroundReconcileJob, error) {
	var payload struct {
		WorkspaceID         string `json:"workspace_id"`
		SessionID           string `json:"session_id"`
		TaskID              string `json:"task_id"`
		ReconcileGeneration int64  `json:"reconcile_generation"`
	}
	if err := decodeBackgroundQueuePayload(job, queue.KindSandboxBackgroundReconcile, &payload); err != nil {
		return SandboxBackgroundReconcileJob{}, err
	}
	if payload.ReconcileGeneration <= 0 || job.GetPartitionKey() != queue.FormatSandboxBackgroundPartitionKey(queueWorkspaceID(payload.WorkspaceID), payload.SessionID, payload.TaskID) ||
		job.GetDedupeKey() != queue.FormatSandboxBackgroundReconcileDedupeKey(queueWorkspaceID(payload.WorkspaceID), payload.SessionID, payload.TaskID, payload.ReconcileGeneration) {
		return SandboxBackgroundReconcileJob{}, errors.New("sandbox background reconcile queue identity is invalid")
	}
	return SandboxBackgroundReconcileJob{JobID: job.GetId(), LeaseToken: job.GetLeaseToken(), WorkspaceID: payload.WorkspaceID, SessionID: payload.SessionID, TaskID: payload.TaskID, ReconcileGeneration: payload.ReconcileGeneration}, nil
}

func DecodeSandboxBackgroundCommandJob(job *queuev1.QueueJob) (SandboxBackgroundCommandJob, error) {
	var payload struct {
		WorkspaceID string `json:"workspace_id"`
		SessionID   string `json:"session_id"`
		TaskID      string `json:"task_id"`
		RequestID   string `json:"request_id"`
	}
	if err := decodeBackgroundQueuePayload(job, queue.KindSandboxBackgroundCommand, &payload); err != nil {
		return SandboxBackgroundCommandJob{}, err
	}
	if payload.RequestID == "" || job.GetPartitionKey() != queue.FormatSandboxBackgroundPartitionKey(queueWorkspaceID(payload.WorkspaceID), payload.SessionID, payload.TaskID) ||
		job.GetDedupeKey() != queue.FormatSandboxBackgroundCommandDedupeKey(queueWorkspaceID(payload.WorkspaceID), payload.SessionID, payload.TaskID, payload.RequestID) {
		return SandboxBackgroundCommandJob{}, errors.New("sandbox background command queue identity is invalid")
	}
	return SandboxBackgroundCommandJob{JobID: job.GetId(), LeaseToken: job.GetLeaseToken(), WorkspaceID: payload.WorkspaceID, SessionID: payload.SessionID, TaskID: payload.TaskID, RequestID: payload.RequestID}, nil
}

func decodeBackgroundQueuePayload(job *queuev1.QueueJob, kind string, target any) error {
	if job == nil || job.GetKind() != kind || job.GetId() == "" || job.GetLeaseToken() == "" {
		return errors.New("sandbox background queue transport identity is incomplete")
	}
	decoder := json.NewDecoder(strings.NewReader(job.GetPayloadJson()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var identity struct {
		WorkspaceID string `json:"workspace_id"`
		SessionID   string `json:"session_id"`
		TaskID      string `json:"task_id"`
	}
	if err := json.Unmarshal([]byte(job.GetPayloadJson()), &identity); err != nil || identity.WorkspaceID == "" || identity.WorkspaceID != job.GetWorkspaceId() || identity.SessionID == "" || identity.TaskID == "" {
		return errors.New("sandbox background queue payload identity is incomplete")
	}
	return nil
}

func normalizeSandboxBackgroundRunnerConfig(cfg SandboxBackgroundRunnerConfig) (SandboxBackgroundRunnerConfig, error) {
	if cfg.LeaseOwner == "" {
		cfg.LeaseOwner = ServiceName
	}
	if cfg.MaxJobs <= 0 {
		cfg.MaxJobs = 1
	}
	if cfg.LeaseDuration <= 0 || cfg.HeartbeatInterval <= 0 || cfg.LeaseDuration <= cfg.HeartbeatInterval {
		return SandboxBackgroundRunnerConfig{}, errors.New("sandbox background runner timing configuration is invalid")
	}
	return cfg, nil
}

func deadLetterBackgroundJob(ctx context.Context, client SandboxQueueClient, job *queuev1.QueueJob, kind string) error {
	return transitionUpdated(client.DeadLetter(ctx, &queuev1.DeadLetterRequest{WorkspaceId: job.GetWorkspaceId(), JobId: job.GetId(), LeaseToken: job.GetLeaseToken(), ErrorKind: kind, ErrorMessage: "sandbox background job could not complete"}))
}

func backgroundTaskFailure(kind string, message string) sandboxdriver.CommandResult {
	if strings.TrimSpace(message) == "" {
		message = "sandbox background command failed"
	}
	errorBody := map[string]any{"kind": kind, "message": message}
	payload, _ := json.Marshal(map[string]any{
		"status": "failed",
		"error":  errorBody,
		"result": map[string]any{
			"stdout": map[string]any{"text": "", "truncated": false},
			"stderr": map[string]any{"text": message, "truncated": false},
			"error":  errorBody,
		},
	})
	return sandboxdriver.CommandResult{ResultJSON: string(payload), TerminalStatus: "failed"}
}

func backgroundCancelReason(inputJSON string) string {
	var payload struct {
		Reason string `json:"reason"`
	}
	_ = json.Unmarshal([]byte(inputJSON), &payload)
	return payload.Reason
}
