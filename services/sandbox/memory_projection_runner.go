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

type SandboxMemoryProjectionJob struct {
	JobID         string
	LeaseToken    string
	WorkspaceID   string
	SessionID     string
	MemoryStoreID string
	MemoryWriteID string
}

type SandboxMemoryProjectionWork struct {
	WorkspaceID        string
	SessionID          string
	SessionThreadID    string
	MemoryStoreID      string
	MemoryWriteID      string
	Provider           string
	ProviderResourceID string
	MountPaths         []string
	Ops                []sandboxdriver.MemoryProjectionOp
}

type SandboxMemoryProjectionStore interface {
	LoadProjection(context.Context, SandboxMemoryProjectionJob) (SandboxMemoryProjectionWork, bool, error)
	SettleProjection(context.Context, SandboxMemoryProjectionWork, string, string, time.Time) error
	FinalizeProjectionExhaustion(context.Context, *queuev1.QueueJob, time.Time) error
}

type SandboxMemoryProjectionRunnerConfig struct {
	WorkspaceID       string
	LeaseOwner        string
	MaxJobs           int
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
}

type SandboxMemoryProjectionJobRunner struct {
	Queue     SessionPrepareQueueClient
	Store     SandboxMemoryProjectionStore
	Providers *ProviderRegistry
	Config    SandboxMemoryProjectionRunnerConfig
	Clock     func() time.Time
}

func (r *SandboxMemoryProjectionJobRunner) RunOnce(ctx context.Context) error {
	_, err := r.RunOnceWithActivity(ctx)
	return err
}

func (r *SandboxMemoryProjectionJobRunner) RunOnceWithActivity(ctx context.Context) (bool, error) {
	if r == nil || r.Queue == nil || r.Store == nil || r.Providers == nil {
		return false, errors.New("sandbox memory projection dependencies are required")
	}
	cfg := r.Config
	if cfg.LeaseOwner == "" {
		cfg.LeaseOwner = ServiceName
	}
	if cfg.MaxJobs <= 0 {
		cfg.MaxJobs = 1
	}
	if cfg.WorkspaceID == "" || cfg.LeaseDuration <= cfg.HeartbeatInterval || cfg.HeartbeatInterval <= 0 {
		return false, errors.New("sandbox memory projection runner configuration is invalid")
	}
	lease, err := r.Queue.Lease(ctx, &queuev1.LeaseRequest{
		WorkspaceId: cfg.WorkspaceID, Kinds: []string{queue.KindSandboxMemoryProjection},
		LeaseOwner: cfg.LeaseOwner, MaxJobs: int32(cfg.MaxJobs), LeaseDurationMs: cfg.LeaseDuration.Milliseconds(),
	})
	if err != nil {
		return false, err
	}
	for _, job := range lease.GetJobs() {
		if err := r.processJob(ctx, job, cfg); err != nil {
			return len(lease.GetJobs()) > 0, err
		}
	}
	return len(lease.GetJobs()) > 0, nil
}

func (r *SandboxMemoryProjectionJobRunner) processJob(ctx context.Context, queueJob *queuev1.QueueJob, cfg SandboxMemoryProjectionRunnerConfig) error {
	if queueJob.GetAttemptCount() > queueJob.GetMaxAttempts() || queueJob.GetMaxAttempts() <= 0 {
		if err := r.Store.FinalizeProjectionExhaustion(ctx, queueJob, r.now()); err != nil {
			return err
		}
		return deadLetterBackgroundJob(ctx, r.Queue, queueJob, "sandbox_memory_projection_exhausted")
	}
	job, err := DecodeSandboxMemoryProjectionJob(queueJob)
	if err != nil {
		if err := r.Store.FinalizeProjectionExhaustion(ctx, queueJob, r.now()); err != nil {
			return err
		}
		return deadLetterBackgroundJob(ctx, r.Queue, queueJob, "invalid_sandbox_memory_projection_payload")
	}
	workCtx, stopHeartbeat := startQueueLeaseGuard(ctx, r.Queue, job.WorkspaceID, job.JobID, job.LeaseToken, cfg.HeartbeatInterval, cfg.LeaseDuration)
	err = r.project(workCtx, job)
	if heartbeatErr := stopHeartbeat(); heartbeatErr != nil {
		return heartbeatErr
	}
	if err != nil {
		var retry *sandboxExecutionRetry
		if errors.As(err, &retry) {
			if queueJob.GetAttemptCount() >= queueJob.GetMaxAttempts() {
				if err := r.Store.FinalizeProjectionExhaustion(ctx, queueJob, r.now()); err != nil {
					return err
				}
				return deadLetterBackgroundJob(ctx, r.Queue, queueJob, "sandbox_memory_projection_exhausted")
			}
			return transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken, ErrorKind: retry.kind, ErrorMessage: retry.message}))
		}
		return err
	}
	return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken}))
}

func (r *SandboxMemoryProjectionJobRunner) project(ctx context.Context, job SandboxMemoryProjectionJob) error {
	work, current, err := r.Store.LoadProjection(ctx, job)
	if err != nil {
		return newSandboxExecutionRetry("sandbox_memory_projection_store_error", "memory projection could not be loaded")
	}
	if !current {
		return nil
	}
	if len(work.Ops) == 0 {
		return r.Store.SettleProjection(ctx, work, "refreshed", "", r.now())
	}
	if work.ProviderResourceID == "" {
		return r.Store.SettleProjection(ctx, work, "skipped_cold", "", r.now())
	}
	adapter, ok := r.Providers.Resolve(work.Provider)
	if !ok {
		return r.Store.SettleProjection(ctx, work, "failed", "sandbox provider is unavailable", r.now())
	}
	projector, ok := adapter.(MemoryProjectionAdapter)
	if !ok {
		return r.Store.SettleProjection(ctx, work, "failed", "sandbox memory projection adapter is unavailable", r.now())
	}
	readiness := adapter.InspectForExecution(ctx, work.ProviderResourceID)
	if readiness.Failed() {
		if readiness.EffectBoundary == ProviderProvedNotStarted && readiness.Disposition == ProviderRetryable {
			return newSandboxExecutionRetry(valueOrDefault(readiness.ErrorKind, "sandbox_memory_projection_inspection_retryable"), "memory projection inspection will be retried")
		}
		return r.Store.SettleProjection(ctx, work, "failed", valueOrDefault(readiness.SafeMessage, "memory projection inspection failed"), r.now())
	}
	if readiness.Value != ExecutionReady {
		return r.Store.SettleProjection(ctx, work, "skipped_cold", "", r.now())
	}
	outcome := projector.RefreshMemoryProjection(ctx, sandboxdriver.MemoryProjectionRefresh{
		Target: sandboxdriver.ToolTarget{
			WorkspaceID: work.WorkspaceID, SessionID: work.SessionID, SessionThreadID: work.SessionThreadID,
			ProviderSandboxID: work.ProviderResourceID,
		},
		MountPaths: work.MountPaths, Ops: work.Ops,
	})
	if outcome.Failed() {
		if outcome.EffectBoundary == ProviderProvedNotStarted && outcome.Disposition == ProviderRetryable {
			return newSandboxExecutionRetry(valueOrDefault(outcome.ErrorKind, "sandbox_memory_projection_retryable"), "memory projection will be retried")
		}
		return r.Store.SettleProjection(ctx, work, "failed", valueOrDefault(outcome.SafeMessage, "memory projection failed"), r.now())
	}
	return r.Store.SettleProjection(ctx, work, "refreshed", "", r.now())
}

func DecodeSandboxMemoryProjectionJob(job *queuev1.QueueJob) (SandboxMemoryProjectionJob, error) {
	if job == nil || job.GetKind() != queue.KindSandboxMemoryProjection || job.GetId() == "" || job.GetLeaseToken() == "" {
		return SandboxMemoryProjectionJob{}, errors.New("sandbox memory projection transport identity is incomplete")
	}
	var payload struct {
		WorkspaceID   string `json:"workspace_id"`
		SessionID     string `json:"session_id"`
		MemoryStoreID string `json:"memory_store_id"`
		MemoryWriteID string `json:"memory_write_id"`
	}
	decoder := json.NewDecoder(strings.NewReader(job.GetPayloadJson()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return SandboxMemoryProjectionJob{}, err
	}
	workspaceID := queueWorkspaceID(payload.WorkspaceID)
	if payload.WorkspaceID == "" || payload.WorkspaceID != job.GetWorkspaceId() || payload.SessionID == "" || payload.MemoryStoreID == "" || payload.MemoryWriteID == "" ||
		job.GetPartitionKey() != queue.FormatSandboxMemoryPartitionKey(workspaceID, payload.MemoryStoreID) ||
		job.GetDedupeKey() != queue.FormatSandboxMemoryProjectionDedupeKey(workspaceID, payload.MemoryStoreID, payload.MemoryWriteID) {
		return SandboxMemoryProjectionJob{}, errors.New("sandbox memory projection identity is invalid")
	}
	return SandboxMemoryProjectionJob{JobID: job.GetId(), LeaseToken: job.GetLeaseToken(), WorkspaceID: payload.WorkspaceID, SessionID: payload.SessionID, MemoryStoreID: payload.MemoryStoreID, MemoryWriteID: payload.MemoryWriteID}, nil
}

func (r *SandboxMemoryProjectionJobRunner) now() time.Time {
	if r != nil && r.Clock != nil {
		return r.Clock().UTC()
	}
	return storage.Now()
}
