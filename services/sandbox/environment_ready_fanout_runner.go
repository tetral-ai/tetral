package tetralsandbox

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

type EnvironmentReadyFanoutStore interface {
	FanoutReadyEnvironment(context.Context, EnvironmentReadyFanoutJob, time.Time) (int, error)
	FinalizeReadyEnvironmentFanout(context.Context, EnvironmentReadyFanoutJob, time.Time) error
}

type EnvironmentReadyFanoutJobRunner struct {
	Queue  SandboxQueueClient
	Store  EnvironmentReadyFanoutStore
	Config EnvironmentRunnerConfig
	Clock  func() time.Time
	Logger *slog.Logger
}

type EnvironmentReadyFanoutJob struct {
	JobID         string
	LeaseToken    string
	WorkspaceID   string
	EnvironmentID string
	Generation    int64
}

func (r *EnvironmentReadyFanoutJobRunner) RunOnce(ctx context.Context) error {
	_, err := r.RunOnceWithActivity(ctx)
	return err
}

func (r *EnvironmentReadyFanoutJobRunner) RunOnceWithActivity(ctx context.Context) (bool, error) {
	if r == nil || r.Queue == nil {
		return false, errors.New("sandbox environment_ready_fanout queue client is required")
	}
	if r.Store == nil {
		return false, errors.New("sandbox environment_ready_fanout store is required")
	}
	cfg := normalizedEnvironmentRunnerConfig(r.Config)
	leaseSentAt := time.Now()
	lease, err := r.Queue.Lease(ctx, &queuev1.LeaseRequest{
		WorkspaceId:     cfg.WorkspaceID,
		Kinds:           []string{queue.KindEnvironmentReadyFanout},
		LeaseOwner:      cfg.LeaseOwner,
		MaxJobs:         int32(cfg.MaxJobs),
		LeaseDurationMs: cfg.LeaseDuration.Milliseconds(),
	})
	if err != nil {
		return false, err
	}
	hadWork := len(lease.GetJobs()) > 0
	for _, queueJob := range lease.GetJobs() {
		if err := r.processJob(ctx, queueJob, cfg, leaseSentAt.Add(wireRoundedQueueLeaseDuration(cfg.LeaseDuration))); err != nil {
			return hadWork, err
		}
	}
	return hadWork, nil
}

func (r *EnvironmentReadyFanoutJobRunner) processJob(ctx context.Context, queueJob *queuev1.QueueJob, cfg EnvironmentRunnerConfig, localExpiry time.Time) (resultErr error) {
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
	jobIdentity := SandboxLifecycleJob{JobID: queueJob.GetId(), WorkspaceID: queueJob.GetWorkspaceId()}
	defer func() {
		if writer := queueAuthorityLossWriter(resultErr); writer != "" {
			logSandboxQueueAuthorityLost(r.Logger, jobIdentity, queue.KindEnvironmentReadyFanout, writer)
		}
	}()
	if queueJob.GetMaxAttempts() <= 0 || queueJob.GetAttemptCount() > queueJob.GetMaxAttempts() {
		if transportJob, identityErr := decodeEnvironmentReadyFanoutTransportIdentity(queueJob); identityErr == nil {
			if err := r.Store.FinalizeReadyEnvironmentFanout(ctx, transportJob, r.now()); err != nil {
				if errors.Is(err, errQueueLeaseLost) {
					return queueAuthorityLostBy("environment_ready_fanout_finalize", err)
				}
				return err
			}
		}
		if heartbeatErr := stopQueueLeaseGuard(ctx); heartbeatErr != nil {
			return heartbeatErr
		}
		errorKind := "environment_ready_fanout_attempts_exhausted"
		errorMessage := "environment ready fanout attempt budget exhausted"
		if queueJob.GetMaxAttempts() <= 0 {
			errorKind = "sandbox_queue_integrity_error"
			errorMessage = "environment ready fanout job has no attempt budget"
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: errorKind, ErrorMessage: errorMessage,
		}))
	}
	job, err := DecodeEnvironmentReadyFanoutJob(queueJob)
	if err != nil {
		if transportJob, identityErr := decodeEnvironmentReadyFanoutTransportIdentity(queueJob); identityErr == nil {
			if err := r.Store.FinalizeReadyEnvironmentFanout(ctx, transportJob, r.now()); err != nil {
				if errors.Is(err, errQueueLeaseLost) {
					return queueAuthorityLostBy("environment_ready_fanout_finalize", err)
				}
				return err
			}
		}
		if heartbeatErr := stopQueueLeaseGuard(ctx); heartbeatErr != nil {
			return heartbeatErr
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId:  queueJob.GetWorkspaceId(),
			JobId:        queueJob.GetId(),
			LeaseToken:   queueJob.GetLeaseToken(),
			ErrorKind:    "invalid_environment_ready_fanout_payload",
			ErrorMessage: "environment_ready_fanout queue payload is invalid",
		}))
	}
	_, fanoutErr := r.Store.FanoutReadyEnvironment(ctx, job, r.now())
	if fanoutErr != nil {
		if errors.Is(fanoutErr, errQueueLeaseLost) {
			return queueAuthorityLostBy("environment_ready_fanout_apply", fanoutErr)
		}
		if queueJob.GetMaxAttempts() > 0 && queueJob.GetAttemptCount() >= queueJob.GetMaxAttempts() {
			if err := r.Store.FinalizeReadyEnvironmentFanout(ctx, job, r.now()); err != nil {
				if errors.Is(err, errQueueLeaseLost) {
					return queueAuthorityLostBy("environment_ready_fanout_finalize", err)
				}
				return err
			}
			if heartbeatErr := stopQueueLeaseGuard(ctx); heartbeatErr != nil {
				return heartbeatErr
			}
			return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
				WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
				ErrorKind: "environment_ready_fanout_attempts_exhausted", ErrorMessage: "environment ready fanout attempt budget exhausted",
			}))
		}
		if heartbeatErr := stopQueueLeaseGuard(ctx); heartbeatErr != nil {
			return heartbeatErr
		}
		return transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{
			WorkspaceId:  job.WorkspaceID,
			JobId:        job.JobID,
			LeaseToken:   job.LeaseToken,
			ErrorKind:    "environment_ready_fanout_error",
			ErrorMessage: "environment ready fanout failed",
		}))
	}
	if heartbeatErr := stopQueueLeaseGuard(ctx); heartbeatErr != nil {
		return heartbeatErr
	}
	return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{
		WorkspaceId: job.WorkspaceID,
		JobId:       job.JobID,
		LeaseToken:  job.LeaseToken,
	}))
}

func decodeEnvironmentReadyFanoutTransportIdentity(queueJob *queuev1.QueueJob) (EnvironmentReadyFanoutJob, error) {
	if queueJob == nil || queueJob.GetKind() != queue.KindEnvironmentReadyFanout ||
		queueJob.GetWorkspaceId() == "" || queueJob.GetId() == "" || queueJob.GetLeaseToken() == "" {
		return EnvironmentReadyFanoutJob{}, errors.New("environment_ready_fanout Queue identity is incomplete")
	}
	ws := workspace.ID(queueJob.GetWorkspaceId())
	partitionPrefix := strings.TrimSuffix(queue.FormatEnvironmentPartitionKey(ws, "identity"), "identity")
	if !strings.HasPrefix(queueJob.GetPartitionKey(), partitionPrefix) {
		return EnvironmentReadyFanoutJob{}, errors.New("environment_ready_fanout partition identity is invalid")
	}
	environmentID := strings.TrimPrefix(queueJob.GetPartitionKey(), partitionPrefix)
	if environmentID == "" {
		return EnvironmentReadyFanoutJob{}, errors.New("environment_ready_fanout environment identity is missing")
	}
	dedupePrefix := strings.TrimSuffix(queue.FormatEnvironmentReadyFanoutDedupeKey(ws, environmentID, "identity"), "identity")
	if !strings.HasPrefix(queueJob.GetDedupeKey(), dedupePrefix) {
		return EnvironmentReadyFanoutJob{}, errors.New("environment_ready_fanout dedupe identity is invalid")
	}
	generationText := strings.TrimPrefix(queueJob.GetDedupeKey(), dedupePrefix)
	generation, err := strconv.ParseInt(generationText, 10, 64)
	if err != nil || generation <= 0 || queueJob.GetDedupeKey() != queue.FormatEnvironmentReadyFanoutDedupeKey(ws, environmentID, generationText) {
		return EnvironmentReadyFanoutJob{}, errors.New("environment_ready_fanout generation identity is invalid")
	}
	return EnvironmentReadyFanoutJob{
		JobID: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
		WorkspaceID: queueJob.GetWorkspaceId(), EnvironmentID: environmentID, Generation: generation,
	}, nil
}

func DecodeEnvironmentReadyFanoutJob(queueJob *queuev1.QueueJob) (EnvironmentReadyFanoutJob, error) {
	if queueJob == nil {
		return EnvironmentReadyFanoutJob{}, errors.New("queue job is required")
	}
	if queueJob.GetKind() != queue.KindEnvironmentReadyFanout {
		return EnvironmentReadyFanoutJob{}, errors.New("queue job kind is not environment_ready_fanout")
	}
	workspaceID, environmentID, generation, err := decodeEnvironmentJobPayload(queueJob)
	if err != nil {
		return EnvironmentReadyFanoutJob{}, err
	}
	if queueJob.GetId() == "" || queueJob.GetLeaseToken() == "" {
		return EnvironmentReadyFanoutJob{}, errors.New("environment_ready_fanout queue identity is incomplete")
	}
	return EnvironmentReadyFanoutJob{
		JobID:         queueJob.GetId(),
		LeaseToken:    queueJob.GetLeaseToken(),
		WorkspaceID:   workspaceID,
		EnvironmentID: environmentID,
		Generation:    generation,
	}, nil
}

func (r *EnvironmentReadyFanoutJobRunner) now() time.Time {
	if r != nil && r.Clock != nil {
		return r.Clock().UTC()
	}
	return storage.Now()
}
