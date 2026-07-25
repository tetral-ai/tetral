package tetralsandbox

import (
	"context"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

type EnvironmentReadyFanoutStore interface {
	FanoutReadyEnvironment(context.Context, EnvironmentReadyFanoutJob, time.Time) (int, error)
}

type EnvironmentReadyFanoutJobRunner struct {
	Queue  SessionPrepareQueueClient
	Store  EnvironmentReadyFanoutStore
	Config EnvironmentRunnerConfig
	Clock  func() time.Time
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
		if err := r.processJob(ctx, queueJob, cfg); err != nil {
			return hadWork, err
		}
	}
	return hadWork, nil
}

func (r *EnvironmentReadyFanoutJobRunner) processJob(ctx context.Context, queueJob *queuev1.QueueJob, cfg EnvironmentRunnerConfig) error {
	job, err := DecodeEnvironmentReadyFanoutJob(queueJob)
	if err != nil {
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId:  queueJob.GetWorkspaceId(),
			JobId:        queueJob.GetId(),
			LeaseToken:   queueJob.GetLeaseToken(),
			ErrorKind:    "invalid_environment_ready_fanout_payload",
			ErrorMessage: "environment_ready_fanout queue payload is invalid",
		}))
	}
	workCtx, stopHeartbeat := startQueueLeaseGuard(ctx, r.Queue, job.WorkspaceID, job.JobID, job.LeaseToken, cfg.HeartbeatInterval, cfg.LeaseDuration)
	_, fanoutErr := r.Store.FanoutReadyEnvironment(workCtx, job, r.now())
	if heartbeatErr := stopHeartbeat(); heartbeatErr != nil {
		return heartbeatErr
	}
	if fanoutErr != nil {
		return transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{
			WorkspaceId:  job.WorkspaceID,
			JobId:        job.JobID,
			LeaseToken:   job.LeaseToken,
			ErrorKind:    "environment_ready_fanout_error",
			ErrorMessage: "environment ready fanout failed",
		}))
	}
	return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{
		WorkspaceId: job.WorkspaceID,
		JobId:       job.JobID,
		LeaseToken:  job.LeaseToken,
	}))
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
