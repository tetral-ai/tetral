package tetralsandbox

import (
	"context"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

type EnvironmentFailedFanoutStore interface {
	FanoutFailedEnvironment(context.Context, EnvironmentFailedFanoutJob, time.Time) (int, error)
}

// EnvironmentFailedFanoutJobRunner consumes the environment_failed_fanout queue
// kind and calls Store.FanoutFailedEnvironment to settle each affected
// session's gated inputs. It closes an artifacts -> preparations -> gated-inputs
// coupling that spans tables and stores: a terminal environment build failure
// (MarkEnvironmentBuildTerminalFailure) moves that generation's
// waiting_environment session_preparations to 'failed' and enqueues the
// environment_failed_fanout job in the same transaction, while the ready path
// (FanoutReadyEnvironment) instead advances the sessions waiting on a ready
// generation. UPDATE-WITH: environment_artifact_store.go
// (MarkEnvironmentBuildTerminalFailure, FanoutReadyEnvironment,
// FanoutFailedEnvironment).
type EnvironmentFailedFanoutJobRunner struct {
	Queue  SessionPrepareQueueClient
	Store  EnvironmentFailedFanoutStore
	Config EnvironmentRunnerConfig
	Clock  func() time.Time
}

type EnvironmentFailedFanoutJob struct {
	JobID         string
	LeaseToken    string
	WorkspaceID   string
	EnvironmentID string
	Generation    int64
}

func (r *EnvironmentFailedFanoutJobRunner) RunOnce(ctx context.Context) error {
	_, err := r.RunOnceWithActivity(ctx)
	return err
}

func (r *EnvironmentFailedFanoutJobRunner) RunOnceWithActivity(ctx context.Context) (bool, error) {
	if r == nil || r.Queue == nil {
		return false, errors.New("sandbox environment_failed_fanout queue client is required")
	}
	if r.Store == nil {
		return false, errors.New("sandbox environment_failed_fanout store is required")
	}
	cfg := normalizedEnvironmentRunnerConfig(r.Config)
	lease, err := r.Queue.Lease(ctx, &queuev1.LeaseRequest{
		WorkspaceId:     cfg.WorkspaceID,
		Kinds:           []string{queue.KindEnvironmentFailedFanout},
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

func (r *EnvironmentFailedFanoutJobRunner) processJob(ctx context.Context, queueJob *queuev1.QueueJob, cfg EnvironmentRunnerConfig) error {
	job, err := DecodeEnvironmentFailedFanoutJob(queueJob)
	if err != nil {
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "invalid_environment_failed_fanout_payload", ErrorMessage: "environment failed fanout queue payload is invalid",
		}))
	}
	workCtx, stopHeartbeat := startQueueLeaseGuard(ctx, r.Queue, job.WorkspaceID, job.JobID, job.LeaseToken, cfg.HeartbeatInterval, cfg.LeaseDuration)
	_, fanoutErr := r.Store.FanoutFailedEnvironment(workCtx, job, r.now())
	if heartbeatErr := stopHeartbeat(); heartbeatErr != nil {
		return heartbeatErr
	}
	if fanoutErr != nil {
		return transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{
			WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
			ErrorKind: "environment_failed_fanout_error", ErrorMessage: "environment failed fanout failed",
		}))
	}
	return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{
		WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
	}))
}

func DecodeEnvironmentFailedFanoutJob(queueJob *queuev1.QueueJob) (EnvironmentFailedFanoutJob, error) {
	if queueJob == nil {
		return EnvironmentFailedFanoutJob{}, errors.New("queue job is required")
	}
	if queueJob.GetKind() != queue.KindEnvironmentFailedFanout {
		return EnvironmentFailedFanoutJob{}, errors.New("queue job kind is not environment_failed_fanout")
	}
	workspaceID, environmentID, generation, err := decodeEnvironmentJobPayload(queueJob)
	if err != nil {
		return EnvironmentFailedFanoutJob{}, err
	}
	if queueJob.GetId() == "" || queueJob.GetLeaseToken() == "" {
		return EnvironmentFailedFanoutJob{}, errors.New("environment_failed_fanout queue identity is incomplete")
	}
	return EnvironmentFailedFanoutJob{
		JobID: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(), WorkspaceID: workspaceID,
		EnvironmentID: environmentID, Generation: generation,
	}, nil
}

func (r *EnvironmentFailedFanoutJobRunner) now() time.Time {
	if r != nil && r.Clock != nil {
		return r.Clock().UTC()
	}
	return storage.Now()
}
