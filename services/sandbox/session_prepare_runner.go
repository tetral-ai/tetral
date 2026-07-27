package tetralsandbox

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

type SessionPrepareQueueClient interface {
	Lease(context.Context, *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error)
	Heartbeat(context.Context, *queuev1.HeartbeatRequest) (*queuev1.TransitionResponse, error)
	Ack(context.Context, *queuev1.AckRequest) (*queuev1.TransitionResponse, error)
	Retry(context.Context, *queuev1.RetryRequest) (*queuev1.TransitionResponse, error)
	Defer(context.Context, *queuev1.DeferRequest) (*queuev1.TransitionResponse, error)
	DeadLetter(context.Context, *queuev1.DeadLetterRequest) (*queuev1.TransitionResponse, error)
}

type sessionPrepareQueueClientAdapter struct {
	client queuev1.QueueServiceClient
}

func SessionPrepareQueueFromGRPC(client queuev1.QueueServiceClient) SessionPrepareQueueClient {
	return sessionPrepareQueueClientAdapter{client: client}
}

func (a sessionPrepareQueueClientAdapter) Lease(ctx context.Context, request *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error) {
	return a.client.Lease(ctx, request)
}

func (a sessionPrepareQueueClientAdapter) Heartbeat(ctx context.Context, request *queuev1.HeartbeatRequest) (*queuev1.TransitionResponse, error) {
	return a.client.Heartbeat(ctx, request)
}

func (a sessionPrepareQueueClientAdapter) Ack(ctx context.Context, request *queuev1.AckRequest) (*queuev1.TransitionResponse, error) {
	return a.client.Ack(ctx, request)
}

func (a sessionPrepareQueueClientAdapter) Retry(ctx context.Context, request *queuev1.RetryRequest) (*queuev1.TransitionResponse, error) {
	return a.client.Retry(ctx, request)
}

func (a sessionPrepareQueueClientAdapter) Defer(ctx context.Context, request *queuev1.DeferRequest) (*queuev1.TransitionResponse, error) {
	return a.client.Defer(ctx, request)
}

func (a sessionPrepareQueueClientAdapter) DeadLetter(ctx context.Context, request *queuev1.DeadLetterRequest) (*queuev1.TransitionResponse, error) {
	return a.client.DeadLetter(ctx, request)
}

type SessionPrepareHandler interface {
	PrepareSession(context.Context, sandbox.SessionPrepareRequest) (sandbox.SessionPrepareResult, error)
}

type SessionPrepareRetryExhaustionFinalizer interface {
	FailSessionPreparationAfterRetryExhaustion(context.Context, sandbox.SessionPrepareRequest, string) (sandbox.SessionPrepareResult, error)
}

type SessionPrepareJobRunner struct {
	Queue   SessionPrepareQueueClient
	Handler SessionPrepareHandler
	Config  SessionPrepareRunnerConfig
}

type SessionPrepareRunnerConfig struct {
	WorkspaceID       string
	LeaseOwner        string
	MaxJobs           int
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
}

type SessionPrepareJob struct {
	JobID                string
	LeaseToken           string
	WorkspaceID          string
	SessionID            string
	PreparationAttemptID string
}

func (r *SessionPrepareJobRunner) RunOnce(ctx context.Context) error {
	_, err := r.RunOnceWithActivity(ctx)
	return err
}

func (r *SessionPrepareJobRunner) RunOnceWithActivity(ctx context.Context) (bool, error) {
	if r == nil || r.Queue == nil {
		return false, errors.New("sandbox session_prepare queue client is required")
	}
	if r.Handler == nil {
		return false, errors.New("sandbox session_prepare handler is required")
	}
	cfg := normalizedSessionPrepareRunnerConfig(r.Config)
	lease, err := r.Queue.Lease(ctx, &queuev1.LeaseRequest{
		WorkspaceId:     cfg.WorkspaceID,
		Kinds:           []string{queue.KindSessionPrepare},
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

func (r *SessionPrepareJobRunner) processJob(ctx context.Context, queueJob *queuev1.QueueJob, cfg SessionPrepareRunnerConfig) error {
	job, err := DecodeSessionPrepareJob(queueJob)
	if err != nil {
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId:  queueJob.GetWorkspaceId(),
			JobId:        queueJob.GetId(),
			LeaseToken:   queueJob.GetLeaseToken(),
			ErrorKind:    "invalid_session_prepare_payload",
			ErrorMessage: "session_prepare queue payload is invalid",
		}))
	}
	workCtx, stopHeartbeat := startQueueLeaseGuard(ctx, r.Queue, job.WorkspaceID, job.JobID, job.LeaseToken, cfg.HeartbeatInterval, cfg.LeaseDuration)
	result, handleErr := r.Handler.PrepareSession(workCtx, sandbox.SessionPrepareRequest{
		WorkspaceID:          workspace.ID(job.WorkspaceID),
		SessionID:            job.SessionID,
		PreparationAttemptID: job.PreparationAttemptID,
	})
	if heartbeatErr := stopHeartbeat(); heartbeatErr != nil {
		return heartbeatErr
	}
	if handleErr != nil {
		if sessionPrepareRetryWillExhaust(queueJob) {
			if finalizer, ok := r.Handler.(SessionPrepareRetryExhaustionFinalizer); ok {
				finalResult, finalErr := finalizer.FailSessionPreparationAfterRetryExhaustion(ctx, sandbox.SessionPrepareRequest{
					WorkspaceID:          workspace.ID(job.WorkspaceID),
					SessionID:            job.SessionID,
					PreparationAttemptID: job.PreparationAttemptID,
				}, "session_prepare_error")
				if finalErr != nil {
					return finalErr
				}
				if finalResult.Status == sandbox.SessionPrepareStatusNoop {
					return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{
						WorkspaceId: job.WorkspaceID,
						JobId:       job.JobID,
						LeaseToken:  job.LeaseToken,
					}))
				}
			}
		}
		return transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{
			WorkspaceId:  job.WorkspaceID,
			JobId:        job.JobID,
			LeaseToken:   job.LeaseToken,
			ErrorKind:    sessionPrepareErrorKind(handleErr),
			ErrorMessage: sessionPrepareErrorMessage(handleErr),
		}))
	}
	if result.Status == sandbox.SessionPrepareStatusFailed {
		errorKind := valueOrDefault(result.FailureReason, "session_prepare_failed")
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId:  job.WorkspaceID,
			JobId:        job.JobID,
			LeaseToken:   job.LeaseToken,
			ErrorKind:    errorKind,
			ErrorMessage: "session_prepare failed",
		}))
	}
	if result.Status == sandbox.SessionPrepareStatusWaitingOnMachine {
		return transitionUpdated(r.Queue.Defer(ctx, &queuev1.DeferRequest{
			WorkspaceId: job.WorkspaceID,
			JobId:       job.JobID,
			LeaseToken:  job.LeaseToken,
		}))
	}
	return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{
		WorkspaceId: job.WorkspaceID,
		JobId:       job.JobID,
		LeaseToken:  job.LeaseToken,
	}))
}

func sessionPrepareRetryWillExhaust(queueJob *queuev1.QueueJob) bool {
	return queueJob != nil && queueJob.GetMaxAttempts() > 0 && queueJob.GetAttemptCount() >= queueJob.GetMaxAttempts()
}

func DecodeSessionPrepareJob(queueJob *queuev1.QueueJob) (SessionPrepareJob, error) {
	if queueJob == nil {
		return SessionPrepareJob{}, errors.New("queue job is required")
	}
	if queueJob.GetKind() != queue.KindSessionPrepare {
		return SessionPrepareJob{}, errors.New("queue job kind is not session_prepare")
	}
	var payload struct {
		WorkspaceID          string `json:"workspace_id"`
		SessionID            string `json:"session_id"`
		PreparationAttemptID string `json:"preparation_attempt_id"`
	}
	decoder := json.NewDecoder(strings.NewReader(queueJob.GetPayloadJson()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return SessionPrepareJob{}, err
	}
	if queueJob.GetId() == "" || queueJob.GetLeaseToken() == "" ||
		payload.WorkspaceID == "" || payload.WorkspaceID != queueJob.GetWorkspaceId() ||
		payload.SessionID == "" || payload.PreparationAttemptID == "" {
		return SessionPrepareJob{}, errors.New("session_prepare payload has missing identity fields")
	}
	return SessionPrepareJob{
		JobID:                queueJob.GetId(),
		LeaseToken:           queueJob.GetLeaseToken(),
		WorkspaceID:          payload.WorkspaceID,
		SessionID:            payload.SessionID,
		PreparationAttemptID: payload.PreparationAttemptID,
	}, nil
}

func normalizedSessionPrepareRunnerConfig(cfg SessionPrepareRunnerConfig) SessionPrepareRunnerConfig {
	if cfg.LeaseOwner == "" {
		cfg.LeaseOwner = ServiceName
	}
	if cfg.MaxJobs <= 0 {
		cfg.MaxJobs = 1
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = time.Minute
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = cfg.LeaseDuration / 3
	}
	return cfg
}

func transitionUpdated(response *queuev1.TransitionResponse, err error) error {
	if err != nil {
		return err
	}
	if response != nil && !response.GetUpdated() {
		return errors.New("queue transition was not applied")
	}
	return nil
}

// sessionPrepareErrorKind and sessionPrepareErrorMessage keep a failed
// preparation diagnosable from its durable queue row. A ProviderError
// already carries the stage it failed in, a bounded kind, and a message the
// provider layer vetted as safe to surface; collapsing all of that into one
// fixed string leaves an operator with a dead-lettered job and no cause.
func sessionPrepareErrorKind(err error) string {
	var providerErr *sandbox.ProviderError
	if errors.As(err, &providerErr) && providerErr.Kind != "" {
		return string(providerErr.Kind)
	}
	return "session_prepare_error"
}

func sessionPrepareErrorMessage(err error) string {
	var providerErr *sandbox.ProviderError
	if errors.As(err, &providerErr) {
		stage := string(providerErr.Stage)
		if stage == "" {
			stage = "unknown_stage"
		}
		if providerErr.SafeMessage != "" {
			return "session_prepare failed at " + stage + ": " + providerErr.SafeMessage
		}
		return "session_prepare failed at " + stage
	}
	return "session_prepare handler failed"
}
